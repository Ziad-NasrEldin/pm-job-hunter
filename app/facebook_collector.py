from __future__ import annotations

import traceback
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from app.adapters import FacebookGroupsAdapter
from app.config import Settings
from app.db import Database
from app.facebook_alerts import FacebookAlertService
from app.models import FacebookRunSummary
from app.security import path_is_within


class AlreadyRunningError(RuntimeError):
    pass


def _parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class FacebookCollector:
    _run_lock = Lock()

    def __init__(
        self,
        settings: Settings,
        db: Database,
        alert_service: FacebookAlertService | None = None,
    ) -> None:
        self.settings = settings
        self.db = db
        self.alert_service = alert_service

    def _build_adapter(self) -> FacebookGroupsAdapter:
        return FacebookGroupsAdapter(self.settings)

    def bootstrap_login(self) -> dict[str, str]:
        adapter = self._build_adapter()
        return adapter.bootstrap_login()

    def check_session_status(self, *, live: bool = False) -> dict[str, Any]:
        adapter = self._build_adapter()
        if live and hasattr(adapter, "validate_session"):
            result = adapter.validate_session()
        elif hasattr(adapter, "inspect_session_file"):
            result = adapter.inspect_session_file()
        else:
            result = {
                "session_file_present": Path(self.settings.facebook_storage_state_path).exists(),
                "session_valid": False,
                "reason": "adapter_does_not_expose_session_validation",
            }
        result["session_checked_at"] = datetime.now(UTC).isoformat()
        return result

    def _record_event(
        self,
        *,
        run_id: int,
        stage: str,
        scope: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.db.add_facebook_run_event(
            run_id=run_id,
            stage=stage,
            scope=scope,
            message=message,
            payload=payload or {},
        )

    def _finalize(
        self,
        *,
        run_id: int,
        mode: str,
        started_at: datetime,
        status: str,
        total_fetched: int,
        total_kept: int,
        total_new: int,
        total_updated: int,
        errors: list[str],
    ) -> FacebookRunSummary:
        finished_at = datetime.now(UTC)
        self.db.finalize_facebook_run(
            run_id=run_id,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
            finished_at=finished_at,
        )
        self._record_event(
            run_id=run_id,
            stage="run.finalize",
            scope=mode,
            message=f"Run finalized with status={status}",
            payload={
                "status": status,
                "total_fetched": total_fetched,
                "total_kept": total_kept,
                "total_new": total_new,
                "total_updated": total_updated,
                "errors": errors,
            },
        )
        return FacebookRunSummary(
            run_id=run_id,
            mode=mode,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )

    def run_discovery(self) -> FacebookRunSummary:
        started_at = datetime.now(UTC)
        run_id = self.db.create_facebook_run(started_at=started_at, mode="discovery")
        errors: list[str] = []
        total_fetched = 0
        total_kept = 0
        total_new = 0
        total_updated = 0

        self._record_event(run_id=run_id, stage="run.start", scope="discovery", message="Facebook discovery started")

        if not self.settings.facebook_enabled:
            errors = ["facebook feature is disabled"]
            return self._finalize(
                run_id=run_id,
                mode="discovery",
                started_at=started_at,
                status="disabled",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        session = self.check_session_status(live=True)
        self._record_event(
            run_id=run_id,
            stage="preflight.session",
            scope="discovery",
            message="Session preflight checked",
            payload=session,
        )
        if not session.get("session_valid"):
            reason = str(session.get("reason") or "Session invalid. Please re-login.")
            errors = [reason]
            return self._finalize(
                run_id=run_id,
                mode="discovery",
                started_at=started_at,
                status="blocked_session_expired",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        adapter = self._build_adapter()
        try:
            candidates = adapter.discover_groups()
            total_fetched = len(candidates)
            self._record_event(
                run_id=run_id,
                stage="discovery.fetch",
                scope="discovery",
                message="Discovery fetched candidates",
                payload={"fetched": total_fetched},
            )
            for candidate in candidates:
                outcome = self.db.upsert_facebook_group_candidate(candidate)
                total_kept += 1
                if outcome == "new":
                    total_new += 1
                elif outcome == "updated":
                    total_updated += 1
        except Exception as exc:  # noqa: BLE001
            message = str(exc).strip() or exc.__class__.__name__
            trace = traceback.format_exc(limit=8)
            errors.append(f"{adapter.source_name}: {message}")
            self._record_event(
                run_id=run_id,
                stage="discovery.error",
                scope="discovery",
                message="Discovery failed",
                payload={"error": message, "traceback": trace[-4000:]},
            )

        if errors and total_kept > 0:
            status = "partial_failed"
        elif errors and total_kept == 0:
            status = "failed"
        else:
            status = "success"

        return self._finalize(
            run_id=run_id,
            mode="discovery",
            started_at=started_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )

    def run_once(self, resume: bool = True) -> FacebookRunSummary:
        if not self._run_lock.acquire(blocking=False):
            raise AlreadyRunningError("A Facebook collection run is already in progress")
        try:
            return self._run_once_locked(resume=resume)
        finally:
            self._run_lock.release()

    def _run_once_locked(self, resume: bool = True) -> FacebookRunSummary:
        started_at = datetime.now(UTC)
        run_id = self.db.create_facebook_run(started_at=started_at, mode="collect")
        errors: list[str] = []
        total_fetched = 0
        total_kept = 0
        total_new = 0
        total_updated = 0

        self._record_event(
            run_id=run_id,
            stage="run.start",
            scope="collect",
            message="Facebook collection started",
            payload={"resume_requested": resume},
        )

        if not self.settings.facebook_enabled:
            errors = ["facebook feature is disabled"]
            return self._finalize(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                status="disabled",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        groups = self.db.list_facebook_groups(active_only=True, crawl_order=True)
        if not groups:
            errors = ["No approved active groups found. Approve groups in dashboard first."]
            return self._finalize(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                status="no_groups",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        session = self.check_session_status(live=True)
        self._record_event(
            run_id=run_id,
            stage="preflight.session",
            scope="collect",
            message="Session preflight checked",
            payload=session,
        )
        if not session.get("session_valid"):
            reason = str(session.get("reason") or "Session invalid. Please re-login.")
            errors = [reason]
            return self._finalize(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                status="blocked_session_expired",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        groups_to_process = list(groups)
        if resume:
            checkpoint = self.db.get_latest_resumable_checkpoint(mode="collect")
            if checkpoint is not None:
                prior = self.db.get_facebook_run(int(checkpoint["run_id"]))
                watermark = _parse_dt(prior.started_at) if prior is not None else None
                if watermark is not None:
                    groups_to_process = [group for group in groups if self._needs_resume(group, watermark)]
                    self._record_event(
                        run_id=run_id,
                        stage="resume.checkpoint",
                        scope="collect",
                        message="Resuming groups not crawled since the last failed run",
                        payload={
                            "from_run_id": checkpoint.get("run_id"),
                            "last_success_group_id": checkpoint.get("last_success_group_id"),
                            "remaining_groups": len(groups_to_process),
                        },
                    )

        if not groups_to_process:
            return self._finalize(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                status="success",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=[],
            )

        adapter = self._build_adapter()
        grouped_posts: dict[str, list] = {}
        grouped_errors: dict[str, str] = {}

        try:
            grouped_posts, grouped_errors = adapter.fetch_groups_posts(groups_to_process)
        except Exception as exc:  # noqa: BLE001
            message = str(exc).strip() or exc.__class__.__name__
            trace = traceback.format_exc(limit=8)
            errors.append(f"{adapter.source_name}: {message}")
            self._record_event(
                run_id=run_id,
                stage="collect.runtime_error",
                scope="collect",
                message="Adapter runtime error",
                payload={"error": message, "traceback": trace[-4000:]},
            )
            return self._finalize(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                status="failed",
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=errors,
            )

        for idx, group in enumerate(groups_to_process):
            group_id = group.get("group_external_id", "unknown")
            self._record_event(
                run_id=run_id,
                stage="group.start",
                scope=group_id,
                message=f"Processing group {group_id}",
                payload={"group_index": idx},
            )

            posts = grouped_posts.get(group_id, [])
            total_fetched += len(posts)
            for post in posts:
                total_kept += 1
                outcome = self.db.upsert_facebook_post(post)
                if outcome == "new":
                    total_new += 1
                elif outcome == "updated":
                    total_updated += 1

            error_message = grouped_errors.get(group_id)
            if error_message:
                errors.append(f"{group_id}: {error_message}")
                self._record_event(
                    run_id=run_id,
                    stage="group.error",
                    scope=group_id,
                    message="Group crawl failed",
                    payload={"error": error_message, "group_index": idx},
                )
                continue

            self.db.touch_facebook_group_crawled(group_external_id=group_id)
            self.db.save_facebook_run_checkpoint(
                run_id=run_id,
                mode="collect",
                last_success_group_id=group_id,
                next_group_index=idx + 1,
            )
            self._record_event(
                run_id=run_id,
                stage="group.success",
                scope=group_id,
                message="Group processed successfully",
                payload={"fetched_posts": len(posts), "group_index": idx},
            )

        if total_kept > 0 or not errors:
            removed_assets = self.db.prune_facebook_posts(retention_days=self.settings.facebook_retention_days)
            self._delete_removed_assets(removed_assets)

        if errors and total_kept > 0:
            status = "partial_failed"
        elif errors and total_kept == 0:
            status = "failed"
        else:
            status = "success"

        summary = self._finalize(
            run_id=run_id,
            mode="collect",
            started_at=started_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )

        if total_new > 0 and self.alert_service is not None:
            alert_result = self.alert_service.notify_new_leads(new_count=total_new, run_id=run_id)
            self._record_event(
                run_id=run_id,
                stage="alerts.sent",
                scope="collect",
                message="Lead alert dispatch completed",
                payload=alert_result,
            )

        return summary

    @staticmethod
    def _needs_resume(group: dict[str, Any], watermark: datetime) -> bool:
        crawled_at = _parse_dt(group.get("last_crawled_at"))
        if crawled_at is None:
            return True
        return crawled_at < watermark

    def _delete_removed_assets(self, removed_assets: list[dict[str, str]]) -> None:
        screenshots_root = Path(self.settings.facebook_screenshots_dir)
        raw_root = Path(self.settings.facebook_raw_dir)
        for item in removed_assets:
            screenshot = item.get("screenshot_path")
            raw_snapshot = item.get("raw_snapshot_path")
            if screenshot:
                self._safe_delete(screenshots_root / screenshot, screenshots_root)
            if raw_snapshot:
                self._safe_delete(raw_root / raw_snapshot, raw_root)

    @staticmethod
    def _safe_delete(path: Path, root: Path) -> None:
        try:
            if not path_is_within(path, root):
                return
            path.resolve().unlink(missing_ok=True)
        except OSError:
            return
