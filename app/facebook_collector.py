from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from app.adapters import FacebookGroupsAdapter
from app.config import Settings
from app.db import Database
from app.models import FacebookRunSummary


class FacebookCollector:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def _build_adapter(self) -> FacebookGroupsAdapter:
        return FacebookGroupsAdapter(self.settings)

    def bootstrap_login(self) -> dict[str, str]:
        adapter = self._build_adapter()
        return adapter.bootstrap_login()

    def run_discovery(self) -> FacebookRunSummary:
        started_at = datetime.now(UTC)
        run_id = self.db.create_facebook_run(started_at=started_at, mode="discovery")
        errors: list[str] = []
        total_fetched = 0
        total_kept = 0
        total_new = 0
        total_updated = 0

        if not self.settings.facebook_enabled:
            status = "disabled"
            finished_at = datetime.now(UTC)
            self.db.finalize_facebook_run(
                run_id=run_id,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=["facebook feature is disabled"],
                finished_at=finished_at,
            )
            return FacebookRunSummary(
                run_id=run_id,
                mode="discovery",
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=["facebook feature is disabled"],
            )

        adapter = self._build_adapter()
        try:
            candidates = adapter.discover_groups()
            total_fetched = len(candidates)
            for candidate in candidates:
                outcome = self.db.upsert_facebook_group_candidate(candidate)
                total_kept += 1
                if outcome == "new":
                    total_new += 1
                elif outcome == "updated":
                    total_updated += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{adapter.source_name}: {exc}")

        finished_at = datetime.now(UTC)
        if errors and total_kept > 0:
            status = "partial_failed"
        elif errors and total_kept == 0:
            status = "failed"
        else:
            status = "success"

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

        return FacebookRunSummary(
            run_id=run_id,
            mode="discovery",
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )

    def run_once(self) -> FacebookRunSummary:
        started_at = datetime.now(UTC)
        run_id = self.db.create_facebook_run(started_at=started_at, mode="collect")
        errors: list[str] = []
        total_fetched = 0
        total_kept = 0
        total_new = 0
        total_updated = 0

        if not self.settings.facebook_enabled:
            status = "disabled"
            finished_at = datetime.now(UTC)
            self.db.finalize_facebook_run(
                run_id=run_id,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=["facebook feature is disabled"],
                finished_at=finished_at,
            )
            return FacebookRunSummary(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=["facebook feature is disabled"],
            )

        groups = self.db.list_facebook_groups(active_only=True)
        if not groups:
            finished_at = datetime.now(UTC)
            status = "success"
            self.db.finalize_facebook_run(
                run_id=run_id,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=[],
                finished_at=finished_at,
            )
            return FacebookRunSummary(
                run_id=run_id,
                mode="collect",
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                total_fetched=0,
                total_kept=0,
                total_new=0,
                total_updated=0,
                errors=[],
            )

        adapter = self._build_adapter()

        for group in groups:
            try:
                posts = adapter.fetch_group_posts(group)
                total_fetched += len(posts)
                for post in posts:
                    total_kept += 1
                    outcome = self.db.upsert_facebook_post(post)
                    if outcome == "new":
                        total_new += 1
                    elif outcome == "updated":
                        total_updated += 1
                self.db.touch_facebook_group_crawled(group_external_id=group["group_external_id"])
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{group.get('group_external_id', 'unknown')}: {exc}")

        removed_assets = self.db.prune_facebook_posts(retention_days=self.settings.facebook_retention_days)
        self._delete_removed_assets(removed_assets)

        finished_at = datetime.now(UTC)
        if errors and total_kept > 0:
            status = "partial_failed"
        elif errors and total_kept == 0:
            status = "failed"
        else:
            status = "success"

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

        return FacebookRunSummary(
            run_id=run_id,
            mode="collect",
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )

    def _delete_removed_assets(self, removed_assets: list[dict[str, str]]) -> None:
        for item in removed_assets:
            screenshot = item.get("screenshot_path")
            raw_snapshot = item.get("raw_snapshot_path")
            if screenshot:
                self._safe_delete(Path(self.settings.facebook_screenshots_dir) / screenshot)
            if raw_snapshot:
                self._safe_delete(Path(self.settings.facebook_raw_dir) / raw_snapshot)

    @staticmethod
    def _safe_delete(path: Path) -> None:
        try:
            path.resolve().unlink(missing_ok=True)
        except OSError:
            return
