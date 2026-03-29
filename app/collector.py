from __future__ import annotations

from datetime import UTC, datetime

from app.adapters import GreenhouseAdapter, LeverAdapter, LinkedInPublicAdapter
from app.config import Settings
from app.db import Database
from app.filters import normalize_raw_job, score_job, should_keep_job
from app.models import RunSummary, SearchQuery


class JobCollector:
    def __init__(self, settings: Settings, db: Database) -> None:
        self.settings = settings
        self.db = db

    def _build_adapters(self):
        return [
            LinkedInPublicAdapter(self.settings),
            GreenhouseAdapter(self.settings),
            LeverAdapter(self.settings),
        ]

    def _build_prioritized_locations(self) -> list[str]:
        priority_first = ["Alexandria", "Cairo", "Remote"]
        raw_locations = self.settings.mnea_locations or []
        seen: set[str] = set()
        ordered: list[str] = []

        for location in priority_first:
            key = location.strip().lower()
            if key in seen:
                continue
            ordered.append(location)
            seen.add(key)

        for location in raw_locations:
            cleaned = location.strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            ordered.append(cleaned)
            seen.add(key)
        return ordered

    def run_once(self) -> RunSummary:
        started_at = datetime.now(UTC)
        run_id = self.db.create_run(started_at)
        errors: list[str] = []
        total_fetched = 0
        total_kept = 0
        total_new = 0
        total_updated = 0

        query = SearchQuery(
            keywords=self.settings.role_keywords,
            locations=self._build_prioritized_locations(),
            max_pages=self.settings.linkedin_max_pages,
        )

        adapters = self._build_adapters()
        for adapter in adapters:
            try:
                raw_jobs = adapter.fetch_jobs(query)
                total_fetched += len(raw_jobs)
                for raw in raw_jobs:
                    normalized = normalize_raw_job(raw)
                    scored = score_job(normalized)
                    if not should_keep_job(scored):
                        continue
                    total_kept += 1
                    outcome = self.db.upsert_job(scored)
                    if outcome == "new":
                        total_new += 1
                    elif outcome == "updated":
                        total_updated += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{adapter.source_name}: {exc}")
            finally:
                adapter.close()

        self.db.prune_old_jobs(self.settings.retention_days)

        finished_at = datetime.now(UTC)
        if errors and total_kept > 0:
            status = "partial_failed"
        elif errors and total_kept == 0:
            status = "failed"
        else:
            status = "success"

        self.db.finalize_run(
            run_id=run_id,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
            finished_at=finished_at,
        )

        return RunSummary(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            total_fetched=total_fetched,
            total_kept=total_kept,
            total_new=total_new,
            total_updated=total_updated,
            errors=errors,
        )
