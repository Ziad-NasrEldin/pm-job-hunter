from __future__ import annotations

import re
from datetime import datetime

from dateutil import parser as date_parser

from app.adapters.base import JobAdapter
from app.models import RawJob, SearchQuery


def _matches_keywords(title: str, keywords: list[str]) -> bool:
    lowered = title.lower()
    for keyword in keywords:
        key = keyword.lower().strip()
        if not key:
            continue
        if len(key) <= 3:
            if re.search(rf"\b{re.escape(key)}\b", lowered):
                return True
        elif key in lowered:
            return True
    return False


def _matches_locations(location: str, accepted: list[str]) -> bool:
    lower_location = location.lower()
    return any(target.lower() in lower_location for target in accepted)


class GreenhouseAdapter(JobAdapter):
    source_name = "greenhouse"
    base_url = "https://boards-api.greenhouse.io/v1/boards"

    @staticmethod
    def parse_jobs_payload(board_token: str, payload: dict) -> list[RawJob]:
        jobs: list[RawJob] = []
        if not isinstance(payload, dict):
            return jobs
        rows = payload.get("jobs")
        if not isinstance(rows, list):
            return jobs
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        company = meta.get("board") or board_token
        for row in rows:
            if not isinstance(row, dict):
                continue
            posted_at: datetime | None = None
            if row.get("updated_at"):
                try:
                    posted_at = date_parser.parse(row["updated_at"])
                except (TypeError, ValueError):
                    posted_at = None
            description = row.get("content") or ""
            location = row.get("location")
            if isinstance(location, dict):
                location_name = location.get("name") or "Unknown"
            elif isinstance(location, str) and location.strip():
                location_name = location.strip()
            else:
                location_name = "Unknown"
            jobs.append(
                RawJob(
                    source="greenhouse",
                    external_id=f"{board_token}:{row.get('id')}",
                    title=row.get("title") or "",
                    company=company,
                    location=location_name,
                    description=description,
                    job_url=row.get("absolute_url") or "",
                    apply_url=row.get("absolute_url") or "",
                    posted_at=posted_at,
                    metadata={"board_token": board_token},
                )
            )
        return jobs

    def fetch_jobs(self, query: SearchQuery) -> list[RawJob]:
        if not self.settings.greenhouse_boards:
            return []
        results: list[RawJob] = []
        for board_token in self.settings.greenhouse_boards:
            url = f"{self.base_url}/{board_token}/jobs"
            response = self.get(url, params={"content": "true"}, min_interval=0.5)
            try:
                payload = response.json()
            except ValueError:
                continue
            for job in self.parse_jobs_payload(board_token, payload):
                if not _matches_keywords(job.title, query.keywords):
                    continue
                if not _matches_locations(job.location, query.locations):
                    continue
                results.append(job)
        return results

