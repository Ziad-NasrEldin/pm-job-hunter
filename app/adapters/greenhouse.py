from __future__ import annotations

from datetime import datetime

from dateutil import parser as date_parser

from app.adapters.base import JobAdapter
from app.models import RawJob, SearchQuery


def _matches_keywords(title: str, keywords: list[str]) -> bool:
    lowered = title.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _matches_locations(location: str, accepted: list[str]) -> bool:
    lower_location = location.lower()
    return any(target.lower() in lower_location for target in accepted)


class GreenhouseAdapter(JobAdapter):
    source_name = "greenhouse"
    base_url = "https://boards-api.greenhouse.io/v1/boards"

    @staticmethod
    def parse_jobs_payload(board_token: str, payload: dict) -> list[RawJob]:
        jobs: list[RawJob] = []
        for row in payload.get("jobs", []):
            posted_at: datetime | None = None
            if row.get("updated_at"):
                try:
                    posted_at = date_parser.parse(row["updated_at"])
                except (TypeError, ValueError):
                    posted_at = None
            description = row.get("content") or ""
            jobs.append(
                RawJob(
                    source="greenhouse",
                    external_id=f"{board_token}:{row.get('id')}",
                    title=row.get("title", ""),
                    company=payload.get("meta", {}).get("board", board_token),
                    location=(row.get("location") or {}).get("name", "Unknown"),
                    description=description,
                    job_url=row.get("absolute_url", ""),
                    apply_url=row.get("absolute_url", ""),
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
            payload = response.json()
            for job in self.parse_jobs_payload(board_token, payload):
                if not _matches_keywords(job.title, query.keywords):
                    continue
                if not _matches_locations(job.location, query.locations):
                    continue
                results.append(job)
        return results

