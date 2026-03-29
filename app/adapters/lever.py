from __future__ import annotations

from datetime import UTC, datetime

from app.adapters.base import JobAdapter
from app.models import RawJob, SearchQuery


def _matches_keywords(title: str, keywords: list[str]) -> bool:
    lowered = title.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _matches_locations(location: str, accepted: list[str]) -> bool:
    lower_location = location.lower()
    return any(target.lower() in lower_location for target in accepted)


def _epoch_ms_to_datetime(value: int | None) -> datetime | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


class LeverAdapter(JobAdapter):
    source_name = "lever"
    base_url = "https://api.lever.co/v0/postings"

    @staticmethod
    def parse_jobs_payload(company: str, payload: list[dict]) -> list[RawJob]:
        jobs: list[RawJob] = []
        for row in payload:
            categories = row.get("categories") or {}
            description_parts = [
                row.get("descriptionPlain") or "",
                row.get("listsPlain") or "",
                row.get("additionalPlain") or "",
            ]
            jobs.append(
                RawJob(
                    source="lever",
                    external_id=f"{company}:{row.get('id')}",
                    title=row.get("text", ""),
                    company=company,
                    location=categories.get("location", "Unknown"),
                    description="\n".join(part for part in description_parts if part),
                    job_url=row.get("hostedUrl", ""),
                    apply_url=row.get("applyUrl", row.get("hostedUrl", "")),
                    posted_at=_epoch_ms_to_datetime(row.get("createdAt")),
                    metadata={"team": categories.get("team"), "company_slug": company},
                )
            )
        return jobs

    def fetch_jobs(self, query: SearchQuery) -> list[RawJob]:
        if not self.settings.lever_companies:
            return []
        results: list[RawJob] = []
        for company in self.settings.lever_companies:
            url = f"{self.base_url}/{company}"
            response = self.get(url, params={"mode": "json"}, min_interval=0.5)
            payload = response.json()
            for job in self.parse_jobs_payload(company, payload):
                if not _matches_keywords(job.title, query.keywords):
                    continue
                if not _matches_locations(job.location, query.locations):
                    continue
                results.append(job)
        return results

