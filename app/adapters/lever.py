from __future__ import annotations

import re
from datetime import UTC, datetime

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
        if not isinstance(payload, list):
            return jobs
        for row in payload:
            if not isinstance(row, dict):
                continue
            categories = row.get("categories") or {}
            if not isinstance(categories, dict):
                categories = {}
            list_plain_parts: list[str] = []
            lists_payload = row.get("lists")
            if isinstance(lists_payload, list):
                for item in lists_payload:
                    if isinstance(item, dict):
                        list_plain_parts.append(str(item.get("text") or ""))
                        list_plain_parts.append(str(item.get("content") or ""))
            description_parts = [
                row.get("descriptionPlain") or "",
                row.get("listsPlain") or "",
                "\n".join(part for part in list_plain_parts if part),
                row.get("additionalPlain") or "",
            ]
            workplace = str(row.get("workplaceType") or "").lower()
            location = categories.get("location") or "Unknown"
            if workplace == "remote" and "remote" not in location.lower():
                location = f"Remote - {location}"
            jobs.append(
                RawJob(
                    source="lever",
                    external_id=f"{company}:{row.get('id')}",
                    title=row.get("text") or "",
                    company=company,
                    location=location,
                    description="\n".join(part for part in description_parts if part),
                    job_url=row.get("hostedUrl") or "",
                    apply_url=row.get("applyUrl") or row.get("hostedUrl") or "",
                    posted_at=_epoch_ms_to_datetime(row.get("createdAt") if isinstance(row.get("createdAt"), int) else None),
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
            try:
                payload = response.json()
            except ValueError:
                continue
            for job in self.parse_jobs_payload(company, payload):
                if not _matches_keywords(job.title, query.keywords):
                    continue
                if not _matches_locations(job.location, query.locations):
                    continue
                results.append(job)
        return results

