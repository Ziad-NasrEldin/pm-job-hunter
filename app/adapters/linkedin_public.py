from __future__ import annotations

import re
from datetime import datetime
from html import unescape
from urllib.parse import parse_qs, urlparse

from app.adapters.base import JobAdapter
from app.models import RawJob, SearchQuery

CARD_RE = re.compile(r"<li[^>]*>(.*?)</li>", re.S | re.I)
ID_RE = re.compile(r"jobPosting:(\d+)", re.I)
LINK_RE = re.compile(r'class="[^"]*base-card__full-link[^"]*"[^>]*href="([^"]+)"', re.I)
TITLE_RE = re.compile(r'class="[^"]*base-search-card__title[^"]*"[^>]*>(.*?)</h3>', re.S | re.I)
COMPANY_RE = re.compile(r'class="[^"]*base-search-card__subtitle[^"]*"[^>]*>(.*?)</h4>', re.S | re.I)
LOCATION_RE = re.compile(r'class="[^"]*job-search-card__location[^"]*"[^>]*>(.*?)</span>', re.S | re.I)
TIME_RE = re.compile(r"<time[^>]*datetime=\"([^\"]+)\"", re.I)


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _canonical_job_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if "/jobs/view/" in parsed.path:
        return f"https://www.linkedin.com{parsed.path}"
    return raw_url


def _extract_external_id(card: str, job_url: str) -> str:
    match = ID_RE.search(card)
    if match:
        return match.group(1)
    parsed = urlparse(job_url)
    if "/jobs/view/" in parsed.path:
        return parsed.path.rsplit("/", 1)[-1]
    if parsed.query:
        qs = parse_qs(parsed.query)
        maybe = qs.get("currentJobId")
        if maybe:
            return maybe[0]
    return str(abs(hash(job_url)))


class LinkedInPublicAdapter(JobAdapter):
    source_name = "linkedin_public"
    base_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

    @staticmethod
    def parse_search_html(html: str) -> list[RawJob]:
        jobs: list[RawJob] = []
        for card_match in CARD_RE.finditer(html):
            card = card_match.group(1)
            link_match = LINK_RE.search(card)
            title_match = TITLE_RE.search(card)
            company_match = COMPANY_RE.search(card)
            location_match = LOCATION_RE.search(card)
            if not (link_match and title_match and company_match and location_match):
                continue

            job_url = _canonical_job_url(unescape(link_match.group(1)))
            external_id = _extract_external_id(card, job_url)
            posted_at = None
            time_match = TIME_RE.search(card)
            if time_match:
                try:
                    posted_at = datetime.fromisoformat(time_match.group(1))
                except ValueError:
                    posted_at = None

            jobs.append(
                RawJob(
                    source="linkedin_public",
                    external_id=external_id,
                    title=_strip_html(title_match.group(1)),
                    company=_strip_html(company_match.group(1)),
                    location=_strip_html(location_match.group(1)),
                    description=None,
                    job_url=job_url,
                    apply_url=job_url,
                    posted_at=posted_at,
                    metadata={},
                )
            )
        return jobs

    def fetch_jobs(self, query: SearchQuery) -> list[RawJob]:
        max_pages = min(query.max_pages, self.settings.linkedin_max_pages)
        results: list[RawJob] = []
        seen_ids: set[str] = set()

        for keyword in query.keywords:
            for location in query.locations:
                for page in range(max_pages):
                    start = page * 25
                    params = {"keywords": keyword, "location": location, "start": start}
                    try:
                        response = self.get(
                            self.base_url,
                            params=params,
                            min_interval=self.settings.linkedin_rate_limit_seconds,
                        )
                    except RuntimeError:
                        break
                    parsed = self.parse_search_html(response.text)
                    if not parsed:
                        break
                    for job in parsed:
                        if job.external_id in seen_ids:
                            continue
                        seen_ids.add(job.external_id)
                        results.append(job)
        return results

