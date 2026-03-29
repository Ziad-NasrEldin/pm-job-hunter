from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class SearchQuery:
    keywords: list[str]
    locations: list[str]
    max_pages: int = 5


@dataclass(slots=True)
class RawJob:
    source: str
    external_id: str
    title: str
    company: str
    location: str
    description: str | None
    job_url: str
    apply_url: str
    posted_at: datetime | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedJob:
    source: str
    external_id: str
    title: str
    normalized_title: str
    company: str
    location: str
    description: str
    job_url: str
    apply_url: str
    posted_at: datetime | None
    role_family: str
    role_priority: int
    dedupe_key: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScoredJob:
    source: str
    external_id: str
    title: str
    normalized_title: str
    company: str
    location: str
    description: str
    job_url: str
    apply_url: str
    posted_at: datetime | None
    role_family: str
    role_priority: int
    dedupe_key: str
    metadata: dict[str, Any]
    early_career_score: float
    is_early_career: bool
    seniority_blocked: bool
    years_min: int | None
    years_max: int | None


@dataclass(slots=True)
class DigestItem:
    title: str
    company: str
    location: str
    role_family: str
    early_career_score: float
    apply_url: str
    source: str
    posted_at: datetime | None
    updated_at: datetime


@dataclass(slots=True)
class RunSummary:
    run_id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    total_fetched: int
    total_kept: int
    total_new: int
    total_updated: int
    errors: list[str]

