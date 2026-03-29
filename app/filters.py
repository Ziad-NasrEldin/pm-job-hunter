from __future__ import annotations

import re
from hashlib import sha1

from app.models import NormalizedJob, RawJob, ScoredJob

ROLE_PRIORITY = {
    "product_owner": 300,
    "product_manager": 200,
    "associate_product_manager": 100,
}

SENIOR_BLOCKLIST = [
    "senior",
    "lead",
    "principal",
    "director",
    "head of",
    "vice president",
    "vp ",
]

JUNIOR_HINTS = [
    "entry level",
    "junior",
    "graduate",
    "new grad",
    "apm",
    "associate product manager",
    "associate",
]

YEAR_RANGE_PATTERNS = [
    re.compile(r"\b(\d{1,2})\s*[-–to]{1,3}\s*(\d{1,2})\s*\+?\s*years?\b", re.I),
    re.compile(r"\b(\d{1,2})\s*\+\s*years?\b", re.I),
    re.compile(r"\bminimum\s+(\d{1,2})\s+years?\b", re.I),
    re.compile(r"\bat\s+least\s+(\d{1,2})\s+years?\b", re.I),
]


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def infer_role_family(title: str) -> str:
    lowered = title.lower()
    if "product owner" in lowered:
        return "product_owner"
    if "associate product manager" in lowered or re.search(r"\bapm\b", lowered):
        return "associate_product_manager"
    if "product manager" in lowered:
        return "product_manager"
    return "unknown"


def normalize_title(title: str) -> str:
    value = clean_text(title).lower()
    value = re.sub(r"[^a-z0-9 ]+", "", value)
    return re.sub(r"\s+", " ", value).strip()


def normalized_dedupe_key(title: str, company: str, location: str, apply_url: str) -> str:
    raw = "|".join(
        [
            normalize_title(title),
            clean_text(company).lower(),
            clean_text(location).lower(),
            clean_text(apply_url).lower(),
        ]
    )
    return sha1(raw.encode("utf-8")).hexdigest()


def extract_years_range(text: str) -> tuple[int | None, int | None]:
    candidate = clean_text(text)
    if not candidate:
        return None, None

    for pattern in YEAR_RANGE_PATTERNS:
        match = pattern.search(candidate)
        if not match:
            continue
        if len(match.groups()) == 2:
            a = int(match.group(1))
            b = int(match.group(2))
            return (min(a, b), max(a, b))
        value = int(match.group(1))
        return value, value
    return None, None


def is_seniority_blocked(text: str) -> bool:
    lowered = clean_text(text).lower()
    return any(keyword in lowered for keyword in SENIOR_BLOCKLIST)


def score_early_career(text: str, title: str, years_min: int | None, years_max: int | None) -> float:
    score = 0.0
    lowered_text = clean_text(text).lower()
    lowered_title = clean_text(title).lower()

    if years_min is not None and years_max is not None:
        if years_max <= 4:
            score += 0.85
        elif years_min <= 4 <= years_max:
            score += 0.55
        elif years_min <= 6:
            score += 0.35
        else:
            score -= 0.25
    elif years_min is not None:
        if years_min <= 4:
            score += 0.55
        elif years_min <= 6:
            score += 0.30
        else:
            score -= 0.25
    else:
        score += 0.10

    if any(hint in lowered_title or hint in lowered_text for hint in JUNIOR_HINTS):
        score += 0.35
    if "intern" in lowered_title:
        score -= 0.20
    if "senior" in lowered_title:
        score -= 0.5
    return max(0.0, min(score, 1.0))


def normalize_raw_job(raw: RawJob) -> NormalizedJob:
    normalized_title_value = normalize_title(raw.title)
    role_family = infer_role_family(raw.title)
    role_priority = ROLE_PRIORITY.get(role_family, 0)
    dedupe_key = normalized_dedupe_key(raw.title, raw.company, raw.location, raw.apply_url)
    return NormalizedJob(
        source=clean_text(raw.source),
        external_id=clean_text(raw.external_id),
        title=clean_text(raw.title),
        normalized_title=normalized_title_value,
        company=clean_text(raw.company),
        location=clean_text(raw.location),
        description=clean_text(raw.description),
        job_url=clean_text(raw.job_url),
        apply_url=clean_text(raw.apply_url or raw.job_url),
        posted_at=raw.posted_at,
        role_family=role_family,
        role_priority=role_priority,
        dedupe_key=dedupe_key,
        metadata=raw.metadata or {},
    )


def score_job(normalized: NormalizedJob) -> ScoredJob:
    combined = f"{normalized.title} {normalized.description}"
    years_min, years_max = extract_years_range(combined)
    seniority_blocked = is_seniority_blocked(combined)
    score = score_early_career(combined, normalized.title, years_min, years_max)
    is_early = score >= 0.45 and not seniority_blocked
    return ScoredJob(
        source=normalized.source,
        external_id=normalized.external_id,
        title=normalized.title,
        normalized_title=normalized.normalized_title,
        company=normalized.company,
        location=normalized.location,
        description=normalized.description,
        job_url=normalized.job_url,
        apply_url=normalized.apply_url,
        posted_at=normalized.posted_at,
        role_family=normalized.role_family,
        role_priority=normalized.role_priority,
        dedupe_key=normalized.dedupe_key,
        metadata=normalized.metadata,
        early_career_score=score,
        is_early_career=is_early,
        seniority_blocked=seniority_blocked,
        years_min=years_min,
        years_max=years_max,
    )


def should_keep_job(scored: ScoredJob) -> bool:
    if scored.role_priority <= 0:
        return False
    if scored.seniority_blocked:
        return False
    return True

