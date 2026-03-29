from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

from dateutil import parser

from app.facebook_filters import normalize_arabic_digits, normalize_search_text

_RELATIVE_PATTERNS = [
    (re.compile(r"(\d+)\s*(?:m|min|mins|minute|minutes|د|دقيقه|دقيقة|دقايق|دقائق)\b", re.I), "minutes"),
    (re.compile(r"(\d+)\s*(?:h|hr|hrs|hour|hours|ساعه|ساعة|ساعات)\b", re.I), "hours"),
    (re.compile(r"(\d+)\s*(?:d|day|days|يوم|ايام|أيام)\b", re.I), "days"),
    (re.compile(r"(\d+)\s*(?:w|week|weeks|اسبوع|أسبوع|اسابيع|أسابيع)\b", re.I), "weeks"),
]


def parse_facebook_time(datetime_value: str | None, label: str | None, now: datetime | None = None) -> datetime | None:
    now = now or datetime.now(UTC)

    if datetime_value:
        try:
            parsed = datetime.fromisoformat(datetime_value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            pass

    if not label:
        return None

    normalized_label = normalize_search_text(normalize_arabic_digits(label))
    if not normalized_label:
        return None

    if normalized_label in {"just now", "الان", "الآن", "now"}:
        return now
    if normalized_label in {"yesterday", "امس", "أمس"}:
        return now - timedelta(days=1)

    for pattern, unit in _RELATIVE_PATTERNS:
        match = pattern.search(normalized_label)
        if not match:
            continue
        value = int(match.group(1))
        if unit == "minutes":
            return now - timedelta(minutes=value)
        if unit == "hours":
            return now - timedelta(hours=value)
        if unit == "days":
            return now - timedelta(days=value)
        if unit == "weeks":
            return now - timedelta(weeks=value)

    try:
        parsed = parser.parse(normalized_label, fuzzy=True)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (ValueError, TypeError, OverflowError):
        return None
