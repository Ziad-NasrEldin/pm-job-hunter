from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _get_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return default
    return [part.strip() for part in raw.split(",") if part.strip()]


@dataclass(slots=True)
class Settings:
    db_path: str = "./data/jobs.db"
    app_timezone: str = "Africa/Cairo"
    resend_api_key: str | None = None
    digest_from_email: str | None = None
    digest_to_email: str | None = None
    enable_scheduler: bool = True
    retention_days: int = 90
    linkedin_max_pages: int = 5
    linkedin_rate_limit_seconds: float = 1.0
    request_timeout_seconds: float = 20.0
    request_max_retries: int = 3
    request_backoff_seconds: float = 1.25
    mnea_locations: list[str] = field(
        default_factory=lambda: [
            "Egypt",
            "Saudi Arabia",
            "United Arab Emirates",
            "Qatar",
            "Bahrain",
            "Kuwait",
            "Oman",
            "Jordan",
            "Morocco",
            "Remote",
        ]
    )
    role_keywords: list[str] = field(
        default_factory=lambda: [
            "Product Owner",
            "Product Manager",
            "Associate Product Manager",
            "APM",
        ]
    )
    greenhouse_boards: list[str] = field(default_factory=list)
    lever_companies: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("DB_PATH", "./data/jobs.db"),
            app_timezone=os.getenv("APP_TIMEZONE", "Africa/Cairo"),
            resend_api_key=os.getenv("RESEND_API_KEY"),
            digest_from_email=os.getenv("DIGEST_FROM_EMAIL"),
            digest_to_email=os.getenv("DIGEST_TO_EMAIL"),
            enable_scheduler=_get_bool("ENABLE_SCHEDULER", True),
            retention_days=_get_int("RETENTION_DAYS", 90),
            linkedin_max_pages=_get_int("LINKEDIN_MAX_PAGES", 5),
            linkedin_rate_limit_seconds=_get_float("LINKEDIN_RATE_LIMIT_SECONDS", 1.0),
            request_timeout_seconds=_get_float("REQUEST_TIMEOUT_SECONDS", 20.0),
            request_max_retries=_get_int("REQUEST_MAX_RETRIES", 3),
            request_backoff_seconds=_get_float("REQUEST_BACKOFF_SECONDS", 1.25),
            mnea_locations=_get_list(
                "MENA_LOCATIONS",
                [
                    "Egypt",
                    "Saudi Arabia",
                    "United Arab Emirates",
                    "Qatar",
                    "Bahrain",
                    "Kuwait",
                    "Oman",
                    "Jordan",
                    "Morocco",
                    "Remote",
                ],
            ),
            role_keywords=_get_list(
                "ROLE_KEYWORDS",
                [
                    "Product Owner",
                    "Product Manager",
                    "Associate Product Manager",
                    "APM",
                ],
            ),
            greenhouse_boards=_get_list("GREENHOUSE_BOARDS", []),
            lever_companies=_get_list("LEVER_COMPANIES", []),
        )

    def ensure_db_dir(self) -> None:
        db_file = Path(self.db_path)
        if db_file.parent and str(db_file.parent) != ".":
            db_file.parent.mkdir(parents=True, exist_ok=True)

