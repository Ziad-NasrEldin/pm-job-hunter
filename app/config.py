from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_env_file(path: str) -> None:
    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


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
            "Alexandria",
            "Cairo",
            "Remote",
            "Egypt",
            "Saudi Arabia",
            "United Arab Emirates",
            "Qatar",
            "Bahrain",
            "Kuwait",
            "Oman",
            "Jordan",
            "Morocco",
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
    facebook_enabled: bool = True
    facebook_profile_dir: str = "./data/facebook_profile"
    facebook_headless: bool = False
    facebook_crawl_days: int = 30
    facebook_retention_days: int = 90
    facebook_collection_interval_hours: int = 2
    facebook_discovery_hour: int = 8
    facebook_discovery_minute: int = 0
    facebook_discovery_keywords: list[str] = field(
        default_factory=lambda: [
            "وظائف عن بعد مصر",
            "work from home egypt",
            "كول سنتر من المنزل",
            "telesales remote egypt",
            "وظائف خدمة عملاء من البيت",
        ]
    )
    facebook_discovery_max_groups: int = 80
    facebook_discovery_scrolls: int = 5
    facebook_max_scrolls_per_group: int = 18
    facebook_max_posts_per_group: int = 240
    facebook_screenshots_dir: str = "./data/screenshots/facebook"
    facebook_raw_dir: str = "./data/raw/facebook"

    @classmethod
    def from_env(cls) -> "Settings":
        _load_env_file(os.getenv("APP_ENV_FILE", ".env.local"))
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
                    "Alexandria",
                    "Cairo",
                    "Remote",
                    "Egypt",
                    "Saudi Arabia",
                    "United Arab Emirates",
                    "Qatar",
                    "Bahrain",
                    "Kuwait",
                    "Oman",
                    "Jordan",
                    "Morocco",
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
            facebook_enabled=_get_bool("FACEBOOK_ENABLED", True),
            facebook_profile_dir=os.getenv("FACEBOOK_PROFILE_DIR", "./data/facebook_profile"),
            facebook_headless=_get_bool("FACEBOOK_HEADLESS", False),
            facebook_crawl_days=_get_int("FACEBOOK_CRAWL_DAYS", 30),
            facebook_retention_days=_get_int("FACEBOOK_RETENTION_DAYS", 90),
            facebook_collection_interval_hours=_get_int("FACEBOOK_COLLECTION_INTERVAL_HOURS", 2),
            facebook_discovery_hour=_get_int("FACEBOOK_DISCOVERY_HOUR", 8),
            facebook_discovery_minute=_get_int("FACEBOOK_DISCOVERY_MINUTE", 0),
            facebook_discovery_keywords=_get_list(
                "FACEBOOK_DISCOVERY_KEYWORDS",
                [
                    "وظائف عن بعد مصر",
                    "work from home egypt",
                    "كول سنتر من المنزل",
                    "telesales remote egypt",
                    "وظائف خدمة عملاء من البيت",
                ],
            ),
            facebook_discovery_max_groups=_get_int("FACEBOOK_DISCOVERY_MAX_GROUPS", 80),
            facebook_discovery_scrolls=_get_int("FACEBOOK_DISCOVERY_SCROLLS", 5),
            facebook_max_scrolls_per_group=_get_int("FACEBOOK_MAX_SCROLLS_PER_GROUP", 18),
            facebook_max_posts_per_group=_get_int("FACEBOOK_MAX_POSTS_PER_GROUP", 240),
            facebook_screenshots_dir=os.getenv("FACEBOOK_SCREENSHOTS_DIR", "./data/screenshots/facebook"),
            facebook_raw_dir=os.getenv("FACEBOOK_RAW_DIR", "./data/raw/facebook"),
        )

    def ensure_db_dir(self) -> None:
        db_file = Path(self.db_path)
        if db_file.parent and str(db_file.parent) != ".":
            db_file.parent.mkdir(parents=True, exist_ok=True)

    def ensure_runtime_dirs(self) -> None:
        self.ensure_db_dir()
        Path(self.facebook_profile_dir).mkdir(parents=True, exist_ok=True)
        Path(self.facebook_screenshots_dir).mkdir(parents=True, exist_ok=True)
        Path(self.facebook_raw_dir).mkdir(parents=True, exist_ok=True)
