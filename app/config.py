from __future__ import annotations

import os
import sys
from shutil import copyfile
from dataclasses import dataclass, field
from pathlib import Path


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _default_runtime_root() -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "PMJobHunter"
    return Path.home() / "AppData" / "Local" / "PMJobHunter"


def _resolve_env_file(path: str) -> Path:
    requested = Path(path).expanduser()
    if requested.is_absolute():
        return requested

    candidates: list[Path] = [Path.cwd() / requested]
    if _is_frozen():
        candidates.append(Path(sys.executable).resolve().parent / requested)
    candidates.append(Path(__file__).resolve().parent.parent / requested)

    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return candidates[0]


def _load_env_file(path: str | Path) -> None:
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


def _bootstrap_env_file(env_path: Path) -> None:
    if env_path.exists():
        return

    env_path.parent.mkdir(parents=True, exist_ok=True)
    template_candidates = [
        env_path.parent / ".env.local.example",
        Path(sys.executable).resolve().parent / ".env.local.example",
        Path(__file__).resolve().parent.parent / ".env.local.example",
    ]
    for template in template_candidates:
        if template.exists() and template.is_file():
            copyfile(template, env_path)
            return

    env_path.write_text("# PM Job Hunter local environment\n", encoding="utf-8")


def _coalesce_frozen_path(
    name: str,
    fallback: str,
    legacy_relative_values: set[str],
) -> str:
    raw = os.getenv(name)
    if raw is None:
        return fallback

    cleaned = raw.strip()
    if not cleaned:
        return fallback

    normalized = cleaned.replace("\\", "/").lstrip("./")
    normalized_candidates = {value.replace("\\", "/").lstrip("./") for value in legacy_relative_values}
    if normalized in normalized_candidates:
        return fallback
    return cleaned


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
    facebook_storage_state_path: str = "./data/facebook_storage_state.json"
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
    facebook_login_timeout_seconds: int = 600
    facebook_screenshots_dir: str = "./data/screenshots/facebook"
    facebook_raw_dir: str = "./data/raw/facebook"
    facebook_alerts_enabled: bool = True
    facebook_alert_email_enabled: bool = True
    facebook_alert_email_to: str | None = None
    playwright_browsers_path: str | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        runtime_root = _default_runtime_root() if _is_frozen() else None
        default_env_file = str(runtime_root / ".env.local") if runtime_root else ".env.local"
        env_file = _resolve_env_file(os.getenv("APP_ENV_FILE", default_env_file))
        if runtime_root and env_file == (runtime_root / ".env.local"):
            _bootstrap_env_file(env_file)
        _load_env_file(env_file)

        default_db_path = str(runtime_root / "data" / "jobs.db") if runtime_root else "./data/jobs.db"
        default_facebook_profile_dir = (
            str(runtime_root / "data" / "facebook_profile") if runtime_root else "./data/facebook_profile"
        )
        default_facebook_storage_state = (
            str(runtime_root / "data" / "facebook_storage_state.json")
            if runtime_root
            else "./data/facebook_storage_state.json"
        )
        default_facebook_screenshots = (
            str(runtime_root / "data" / "screenshots" / "facebook")
            if runtime_root
            else "./data/screenshots/facebook"
        )
        default_facebook_raw = str(runtime_root / "data" / "raw" / "facebook") if runtime_root else "./data/raw/facebook"
        default_playwright_browsers_path = str(runtime_root / "ms-playwright") if runtime_root else None
        playwright_browsers_path = os.getenv("PLAYWRIGHT_BROWSERS_PATH", default_playwright_browsers_path or "")
        if playwright_browsers_path:
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", playwright_browsers_path)

        db_path_value = os.getenv("DB_PATH", default_db_path)
        facebook_profile_dir_value = os.getenv("FACEBOOK_PROFILE_DIR", default_facebook_profile_dir)
        facebook_storage_state_value = os.getenv("FACEBOOK_STORAGE_STATE_PATH", default_facebook_storage_state)
        facebook_screenshots_dir_value = os.getenv("FACEBOOK_SCREENSHOTS_DIR", default_facebook_screenshots)
        facebook_raw_dir_value = os.getenv("FACEBOOK_RAW_DIR", default_facebook_raw)

        if runtime_root:
            db_path_value = _coalesce_frozen_path("DB_PATH", default_db_path, {"./data/jobs.db"})
            facebook_profile_dir_value = _coalesce_frozen_path(
                "FACEBOOK_PROFILE_DIR",
                default_facebook_profile_dir,
                {"./data/facebook_profile"},
            )
            facebook_storage_state_value = _coalesce_frozen_path(
                "FACEBOOK_STORAGE_STATE_PATH",
                default_facebook_storage_state,
                {"./data/facebook_storage_state.json"},
            )
            facebook_screenshots_dir_value = _coalesce_frozen_path(
                "FACEBOOK_SCREENSHOTS_DIR",
                default_facebook_screenshots,
                {"./data/screenshots/facebook"},
            )
            facebook_raw_dir_value = _coalesce_frozen_path(
                "FACEBOOK_RAW_DIR",
                default_facebook_raw,
                {"./data/raw/facebook"},
            )
            playwright_browsers_path = _coalesce_frozen_path(
                "PLAYWRIGHT_BROWSERS_PATH",
                default_playwright_browsers_path or "",
                {"./ms-playwright"},
            )
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", playwright_browsers_path)

        return cls(
            db_path=db_path_value,
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
            facebook_profile_dir=facebook_profile_dir_value,
            facebook_storage_state_path=facebook_storage_state_value,
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
            facebook_login_timeout_seconds=_get_int("FACEBOOK_LOGIN_TIMEOUT_SECONDS", 600),
            facebook_screenshots_dir=facebook_screenshots_dir_value,
            facebook_raw_dir=facebook_raw_dir_value,
            facebook_alerts_enabled=_get_bool("FACEBOOK_ALERTS_ENABLED", True),
            facebook_alert_email_enabled=_get_bool("FACEBOOK_ALERT_EMAIL_ENABLED", True),
            facebook_alert_email_to=os.getenv("FACEBOOK_ALERT_EMAIL_TO"),
            playwright_browsers_path=playwright_browsers_path or None,
        )

    def ensure_db_dir(self) -> None:
        db_file = Path(self.db_path)
        if db_file.parent and str(db_file.parent) != ".":
            db_file.parent.mkdir(parents=True, exist_ok=True)

    def ensure_runtime_dirs(self) -> None:
        self.ensure_db_dir()
        Path(self.facebook_profile_dir).mkdir(parents=True, exist_ok=True)
        storage_path = Path(self.facebook_storage_state_path)
        if storage_path.parent:
            storage_path.parent.mkdir(parents=True, exist_ok=True)
        Path(self.facebook_screenshots_dir).mkdir(parents=True, exist_ok=True)
        Path(self.facebook_raw_dir).mkdir(parents=True, exist_ok=True)
        if self.playwright_browsers_path:
            Path(self.playwright_browsers_path).mkdir(parents=True, exist_ok=True)
