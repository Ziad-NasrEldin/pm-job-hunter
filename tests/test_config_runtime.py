import sys
from pathlib import Path

from app.config import Settings


def test_frozen_mode_uses_localappdata_defaults_and_bootstraps_env(monkeypatch, tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    exe_dir = tmp_path / "ProgramFiles" / "PMJobHunter"
    exe_dir.mkdir(parents=True, exist_ok=True)
    fake_exe = exe_dir / "PMJobHunter.exe"
    fake_exe.write_text("", encoding="utf-8")

    monkeypatch.setenv("LOCALAPPDATA", str(local_app_data))
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(fake_exe), raising=False)
    monkeypatch.delenv("APP_ENV_FILE", raising=False)
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("FACEBOOK_PROFILE_DIR", raising=False)
    monkeypatch.delenv("FACEBOOK_STORAGE_STATE_PATH", raising=False)
    monkeypatch.delenv("FACEBOOK_SCREENSHOTS_DIR", raising=False)
    monkeypatch.delenv("FACEBOOK_RAW_DIR", raising=False)
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)

    settings = Settings.from_env()
    runtime_root = local_app_data / "PMJobHunter"

    assert settings.db_path == str(runtime_root / "data" / "jobs.db")
    assert settings.facebook_profile_dir == str(runtime_root / "data" / "facebook_profile")
    assert settings.facebook_storage_state_path == str(runtime_root / "data" / "facebook_storage_state.json")
    assert settings.facebook_screenshots_dir == str(runtime_root / "data" / "screenshots" / "facebook")
    assert settings.facebook_raw_dir == str(runtime_root / "data" / "raw" / "facebook")
    assert settings.playwright_browsers_path == str(runtime_root / "ms-playwright")
    assert (runtime_root / ".env.local").exists()


def test_non_frozen_defaults_unchanged(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setenv("APP_ENV_FILE", str(tmp_path / "does-not-exist.env"))
    monkeypatch.delenv("DB_PATH", raising=False)
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)

    settings = Settings.from_env()
    assert settings.db_path == "./data/jobs.db"
    assert settings.playwright_browsers_path is None


def test_login_timeout_from_env(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setenv("APP_ENV_FILE", str(tmp_path / "does-not-exist.env"))
    monkeypatch.setenv("FACEBOOK_LOGIN_TIMEOUT_SECONDS", "321")

    settings = Settings.from_env()
    assert settings.facebook_login_timeout_seconds == 321
