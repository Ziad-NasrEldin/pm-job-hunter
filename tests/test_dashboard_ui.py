from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def _settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "test.db"),
        enable_scheduler=False,
        facebook_enabled=True,
        facebook_storage_state_path=str(tmp_path / "facebook_storage_state.json"),
        facebook_screenshots_dir=str(tmp_path / "screenshots"),
        facebook_raw_dir=str(tmp_path / "raw"),
        facebook_profile_dir=str(tmp_path / "profile"),
    )


def test_global_facebook_quick_actions_visible_on_pm_tab(tmp_path):
    app = create_app(_settings(tmp_path))

    with TestClient(app) as client:
        resp = client.get("/?tab=pm")
        assert resp.status_code == 200
        assert "Quick Actions" in resp.text
        assert "Run Facebook Scraper" in resp.text
        assert "Run Group Discovery" in resp.text


def test_global_facebook_quick_actions_visible_on_facebook_tab(tmp_path):
    app = create_app(_settings(tmp_path))

    with TestClient(app) as client:
        resp = client.get("/?tab=facebook")
        assert resp.status_code == 200
        assert "Quick Actions" in resp.text
        assert "Run Facebook Scraper" in resp.text
        assert "Run Group Discovery" in resp.text
