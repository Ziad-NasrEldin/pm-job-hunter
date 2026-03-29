from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.config import Settings
from app.facebook_filters import facebook_post_dedupe_key
from app.main import create_app
from app.models import FacebookGroupCandidate, FacebookPost, FacebookRunSummary


def _settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "test.db"),
        enable_scheduler=False,
        facebook_enabled=True,
        facebook_screenshots_dir=str(tmp_path / "screenshots"),
        facebook_raw_dir=str(tmp_path / "raw"),
        facebook_profile_dir=str(tmp_path / "profile"),
    )


def test_facebook_discovery_approval_and_export(tmp_path):
    app = create_app(_settings(tmp_path))

    with TestClient(app) as client:
        db = client.app.state.db

        def fake_discovery():
            candidate = FacebookGroupCandidate(
                group_external_id="1234567890",
                name="وظائف عن بعد مصر",
                group_url="https://www.facebook.com/groups/1234567890/",
                description="Egypt remote jobs",
                relevance_score=0.9,
                discovered_keyword="وظائف عن بعد مصر",
                metadata={},
            )
            db.upsert_facebook_group_candidate(candidate)
            now = datetime.now(UTC)
            return FacebookRunSummary(
                run_id=1,
                mode="discovery",
                started_at=now,
                finished_at=now,
                status="success",
                total_fetched=1,
                total_kept=1,
                total_new=1,
                total_updated=0,
                errors=[],
            )

        client.app.state.facebook_collector.run_discovery = fake_discovery

        discovery_resp = client.post("/facebook/discovery/run")
        assert discovery_resp.status_code == 200
        assert discovery_resp.json()["status"] == "success"

        candidates_resp = client.get("/facebook/groups/candidates?status=pending")
        assert candidates_resp.status_code == 200
        payload = candidates_resp.json()
        assert payload["count"] == 1
        assert payload["items"][0]["group_external_id"] == "1234567890"

        approve_resp = client.post("/facebook/groups/1234567890/approve")
        assert approve_resp.status_code == 200
        assert approve_resp.json()["status"] == "approved"

        def fake_collect():
            post_url = "https://www.facebook.com/groups/1234567890/posts/987654321/"
            post = FacebookPost(
                group_external_id="1234567890",
                group_name="وظائف عن بعد مصر",
                post_external_id="987654321",
                post_url=post_url,
                post_text="مطلوب خدمة عملاء من المنزل للتواصل 01001112223",
                post_excerpt="مطلوب خدمة عملاء من المنزل للتواصل 01001112223",
                posted_at=datetime.now(UTC),
                category_tag="customer_support",
                is_remote=True,
                phone_numbers=["01001112223"],
                whatsapp_links=["https://wa.me/01001112223"],
                screenshot_path="1234567890/987654321.png",
                raw_snapshot_path="1234567890/987654321.html",
                dedupe_key=facebook_post_dedupe_key("1234567890", "987654321", post_url),
                metadata={},
            )
            db.upsert_facebook_post(post)
            now = datetime.now(UTC)
            return FacebookRunSummary(
                run_id=2,
                mode="collect",
                started_at=now,
                finished_at=now,
                status="success",
                total_fetched=1,
                total_kept=1,
                total_new=1,
                total_updated=0,
                errors=[],
            )

        client.app.state.facebook_collector.run_once = fake_collect

        collect_resp = client.post("/facebook/runs/manual")
        assert collect_resp.status_code == 200
        assert collect_resp.json()["status"] == "success"

        posts_resp = client.get("/facebook/posts?has_phone=true")
        assert posts_resp.status_code == 200
        posts_payload = posts_resp.json()
        assert posts_payload["count"] == 1
        assert posts_payload["items"][0]["phone_numbers"] == ["01001112223"]

        csv_resp = client.get("/facebook/posts/export.csv")
        assert csv_resp.status_code == 200
        assert "group_external_id,group_name,post_url" in csv_resp.text
        assert "01001112223" in csv_resp.text

        disable_resp = client.post("/facebook/groups/1234567890/disable")
        assert disable_resp.status_code == 200
        assert disable_resp.json()["status"] == "disabled"
