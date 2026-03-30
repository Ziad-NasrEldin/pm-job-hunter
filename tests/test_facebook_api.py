from datetime import UTC, datetime
from pathlib import Path

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
        facebook_storage_state_path=str(tmp_path / "facebook_storage_state.json"),
        facebook_screenshots_dir=str(tmp_path / "screenshots"),
        facebook_raw_dir=str(tmp_path / "raw"),
        facebook_profile_dir=str(tmp_path / "profile"),
    )


def _approve_group(db, group_id: str = "1234567890") -> None:
    candidate = FacebookGroupCandidate(
        group_external_id=group_id,
        name="وظائف عن بعد مصر",
        group_url=f"https://www.facebook.com/groups/{group_id}/",
        description="Egypt remote jobs",
        relevance_score=0.9,
        discovered_keyword="وظائف عن بعد مصر",
        metadata={},
    )
    db.upsert_facebook_group_candidate(candidate)
    approved = db.approve_facebook_group(group_id)
    assert approved is not None


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

        def fake_collect(resume=True):
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


def test_facebook_status_ready(tmp_path):
    settings = _settings(tmp_path)
    Path(settings.facebook_storage_state_path).write_text("{}", encoding="utf-8")
    app = create_app(settings)

    with TestClient(app) as client:
        db = client.app.state.db
        _approve_group(db)
        now = datetime.now(UTC)
        run_id = db.create_facebook_run(now, mode="collect")
        db.finalize_facebook_run(
            run_id=run_id,
            status="success",
            total_fetched=3,
            total_kept=2,
            total_new=2,
            total_updated=0,
            errors=[],
            finished_at=now,
        )

        resp = client.get("/facebook/status")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["facebook_enabled"] is True
        assert payload["session_file_present"] is True
        assert payload["approved_groups_count"] == 1
        if payload["session_valid"] is True:
            assert payload["can_collect"] is True
            assert payload["blocking_reason"] is None
        else:
            assert payload["can_collect"] is False
            assert "session" in payload["blocking_reason"].lower()
        assert payload["latest_collect_run"]["status"] == "success"


def test_facebook_status_missing_login(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        db = client.app.state.db
        _approve_group(db)
        resp = client.get("/facebook/status")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["session_ready"] is False
        assert payload["can_collect"] is False
        assert "login session not found" in payload["blocking_reason"].lower()


def test_facebook_status_no_groups(tmp_path):
    settings = _settings(tmp_path)
    Path(settings.facebook_storage_state_path).write_text("{}", encoding="utf-8")
    app = create_app(settings)

    with TestClient(app) as client:
        resp = client.get("/facebook/status")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["approved_groups_count"] == 0
        assert payload["can_collect"] is False
        reason = payload["blocking_reason"].lower()
        assert ("no approved active facebook groups" in reason) or ("session" in reason)


def test_facebook_status_disabled(tmp_path):
    settings = _settings(tmp_path)
    settings.facebook_enabled = False
    Path(settings.facebook_storage_state_path).write_text("{}", encoding="utf-8")
    app = create_app(settings)

    with TestClient(app) as client:
        resp = client.get("/facebook/status")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["facebook_enabled"] is False
        assert payload["can_collect"] is False
        assert "disabled" in payload["blocking_reason"].lower()


def test_import_facebook_groups_endpoint(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        payload = {
            "text": "\n".join(
                [
                    "https://www.facebook.com/groups/1386469535434819/",
                    "Call Center Egypt | https://www.facebook.com/groups/1234567890/",
                    "egypt.remote.jobs",
                ]
            )
        }
        resp = client.post("/facebook/groups/import", json=payload)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "success"
        assert body["imported"] == 3
        assert body["new"] == 3

        groups_resp = client.get("/facebook/groups/candidates?status=approved")
        assert groups_resp.status_code == 200
        groups_body = groups_resp.json()
        assert groups_body["count"] >= 3

        status_resp = client.get("/facebook/status")
        assert status_resp.status_code == 200
        status_body = status_resp.json()
        assert status_body["approved_groups_count"] >= 3


def test_import_facebook_groups_endpoint_rejects_empty(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        resp = client.post("/facebook/groups/import", json={"text": "not-a-group-line"})
        assert resp.status_code == 400
        body = resp.json()
        assert body["status"] == "empty"


def test_import_facebook_groups_csv_endpoint(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        csv_content = "name,url\nGroup A,https://www.facebook.com/groups/1386469535434819/\n"
        files = {"file": ("groups.csv", csv_content, "text/csv")}
        resp = client.post("/facebook/groups/import.csv", files=files)
        assert resp.status_code == 200
        body = resp.json()
        assert body["imported"] == 1
        assert body["new"] == 1


def test_facebook_run_history_and_events(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        now = datetime.now(UTC)
        run_id = client.app.state.db.create_facebook_run(now, mode="collect")
        client.app.state.db.add_facebook_run_event(
            run_id=run_id,
            stage="group.error",
            scope="123",
            message="boom",
            payload={"error": "x"},
        )
        client.app.state.db.finalize_facebook_run(
            run_id=run_id,
            status="failed",
            total_fetched=0,
            total_kept=0,
            total_new=0,
            total_updated=0,
            errors=["boom"],
            finished_at=now,
        )

        history_resp = client.get("/facebook/runs?mode=collect")
        assert history_resp.status_code == 200
        assert history_resp.json()["count"] >= 1

        detail_resp = client.get(f"/facebook/runs/{run_id}")
        assert detail_resp.status_code == 200
        assert detail_resp.json()["status"] == "failed"

        events_resp = client.get(f"/facebook/runs/{run_id}/events")
        assert events_resp.status_code == 200
        assert events_resp.json()["count"] >= 1


def test_facebook_post_status_and_note_patch(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        db = client.app.state.db
        _approve_group(db)
        post_url = "https://www.facebook.com/groups/1234567890/posts/555/"
        dedupe_key = facebook_post_dedupe_key("1234567890", "555", post_url)
        post = FacebookPost(
            group_external_id="1234567890",
            group_name="Test Group",
            post_external_id="555",
            post_url=post_url,
            post_text="remote job text 01001112223",
            post_excerpt="remote job text 01001112223",
            posted_at=datetime.now(UTC),
            category_tag="customer_support",
            is_remote=True,
            phone_numbers=["01001112223"],
            whatsapp_links=[],
            screenshot_path=None,
            raw_snapshot_path=None,
            dedupe_key=dedupe_key,
            metadata={},
        )
        db.upsert_facebook_post(post)

        status_resp = client.patch(f"/facebook/posts/{dedupe_key}/status", json={"lead_status": "archived"})
        assert status_resp.status_code == 200

        note_resp = client.patch(f"/facebook/posts/{dedupe_key}/note", json={"lead_note": "follow up tomorrow"})
        assert note_resp.status_code == 200

        posts_resp = client.get("/facebook/posts?lead_status=archived")
        assert posts_resp.status_code == 200
        assert posts_resp.json()["count"] == 1
        assert posts_resp.json()["items"][0]["lead_note"] == "follow up tomorrow"
