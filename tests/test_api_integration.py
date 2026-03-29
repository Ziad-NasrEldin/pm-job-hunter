from datetime import UTC, datetime

from fastapi.testclient import TestClient

from app.collector import JobCollector
from app.config import Settings
from app.main import create_app
from app.models import RawJob


class FakeAdapter:
    source_name = "fake_source"

    def __init__(self, *_args, **_kwargs):
        pass

    def fetch_jobs(self, _query):
        return [
            RawJob(
                source="fake_source",
                external_id="1",
                title="Product Owner",
                company="Acme",
                location="Cairo, Egypt",
                description="0-3 years experience in product",
                job_url="https://example.com/jobs/1",
                apply_url="https://example.com/jobs/1/apply",
                posted_at=datetime.now(UTC),
                metadata={},
            ),
            RawJob(
                source="fake_source",
                external_id="2",
                title="Senior Product Manager",
                company="Acme",
                location="Remote",
                description="8+ years required",
                job_url="https://example.com/jobs/2",
                apply_url="https://example.com/jobs/2/apply",
                posted_at=datetime.now(UTC),
                metadata={},
            ),
        ]

    def close(self):
        return None


def _settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "test.db"),
        enable_scheduler=False,
        resend_api_key="re_test",
        digest_from_email="from@example.com",
        digest_to_email="to@example.com",
    )


def test_manual_run_jobs_and_csv_export(monkeypatch, tmp_path):
    monkeypatch.setattr(JobCollector, "_build_adapters", lambda self: [FakeAdapter(self.settings)])
    app = create_app(_settings(tmp_path))

    with TestClient(app) as client:
        run_resp = client.post("/runs/manual")
        assert run_resp.status_code == 200
        run_data = run_resp.json()
        assert run_data["status"] == "success"
        assert run_data["total_kept"] == 1
        assert run_data["total_new"] == 1

        latest_resp = client.get("/runs/latest")
        assert latest_resp.status_code == 200
        assert latest_resp.json()["run_id"] == run_data["run_id"]

        jobs_resp = client.get("/jobs")
        assert jobs_resp.status_code == 200
        payload = jobs_resp.json()
        assert payload["count"] == 1
        assert payload["items"][0]["title"] == "Product Owner"

        blank_filter_resp = client.get("/jobs?early_career=&min_experience_score=&new_since_hours=")
        assert blank_filter_resp.status_code == 200
        blank_payload = blank_filter_resp.json()
        assert blank_payload["count"] == 1

        csv_resp = client.get("/jobs/export.csv")
        assert csv_resp.status_code == 200
        csv_text = csv_resp.text
        assert "title,company,location,source" in csv_text
        assert "Product Owner,Acme" in csv_text

        dashboard_resp = client.get("/?early_career=&min_experience_score=&new_since_hours=")
        assert dashboard_resp.status_code == 200


def test_digest_payload_is_sent(monkeypatch, tmp_path):
    monkeypatch.setattr(JobCollector, "_build_adapters", lambda self: [FakeAdapter(self.settings)])
    captured = {"payload": None}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"id": "email_123"}

    def fake_post(self, url, *args, **kwargs):  # noqa: ANN001
        captured["payload"] = {"url": url, "headers": kwargs.get("headers"), "json": kwargs.get("json")}
        return FakeResponse()

    app = create_app(_settings(tmp_path))

    with TestClient(app) as client:
        client.post("/runs/manual")
        monkeypatch.setattr("httpx.Client.post", fake_post)
        body = client.app.state.digest_service.send_daily_digest(hours=24)
        assert body["status"] == "sent"
        assert body["count"] == 1
        assert captured["payload"]["url"] == "https://api.resend.com/emails"
        assert captured["payload"]["json"]["to"] == ["to@example.com"]
