from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from app.adapters.greenhouse import GreenhouseAdapter
from app.adapters.lever import LeverAdapter
from app.adapters.linkedin_public import _extract_external_id
from app.config import Settings
from app.db import Database
from app.digest import DigestService
from app.facebook_collector import FacebookCollector
from app.facebook_filters import (
    classify_job_category,
    extract_phone_numbers,
    is_strict_remote_post,
    score_group_relevance,
)
from app.facebook_parser import parse_imported_groups_text_detailed
from app.filters import (
    extract_years_range,
    infer_role_family,
    is_seniority_blocked,
    normalize_raw_job,
    score_job,
    should_keep_job,
)
from app.main import create_app
from app.models import FacebookPost, RawJob
from app.security import assert_allowed_import_url, csv_safe_cell, google_sheet_to_csv_url, path_is_within


def _settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "test.db"),
        enable_scheduler=False,
        facebook_enabled=True,
        facebook_storage_state_path=str(tmp_path / "state.json"),
        facebook_screenshots_dir=str(tmp_path / "shots"),
        facebook_raw_dir=str(tmp_path / "raw"),
        facebook_profile_dir=str(tmp_path / "profile"),
    )


def _scored(title: str, description: str, location: str = "Cairo"):
    raw = RawJob(
        source="t",
        external_id="1",
        title=title,
        company="Acme",
        location=location,
        description=description,
        job_url="https://example.com/j",
        apply_url="https://example.com/j",
        posted_at=datetime.now(UTC),
    )
    return score_job(normalize_raw_job(raw))


def test_facebook_runs_latest_is_not_shadowed(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        missing = client.get("/facebook/runs/latest")
        assert missing.status_code == 404
        now = datetime.now(UTC)
        run_id = client.app.state.db.create_facebook_run(now, "collect")
        client.app.state.db.finalize_facebook_run(run_id, "success", 0, 0, 0, 0, [], now)
        latest = client.get("/facebook/runs/latest")
        assert latest.status_code == 200
        assert latest.json()["run_id"] == run_id


def test_ssrf_import_url_is_rejected(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        loopback = client.post("/facebook/groups/import.url", json={"url": "http://127.0.0.1:9/"})
        assert loopback.status_code == 400
        assert "127.0.0.1" not in loopback.text
        assert "Google Sheet" in loopback.json()["message"] or "HTTPS" in loopback.json()["message"]

        forbidden = client.post(
            "/facebook/groups/import.url",
            json={"url": "http://127.0.0.1:9/"},
            headers={"Origin": "https://evil.example"},
        )
        assert forbidden.status_code == 403


def test_google_sheet_gid_is_sanitized():
    converted = google_sheet_to_csv_url(
        "https://docs.google.com/spreadsheets/d/abc123/edit?gid=999<script>alert(1)"
    )
    assert "<script>" not in converted
    assert converted.endswith("gid=999")


def test_location_percent_is_not_a_wildcard(tmp_path):
    app = create_app(_settings(tmp_path))
    with TestClient(app) as client:
        client.app.state.db.upsert_job(_scored("Product Owner", "0-2 years", "Cairo, Egypt"))
        wild = client.get("/jobs?location=%")
        assert wild.json()["count"] == 0
        cairo = client.get("/jobs?location=Cairo")
        assert cairo.json()["count"] == 1


def test_seniority_false_positives_are_gone():
    assert is_seniority_blocked("Product Manager", "We are a leading company. 0-3 years.") is False
    assert is_seniority_blocked("Product Owner", "Stay ahead of the market") is False
    assert is_seniority_blocked("APM", "strong leadership skills") is False
    assert is_seniority_blocked("Senior Product Manager") is True
    assert is_seniority_blocked("Staff Product Manager") is True
    job = _scored("Product Manager", "We are a leading company. 0-3 years.")
    assert should_keep_job(job) is True


def test_eight_plus_years_is_dropped():
    job = _scored("Product Manager", "8+ years required")
    assert should_keep_job(job) is False


def test_year_range_does_not_treat_t_as_separator():
    assert extract_years_range("needs 1t3 years experience") == (None, None)
    assert extract_years_range("3 to 5 years") == (3, 5)
    assert extract_years_range("3ooo 5 years") == (None, None)


def test_linkedin_fallback_id_is_stable():
    first = _extract_external_id("<li>no id</li>", "https://example.com/x?q=1")
    second = _extract_external_id("<li>no id</li>", "https://example.com/x?q=1")
    assert first == second
    assert first.isalnum()


def test_greenhouse_and_lever_tolerate_bad_payloads():
    assert GreenhouseAdapter.parse_jobs_payload("board", []) == []
    assert GreenhouseAdapter.parse_jobs_payload("board", {"jobs": None}) == []
    jobs = GreenhouseAdapter.parse_jobs_payload(
        "board",
        {"jobs": [{"id": 1, "title": "Product Owner", "location": "Cairo", "absolute_url": "https://x"}]},
    )
    assert len(jobs) == 1
    assert LeverAdapter.parse_jobs_payload("co", {"id": "x"}) == []


def test_remote_and_category_false_positives():
    assert is_strict_remote_post("We are hiring! Please apply online for this job in Maadi.") is False
    assert is_strict_remote_post("Hiring customer support work from home") is True
    assert score_group_relevance("Strategy Hiring Club", "jobs and hiring", "jobs") < 0.5
    assert classify_job_category("excellent communication required for remote job") != "data_entry"


def test_phone_extractor_skips_dates():
    assert extract_phone_numbers("Posted 2023-08-20 and ref 12345678") == ["12345678"]


def test_facebook_import_accepts_bare_host_and_vanity_slug():
    bare = parse_imported_groups_text_detailed("https://facebook.com/groups/1234567890/")
    assert [item["group_external_id"] for item in bare["accepted"]] == ["1234567890"]
    vanity = parse_imported_groups_text_detailed("remotejobs")
    assert [item["group_external_id"] for item in vanity["accepted"]] == ["remotejobs"]
    evil = parse_imported_groups_text_detailed("https://evil.com/facebook.com/groups/1")
    assert evil["accepted"] == []


def test_digest_html_is_escaped(tmp_path):
    db = Database(str(tmp_path / "xss.db"))
    db.init()
    raw = RawJob(
        source="t",
        external_id="xss",
        title="<img src=x onerror=alert(1)> Product Owner",
        company="</td><script>alert(2)</script>",
        location="Cairo",
        description="0-2 years",
        job_url="https://example.com/apply",
        apply_url="javascript:alert(3)",
        posted_at=datetime.now(UTC),
    )
    db.upsert_job(score_job(normalize_raw_job(raw)))
    html = DigestService(Settings(db_path=str(tmp_path / "xss.db")), db)._render_html(
        db.list_digest_items(hours=24)
    )
    assert "<img src=x onerror=alert(1)>" not in html
    assert "<script>alert(2)</script>" not in html
    assert "javascript:alert(3)" not in html
    assert "&lt;img" in html


def test_prune_delete_stays_inside_asset_dirs(tmp_path):
    screenshots = tmp_path / "shots"
    screenshots.mkdir()
    victim = tmp_path / "victim.txt"
    victim.write_text("do-not-delete", encoding="utf-8")
    db = Database(str(tmp_path / "trav.db"))
    db.init()
    post = FacebookPost(
        group_external_id="g",
        group_name="g",
        post_external_id="p",
        post_url="https://www.facebook.com/groups/g/posts/p",
        post_text="remote job from home hiring",
        post_excerpt="x",
        posted_at=datetime.now(UTC) - timedelta(days=200),
        category_tag="other_remote_job",
        is_remote=True,
        phone_numbers=[],
        whatsapp_links=[],
        screenshot_path=f"../{victim.name}",
        raw_snapshot_path=None,
        dedupe_key="abc",
        metadata={},
    )
    db.upsert_facebook_post(post, now=datetime.now(UTC) - timedelta(days=200))
    import sqlite3

    with sqlite3.connect(db.path) as conn:
        conn.execute(
            "UPDATE facebook_posts SET last_seen_at = ?",
            ((datetime.now(UTC) - timedelta(days=200)).isoformat(),),
        )
        conn.commit()
    collector = FacebookCollector(
        Settings(
            db_path=db.path,
            facebook_screenshots_dir=str(screenshots),
            facebook_raw_dir=str(tmp_path / "raw"),
            enable_scheduler=False,
        ),
        db,
    )
    collector._delete_removed_assets(db.prune_facebook_posts(retention_days=90))
    assert victim.exists()
    assert not path_is_within(screenshots / f"../{victim.name}", screenshots)


def test_retention_zero_does_not_wipe(tmp_path):
    db = Database(str(tmp_path / "r.db"))
    db.init()
    db.upsert_job(_scored("Product Owner", "0-2 years"))
    assert db.prune_old_jobs(0) == 0
    assert len(db.list_jobs()) == 1


def test_unknown_pm_titles_and_csv_formula_escape():
    assert infer_role_family("Product Management Specialist") == "unknown"
    assert csv_safe_cell("=cmd") == "'=cmd"


def test_assert_allowed_import_url_blocks_loopback():
    try:
        assert_allowed_import_url("http://127.0.0.1/jobs")
        raise AssertionError("expected ImportUrlError")
    except Exception as exc:
        assert "HTTPS" in str(exc) or "allowed" in str(exc) or "Google" in str(exc)
