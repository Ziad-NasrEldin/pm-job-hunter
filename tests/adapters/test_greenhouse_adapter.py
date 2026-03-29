import json
from pathlib import Path

from app.adapters.greenhouse import GreenhouseAdapter


def test_parse_greenhouse_fixture():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "greenhouse.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    jobs = GreenhouseAdapter.parse_jobs_payload("test-board", payload)

    assert len(jobs) == 2
    assert jobs[0].external_id == "test-board:11"
    assert jobs[0].source == "greenhouse"
    assert jobs[0].title == "Product Manager"
    assert "0-3 years" in (jobs[0].description or "")


def test_greenhouse_contract_fields_present():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "greenhouse.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    jobs = GreenhouseAdapter.parse_jobs_payload("test-board", payload)
    required = ["source", "external_id", "title", "company", "location", "job_url", "apply_url"]
    for job in jobs:
        for key in required:
            assert getattr(job, key)

