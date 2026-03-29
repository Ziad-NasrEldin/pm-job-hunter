import json
from pathlib import Path

from app.adapters.lever import LeverAdapter


def test_parse_lever_fixture():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "lever.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    jobs = LeverAdapter.parse_jobs_payload("test", payload)

    assert len(jobs) == 2
    assert jobs[0].external_id == "test:abc123"
    assert jobs[0].source == "lever"
    assert jobs[0].title == "Product Owner"
    assert jobs[0].apply_url.endswith("/apply")


def test_lever_contract_fields_present():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "lever.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    jobs = LeverAdapter.parse_jobs_payload("test", payload)
    required = ["source", "external_id", "title", "company", "location", "job_url", "apply_url"]
    for job in jobs:
        for key in required:
            assert getattr(job, key)

