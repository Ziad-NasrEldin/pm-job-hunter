from pathlib import Path

from app.adapters.linkedin_public import LinkedInPublicAdapter


def test_parse_linkedin_fixture():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "linkedin_search.html"
    html = fixture_path.read_text(encoding="utf-8")
    jobs = LinkedInPublicAdapter.parse_search_html(html)

    assert len(jobs) == 2
    first = jobs[0]
    assert first.source == "linkedin_public"
    assert first.external_id == "12345"
    assert first.title == "Product Owner - Payments"
    assert first.company == "Acme Corp"
    assert first.location == "Cairo, Egypt"
    assert first.apply_url.startswith("https://www.linkedin.com/jobs/view/")


def test_linkedin_contract_fields_present():
    fixture_path = Path(__file__).parents[1] / "fixtures" / "linkedin_search.html"
    html = fixture_path.read_text(encoding="utf-8")
    jobs = LinkedInPublicAdapter.parse_search_html(html)
    required = ["source", "external_id", "title", "company", "location", "job_url", "apply_url"]
    for job in jobs:
        for key in required:
            assert getattr(job, key)

