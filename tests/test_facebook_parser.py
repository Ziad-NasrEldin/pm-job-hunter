from pathlib import Path

from app.facebook_parser import (
    normalize_facebook_url,
    parse_imported_groups_text,
    parse_group_candidates_from_html,
    parse_group_external_id,
    parse_posts_from_html,
)


def test_parse_group_external_id_from_url():
    assert parse_group_external_id("https://www.facebook.com/groups/1234567890/") == "1234567890"
    assert parse_group_external_id("https://www.facebook.com/groups/egypt.remote.jobs/") == "egypt.remote.jobs"


def test_normalize_facebook_url_from_redirect_wrapper():
    raw = "https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.facebook.com%2Fgroups%2F1234567890%2F"
    assert normalize_facebook_url(raw) == "https://www.facebook.com/groups/1234567890/"


def test_parse_group_candidates_fixture():
    html = Path("tests/fixtures/facebook_groups_search.html").read_text(encoding="utf-8")
    candidates = parse_group_candidates_from_html(html, discovered_keyword="وظائف عن بعد مصر")
    assert len(candidates) == 2
    assert candidates[0]["group_external_id"] == "1234567890"


def test_parse_group_posts_fixture():
    html = Path("tests/fixtures/facebook_group_posts.html").read_text(encoding="utf-8")
    posts = parse_posts_from_html(html)
    assert len(posts) == 3
    assert posts[0]["post_external_id"] == "987654321"
    assert "groups/1234567890/posts/987654321" in posts[0]["post_url"]


def test_parse_imported_groups_text_supports_urls_ids_and_name_pipe():
    raw = """
    https://www.facebook.com/groups/1386469535434819/
    egypt.remote.jobs
    Cairo WFH | https://www.facebook.com/groups/1234567890/
    """
    parsed = parse_imported_groups_text(raw)
    assert len(parsed) == 3
    assert parsed[0]["group_external_id"] == "1386469535434819"
    assert parsed[1]["group_external_id"] == "egypt.remote.jobs"
    assert parsed[2]["name"] == "Cairo WFH"
