from app.filters import normalized_dedupe_key


def test_dedupe_same_normalized_values():
    a = normalized_dedupe_key(
        "Product Owner ",
        "Acme",
        " Cairo, Egypt ",
        "https://example.com/jobs/1",
    )
    b = normalized_dedupe_key(
        "product owner",
        "acme",
        "cairo, egypt",
        "https://example.com/jobs/1",
    )
    assert a == b


def test_dedupe_changes_with_apply_url():
    a = normalized_dedupe_key("Product Owner", "Acme", "Remote", "https://example.com/jobs/1")
    b = normalized_dedupe_key("Product Owner", "Acme", "Remote", "https://example.com/jobs/2")
    assert a != b

