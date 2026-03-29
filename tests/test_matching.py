from app.filters import (
    extract_years_range,
    infer_role_family,
    is_seniority_blocked,
    score_early_career,
)


def test_role_priority_family_inference():
    assert infer_role_family("Product Owner") == "product_owner"
    assert infer_role_family("Product Manager - Growth") == "product_manager"
    assert infer_role_family("Associate Product Manager") == "associate_product_manager"


def test_extract_year_range_patterns():
    assert extract_years_range("Needs 0-4 years in product") == (0, 4)
    assert extract_years_range("Requires 3+ years experience") == (3, 3)
    assert extract_years_range("Minimum 2 years in SaaS") == (2, 2)
    assert extract_years_range("No years listed") == (None, None)


def test_seniority_blocking():
    assert is_seniority_blocked("Senior Product Manager") is True
    assert is_seniority_blocked("Product Owner") is False


def test_early_career_scoring():
    high = score_early_career("0-3 years experience", "Product Owner", 0, 3)
    low = score_early_career("8+ years required", "Product Manager", 8, 8)
    junior = score_early_career("Great communication", "Junior Product Manager", None, None)
    assert high > 0.7
    assert low < 0.3
    assert junior >= 0.44
