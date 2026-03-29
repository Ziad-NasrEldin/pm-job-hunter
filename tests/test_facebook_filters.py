from app.facebook_filters import (
    classify_job_category,
    extract_phone_numbers,
    extract_whatsapp_links,
    facebook_post_dedupe_key,
    is_strict_remote_post,
    normalize_arabic_digits,
)


def test_normalize_arabic_digits():
    assert normalize_arabic_digits("٠١٢٣٤٥٦٧٨٩") == "0123456789"


def test_remote_filter_strict_arabic_and_english():
    assert is_strict_remote_post("مطلوب خدمة عملاء من المنزل") is True
    assert is_strict_remote_post("Hiring customer support work from home") is True
    assert is_strict_remote_post("مطلوب مندوب مبيعات في القاهرة") is False


def test_extract_phone_numbers_supports_arabic_digits():
    text = "للتقديم واتساب ٠١٢٢٣٣٤٤٥٥ أو +20 122 334 4556"
    numbers = extract_phone_numbers(text)
    assert "0122334455" in numbers
    assert "+201223344556" in numbers


def test_extract_whatsapp_links_and_phone_hints():
    text = "تواصل عبر https://wa.me/201001112223 أو واتساب 01001112223"
    links = extract_whatsapp_links(text)
    assert "https://wa.me/201001112223" in links
    assert "https://wa.me/01001112223" in links


def test_category_tagging():
    assert classify_job_category("مطلوب تيلي سيلز من البيت") == "cold_calling"
    assert classify_job_category("Hiring customer support remote") == "customer_support"


def test_facebook_dedupe_key_is_stable():
    key1 = facebook_post_dedupe_key("group1", "post1", "https://facebook.com/post1")
    key2 = facebook_post_dedupe_key("group1", "post1", "https://facebook.com/post1")
    assert key1 == key2
