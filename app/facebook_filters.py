from __future__ import annotations

import re
from hashlib import sha1

ARABIC_DIGIT_TRANSLATION = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")

REMOTE_KEYWORDS = [
    "remote",
    "work from home",
    "wfh",
    "home based",
    "online",
    "عن بعد",
    "من المنزل",
    "من البيت",
    "عن ب عد",
    "اونلاين",
    "أونلاين",
    "ريموت",
]

JOB_KEYWORDS = [
    "job",
    "jobs",
    "hiring",
    "vacancy",
    "apply now",
    "مطلوب",
    "وظيفة",
    "وظائف",
    "فرصة عمل",
    "فرص عمل",
    "شغل",
    "تعيين",
    "التوظيف",
]

GROUP_EGYPT_MARKERS = ["egypt", "cairo", "alexandria", "egy", "مصر", "القاهرة", "اسكندرية", "الإسكندرية"]
GROUP_REMOTE_MARKERS = ["remote", "work from home", "عن بعد", "من المنزل", "من البيت", "اونلاين", "أونلاين"]
GROUP_JOB_MARKERS = ["jobs", "job", "hiring", "وظائف", "وظيفة", "فرص عمل", "مطلوب"]

CATEGORY_KEYWORDS = {
    "cold_calling": [
        "cold calling",
        "cold caller",
        "telesales",
        "tele sales",
        "call center sales",
        "كول كول",
        "كولد كولينج",
        "تيلي سيلز",
        "تلي سيلز",
        "مبيعات هاتفية",
    ],
    "customer_support": [
        "customer support",
        "customer service",
        "call center",
        "support agent",
        "خدمة عملاء",
        "كول سنتر",
        "خدمه عملاء",
    ],
    "sales": [
        "sales",
        "inside sales",
        "outbound",
        "b2b sales",
        "مبيعات",
        "سيلز",
    ],
    "data_entry": [
        "data entry",
        "excel",
        "ادخال بيانات",
        "إدخال بيانات",
    ],
}

_WHITESPACE_RE = re.compile(r"\s+")
_PHONE_CANDIDATE_RE = re.compile(r"(?:\+?\d[\d\s\-\(\)]{6,}\d)")
_WHATSAPP_URL_RE = re.compile(
    r"https?://(?:wa\.me/\d+|api\.whatsapp\.com/send\?phone=\d+|chat\.whatsapp\.com/[A-Za-z0-9]+|(?:www\.)?whatsapp\.com/[^\s]+)",
    re.I,
)
_WHATSAPP_PHONE_HINT_RE = re.compile(r"(?:واتساب|واتس\s*اب|whats?\s*app)\s*[:\-]?\s*(\+?\d[\d\s\-\(\)]{6,}\d)", re.I)


def normalize_arabic_digits(text: str) -> str:
    return text.translate(ARABIC_DIGIT_TRANSLATION)


def normalize_search_text(text: str) -> str:
    cleaned = normalize_arabic_digits(text or "")
    cleaned = cleaned.lower()
    cleaned = cleaned.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    cleaned = cleaned.replace("ى", "ي")
    cleaned = _WHITESPACE_RE.sub(" ", cleaned)
    return cleaned.strip()


def is_strict_remote_post(text: str) -> bool:
    normalized = normalize_search_text(text)
    if not normalized:
        return False
    has_remote = any(keyword in normalized for keyword in REMOTE_KEYWORDS)
    has_job = any(keyword in normalized for keyword in JOB_KEYWORDS)
    return has_remote and has_job


def _normalize_phone_candidate(raw: str) -> str | None:
    token = normalize_arabic_digits(raw)
    token = token.strip()
    if token.startswith("00"):
        token = "+" + token[2:]
    token = re.sub(r"[^\d+]", "", token)
    if token.count("+") > 1:
        return None
    if "+" in token and not token.startswith("+"):
        return None

    digits = re.sub(r"\D", "", token)
    if len(digits) < 8 or len(digits) > 15:
        return None

    if token.startswith("+"):
        return f"+{digits}"
    return digits


def extract_phone_numbers(text: str) -> list[str]:
    normalized = normalize_arabic_digits(text or "")
    seen: set[str] = set()
    numbers: list[str] = []

    for match in _PHONE_CANDIDATE_RE.finditer(normalized):
        phone = _normalize_phone_candidate(match.group(0))
        if not phone or phone in seen:
            continue
        seen.add(phone)
        numbers.append(phone)

    return numbers


def extract_whatsapp_links(text: str) -> list[str]:
    normalized = normalize_arabic_digits(text or "")
    seen: set[str] = set()
    links: list[str] = []

    for match in _WHATSAPP_URL_RE.finditer(normalized):
        link = match.group(0).rstrip(".,)")
        if link in seen:
            continue
        seen.add(link)
        links.append(link)

    for match in _WHATSAPP_PHONE_HINT_RE.finditer(normalized):
        phone = _normalize_phone_candidate(match.group(1))
        if not phone:
            continue
        digits = re.sub(r"\D", "", phone)
        link = f"https://wa.me/{digits}"
        if link in seen:
            continue
        seen.add(link)
        links.append(link)

    return links


def classify_job_category(text: str) -> str:
    normalized = normalize_search_text(text)
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            return category
    return "other_remote_job"


def score_group_relevance(name: str, description: str, keyword: str) -> float:
    haystack = normalize_search_text(f"{name} {description} {keyword}")
    score = 0.0
    if any(marker in haystack for marker in GROUP_EGYPT_MARKERS):
        score += 0.45
    if any(marker in haystack for marker in GROUP_JOB_MARKERS):
        score += 0.35
    if any(marker in haystack for marker in GROUP_REMOTE_MARKERS):
        score += 0.20
    return round(min(score, 1.0), 3)


def facebook_post_dedupe_key(group_external_id: str, post_external_id: str, post_url: str) -> str:
    blob = "|".join([group_external_id.strip(), post_external_id.strip(), post_url.strip()])
    return sha1(blob.encode("utf-8")).hexdigest()
