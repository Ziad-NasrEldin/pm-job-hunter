from __future__ import annotations

from ipaddress import ip_address
from urllib.parse import urljoin, urlparse

ALLOWED_IMPORT_HOSTS = {
    "docs.google.com",
    "spreadsheets.google.com",
    "googleusercontent.com",
}
ALLOWED_IMPORT_HOST_SUFFIXES = (".googleusercontent.com",)

FACEBOOK_HOSTS = {
    "facebook.com",
    "www.facebook.com",
    "m.facebook.com",
    "web.facebook.com",
    "l.facebook.com",
}

LOCAL_WEB_HOSTS = {"localhost", "127.0.0.1", "::1"}

MAX_IMPORT_BYTES = 1_000_000


class ImportUrlError(ValueError):
    pass


def is_facebook_host(hostname: str | None) -> bool:
    host = (hostname or "").lower().rstrip(".")
    if host in FACEBOOK_HOSTS:
        return True
    return host.endswith(".facebook.com")


def is_local_web_origin(value: str | None) -> bool:
    if not value:
        return True
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    return host in LOCAL_WEB_HOSTS


def _host_allowed_for_import(hostname: str | None) -> bool:
    host = (hostname or "").lower().rstrip(".")
    if not host:
        return False
    if host in ALLOWED_IMPORT_HOSTS:
        return True
    return any(host.endswith(suffix) for suffix in ALLOWED_IMPORT_HOST_SUFFIXES)


def _is_private_or_local_host(hostname: str | None) -> bool:
    host = (hostname or "").strip().lower().rstrip(".")
    if not host:
        return True
    if host in LOCAL_WEB_HOSTS or host.endswith(".local"):
        return True
    try:
        parsed_ip = ip_address(host)
    except ValueError:
        return False
    return bool(
        parsed_ip.is_private
        or parsed_ip.is_loopback
        or parsed_ip.is_link_local
        or parsed_ip.is_reserved
        or parsed_ip.is_multicast
    )


def sanitize_google_sheet_gid(raw_gid: str) -> str:
    digits: list[str] = []
    for ch in raw_gid:
        if not ch.isdigit():
            break
        digits.append(ch)
    return "".join(digits) or "0"


def google_sheet_to_csv_url(raw_url: str) -> str:
    cleaned = (raw_url or "").strip()
    if "docs.google.com/spreadsheets/d/" not in cleaned:
        return cleaned

    parts = cleaned.split("/d/", 1)
    if len(parts) < 2:
        return cleaned
    doc_id = "".join(ch for ch in parts[1].split("/", 1)[0] if ch.isalnum() or ch in {"-", "_"})
    if not doc_id:
        return cleaned

    gid = "0"
    if "gid=" in cleaned:
        gid = sanitize_google_sheet_gid(cleaned.split("gid=", 1)[1].split("&", 1)[0])
    return f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}"


def assert_allowed_import_url(raw_url: str) -> str:
    cleaned = google_sheet_to_csv_url(raw_url)
    parsed = urlparse(cleaned)
    if parsed.scheme != "https":
        raise ImportUrlError("Import URL must be HTTPS.")
    if _is_private_or_local_host(parsed.hostname):
        raise ImportUrlError("Import URL host is not allowed.")
    if not _host_allowed_for_import(parsed.hostname):
        raise ImportUrlError("Import URL must be a public Google Sheet.")
    return cleaned


def csv_safe_cell(value: object) -> str:
    text = "" if value is None else str(value)
    if text[:1] in {"=", "+", "-", "@", "\t", "\r"}:
        return f"'{text}"
    return text


def like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def path_is_within(path, root) -> bool:
    try:
        return path.resolve().is_relative_to(root.resolve())
    except (OSError, ValueError):
        return False


def next_redirect_url(current_url: str, location: str | None) -> str:
    if not location:
        raise ImportUrlError("Import URL redirect was missing a Location header.")
    return urljoin(current_url, location)


def fetch_allowed_csv(url: str, *, timeout: float) -> str:
    import httpx

    current = assert_allowed_import_url(url)
    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        for _ in range(5):
            current = assert_allowed_import_url(current)
            response = client.get(current)
            if response.is_redirect:
                current = next_redirect_url(current, response.headers.get("location"))
                continue
            response.raise_for_status()
            if len(response.content) > MAX_IMPORT_BYTES:
                raise ImportUrlError("Import file is too large (1MB max).")
            return response.text
    raise ImportUrlError("Too many redirects.")
