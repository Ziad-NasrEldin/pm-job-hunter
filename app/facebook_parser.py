from __future__ import annotations

import re
from hashlib import sha1
from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup


def parse_group_external_id(group_url: str) -> str:
    parsed = urlparse(group_url)
    path = parsed.path.strip("/")
    parts = [part for part in path.split("/") if part]
    if "groups" in parts:
        idx = parts.index("groups")
        if idx + 1 < len(parts):
            return parts[idx + 1]

    query = parse_qs(parsed.query)
    if "id" in query and query["id"]:
        return query["id"][0]

    return sha1(group_url.encode("utf-8")).hexdigest()[:16]


def normalize_facebook_url(raw_url: str) -> str:
    url = (raw_url or "").strip()
    if not url:
        return ""
    if url.startswith("/"):
        url = f"https://www.facebook.com{url}"
    if "l.facebook.com/l.php" in url:
        parsed = urlparse(url)
        target = parse_qs(parsed.query).get("u", [""])[0]
        if target:
            url = unquote(target)
    return url


def parse_group_candidates_from_html(html: str, discovered_keyword: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    candidates: list[dict] = []

    for anchor in soup.select("a[href*='/groups/']"):
        href = normalize_facebook_url(anchor.get("href") or "")
        if not href:
            continue
        if "facebook.com/groups/" not in href:
            continue

        name = anchor.get_text(" ", strip=True)
        if not name:
            continue

        group_external_id = parse_group_external_id(href)
        if group_external_id in seen:
            continue
        seen.add(group_external_id)

        aria = (anchor.get("aria-label") or "").strip()
        container_text = aria or name
        candidates.append(
            {
                "group_external_id": group_external_id,
                "name": name,
                "group_url": href,
                "description": container_text[:400],
                "discovered_keyword": discovered_keyword,
            }
        )

    return candidates


_POST_ID_PATTERNS = [
    re.compile(r"/posts/(\d+)", re.I),
    re.compile(r"/permalink/(\d+)", re.I),
    re.compile(r"story_fbid=(\d+)", re.I),
    re.compile(r"multi_permalinks=(\d+)", re.I),
]


def _extract_post_external_id(post_url: str, fallback_text: str) -> str:
    for pattern in _POST_ID_PATTERNS:
        match = pattern.search(post_url)
        if match:
            return match.group(1)
    return sha1(f"{post_url}|{fallback_text[:120]}".encode("utf-8")).hexdigest()[:20]


def _is_candidate_post_link(url: str) -> bool:
    lowered = url.lower()
    if "facebook.com/groups/" not in lowered:
        return False
    return any(token in lowered for token in ["/posts/", "/permalink/", "story_fbid=", "multi_permalinks="])


def parse_posts_from_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    posts: list[dict] = []

    article_nodes = soup.select("div[role='article']")
    article_nodes.extend(soup.find_all("article"))

    for node in article_nodes:
        post_text = node.get_text(" ", strip=True)
        if not post_text:
            continue

        links = [
            normalize_facebook_url(href)
            for href in [a.get("href", "") for a in node.find_all("a")]
            if normalize_facebook_url(href)
        ]
        permalink = next((link for link in links if _is_candidate_post_link(link)), None)
        if permalink is None:
            continue

        time_tag = node.find("time")
        posted_at_raw = time_tag.get("datetime") if time_tag else None
        post_external_id = _extract_post_external_id(permalink, post_text)

        posts.append(
            {
                "post_external_id": post_external_id,
                "post_url": permalink,
                "post_text": post_text,
                "posted_at_raw": posted_at_raw,
            }
        )

    return posts
