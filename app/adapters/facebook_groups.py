from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from hashlib import sha1
from pathlib import Path
from urllib.parse import quote_plus

from app.config import Settings
from app.facebook_filters import (
    classify_job_category,
    extract_phone_numbers,
    extract_whatsapp_links,
    facebook_post_dedupe_key,
    is_strict_remote_post,
    score_group_relevance,
)
from app.facebook_parser import parse_group_candidates_from_html
from app.facebook_time import parse_facebook_time
from app.models import FacebookGroupCandidate, FacebookPost

try:  # pragma: no cover - optional dependency for non-Facebook test paths
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import Locator, sync_playwright
except Exception:  # noqa: BLE001
    PlaywrightError = Exception
    PlaywrightTimeoutError = Exception
    Locator = object
    sync_playwright = None

_POST_ID_PATTERNS = [
    re.compile(r"/posts/(\d+)", re.I),
    re.compile(r"/permalink/(\d+)", re.I),
    re.compile(r"story_fbid=(\d+)", re.I),
    re.compile(r"multi_permalinks=(\d+)", re.I),
]


class FacebookGroupsAdapter:
    source_name = "facebook_groups"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _require_playwright(self) -> None:
        if sync_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: pip install -r requirements.txt")

    def _open_context(self, *, headless: bool):
        self._require_playwright()
        playwright = sync_playwright().start()
        try:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(Path(self.settings.facebook_profile_dir).resolve()),
                headless=headless,
                viewport={"width": 1440, "height": 1080},
            )
            return playwright, context
        except Exception:  # noqa: BLE001
            playwright.stop()
            raise

    def bootstrap_login(self) -> dict[str, str]:
        playwright, context = self._open_context(headless=False)
        try:
            page = context.new_page()
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=90_000)
            print(
                "A browser window has opened. Log in to Facebook, then press ENTER in this terminal to save the session."
            )
            input()
            page.goto("https://www.facebook.com/me", wait_until="domcontentloaded", timeout=90_000)
            return {"status": "ready", "message": "Facebook session saved to persistent profile."}
        finally:
            context.close()
            playwright.stop()

    def discover_groups(self) -> list[FacebookGroupCandidate]:
        playwright, context = self._open_context(headless=self.settings.facebook_headless)
        try:
            page = context.new_page()
            deduped: dict[str, FacebookGroupCandidate] = {}

            for keyword in self.settings.facebook_discovery_keywords:
                url = f"https://www.facebook.com/search/groups/?q={quote_plus(keyword)}"
                page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                self._scroll_page(page, self.settings.facebook_discovery_scrolls)
                html = page.content()

                for item in parse_group_candidates_from_html(html, discovered_keyword=keyword):
                    score = score_group_relevance(item["name"], item["description"], keyword)
                    if score <= 0:
                        continue
                    candidate = FacebookGroupCandidate(
                        group_external_id=item["group_external_id"],
                        name=item["name"],
                        group_url=item["group_url"],
                        description=item["description"],
                        relevance_score=score,
                        discovered_keyword=keyword,
                        metadata={"search_url": url},
                    )
                    existing = deduped.get(candidate.group_external_id)
                    if existing is None or candidate.relevance_score > existing.relevance_score:
                        deduped[candidate.group_external_id] = candidate

            ordered = sorted(deduped.values(), key=lambda c: c.relevance_score, reverse=True)
            return ordered[: self.settings.facebook_discovery_max_groups]
        finally:
            context.close()
            playwright.stop()

    def fetch_group_posts(self, group: dict[str, str]) -> list[FacebookPost]:
        playwright, context = self._open_context(headless=self.settings.facebook_headless)
        try:
            page = context.new_page()
            page.goto(group["group_url"], wait_until="domcontentloaded", timeout=90_000)

            now = datetime.now(UTC)
            cutoff = now - timedelta(days=max(1, self.settings.facebook_crawl_days))
            max_posts = max(1, self.settings.facebook_max_posts_per_group)
            max_scrolls = max(1, self.settings.facebook_max_scrolls_per_group)
            posts: dict[str, FacebookPost] = {}
            oldest_seen = now

            for scroll_idx in range(max_scrolls):
                page.wait_for_timeout(1_100)
                articles = page.locator("div[role='article']")
                try:
                    count = articles.count()
                except PlaywrightError:
                    count = 0

                for idx in range(min(count, max_posts)):
                    article = articles.nth(idx)
                    post = self._extract_post_from_article(article=article, group=group, cutoff=cutoff)
                    if post is None:
                        continue
                    if post.posted_at is not None:
                        oldest_seen = min(oldest_seen, post.posted_at)
                    posts[post.dedupe_key] = post

                if len(posts) >= max_posts:
                    break
                if oldest_seen < cutoff and scroll_idx >= 2:
                    break

                self._scroll_page(page, 1)

            return list(posts.values())
        finally:
            context.close()
            playwright.stop()

    def _extract_post_from_article(
        self,
        *,
        article: Locator,
        group: dict[str, str],
        cutoff: datetime,
    ) -> FacebookPost | None:
        try:
            post_text = article.inner_text(timeout=2_000)
        except (PlaywrightTimeoutError, PlaywrightError):
            return None

        post_text = (post_text or "").strip()
        if len(post_text) < 30:
            return None
        if not is_strict_remote_post(post_text):
            return None

        links = self._safe_extract_links(article)
        permalink = self._pick_post_permalink(links)
        if permalink is None:
            return None

        post_external_id = self._extract_post_external_id(permalink, post_text)
        posted_at = self._extract_post_datetime(article)
        if posted_at is not None and posted_at < cutoff:
            return None

        category_tag = classify_job_category(post_text)
        phone_numbers = extract_phone_numbers(post_text)
        whatsapp_links = extract_whatsapp_links(post_text)

        screenshot_path = self._save_post_screenshot(article, group["group_external_id"], post_external_id)
        raw_snapshot_path = self._save_raw_snapshot(article, group["group_external_id"], post_external_id)
        dedupe_key = facebook_post_dedupe_key(group["group_external_id"], post_external_id, permalink)

        return FacebookPost(
            group_external_id=group["group_external_id"],
            group_name=group["name"],
            post_external_id=post_external_id,
            post_url=permalink,
            post_text=post_text,
            post_excerpt=post_text[:260],
            posted_at=posted_at,
            category_tag=category_tag,
            is_remote=True,
            phone_numbers=phone_numbers,
            whatsapp_links=whatsapp_links,
            screenshot_path=screenshot_path,
            raw_snapshot_path=raw_snapshot_path,
            dedupe_key=dedupe_key,
            metadata={
                "source": "facebook_group",
                "link_count": len(links),
            },
        )

    def _safe_extract_links(self, article: Locator) -> list[str]:
        try:
            raw_links = article.locator("a").evaluate_all("els => els.map(e => e.href || '').filter(Boolean)")
        except (PlaywrightTimeoutError, PlaywrightError):
            return []
        links: list[str] = []
        for link in raw_links:
            normalized = str(link).strip()
            if not normalized:
                continue
            links.append(normalized)
        return links

    def _pick_post_permalink(self, links: list[str]) -> str | None:
        for link in links:
            lowered = link.lower()
            if "facebook.com/groups/" not in lowered:
                continue
            if any(token in lowered for token in ["/posts/", "/permalink/", "story_fbid=", "multi_permalinks="]):
                return link
        return None

    def _extract_post_external_id(self, permalink: str, post_text: str) -> str:
        for pattern in _POST_ID_PATTERNS:
            match = pattern.search(permalink)
            if match:
                return match.group(1)
        return sha1(f"{permalink}|{post_text[:120]}".encode("utf-8")).hexdigest()[:20]

    def _extract_post_datetime(self, article: Locator) -> datetime | None:
        datetime_value: str | None = None
        label: str | None = None

        try:
            time_el = article.locator("time").first
            if time_el.count() > 0:
                datetime_value = time_el.get_attribute("datetime", timeout=800)
                label = time_el.inner_text(timeout=800)
        except (PlaywrightTimeoutError, PlaywrightError):
            pass

        if not label:
            try:
                link = article.locator("a[aria-label]").first
                if link.count() > 0:
                    label = link.get_attribute("aria-label", timeout=800)
            except (PlaywrightTimeoutError, PlaywrightError):
                pass

        return parse_facebook_time(datetime_value=datetime_value, label=label)

    def _save_post_screenshot(self, article: Locator, group_external_id: str, post_external_id: str) -> str | None:
        safe_group = self._safe_component(group_external_id)
        safe_post = self._safe_component(post_external_id)
        rel_path = Path(safe_group) / f"{safe_post}.png"
        full_path = Path(self.settings.facebook_screenshots_dir) / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            article.screenshot(path=str(full_path), timeout=4_000)
            return rel_path.as_posix()
        except (PlaywrightTimeoutError, PlaywrightError):
            return None

    def _save_raw_snapshot(self, article: Locator, group_external_id: str, post_external_id: str) -> str | None:
        safe_group = self._safe_component(group_external_id)
        safe_post = self._safe_component(post_external_id)
        rel_path = Path(safe_group) / f"{safe_post}.html"
        full_path = Path(self.settings.facebook_raw_dir) / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            html = article.inner_html(timeout=2_500)
            full_path.write_text(html, encoding="utf-8")
            return rel_path.as_posix()
        except (PlaywrightTimeoutError, PlaywrightError, OSError):
            return None

    @staticmethod
    def _safe_component(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return cleaned.strip("._") or "item"

    @staticmethod
    def _scroll_page(page, rounds: int) -> None:
        for _ in range(max(1, rounds)):
            page.mouse.wheel(0, 4_500)
            page.wait_for_timeout(1_000)
