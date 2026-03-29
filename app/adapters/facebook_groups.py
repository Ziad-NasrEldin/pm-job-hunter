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
from app.facebook_parser import (
    normalize_facebook_url,
    parse_group_candidates_from_html,
    parse_group_external_id,
)
from app.facebook_time import parse_facebook_time
from app.models import FacebookGroupCandidate, FacebookPost

try:  # pragma: no cover - optional dependency for non-Facebook test paths
    from playwright.sync_api import Browser, BrowserContext, Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import Locator, sync_playwright
except Exception:  # noqa: BLE001
    Browser = object
    BrowserContext = object
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

_GROUP_SEARCH_URLS = [
    "https://www.facebook.com/groups/search/?q={query}",
    "https://www.facebook.com/search/groups/?q={query}",
]

_GROUP_URL_EXCLUDE_TOKENS = [
    "/groups/feed",
    "/groups/discover",
    "/groups/you",
    "/groups/join",
    "/groups/create",
    "/groups/?",
    "/groups/?ref",
]

_GROUP_NAME_NOISE_PREFIXES = [
    "profile photo of ",
    "صورة الملف الشخصي لـ",
]


class FacebookGroupsAdapter:
    source_name = "facebook_groups"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def _require_playwright(self) -> None:
        if sync_playwright is None:
            raise RuntimeError("Playwright is not installed. Run: pip install -r requirements.txt")

    def _open_login_context(self, *, headless: bool):
        self._require_playwright()
        playwright = sync_playwright().start()
        browser: Browser | None = None
        context: BrowserContext | None = None
        try:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(viewport={"width": 1440, "height": 1080})
            return playwright, browser, context
        except Exception:  # noqa: BLE001
            if context is not None:
                context.close()
            if browser is not None:
                browser.close()
            playwright.stop()
            raise

    def _open_runtime_context(self, *, headless: bool):
        self._require_playwright()
        state_path = Path(self.settings.facebook_storage_state_path).resolve()
        if not state_path.exists():
            raise RuntimeError(
                f"Facebook storage state not found at {state_path}. Run `python -m app.cli facebook-login` first."
            )

        playwright = sync_playwright().start()
        browser: Browser | None = None
        context: BrowserContext | None = None
        try:
            browser = playwright.chromium.launch(headless=headless)
            context = browser.new_context(
                storage_state=str(state_path),
                viewport={"width": 1440, "height": 1080},
            )
            return playwright, browser, context
        except Exception:  # noqa: BLE001
            if context is not None:
                context.close()
            if browser is not None:
                browser.close()
            playwright.stop()
            raise

    def bootstrap_login(self) -> dict[str, str]:
        playwright, browser, context = self._open_login_context(headless=False)
        try:
            page = context.new_page()
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=90_000)
            timeout_seconds = max(30, self.settings.facebook_login_timeout_seconds)
            deadline = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
            while datetime.now(UTC) < deadline:
                if self._is_authenticated(page):
                    break
                page.wait_for_timeout(1_500)
            else:
                raise RuntimeError(
                    f"Facebook login timed out after {timeout_seconds} seconds. "
                    "Open login again and complete authentication in the browser window."
                )

            state_path = Path(self.settings.facebook_storage_state_path).resolve()
            state_path.parent.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=str(state_path))

            return {
                "status": "ready",
                "message": f"Facebook session saved ({state_path}).",
            }
        finally:
            context.close()
            browser.close()
            playwright.stop()

    def discover_groups(self) -> list[FacebookGroupCandidate]:
        playwright, browser, context = self._open_runtime_context(headless=self.settings.facebook_headless)
        try:
            page = context.new_page()
            page.goto("https://www.facebook.com/me", wait_until="domcontentloaded", timeout=90_000)
            self._ensure_authenticated(page, "group discovery")

            deduped: dict[str, FacebookGroupCandidate] = {}

            for keyword in self.settings.facebook_discovery_keywords:
                query = quote_plus(keyword)
                for url_template in _GROUP_SEARCH_URLS:
                    search_url = url_template.format(query=query)
                    page.goto(search_url, wait_until="domcontentloaded", timeout=90_000)
                    self._ensure_authenticated(page, "group discovery")
                    self._scroll_page(page, self.settings.facebook_discovery_scrolls)

                    extracted = self._extract_group_candidates_from_page(page=page, keyword=keyword, search_url=search_url)
                    if not extracted:
                        html = page.content()
                        extracted = self._extract_group_candidates_from_html(html=html, keyword=keyword, search_url=search_url)

                    for candidate in extracted:
                        existing = deduped.get(candidate.group_external_id)
                        if existing is None or candidate.relevance_score > existing.relevance_score:
                            deduped[candidate.group_external_id] = candidate

            ordered = sorted(deduped.values(), key=lambda c: c.relevance_score, reverse=True)
            limited = ordered[: self.settings.facebook_discovery_max_groups]
            if not limited:
                raise RuntimeError(
                    "No groups discovered. Ensure session is valid (run facebook-login), keep FACEBOOK_HEADLESS=false, and retry."
                )
            return limited
        finally:
            context.close()
            browser.close()
            playwright.stop()

    def fetch_group_posts(self, group: dict[str, str]) -> list[FacebookPost]:
        groups_data, errors = self.fetch_groups_posts([group])
        if errors:
            key = group.get("group_external_id", "unknown")
            message = errors.get(key) or next(iter(errors.values()))
            raise RuntimeError(message)
        return groups_data.get(group.get("group_external_id", "unknown"), [])

    def fetch_groups_posts(self, groups: list[dict[str, str]]) -> tuple[dict[str, list[FacebookPost]], dict[str, str]]:
        if not groups:
            return {}, {}

        groups_data: dict[str, list[FacebookPost]] = {}
        errors: dict[str, str] = {}

        playwright, browser, context = self._open_runtime_context(headless=self.settings.facebook_headless)
        try:
            page = context.new_page()
            page.goto("https://www.facebook.com/me", wait_until="domcontentloaded", timeout=90_000)
            self._ensure_authenticated(page, "group crawl")

            for group in groups:
                group_id = group.get("group_external_id", "unknown")
                try:
                    groups_data[group_id] = self._crawl_group_posts(page=page, group=group)
                except Exception as exc:  # noqa: BLE001
                    message = str(exc).strip() or exc.__class__.__name__
                    errors[group_id] = message
            return groups_data, errors
        finally:
            context.close()
            browser.close()
            playwright.stop()

    def _crawl_group_posts(self, *, page, group: dict[str, str]) -> list[FacebookPost]:
        group_url = group["group_url"]
        page.goto(group_url, wait_until="domcontentloaded", timeout=90_000)
        self._ensure_authenticated(page, f"group crawl ({group.get('group_external_id', 'unknown')})")
        self._ensure_group_accessible(page, group_url)

        now = datetime.now(UTC)
        cutoff = now - timedelta(days=max(1, self.settings.facebook_crawl_days))
        max_posts = max(1, self.settings.facebook_max_posts_per_group)
        max_scrolls = max(1, self.settings.facebook_max_scrolls_per_group)
        posts: dict[str, FacebookPost] = {}
        oldest_seen = now

        for scroll_idx in range(max_scrolls):
            page.wait_for_timeout(1_100)
            articles = self._article_locator(page)
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

    def _extract_group_candidates_from_page(
        self,
        *,
        page,
        keyword: str,
        search_url: str,
    ) -> list[FacebookGroupCandidate]:
        try:
            raw_items = page.locator("a[href*='/groups/']").evaluate_all(
                "els => els.map(el => ({ href: el.href || '', text: (el.innerText || '').trim(), aria: (el.getAttribute('aria-label') || '').trim() }))"
            )
        except (PlaywrightTimeoutError, PlaywrightError):
            return []

        deduped: dict[str, FacebookGroupCandidate] = {}
        for item in raw_items:
            href = normalize_facebook_url(str(item.get("href", "")))
            if not self._looks_like_group_url(href):
                continue

            raw_name = str(item.get("text") or item.get("aria") or "").strip()
            name = self._clean_group_name(raw_name)
            if len(name) < 3:
                continue

            description = f"{name}"
            score = score_group_relevance(name=name, description=description, keyword=keyword)
            if score < 0.35:
                continue

            group_external_id = parse_group_external_id(href)
            candidate = FacebookGroupCandidate(
                group_external_id=group_external_id,
                name=name,
                group_url=href,
                description=description,
                relevance_score=score,
                discovered_keyword=keyword,
                metadata={"search_url": search_url, "extraction": "dom"},
            )
            existing = deduped.get(group_external_id)
            if existing is None or candidate.relevance_score > existing.relevance_score:
                deduped[group_external_id] = candidate

        return list(deduped.values())

    def _extract_group_candidates_from_html(
        self,
        *,
        html: str,
        keyword: str,
        search_url: str,
    ) -> list[FacebookGroupCandidate]:
        candidates: list[FacebookGroupCandidate] = []
        for item in parse_group_candidates_from_html(html, discovered_keyword=keyword):
            score = score_group_relevance(item["name"], item["description"], keyword)
            if score < 0.35:
                continue
            candidate = FacebookGroupCandidate(
                group_external_id=item["group_external_id"],
                name=self._clean_group_name(item["name"]),
                group_url=item["group_url"],
                description=item["description"],
                relevance_score=score,
                discovered_keyword=keyword,
                metadata={"search_url": search_url, "extraction": "html"},
            )
            candidates.append(candidate)
        return candidates

    def _ensure_authenticated(self, page, action: str) -> None:
        if not self._is_authenticated(page):
            login_hint = (
                "Use the dashboard quick action 'Facebook Login' and complete login in the opened browser."
                if action == "login bootstrap"
                else "Use dashboard quick action 'Facebook Login' or run `python -m app.cli facebook-login`."
            )
            raise RuntimeError(
                f"Facebook session is not authenticated during {action}. {login_hint}"
            )

    def _is_authenticated(self, page) -> bool:
        url = (page.url or "").lower()
        if any(token in url for token in ["/login", "checkpoint", "recover"]):
            return False

        try:
            if page.locator("input[name='email']").count() > 0:
                return False
        except PlaywrightError:
            pass
        return True

    def _ensure_group_accessible(self, page, group_url: str) -> None:
        try:
            unavailable = page.get_by_text("This content isn't available right now", exact=False).count()
            private_hint = page.get_by_text("Private group", exact=False).count()
        except PlaywrightError:
            unavailable = 0
            private_hint = 0

        if unavailable > 0 or private_hint > 0:
            raise RuntimeError(f"Group is unavailable or private for this account: {group_url}")

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
            normalized = normalize_facebook_url(str(link).strip())
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

    def _article_locator(self, page):
        primary = page.locator("div[role='article']")
        try:
            if primary.count() > 0:
                return primary
        except PlaywrightError:
            pass
        return page.locator("article")

    @staticmethod
    def _safe_component(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return cleaned.strip("._") or "item"

    def _looks_like_group_url(self, url: str) -> bool:
        lowered = (url or "").lower()
        if "facebook.com/groups/" not in lowered:
            return False
        if any(token in lowered for token in _GROUP_URL_EXCLUDE_TOKENS):
            return False
        group_id = parse_group_external_id(url)
        return bool(group_id)

    def _clean_group_name(self, value: str) -> str:
        name = (value or "").strip()
        lowered = name.lower()
        for prefix in _GROUP_NAME_NOISE_PREFIXES:
            if lowered.startswith(prefix):
                name = name[len(prefix) :].strip()
                lowered = name.lower()
        name = re.sub(r"\s+", " ", name)
        return name.strip()

    @staticmethod
    def _scroll_page(page, rounds: int) -> None:
        for _ in range(max(1, rounds)):
            page.mouse.wheel(0, 4_500)
            page.wait_for_timeout(1_000)
