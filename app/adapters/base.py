from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime

import httpx

from app.config import Settings
from app.models import RawJob, SearchQuery


class JobAdapter(ABC):
    source_name: str = "unknown"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._last_request_ts = 0.0
        self._client = httpx.Client(
            timeout=settings.request_timeout_seconds,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0.0.0 Safari/537.36"
                )
            },
        )

    @abstractmethod
    def fetch_jobs(self, query: SearchQuery) -> list[RawJob]:
        pass

    def now_utc(self) -> datetime:
        return datetime.now(UTC)

    def _rate_limit_wait(self, min_seconds: float) -> None:
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < min_seconds:
            time.sleep(min_seconds - elapsed)

    def get(self, url: str, params: dict | None = None, min_interval: float = 0.8) -> httpx.Response:
        self._rate_limit_wait(min_interval)
        retries = max(1, self.settings.request_max_retries)
        backoff = self.settings.request_backoff_seconds
        last_error: Exception | None = None

        for attempt in range(retries):
            try:
                resp = self._client.get(url, params=params)
                self._last_request_ts = time.monotonic()
                if resp.status_code in {429, 500, 502, 503, 504}:
                    raise httpx.HTTPStatusError(
                        f"Retryable status {resp.status_code}",
                        request=resp.request,
                        response=resp,
                    )
                resp.raise_for_status()
                return resp
            except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                last_error = exc
                if attempt == retries - 1:
                    break
                time.sleep(backoff * (2**attempt))
        raise RuntimeError(f"{self.source_name} request failed: {last_error}") from last_error

    def close(self) -> None:
        self._client.close()

