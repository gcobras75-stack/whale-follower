# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import os
import time

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BLOCK_KEYWORDS: list[str] = [
    "Fed", "SEC", "crash", "ban", "hack", "regulation",
    "Trump", "tariff", "ETF", "halving", "whale",
]

_NEWS_URL = "https://newsapi.org/v2/everything"
_REFRESH_INTERVAL = 900    # 15 minutes
_BLOCK_DURATION = 1800     # 30 minutes after a keyword hit
_ARTICLE_WINDOW = 1800     # only look at articles published in the last 30 min


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class NewsFilter:
    """Polls NewsAPI for crypto headlines and blocks trading when a keyword is
    detected.

    Degrades gracefully:
    - No API key  -> always returns (False, "no_key")
    - API error   -> returns (False, "api_error"), never blocks on failure
    """

    def __init__(self) -> None:
        self._api_key: str = os.environ.get("NEWS_API_KEY", "")
        self._blocked_until: float = 0.0
        self._block_reason: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Infinite background loop.  Only polls if an API key is present."""
        if not self._api_key:
            logger.info("[news_filter] no NEWS_API_KEY set — running in degraded mode")
            return  # nothing to poll; is_blocked() returns (False, "no_key")

        logger.info("[news_filter] background task started, interval={}s", _REFRESH_INTERVAL)
        while True:
            await self._fetch()
            await asyncio.sleep(_REFRESH_INTERVAL)

    def is_blocked(self) -> tuple[bool, str]:
        """Return (blocked, reason).  Never raises."""
        if not self._api_key:
            return False, "no_key"

        now = time.time()
        if now < self._blocked_until:
            return True, self._block_reason
        return False, ""

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        params = {
            "q": "bitcoin",
            "sortBy": "publishedAt",
            "pageSize": 20,
        }
        headers = {"X-Api-Key": self._api_key}

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_NEWS_URL, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            articles = data.get("articles", [])
            now = time.time()
            cutoff = now - _ARTICLE_WINDOW

            triggered_keyword: str | None = None
            for article in articles:
                published_at = article.get("publishedAt", "")
                # NewsAPI returns ISO-8601; parse epoch for comparison
                pub_ts = _parse_iso(published_at)
                if pub_ts < cutoff:
                    continue  # article too old

                title = article.get("title") or ""
                for kw in BLOCK_KEYWORDS:
                    if kw.lower() in title.lower():
                        triggered_keyword = kw
                        break
                if triggered_keyword:
                    break

            if triggered_keyword:
                self._blocked_until = now + _BLOCK_DURATION
                self._block_reason = f"noticia: {triggered_keyword}"
                logger.warning(
                    "[news_filter] keyword '{}' detected — blocking for {}s",
                    triggered_keyword, _BLOCK_DURATION,
                )
            else:
                logger.info("[news_filter] no blocking keywords found in recent articles")

        except Exception as exc:
            logger.warning("[news_filter] fetch failed, not blocking: {}", exc)
            # On error: do NOT extend the block; let any existing block expire naturally


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_iso(iso_str: str) -> float:
    """Parse an ISO-8601 string (UTC) to a Unix timestamp.  Returns 0.0 on failure."""
    try:
        import datetime
        # NewsAPI format: "2024-01-15T12:34:56Z"
        dt = datetime.datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
        return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
    except Exception:
        return 0.0
