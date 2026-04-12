# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FNG_URL = "https://api.alternative.me/fng/"
REFRESH_INTERVAL = 3600  # seconds
_NEUTRAL_VALUE = 50

# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------

@dataclass
class FearGreedSnapshot:
    value: int = _NEUTRAL_VALUE
    label: str = "Neutral"
    multiplier: float = 1.0
    block_long: bool = False
    block_short: bool = False
    stale: bool = True
    last_update: float = field(default_factory=lambda: 0.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify(value: int) -> tuple[str, float, bool, bool]:
    """Return (label, multiplier, block_long, block_short) for a given index value."""
    if value < 10:
        return "Extreme Fear", 0.8, True, False   # solo pánico absoluto bloquea
    if value <= 25:
        return "Fear", 0.8, False, False           # miedo fuerte pero no bloquea (rebote Wyckoff)
    if value <= 45:
        return "Fear", 0.8, False, False
    if value <= 55:
        return "Neutral", 1.0, False, False
    if value <= 75:
        return "Greed", 1.2, False, False
    return "Extreme Greed", 1.2, False, True


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class FearGreedEngine:
    """Fetches the Fear & Greed Index from alternative.me every hour.

    Designed to run as a background asyncio task via ``await engine.run()``.
    The ``snapshot()`` method is always safe to call from any coroutine —
    it never blocks.
    """

    def __init__(self) -> None:
        self._snapshot = FearGreedSnapshot()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Infinite background loop — fetch, sleep, repeat."""
        logger.info("[fear_greed] background task started, interval={}s", REFRESH_INTERVAL)
        while True:
            await self._fetch()
            await asyncio.sleep(REFRESH_INTERVAL)

    def snapshot(self) -> FearGreedSnapshot:
        """Return the latest cached snapshot (never blocks)."""
        return self._snapshot

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(FNG_URL) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            raw_value = int(data["data"][0]["value"])
            label, multiplier, block_long, block_short = _classify(raw_value)

            self._snapshot = FearGreedSnapshot(
                value=raw_value,
                label=label,
                multiplier=multiplier,
                block_long=block_long,
                block_short=block_short,
                stale=False,
                last_update=time.time(),
            )
            logger.info(
                "[fear_greed] value={} label={} multiplier={} block_long={} block_short={}",
                raw_value, label, multiplier, block_long, block_short,
            )

        except Exception as exc:
            logger.warning("[fear_greed] fetch failed, using neutral defaults: {}", exc)
            # Preserve any previously good value if available; mark stale
            prev = self._snapshot
            self._snapshot = FearGreedSnapshot(
                value=prev.value if not prev.stale else _NEUTRAL_VALUE,
                label=prev.label if not prev.stale else "Neutral",
                multiplier=prev.multiplier if not prev.stale else 1.0,
                block_long=False,
                block_short=False,
                stale=True,
                last_update=prev.last_update,
            )
