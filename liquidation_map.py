# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_BYBIT_LIQ_URL = "https://api.bybit.com/v5/market/liquidations"
_REFRESH_INTERVAL = 120    # 2 minutes
_HISTORY_WINDOW = 1800     # 30 minutes
_ZONE_RADIUS = 0.005       # +-0.5%
_MAJOR_ZONE_USD = 5_000_000
_MINOR_ZONE_USD = 1_000_000
_RING_MAXLEN = 5000


# ---------------------------------------------------------------------------
# Internal data structure
# ---------------------------------------------------------------------------

@dataclass
class _LiqEntry:
    timestamp: float
    price: float
    value_usd: float


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class LiquidationMap:
    """Tracks Bybit linear-perpetual liquidation history and identifies
    price zones where significant open interest has been wiped.

    The ring buffer keeps at most ``_RING_MAXLEN`` entries; stale entries
    older than ``_HISTORY_WINDOW`` are ignored at query time (not evicted
    eagerly) to keep the hot path allocation-free.
    """

    def __init__(self) -> None:
        self._buffer: Deque[_LiqEntry] = deque(maxlen=_RING_MAXLEN)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Infinite background loop — poll, sleep, repeat."""
        logger.info("[liquidation_map] background task started, interval={}s", _REFRESH_INTERVAL)
        while True:
            await self._fetch()
            await asyncio.sleep(_REFRESH_INTERVAL)

    def is_zone_active(self, current_price: float) -> tuple[bool, int]:
        """Evaluate whether the area around *current_price* is a hot liquidation zone.

        Returns:
            (True, 15)  — major zone (>$5M liquidated) — adds +15 pts
            (True, 0)   — minor zone ($1M-$5M) — zone active but no bonus pts
            (False, 0)  — no significant zone
        """
        if current_price <= 0:
            return False, 0

        low = current_price * (1.0 - _ZONE_RADIUS)
        high = current_price * (1.0 + _ZONE_RADIUS)
        cutoff = time.time() - _HISTORY_WINDOW

        total_usd = 0.0
        for entry in self._buffer:
            if entry.timestamp < cutoff:
                continue
            if low <= entry.price <= high:
                total_usd += entry.value_usd

        if total_usd > _MAJOR_ZONE_USD:
            logger.debug(
                "[liquidation_map] major zone at price={:.2f} total_usd={:.0f}",
                current_price, total_usd,
            )
            return True, 15
        if total_usd > _MINOR_ZONE_USD:
            logger.debug(
                "[liquidation_map] minor zone at price={:.2f} total_usd={:.0f}",
                current_price, total_usd,
            )
            return True, 0
        return False, 0

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch(self) -> None:
        params = {
            "category": "linear",
            "symbol": "BTCUSDT",
            "limit": 200,
        }
        timeout = aiohttp.ClientTimeout(total=10)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_BYBIT_LIQ_URL, params=params) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            ret_code = data.get("retCode", -1)
            if ret_code != 0:
                raise ValueError(f"Bybit retCode={ret_code} msg={data.get('retMsg')}")

            rows = (
                data.get("result", {}).get("list") or []
            )
            now = time.time()
            cutoff = now - _HISTORY_WINDOW
            added = 0

            for row in rows:
                # Bybit returns timestamps in milliseconds
                ts_ms = float(row.get("updatedTime", 0))
                ts = ts_ms / 1000.0 if ts_ms > 1e10 else ts_ms

                if ts < cutoff:
                    continue

                price = float(row.get("price", 0))
                qty = float(row.get("qty", 0))
                value_usd = price * qty  # approximation; Bybit qty is in contracts (1 BTC)

                if price > 0 and value_usd > 0:
                    self._buffer.append(_LiqEntry(timestamp=ts, price=price, value_usd=value_usd))
                    added += 1

            logger.info(
                "[liquidation_map] fetched {} liquidations, {} added to buffer (buf_size={})",
                len(rows), added, len(self._buffer),
            )

        except Exception as exc:
            logger.warning("[liquidation_map] fetch failed, no zone update: {}", exc)
            # Do not clear the buffer — stale data is better than no data for zone detection
