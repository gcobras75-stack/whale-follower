# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_WHALE_ALERT_URL = "https://api.whale-alert.io/v1/transactions"
_REFRESH_INTERVAL = 300   # 5 minutes
_TX_WINDOW = 600          # look back 10 minutes

# Exchange names that indicate CEX inflow/outflow
_KNOWN_EXCHANGES: frozenset[str] = frozenset(
    {"binance", "bybit", "coinbase", "kraken", "okx", "huobi", "kucoin", "gate"}
)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------

@dataclass
class OnChainSnapshot:
    signal: str = "neutral"       # "bullish" | "bearish" | "neutral"
    pts: int = 0                  # +10, -10, or 0
    last_tx_usd: float = 0.0
    last_tx_type: str = "unknown" # "exchange_inflow" | "exchange_outflow" | "unknown"
    stale: bool = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _exchange_name(owner_info: dict) -> str:
    """Extract lower-case owner name from a Whale Alert address object."""
    return (owner_info.get("owner_type", "") or "").lower()


def _is_exchange(owner_info: dict) -> bool:
    name = _exchange_name(owner_info)
    return name in _KNOWN_EXCHANGES or "exchange" in name


def _classify_tx(tx: dict) -> tuple[str, str, int]:
    """Return (signal, tx_type, pts) for a single transaction."""
    from_info = tx.get("from", [{}])
    to_info = tx.get("to", [{}])

    # Whale Alert returns lists of address objects
    from_addrs = from_info if isinstance(from_info, list) else [from_info]
    to_addrs = to_info if isinstance(to_info, list) else [to_info]

    to_exchange = any(_is_exchange(a) for a in to_addrs)
    from_exchange = any(_is_exchange(a) for a in from_addrs)

    if to_exchange and not from_exchange:
        return "bearish", "exchange_inflow", -10
    if from_exchange and not to_exchange:
        return "bullish", "exchange_outflow", 10
    return "neutral", "unknown", 0


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class OnChainEngine:
    """Polls Whale Alert API every 5 minutes for large on-chain transactions.

    Degrades gracefully when the API key is absent or the request fails.
    """

    def __init__(self) -> None:
        self._api_key: str = os.environ.get("WHALE_ALERT_KEY", "")
        self._snapshot = OnChainSnapshot()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        if not self._api_key:
            logger.info("[onchain] no WHALE_ALERT_KEY set — running in degraded mode (neutral)")
            return

        logger.info("[onchain] background task started, interval={}s", _REFRESH_INTERVAL)
        while True:
            await self._fetch()
            await asyncio.sleep(_REFRESH_INTERVAL)

    def snapshot(self) -> OnChainSnapshot:
        """Return the latest cached snapshot (never blocks)."""
        return self._snapshot

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch(self) -> None:
        now = time.time()
        params = {
            "api_key": self._api_key,
            "min_value": 10_000_000,
            "limit": 10,
            "start": int(now - _TX_WINDOW),
        }
        timeout = aiohttp.ClientTimeout(total=10)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_WHALE_ALERT_URL, params=params) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            transactions = data.get("transactions") or []
            cutoff = now - _TX_WINDOW

            # Most recent transaction drives the snapshot
            best_signal = "neutral"
            best_pts = 0
            best_type = "unknown"
            best_usd = 0.0
            best_ts = 0.0

            for tx in transactions:
                ts = float(tx.get("timestamp", 0))
                if ts < cutoff:
                    continue
                usd = float(tx.get("amount_usd", 0))
                signal, tx_type, pts = _classify_tx(tx)

                if ts > best_ts:
                    best_ts = ts
                    best_signal = signal
                    best_pts = pts
                    best_type = tx_type
                    best_usd = usd

            self._snapshot = OnChainSnapshot(
                signal=best_signal,
                pts=best_pts,
                last_tx_usd=best_usd,
                last_tx_type=best_type,
                stale=False,
            )
            logger.info(
                "[onchain] signal={} pts={} tx_type={} usd={:.0f}",
                best_signal, best_pts, best_type, best_usd,
            )

        except Exception as exc:
            logger.warning("[onchain] fetch failed, returning neutral snapshot: {}", exc)
            self._snapshot = OnChainSnapshot(stale=True)
