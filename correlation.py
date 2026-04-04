# -*- coding: utf-8 -*-
"""
correlation.py -- Whale Follower Bot
Real-time BTC/ETH correlation engine to confirm or diverge spring signals.
"""
from __future__ import annotations

import time
from collections import deque
from typing import Deque, Dict, Tuple

from loguru import logger

# Import Trade from aggregator at runtime to avoid circular deps
try:
    from aggregator import Trade
except Exception:
    from dataclasses import dataclass

    @dataclass
    class Trade:  # type: ignore[no-redef]
        exchange: str = ""
        price: float = 0.0
        quantity: float = 0.0
        side: str = "buy"
        timestamp: float = 0.0
        pair: str = "BTCUSDT"

# ── Constants ─────────────────────────────────────────────────────────────────

_WINDOW_SECS = 10 * 60     # 10-minute history window
_MIN_SAMPLES = 60          # minimum data points per pair to compute signal
_SPRING_VELOCITY_SECS = 30 # look-back for velocity calculation
_BTC_SPRING_THRESHOLD = 0.002  # 0.2% price change in 30s = spring detected


class CorrelationEngine:
    """
    Tracks BTC and ETH prices from incoming trades and signals whether
    ETH is confirming or diverging from a detected BTC spring.
    """

    def __init__(self) -> None:
        # pair -> deque of (timestamp, price)
        self._prices: Dict[str, Deque[Tuple[float, float]]] = {
            "BTCUSDT": deque(maxlen=1000),
            "ETHUSDT": deque(maxlen=1000),
        }

    # ── Public API ─────────────────────────────────────────────────────────────

    def update(self, trade: Trade) -> None:
        """Record a trade price tick (synchronous, called from trade loop)."""
        buf = self._prices.get(trade.pair)
        if buf is not None:
            buf.append((trade.timestamp, trade.price))

    def btc_eth_signal(self) -> Tuple[str, int]:
        """
        Returns ("confirming", +10) | ("diverging", -10) | ("neutral", 0).

        Logic:
        - If BTC spring detected (price_change_pct > 0.2% over last 30s)
          AND ETH velocity > 0 over same window -> "confirming"
        - If BTC spring but ETH velocity < 0 -> "diverging"
        - Otherwise -> "neutral"
        """
        try:
            return self._compute_signal()
        except Exception as exc:
            logger.warning("[correlation] Error computing signal: {}", exc)
            return ("neutral", 0)

    # ── Internal Logic ─────────────────────────────────────────────────────────

    def _compute_signal(self) -> Tuple[str, int]:
        btc_buf = self._prices["BTCUSDT"]
        eth_buf = self._prices["ETHUSDT"]

        now = time.time()
        cutoff_window = now - _WINDOW_SECS
        cutoff_velocity = now - _SPRING_VELOCITY_SECS

        # Filter to 10-minute window for sample count check
        btc_recent = [(ts, px) for ts, px in btc_buf if ts >= cutoff_window]
        eth_recent = [(ts, px) for ts, px in eth_buf if ts >= cutoff_window]

        if len(btc_recent) < _MIN_SAMPLES or len(eth_recent) < _MIN_SAMPLES:
            logger.debug(
                "[correlation] Not enough samples BTC={} ETH={}, returning neutral",
                len(btc_recent), len(eth_recent),
            )
            return ("neutral", 0)

        # Compute velocity over last 30s
        btc_velocity = self._velocity(btc_buf, cutoff_velocity)
        eth_velocity = self._velocity(eth_buf, cutoff_velocity)

        if btc_velocity is None or eth_velocity is None:
            return ("neutral", 0)

        # Check BTC spring condition
        btc_spring = btc_velocity > _BTC_SPRING_THRESHOLD

        if not btc_spring:
            return ("neutral", 0)

        if eth_velocity > 0:
            logger.info(
                "[correlation] Confirming: BTC vel={:.4f} ETH vel={:.4f}",
                btc_velocity, eth_velocity,
            )
            return ("confirming", 10)
        else:
            logger.info(
                "[correlation] Diverging: BTC vel={:.4f} ETH vel={:.4f}",
                btc_velocity, eth_velocity,
            )
            return ("diverging", -10)

    def _velocity(
        self, buf: Deque[Tuple[float, float]], cutoff: float
    ) -> float | None:
        """
        Returns (price_now - price_at_cutoff) / price_at_cutoff.
        Returns None if insufficient data.
        """
        if not buf:
            return None

        # Current price: latest entry
        current_price = buf[-1][1]

        # Reference price: oldest entry still within the velocity window
        ref_entries = [(ts, px) for ts, px in buf if ts >= cutoff]
        if not ref_entries:
            return None

        ref_price = ref_entries[0][1]
        if ref_price == 0.0:
            return None

        return (current_price - ref_price) / ref_price
