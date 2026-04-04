# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Tuple

from loguru import logger

# Import Trade from aggregator — tolerate missing module at import time
try:
    from aggregator import Trade
except ImportError:
    from dataclasses import dataclass as _dc

    @_dc
    class Trade:  # type: ignore[no-redef]
        exchange: str
        price: float
        quantity: float
        side: str
        timestamp: float
        pair: str


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EXCHANGE_WEIGHTS: dict[str, float] = {
    "binance": 0.40,
    "bybit": 0.35,
    "okx": 0.25,
}
WINDOW_30S: float = 30.0
WINDOW_10S: float = 10.0
RING_MAXLEN: int = 3000

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

@dataclass
class _TradeEntry:
    timestamp: float
    net: float  # buy_qty - sell_qty (negative for sells)


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------

@dataclass
class CVDCombinedSnapshot:
    all_three_positive: bool = False   # all 3 exchanges have positive 10s velocity -> +20 pts
    two_positive: bool = False         # exactly 2 exchanges positive -> +10 pts
    weighted_velocity: float = 0.0    # weighted sum of 10s velocities
    exchange_velocities: dict = field(default_factory=lambda: {
        "binance": 0.0,
        "bybit": 0.0,
        "okx": 0.0,
    })


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class CVDCombinedEngine:
    """Tracks cumulative volume delta (CVD) across three exchanges.

    Ring buffers (maxlen=3000) hold TradeEntry objects per exchange.
    Velocities are computed over configurable time windows without
    iterating more data than necessary.
    """

    def __init__(self) -> None:
        self._buffers: dict[str, Deque[_TradeEntry]] = {
            ex: deque(maxlen=RING_MAXLEN) for ex in EXCHANGE_WEIGHTS
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, trade: Trade) -> None:
        """Ingest a single trade.  Unknown exchanges are silently ignored."""
        exchange = trade.exchange.lower()
        if exchange not in self._buffers:
            return

        net = trade.quantity if trade.side == "buy" else -trade.quantity
        entry = _TradeEntry(timestamp=trade.timestamp, net=net)
        self._buffers[exchange].append(entry)

    def snapshot(self) -> CVDCombinedSnapshot:
        """Compute and return a point-in-time snapshot."""
        now = time.time()
        velocities: dict[str, float] = {}

        for exchange, buf in self._buffers.items():
            vel_10s = self._velocity(buf, now, WINDOW_10S)
            velocities[exchange] = vel_10s

        positive_count = sum(1 for v in velocities.values() if v > 0)
        all_three = positive_count == 3
        two = positive_count == 2

        weighted_vel = sum(
            velocities[ex] * weight
            for ex, weight in EXCHANGE_WEIGHTS.items()
        )

        snap = CVDCombinedSnapshot(
            all_three_positive=all_three,
            two_positive=two,
            weighted_velocity=round(weighted_vel, 6),
            exchange_velocities={ex: round(v, 6) for ex, v in velocities.items()},
        )

        logger.debug(
            "[cvd_combined] all_three={} two={} weighted_vel={:.4f} vels={}",
            all_three,
            two,
            weighted_vel,
            velocities,
        )
        return snap

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _velocity(buf: Deque[_TradeEntry], now: float, window: float) -> float:
        """Sum net quantity for trades inside the time window."""
        cutoff = now - window
        total = 0.0
        # Iterate from newest to oldest; stop when outside window
        for entry in reversed(buf):
            if entry.timestamp < cutoff:
                break
            total += entry.net
        return total
