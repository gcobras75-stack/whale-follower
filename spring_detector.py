"""
spring_detector.py — Whale Follower Bot
Sliding-window Spring pattern detector (Wyckoff-style).

A Spring is detected when the price fakes a breakdown below a support level,
traps sellers, then snaps back — all while smart money absorbs supply (CVD ↑).

Score 0-100.  Signal emitted when score >= SIGNAL_SCORE_THRESHOLD.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from loguru import logger

import config
from aggregator import Trade


# ── Price snapshot ─────────────────────────────────────────────────────────────

@dataclass
class PricePoint:
    price: float
    timestamp: float
    cvd: float
    exchange: str
    volume: float


# ── Spring signal ──────────────────────────────────────────────────────────────

@dataclass
class SpringSignal:
    score: int
    price_entry: float
    stop_loss: float
    take_profit: float
    conditions: Dict[str, bool | float]
    strongest_exchange: str
    cvd_velocity: float
    spring_low: float
    timestamp: float


# ── Per-exchange volume tracker ────────────────────────────────────────────────

class VolumeTracker:
    """Tracks 1-minute rolling volume per exchange."""

    def __init__(self) -> None:
        # exchange → deque of (timestamp, volume)
        self._windows: Dict[str, Deque[Tuple[float, float]]] = {}

    def add(self, exchange: str, volume: float, ts: float) -> None:
        if exchange not in self._windows:
            self._windows[exchange] = deque()
        self._windows[exchange].append((ts, volume))
        self._prune(exchange, ts)

    def total_last_minute(self, ts: float) -> float:
        total = 0.0
        for exc, dq in self._windows.items():
            self._prune(exc, ts)
            total += sum(v for _, v in dq)
        return total

    def strongest(self, ts: float) -> str:
        """Exchange with the most volume in the last minute."""
        best_exc, best_vol = "", 0.0
        for exc, dq in self._windows.items():
            self._prune(exc, ts)
            vol = sum(v for _, v in dq)
            if vol > best_vol:
                best_vol, best_exc = vol, exc
        return best_exc or "unknown"

    def _prune(self, exchange: str, now: float) -> None:
        dq = self._windows[exchange]
        cutoff = now - 60.0
        while dq and dq[0][0] < cutoff:
            dq.popleft()


# ── Spring Detector ────────────────────────────────────────────────────────────

class SpringDetector:
    """
    Receives normalized trades from the Aggregator and detects Spring patterns.

    Call `feed(trade, state)` for every trade; it returns a SpringSignal when
    a qualifying event is detected, otherwise None.
    """

    def __init__(self) -> None:
        # Sliding window: last WINDOW_SECS of price points
        self._window: Deque[PricePoint] = deque()
        self._vol_tracker = VolumeTracker()

        # Rolling 1-minute volume baseline (for multiplier check)
        self._baseline_window: Deque[Tuple[float, float]] = deque()  # (ts, vol)

        # Cooldown: avoid re-firing on the same spring
        self._last_signal_ts: float = 0.0
        self._cooldown_secs: float = 30.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def feed(
        self, trade: Trade, state: Dict
    ) -> Optional[SpringSignal]:
        now = trade.timestamp
        pt = PricePoint(
            price=trade.price,
            timestamp=now,
            cvd=state["cvd"],
            exchange=trade.exchange,
            volume=trade.quantity,
        )
        self._window.append(pt)
        self._vol_tracker.add(trade.exchange, trade.quantity, now)
        self._update_baseline(trade.quantity, now)
        self._prune_window(now)

        if now - self._last_signal_ts < self._cooldown_secs:
            return None
        if len(self._window) < 10:
            return None

        return self._evaluate(now, state)

    # ── Evaluation ─────────────────────────────────────────────────────────────

    def _evaluate(self, now: float, state: Dict) -> Optional[SpringSignal]:
        pts = list(self._window)
        current_price = pts[-1].price

        # ── Condition A: price dropped >= 0.3% in last 10 seconds ─────────────
        drop_window = [p for p in pts if now - p.timestamp <= config.SPRING_DROP_SECS]
        if not drop_window:
            return None
        high_before_drop = max(p.price for p in drop_window)
        spring_low = min(p.price for p in drop_window)
        drop_pct = (high_before_drop - spring_low) / high_before_drop

        cond_a = drop_pct >= config.SPRING_DROP_PCT
        drop_intensity = min(drop_pct / (config.SPRING_DROP_PCT * 2), 1.0)  # 0→1

        # ── Condition B: price bounced >= 0.2% from low in last 5 seconds ─────
        bounce_window = [p for p in pts if now - p.timestamp <= config.SPRING_BOUNCE_SECS]
        bounce_low = min((p.price for p in bounce_window), default=current_price)
        bounce_pct = (current_price - bounce_low) / bounce_low if bounce_low > 0 else 0.0

        cond_b = bounce_pct >= config.SPRING_BOUNCE_PCT
        bounce_intensity = min(bounce_pct / (config.SPRING_BOUNCE_PCT * 2), 1.0)

        # ── Condition C: CVD divergence (CVD rose while price fell) ───────────
        # Compare CVD at the high-before-drop vs now
        if len(drop_window) >= 2:
            cvd_at_high = drop_window[0].cvd    # oldest point in drop window
            cvd_now = state["cvd"]
            cond_c = (cvd_now > cvd_at_high) and cond_a
            cvd_delta = cvd_now - cvd_at_high
            cvd_intensity = min(abs(cvd_delta) / 5.0, 1.0)   # 5 BTC delta → full score
        else:
            cond_c = False
            cvd_intensity = 0.0

        # ── Condition D: combined volume > 1.5x 1-min average ─────────────────
        recent_vol = self._vol_tracker.total_last_minute(now)
        baseline_avg = self._baseline_avg()
        vol_ratio = (recent_vol / baseline_avg) if baseline_avg > 0 else 0.0
        cond_d = vol_ratio >= config.VOLUME_MULTIPLIER
        vol_intensity = min(vol_ratio / (config.VOLUME_MULTIPLIER * 1.5), 1.0)

        # ── Score (0–100) ──────────────────────────────────────────────────────
        # Each condition carries base points; intensity modulates the bonus.
        score = 0
        if cond_a:
            score += 20 + int(10 * drop_intensity)
        if cond_b:
            score += 20 + int(10 * bounce_intensity)
        if cond_c:
            score += 20 + int(10 * cvd_intensity)
        if cond_d:
            score += 10 + int(10 * vol_intensity)

        # Extra: cascade bonus
        if state.get("cascade"):
            score = min(score + 10, 100)

        conditions = {
            "drop_pct": round(drop_pct * 100, 3),
            "bounce_pct": round(bounce_pct * 100, 3),
            "cvd_divergence": cond_c,
            "volume_spike": cond_d,
            "vol_ratio": round(vol_ratio, 2),
            "cascade_detected": bool(state.get("cascade")),
            "cond_a": cond_a,
            "cond_b": cond_b,
            "cond_c": cond_c,
            "cond_d": cond_d,
        }

        if score < config.SIGNAL_SCORE_THRESHOLD:
            return None

        # ── Build signal ───────────────────────────────────────────────────────
        stop_loss   = spring_low * (1 - config.STOP_LOSS_PCT)
        risk        = current_price - stop_loss
        take_profit = current_price + risk * config.RISK_REWARD
        strongest   = self._vol_tracker.strongest(now)

        self._last_signal_ts = now

        signal = SpringSignal(
            score=score,
            price_entry=round(current_price, 2),
            stop_loss=round(stop_loss, 2),
            take_profit=round(take_profit, 2),
            conditions=conditions,
            strongest_exchange=strongest,
            cvd_velocity=round(state.get("cvd_velocity_10s", 0.0), 4),
            spring_low=round(spring_low, 2),
            timestamp=now,
        )
        logger.info(
            f"[spring] SIGNAL score={score} entry={signal.price_entry} "
            f"sl={signal.stop_loss} tp={signal.take_profit}"
        )
        return signal

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _prune_window(self, now: float) -> None:
        cutoff = now - config.WINDOW_SECS
        while self._window and self._window[0].timestamp < cutoff:
            self._window.popleft()

    def _update_baseline(self, volume: float, ts: float) -> None:
        self._baseline_window.append((ts, volume))
        cutoff = ts - 60.0
        while self._baseline_window and self._baseline_window[0][0] < cutoff:
            self._baseline_window.popleft()

    def _baseline_avg(self) -> float:
        if not self._baseline_window:
            return 0.0
        return sum(v for _, v in self._baseline_window) / max(len(self._baseline_window), 1)
