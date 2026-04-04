"""
spring_detector.py — Whale Follower Bot — Sprint 2
Detecta el patrón Spring de Wyckoff en la ventana deslizante de precios.
En Sprint 2 devuelve spring_data dict que el ScoringEngine procesa.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

from loguru import logger

import config
from aggregator import Trade


@dataclass
class PricePoint:
    price:     float
    timestamp: float
    cvd:       float
    exchange:  str
    volume:    float


class VolumeTracker:
    """Volumen rodante por exchange (1 minuto)."""

    def __init__(self) -> None:
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
        best_exc, best_vol = "", 0.0
        for exc, dq in self._windows.items():
            self._prune(exc, ts)
            vol = sum(v for _, v in dq)
            if vol > best_vol:
                best_vol, best_exc = vol, exc
        return best_exc or "unknown"

    def _prune(self, exchange: str, now: float) -> None:
        dq     = self._windows[exchange]
        cutoff = now - 60.0
        while dq and dq[0][0] < cutoff:
            dq.popleft()


class SpringDetector:
    """
    Detecta el patrón Spring (caída + rebote + divergencia CVD).
    Devuelve un dict con los datos crudos del spring para que el
    ScoringEngine calcule el score final.
    """

    def __init__(self) -> None:
        self._window:   Deque[PricePoint] = deque()
        self._vol_tracker                  = VolumeTracker()
        self._baseline: Deque[Tuple[float, float]] = deque()

        self._last_signal_ts:  float = 0.0
        self._cooldown_secs:   float = 30.0

    # ── API pública ────────────────────────────────────────────────────────────

    def feed(
        self, trade: Trade, cvd_value: float
    ) -> Optional[Dict]:
        """
        Alimenta un trade. Devuelve spring_data dict si detecta patrón,
        o None si no hay patrón o está en cooldown.
        """
        now = trade.timestamp
        pt  = PricePoint(
            price     = trade.price,
            timestamp = now,
            cvd       = cvd_value,
            exchange  = trade.exchange,
            volume    = trade.quantity,
        )
        self._window.append(pt)
        self._vol_tracker.add(trade.exchange, trade.quantity, now)
        self._update_baseline(trade.quantity, now)
        self._prune_window(now)

        if now - self._last_signal_ts < self._cooldown_secs:
            return None
        if len(self._window) < 10:
            return None

        return self._evaluate(now, cvd_value)

    def strongest_exchange(self, ts: float) -> str:
        return self._vol_tracker.strongest(ts)

    def current_price(self) -> float:
        if self._window:
            return self._window[-1].price
        return 0.0

    # ── Evaluación ─────────────────────────────────────────────────────────────

    def _evaluate(self, now: float, cvd_now: float) -> Optional[Dict]:
        pts           = list(self._window)
        current_price = pts[-1].price

        # Condición A: caída >= 0.3% en ≤ 10 segundos
        drop_window     = [p for p in pts if now - p.timestamp <= config.SPRING_DROP_SECS]
        if not drop_window:
            return None
        high_before     = max(p.price for p in drop_window)
        spring_low      = min(p.price for p in drop_window)
        drop_pct        = (high_before - spring_low) / high_before
        cond_a          = drop_pct >= config.SPRING_DROP_PCT
        drop_intensity  = min(drop_pct / (config.SPRING_DROP_PCT * 2), 1.0)

        # Condición B: rebote >= 0.2% desde el mínimo en ≤ 5 segundos
        bounce_window   = [p for p in pts if now - p.timestamp <= config.SPRING_BOUNCE_SECS]
        bounce_low      = min((p.price for p in bounce_window), default=current_price)
        bounce_pct      = (current_price - bounce_low) / bounce_low if bounce_low > 0 else 0.0
        cond_b          = bounce_pct >= config.SPRING_BOUNCE_PCT
        bounce_intensity = min(bounce_pct / (config.SPRING_BOUNCE_PCT * 2), 1.0)

        # Condición C: CVD sube mientras precio baja (divergencia)
        if len(drop_window) >= 2:
            cvd_at_high = drop_window[0].cvd
            cond_c      = (cvd_now > cvd_at_high) and cond_a
        else:
            cond_c = False

        # Condición D: volumen combinado > 1.5x promedio
        recent_vol    = self._vol_tracker.total_last_minute(now)
        baseline_avg  = self._baseline_avg()
        vol_ratio     = (recent_vol / baseline_avg) if baseline_avg > 0 else 0.0
        cond_d        = vol_ratio >= config.VOLUME_MULTIPLIER

        # Si ninguna condición primaria → ignorar
        if not cond_a and not cond_b:
            return None

        self._last_signal_ts = now

        return {
            "cond_a":          cond_a,
            "cond_b":          cond_b,
            "cond_c":          cond_c,
            "cond_d":          cond_d,
            "drop_pct":        round(drop_pct * 100, 3),
            "bounce_pct":      round(bounce_pct * 100, 3),
            "drop_intensity":  drop_intensity,
            "bounce_intensity": bounce_intensity,
            "spring_low":      round(spring_low, 2),
            "current_price":   round(current_price, 2),
            "vol_ratio":       round(vol_ratio, 2),
            "strongest_exchange": self._vol_tracker.strongest(now),
        }

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _prune_window(self, now: float) -> None:
        cutoff = now - config.WINDOW_SECS
        while self._window and self._window[0].timestamp < cutoff:
            self._window.popleft()

    def _update_baseline(self, volume: float, ts: float) -> None:
        self._baseline.append((ts, volume))
        cutoff = ts - 60.0
        while self._baseline and self._baseline[0][0] < cutoff:
            self._baseline.popleft()

    def _baseline_avg(self) -> float:
        if not self._baseline:
            return 0.0
        return sum(v for _, v in self._baseline) / max(len(self._baseline), 1)
