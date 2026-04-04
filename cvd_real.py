"""
cvd_real.py — Whale Follower Bot — Sprint 2
CVD tick-by-tick con reset diario, velocidad y aceleración.

CVD (Cumulative Volume Delta) = suma de (buy_vol - sell_vol) por cada trade.
La clave está en el "aggressor side": el lado que cruza el spread.
- Trade buy  → precio sube → buyer es el aggresor → +cantidad
- Trade sell → precio baja → seller es el agresor → -cantidad
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, Tuple


@dataclass
class CVDSnapshot:
    """Un punto en el historial del CVD."""
    ts: float    # unix timestamp
    cvd: float   # valor acumulado en ese momento


@dataclass
class CVDMetrics:
    """Métricas actuales del CVD para pasar al scoring engine."""
    cumulative: float        # CVD total de la sesión
    velocity_3s: float       # cambio en últimos 3 segundos
    velocity_10s: float      # cambio en últimos 10 segundos
    velocity_30s: float      # cambio en últimos 30 segundos
    acceleration: float      # (velocity_10s - velocity_30s): ¿acelerando o frenando?
    session_delta: float     # delta desde inicio de sesión (reset medianoche UTC)
    buy_volume: float        # volumen comprador acumulado sesión
    sell_volume: float       # volumen vendedor acumulado sesión


class CVDEngine:
    """
    Motor de CVD real tick-a-tick.

    Uso:
        engine = CVDEngine()
        engine.update(side="buy", quantity=0.5, timestamp=time.time())
        metrics = engine.metrics()
    """

    def __init__(self) -> None:
        # Acumuladores de sesión (reset a medianoche UTC)
        self._session_cvd:   float = 0.0
        self._buy_volume:    float = 0.0
        self._sell_volume:   float = 0.0
        self._session_date:  str   = self._today_utc()

        # Ring buffer de snapshots para calcular velocidades
        # maxlen=5000 ≈ 5000 trades, suficiente para ventanas de 30s a 50 trades/s
        self._history: Deque[CVDSnapshot] = deque(maxlen=5000)

        # Velocidad previa para calcular aceleración
        self._prev_velocity_10s: float = 0.0

    # ── API pública ────────────────────────────────────────────────────────────

    def update(self, side: str, quantity: float, timestamp: float) -> None:
        """Registrar un trade. side='buy'|'sell'."""
        # Reset diario a medianoche UTC
        today = self._today_utc()
        if today != self._session_date:
            self._session_cvd  = 0.0
            self._buy_volume   = 0.0
            self._sell_volume  = 0.0
            self._session_date = today

        if side == "buy":
            self._session_cvd += quantity
            self._buy_volume  += quantity
        else:
            self._session_cvd -= quantity
            self._sell_volume += quantity

        self._history.append(CVDSnapshot(ts=timestamp, cvd=self._session_cvd))

    def metrics(self) -> CVDMetrics:
        """Devuelve las métricas actuales del CVD."""
        now = time.time()
        v3  = self._velocity(now, 3.0)
        v10 = self._velocity(now, 10.0)
        v30 = self._velocity(now, 30.0)
        acc = v10 - self._prev_velocity_10s
        self._prev_velocity_10s = v10

        return CVDMetrics(
            cumulative    = self._session_cvd,
            velocity_3s   = round(v3,  4),
            velocity_10s  = round(v10, 4),
            velocity_30s  = round(v30, 4),
            acceleration  = round(acc, 4),
            session_delta = round(self._session_cvd, 4),
            buy_volume    = round(self._buy_volume,  4),
            sell_volume   = round(self._sell_volume, 4),
        )

    # ── Internos ───────────────────────────────────────────────────────────────

    def _velocity(self, now: float, window_secs: float) -> float:
        """CVD change over last window_secs seconds."""
        cutoff = now - window_secs
        baseline: float | None = None
        for snap in self._history:
            if snap.ts >= cutoff:
                baseline = snap.cvd
                break
        if baseline is None:
            return 0.0
        return self._session_cvd - baseline

    @staticmethod
    def _today_utc() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
