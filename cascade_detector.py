"""
cascade_detector.py — Whale Follower Bot — Sprint 2
Detector de Stop Cascade en tiempo real.

Una Stop Cascade ocurre cuando una cadena de stops se activa en cascada:
- Precio baja → activa stops → más ventas → precio baja más → más stops...
- Se manifiesta como un burst de trades de venta en muy poco tiempo.
- Señal alcista paradójica: cuando termina la cascade, el precio suele rebotar
  porque el mercado "limpió" todos los stops débiles.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class CascadeEvent:
    """Datos de una cascade detectada."""
    active:         bool    # si hay cascade activa ahora mismo
    intensity:      int     # 0-100
    trades_per_sec: float   # velocidad: trades de venta por segundo
    volume_btc:     float   # volumen total de la cascade en BTC
    duration_secs:  float   # duración de la cascade
    sell_count:     int     # número de trades de venta en la ventana


class CascadeDetector:
    """
    Detector de Stop Cascade tick-a-tick.

    Uso:
        detector = CascadeDetector()
        event = detector.add(side="sell", quantity=0.5, timestamp=time.time())
        if event.active:
            print(f"Cascade: intensity={event.intensity}")
    """

    def __init__(
        self,
        window_secs: int = 5,
        min_trades:  int = 200,
    ) -> None:
        self._window_secs = window_secs
        self._min_trades  = min_trades

        # Ring buffer: (timestamp, quantity) de trades de VENTA
        self._sells: Deque[tuple[float, float]] = deque()

        # Para rastrear inicio de la cascade actual
        self._cascade_start: Optional[float] = None
        self._cascade_volume: float = 0.0

    # ── API pública ────────────────────────────────────────────────────────────

    def add(self, side: str, quantity: float, timestamp: float) -> CascadeEvent:
        """
        Registrar un trade. Devuelve CascadeEvent con el estado actual.
        Solo los trades 'sell' contribuyen a la cascade.
        """
        if side == "sell":
            self._sells.append((timestamp, quantity))

        # Limpiar la ventana deslizante
        cutoff = timestamp - self._window_secs
        while self._sells and self._sells[0][0] < cutoff:
            self._sells.popleft()

        return self._evaluate(timestamp)

    def is_active(self) -> bool:
        """Conveniencia: ¿hay una cascade activa ahora mismo?"""
        now = time.time()
        cutoff = now - self._window_secs
        recent = [(ts, q) for ts, q in self._sells if ts >= cutoff]
        return len(recent) >= self._min_trades

    # ── Internos ───────────────────────────────────────────────────────────────

    def _evaluate(self, now: float) -> CascadeEvent:
        sells = list(self._sells)
        count = len(sells)
        active = count >= self._min_trades

        if not sells:
            return CascadeEvent(
                active=False, intensity=0,
                trades_per_sec=0.0, volume_btc=0.0,
                duration_secs=0.0, sell_count=0,
            )

        volume_btc   = sum(q for _, q in sells)
        earliest_ts  = sells[0][0]
        duration     = max(now - earliest_ts, 0.001)
        trades_per_sec = count / duration

        # Intensidad 0-100:
        # 200 trades/5s = baseline (score 50)
        # 400+ trades/5s = máximo (score 100)
        raw_intensity = min(count / self._min_trades, 2.0)  # 0→2
        intensity = int(raw_intensity * 50)                 # 0→100

        # Rastrear inicio de cascade para duración
        if active and self._cascade_start is None:
            self._cascade_start  = earliest_ts
            self._cascade_volume = 0.0
        if active:
            self._cascade_volume = volume_btc
        else:
            self._cascade_start  = None
            self._cascade_volume = 0.0

        return CascadeEvent(
            active         = active,
            intensity      = min(intensity, 100),
            trades_per_sec = round(trades_per_sec, 1),
            volume_btc     = round(volume_btc, 4),
            duration_secs  = round(duration, 2),
            sell_count     = count,
        )
