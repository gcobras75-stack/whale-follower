# -*- coding: utf-8 -*-
"""
market_regime.py — Whale Follower Bot — Sprint 6
Detector de régimen de mercado en tiempo real.

Regímenes:
  LATERAL       BB estrecho + momentum bajo  → threshold -10, range_trader activo
  TRENDING_UP   BB ancho + momentum +         → threshold +0, spring más fiable
  TRENDING_DOWN BB ancho + momentum -         → threshold +15, evitar longs
  HIGH_VOL      BB muy ancho                  → threshold +20, casi nada pasa

Integración en main.py:
  effective_threshold = config.SIGNAL_SCORE_THRESHOLD + regime.threshold_adjustment(pair)
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

import aiohttp
from loguru import logger

import config


# ── Régimen ───────────────────────────────────────────────────────────────────
class Regime(str, Enum):
    LATERAL       = "LATERAL"
    TRENDING_UP   = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    HIGH_VOL      = "HIGH_VOL"
    UNKNOWN       = "UNKNOWN"


# ── Parámetros por régimen ────────────────────────────────────────────────────
_THRESHOLD_ADJ: Dict[Regime, int] = {
    Regime.LATERAL:       -10,   # más señales, mercado predecible
    Regime.TRENDING_UP:     0,   # neutral — spring sigue bien
    Regime.TRENDING_DOWN:  15,   # evitar longs contra tendencia
    Regime.HIGH_VOL:       20,   # casi nada pasa, mercado impredecible
    Regime.UNKNOWN:         0,
}

# Ajuste de tamaño de posición por régimen (multiplicador sobre size_usd base)
_SIZE_MULT: Dict[Regime, float] = {
    Regime.LATERAL:       0.80,  # posiciones más pequeñas en lateral (menor recorrido)
    Regime.TRENDING_UP:   1.20,  # máximo tamaño cuando trend a favor
    Regime.TRENDING_DOWN: 0.50,  # mínimo, muy selectivo
    Regime.HIGH_VOL:      0.30,  # protección capital
    Regime.UNKNOWN:       1.00,
}

# ── Umbrales de detección ─────────────────────────────────────────────────────
_BB_LATERAL_MAX  = 0.020   # BB width < 2% del precio → lateral
_BB_TRENDING_MIN = 0.035   # BB width > 3.5% → tendencia clara
_BB_HIGH_VOL_MIN = 0.060   # BB width > 6% → alta volatilidad
_MOMENTUM_TREND  = 0.015   # |momentum 20 periodos| > 1.5% → tendencia
_BB_PERIOD       = 20
_RSI_PERIOD      = 14
_SAMPLE_INTERVAL = 15.0    # segundos entre muestras de precio
_MIN_SAMPLES     = 25      # mínimo para clasificar


@dataclass
class RegimeSnapshot:
    regime: Regime = Regime.UNKNOWN
    bb_width_pct: float = 0.0
    momentum_pct: float = 0.0
    rsi: float = 50.0
    threshold_adj: int = 0
    size_multiplier: float = 1.0
    last_update: float = 0.0
    samples: int = 0


class MarketRegimeDetector:
    """
    Actualizado en tiempo real por cada llamada a on_price().
    Internamente hace muestreo cada _SAMPLE_INTERVAL segundos para evitar
    calcular BB/RSI en cada tick.
    """

    def __init__(self) -> None:
        # Buffer de precios muestreados por par
        self._prices: Dict[str, deque] = {}
        self._last_sample: Dict[str, float] = {}
        self._snapshots: Dict[str, RegimeSnapshot] = {}

    # ── API pública ────────────────────────────────────────────────────────────

    def on_price(self, pair: str, price: float) -> None:
        """Llamar en cada tick de precio. Muestrea internamente."""
        now = time.time()
        last = self._last_sample.get(pair, 0.0)
        if now - last < _SAMPLE_INTERVAL:
            return

        if pair not in self._prices:
            self._prices[pair] = deque(maxlen=_BB_PERIOD * 3)
        self._prices[pair].append(price)
        self._last_sample[pair] = now

        if len(self._prices[pair]) >= _MIN_SAMPLES:
            self._classify(pair)

    def regime(self, pair: str) -> Regime:
        return self._snapshots.get(pair, RegimeSnapshot()).regime

    def threshold_adjustment(self, pair: str) -> int:
        return self._snapshots.get(pair, RegimeSnapshot()).threshold_adj

    def size_multiplier(self, pair: str) -> float:
        return self._snapshots.get(pair, RegimeSnapshot()).size_multiplier

    def snapshot(self, pair: str) -> RegimeSnapshot:
        return self._snapshots.get(pair, RegimeSnapshot())

    def all_regimes(self) -> Dict[str, Regime]:
        return {p: s.regime for p, s in self._snapshots.items()}

    # ── Clasificación ─────────────────────────────────────────────────────────

    def _classify(self, pair: str) -> None:
        closes = list(self._prices[pair])
        mid, upper, lower = _bollinger_bands(closes, _BB_PERIOD)
        bb_width = (upper - lower) / mid if mid > 0 else 0.0
        momentum = (closes[-1] - closes[-_BB_PERIOD]) / closes[-_BB_PERIOD] if closes[-_BB_PERIOD] > 0 else 0.0
        rsi = _rsi(closes, _RSI_PERIOD)

        # Clasificar
        if bb_width > _BB_HIGH_VOL_MIN:
            regime = Regime.HIGH_VOL
        elif bb_width > _BB_TRENDING_MIN:
            if momentum > _MOMENTUM_TREND:
                regime = Regime.TRENDING_UP
            elif momentum < -_MOMENTUM_TREND:
                regime = Regime.TRENDING_DOWN
            else:
                regime = Regime.TRENDING_UP if momentum >= 0 else Regime.TRENDING_DOWN
        elif bb_width <= _BB_LATERAL_MAX:
            regime = Regime.LATERAL
        else:
            # Zona intermedia: usar momentum para decidir
            if abs(momentum) > _MOMENTUM_TREND:
                regime = Regime.TRENDING_UP if momentum > 0 else Regime.TRENDING_DOWN
            else:
                regime = Regime.LATERAL

        prev = self._snapshots.get(pair)
        snap = RegimeSnapshot(
            regime          = regime,
            bb_width_pct    = round(bb_width * 100, 3),
            momentum_pct    = round(momentum * 100, 3),
            rsi             = round(rsi, 1),
            threshold_adj   = _THRESHOLD_ADJ[regime],
            size_multiplier = _SIZE_MULT[regime],
            last_update     = time.time(),
            samples         = len(closes),
        )
        self._snapshots[pair] = snap

        # Log + alerta Telegram cuando cambia el régimen
        if prev is None or prev.regime != regime:
            old_label = prev.regime.value if prev else "UNKNOWN"
            logger.info(
                "[regime] {} → {} | BB_width={:.2f}% momentum={:+.2f}% RSI={:.0f} "
                "threshold_adj={:+d} size_mult={:.1f}x",
                pair, regime.value,
                snap.bb_width_pct, snap.momentum_pct, snap.rsi,
                snap.threshold_adj, snap.size_multiplier,
            )
            asyncio.create_task(
                self._alert_regime_change(pair, old_label, regime.value)
            )

    async def _alert_regime_change(self, pair: str, old: str, new: str) -> None:
        """Envia cambio de régimen a Telegram con los umbrales spring actualizados."""
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        old_p = config.SPRING_PARAMS.get(old, config.SPRING_PARAMS["LATERAL"])
        new_p = config.SPRING_PARAMS.get(new, config.SPRING_PARAMS["LATERAL"])

        msg = (
            f"\U0001f4ca R\u00e9gimen cambiado: {pair}\n"
            f"{old} \u2192 {new}\n"
            f"Umbrales spring ajustados:\n"
            f"  Drop:   {old_p['drop_pct']*100:.2f}% \u2192 {new_p['drop_pct']*100:.2f}%\n"
            f"  Bounce: {old_p['bounce_pct']*100:.2f}% \u2192 {new_p['bounce_pct']*100:.2f}%\n"
            f"  Vol:    {old_p['vol_mult']:.2f}x \u2192 {new_p['vol_mult']:.2f}x\n"
            f"  Score m\u00edn: {old_p['score_min']} \u2192 {new_p['score_min']}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception:
            pass


# ── Indicadores técnicos ───────────────────────────────────────────────────────

def _bollinger_bands(
    closes: list[float], period: int = 20, std_mult: float = 2.0
) -> tuple[float, float, float]:
    """Retorna (mid, upper, lower)."""
    if len(closes) < period:
        mid = closes[-1] if closes else 0.0
        return mid, mid * 1.02, mid * 0.98
    window = closes[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = variance ** 0.5
    return mid, mid + std_mult * std, mid - std_mult * std


def _rsi(closes: list[float], period: int = 14) -> float:
    """RSI Wilder. Retorna 50.0 si no hay suficientes datos."""
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(len(closes) - period, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
