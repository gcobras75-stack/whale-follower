# -*- coding: utf-8 -*-
"""
liquidations_global.py -- Whale Follower Bot
Termometro 2: Liquidaciones globales via Binance Futures (sin API key).

Usa endpoints publicos de Binance fapi:
  - Open Interest (BTCUSDT) para estimar magnitud
  - Long/Short account ratio para determinar direccion
  - Precio actual para convertir BTC → USD

Poll: cada 2 minutos.
Logica:
  Liq LONG  > $50M en 1h → bloquear senales LONG 15 minutos
  Liq SHORT > $50M en 1h → +10pts scoring (rebote probable)
  Normal < $20M          → sin ajuste
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass

import aiohttp
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
_OI_URL      = "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"
_PRICE_URL   = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"
_LS_URL      = ("https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
                "?symbol=BTCUSDT&period=5m&limit=12")

_POLL_SECS      = 120     # 2 minutos
_LIQ_LONG_BLOCK = 50e6    # $50M liq LONG  → bloquear LONGS
_LIQ_SHORT_BOOST = 50e6   # $50M liq SHORT → +10pts rebote
_NORMAL_MAX      = 20e6   # < $20M → normal
_BLOCK_SECS      = 900    # 15 minutos de bloqueo


@dataclass
class LiqSnapshot:
    liq_long_m:    float = 0.0    # USD millones liquidados LONG en ~1h (estimado)
    liq_short_m:   float = 0.0    # USD millones liquidados SHORT en ~1h (estimado)
    signal:        str   = "normal"
    long_blocked:  bool  = False
    oi_usd_b:      float = 0.0    # Open Interest en USD billions


class LiquidationsGlobal:
    """
    Estima liquidaciones globales de BTC Futures a partir de cambios en OI
    y variacion del ratio long/short de cuentas en Binance.

    Nota: son estimaciones (Binance no expone el dato exacto sin API key).
    La logica de bloqueo/boost se basa en umbrales conservadores.
    Solo lectura — no ejecuta ordenes.
    """

    def __init__(self) -> None:
        self._oi_btc:       float = 0.0
        self._price:        float = 0.0
        self._oi_usd:       float = 0.0
        self._liq_long:     float = 0.0    # acumulado 1h (decae)
        self._liq_short:    float = 0.0    # acumulado 1h (decae)
        self._signal:       str   = "normal"
        self._block_until:  float = 0.0
        self._long_pct_hist: deque = deque(maxlen=12)   # ultimas 12 lecturas

        logger.info(
            "[liq_global] Monitor iniciado | poll={}s | long_block>${:.0f}M"
            " | short_boost>${:.0f}M | block_dur={}min",
            _POLL_SECS,
            _LIQ_LONG_BLOCK / 1e6,
            _LIQ_SHORT_BOOST / 1e6,
            _BLOCK_SECS // 60,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        while True:
            await self._fetch()
            await asyncio.sleep(_POLL_SECS)

    # ── Public API ─────────────────────────────────────────────────────────────

    def is_long_blocked(self) -> bool:
        """True si hubo liquidaciones LONG masivas recientes."""
        return time.time() < self._block_until

    def adjustment(self) -> int:
        """+10 si liquidaciones SHORT masivas (rebote probable), 0 en caso contrario."""
        if self._liq_short >= _LIQ_SHORT_BOOST:
            return +10
        return 0

    def snapshot(self) -> LiqSnapshot:
        return LiqSnapshot(
            liq_long_m   = round(self._liq_long  / 1e6, 2),
            liq_short_m  = round(self._liq_short / 1e6, 2),
            signal       = self._signal,
            long_blocked = self.is_long_blocked(),
            oi_usd_b     = round(self._oi_usd / 1e9, 2),
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch(self) -> None:
        try:
            async with aiohttp.ClientSession() as s:
                oi_r, px_r, ls_r = await asyncio.gather(
                    s.get(_OI_URL,    timeout=aiohttp.ClientTimeout(total=10)),
                    s.get(_PRICE_URL, timeout=aiohttp.ClientTimeout(total=10)),
                    s.get(_LS_URL,    timeout=aiohttp.ClientTimeout(total=10)),
                    return_exceptions=True,
                )

                # Open Interest
                if isinstance(oi_r, Exception) or isinstance(px_r, Exception):
                    logger.warning("[liq_global] OI/price fetch error")
                    return

                oi_data = await oi_r.json()
                px_data = await px_r.json()

                new_oi    = float(oi_data["openInterest"])
                new_price = float(px_data["price"])
                new_oi_usd = new_oi * new_price

                # Estimar liquidaciones desde cambio de OI
                if self._oi_btc > 0 and self._price > 0:
                    prev_oi_usd = self._oi_btc * self._price
                    oi_drop     = prev_oi_usd - new_oi_usd
                    if oi_drop > 1e6:   # drop > $1M = evento relevante
                        price_change = new_price - self._price
                        if price_change < 0:
                            self._liq_long  += oi_drop   # caida precio → liq LONG
                        else:
                            self._liq_short += oi_drop   # suba precio  → liq SHORT

                self._oi_btc  = new_oi
                self._price   = new_price
                self._oi_usd  = new_oi_usd

                # Long/Short ratio para info adicional
                if not isinstance(ls_r, Exception):
                    try:
                        ls_data = await ls_r.json()
                        if ls_data and isinstance(ls_data, list):
                            long_pcts = [float(d.get("longAccount", 0.5)) for d in ls_data]
                            self._long_pct_hist.extend(long_pcts)
                    except Exception:
                        pass

                # Decaimiento progresivo de acumulados (vida media ~40min)
                self._liq_long  *= 0.92
                self._liq_short *= 0.92

                self._evaluate()

        except Exception as exc:
            logger.warning("[liq_global] Fetch error: {} — reintento en {}s", exc, _POLL_SECS)

    def _evaluate(self) -> None:
        ll = self._liq_long
        ls = self._liq_short

        if ll >= _LIQ_LONG_BLOCK:
            self._signal      = "long_liq"
            self._block_until = time.time() + _BLOCK_SECS
            logger.info(
                "[liq_global] 🚨 Liq LONG=${:.1f}M → caída fuerte, bloqueando LONGS {}min",
                ll / 1e6, _BLOCK_SECS // 60,
            )
        elif ls >= _LIQ_SHORT_BOOST:
            self._signal = "short_liq"
            logger.info(
                "[liq_global] 🚨 Liq SHORT=${:.1f}M → rebote probable +10pts",
                ls / 1e6,
            )
        else:
            self._signal = "normal"
            logger.info(
                "[liq_global] 📊 Liq LONG=${:.1f}M Liq SHORT=${:.1f}M → normal",
                ll / 1e6, ls / 1e6,
            )

        try:
            import alerts as _alerts
            snap = self.snapshot()
            _alerts.update_thermometers(
                liq_long_m  = snap.liq_long_m,
                liq_short_m = snap.liq_short_m,
                liq_signal  = self._signal,
            )
        except Exception:
            pass
