# -*- coding: utf-8 -*-
"""
bitso_arb.py -- Whale Follower Bot
Monitor de arbitraje Bitso (BTC/MXN) vs Bybit/OKX (BTC/USDT)

Modo actual: solo lectura (monitoreo).
  - Obtiene BTC/MXN de Bitso API publica cada 30s
  - Obtiene tipo de cambio USD/MXN de open.er-api.com cada 5min
  - Calcula BTC_USD = BTC_MXN / USD_MXN
  - Compara vs precio Bybit y OKX
  - Loguea oportunidades cuando spread > 0.3%

No ejecuta ordenes reales hasta que se provean BITSO_API_KEY / BITSO_SECRET.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import aiohttp
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT   = 0.30   # umbral para detectar oportunidad (0.30%)
_TICKER_INTERVAL  = 30     # segundos entre polls de precio Bitso
_FX_INTERVAL      = 300    # segundos entre actualizaciones USD/MXN
_LOG_COOLDOWN     = 60     # minimo entre logs del mismo par (evitar spam)

_BITSO_TICKER_URL = "https://api.bitso.com/v3/ticker/?book=btc_mxn"
_FX_URL           = "https://open.er-api.com/v6/latest/USD"


@dataclass
class BitsoSnapshot:
    btc_usd:        float = 0.0
    usd_mxn:        float = 0.0
    btc_mxn:        float = 0.0
    spread_bybit:   float = 0.0
    spread_okx:     float = 0.0
    opportunities:  int   = 0
    avg_spread_pct: float = 0.0
    active:         bool  = False


class BitsoArb:
    """
    Monitor de oportunidades entre Bitso (BTC/MXN) y exchanges USD.
    Solo monitoreo — sin ejecucion real hasta habilitar API keys.
    """

    def __init__(self) -> None:
        self._btc_mxn:       float = 0.0
        self._usd_mxn:       float = 0.0
        self._btc_usd:       float = 0.0
        self._bybit_price:   float = 0.0
        self._okx_price:     float = 0.0
        self._opportunities: int   = 0
        self._spread_sum:    float = 0.0
        self._spread_count:  int   = 0
        self._last_log:      dict  = {"Bybit": 0.0, "OKX": 0.0}

        logger.info(
            "[bitso_arb] Monitor iniciado | deteccion={}% | poll={}s | fx={}s",
            _MIN_SPREAD_PCT, _TICKER_INTERVAL, _FX_INTERVAL,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Lanza los loops de polling de precio y tipo de cambio en paralelo."""
        await asyncio.gather(
            self._poll_ticker(),
            self._poll_fx(),
        )

    # ── Public price updates (llamadas desde arb_engine) ──────────────────────

    def update_bybit(self, price: float) -> None:
        self._bybit_price = price
        self._check_vs("Bybit", price)

    def update_okx(self, price: float) -> None:
        self._okx_price = price
        self._check_vs("OKX", price)

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> BitsoSnapshot:
        avg    = self._spread_sum / self._spread_count if self._spread_count > 0 else 0.0
        s_byb  = self._spread_vs(self._bybit_price) if self._bybit_price else 0.0
        s_okx  = self._spread_vs(self._okx_price)   if self._okx_price   else 0.0
        return BitsoSnapshot(
            btc_usd        = round(self._btc_usd, 2),
            usd_mxn        = round(self._usd_mxn, 4),
            btc_mxn        = round(self._btc_mxn, 2),
            spread_bybit   = round(s_byb, 4),
            spread_okx     = round(s_okx, 4),
            opportunities  = self._opportunities,
            avg_spread_pct = round(avg, 4),
            active         = self._btc_usd > 0,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _poll_ticker(self) -> None:
        """Poll Bitso BTC/MXN ticker cada _TICKER_INTERVAL segundos."""
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        _BITSO_TICKER_URL,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        data = await resp.json()
                        if data.get("success"):
                            self._btc_mxn = float(data["payload"]["last"])
                            self._recalc()
                        else:
                            logger.warning("[bitso_arb] Ticker respuesta inesperada: {}", data)
            except Exception as exc:
                logger.warning("[bitso_arb] Ticker error: {}", exc)
            await asyncio.sleep(_TICKER_INTERVAL)

    async def _poll_fx(self) -> None:
        """Poll tipo de cambio USD/MXN cada _FX_INTERVAL segundos."""
        while True:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        _FX_URL,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        data = await resp.json()
                        mxn = data.get("rates", {}).get("MXN")
                        if mxn:
                            self._usd_mxn = float(mxn)
                            logger.info(
                                "[bitso_arb] Tipo de cambio: 1 USD = {:.4f} MXN",
                                self._usd_mxn,
                            )
                            self._recalc()
                        else:
                            logger.warning("[bitso_arb] FX sin MXN: {}", data)
            except Exception as exc:
                logger.warning("[bitso_arb] FX error: {}", exc)
            await asyncio.sleep(_FX_INTERVAL)

    def _recalc(self) -> None:
        """Recalcula BTC_USD y compara vs ambos exchanges."""
        if self._usd_mxn > 0 and self._btc_mxn > 0:
            self._btc_usd = self._btc_mxn / self._usd_mxn
            if self._bybit_price > 0:
                self._check_vs("Bybit", self._bybit_price)
            if self._okx_price > 0:
                self._check_vs("OKX", self._okx_price)

    def _spread_vs(self, other_price: float) -> float:
        """Spread % de Bitso vs otro exchange (positivo = Bitso mas caro)."""
        if not self._btc_usd or not other_price:
            return 0.0
        return (self._btc_usd - other_price) / other_price * 100

    def _check_vs(self, exchange: str, price: float) -> None:
        """Evalua y loguea si hay oportunidad vs un exchange dado."""
        if not self._btc_usd or not price:
            return

        spread     = self._spread_vs(price)
        abs_spread = abs(spread)

        if abs_spread < _MIN_SPREAD_PCT:
            return

        now = time.time()
        if now - self._last_log[exchange] < _LOG_COOLDOWN:
            return
        self._last_log[exchange] = now

        self._opportunities  += 1
        self._spread_sum     += abs_spread
        self._spread_count   += 1

        sign = "+" if spread > 0 else ""
        logger.info(
            "[bitso_arb] 📊 Bitso=${:,.0f} {}=${:,.0f} spread={}{:.2f}%"
            "  (MXN={:,.0f} fx={:.2f})",
            self._btc_usd, exchange, price, sign, spread,
            self._btc_mxn, self._usd_mxn,
        )

        if abs_spread >= 5.0:
            logger.info(
                "[bitso_arb] 💰 OPORTUNIDAD ENORME detectada — requiere API keys Bitso"
            )

        try:
            import alerts as _alerts
            _alerts.record_bitso_opportunity(abs_spread)
        except Exception:
            pass
