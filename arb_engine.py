# -*- coding: utf-8 -*-
"""
arb_engine.py -- Whale Follower Bot
Orchestrador de todas las estrategias de arbitraje.

Estrategias integradas:
  1. Funding Rate Arb  (funding_arb.py)     — delta-neutral, 8h payments
  2. Cross-Exchange Arb (cross_exchange_arb.py) — spread Bybit vs OKX
  3. Lead-Lag Arb      (lead_lag_arb.py)    — BTC spring -> altcoins
  4. Triangular Arb    (triangular_arb.py)  — BTC/USDT × ETH/BTC vs ETH/USDT

El ArbEngine:
  - Instancia todos los motores con el mismo flag production
  - Recibe precio ticks desde el aggregator y los enruta
  - Recibe seniales BTC spring desde main.py
  - Expone snapshot unificado para healthcheck y dashboard
  - Persiste trades en Supabase tabla arb_trades
  - Envia resumen diario via Telegram
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, asdict
from typing import Optional

import aiohttp
from loguru import logger

from funding_arb       import FundingArbEngine
# from cross_exchange_arb import CrossExchangeArb  # DESHABILITADO — ENABLE_CROSS_ARB=false
# from lead_lag_arb      import LeadLagArb         # DESHABILITADO — ENABLE_LEAD_LAG_ARB=false
from triangular_arb    import TriangularArb
from bitso_arb         import BitsoArb

# DESHABILITADO — ENABLE_CROSS_ARB=false por defecto (ver .env para activar)
ARB_ENGINE_ENABLED = False


@dataclass
class ArbSummary:
    funding_rate_pct:     float = 0.0
    funding_open_trades:  int   = 0
    funding_pnl_usd:      float = 0.0

    cross_opps_1h:        int   = 0
    cross_trades_total:   int   = 0
    cross_pnl_usd:        float = 0.0
    cross_best_pair:      str   = ""
    cross_best_spread:    float = 0.0

    lead_active_trades:   int   = 0
    lead_triggers_1h:     int   = 0
    lead_pnl_usd:         float = 0.0

    tri_spread_pct:       float = 0.0
    tri_opps_1h:          int   = 0
    tri_pnl_usd:          float = 0.0

    bitso_btc_usd:        float = 0.0
    bitso_usd_mxn:        float = 0.0
    bitso_opps:           int   = 0
    bitso_avg_spread:     float = 0.0

    total_pnl_usd:        float = 0.0


class ArbEngine:
    """
    Motor unificado de arbitraje para Whale Follower Bot.

    Uso en main.py:
        arb = ArbEngine(production=False)
        asyncio.create_task(arb.run())

        # En el loop de trades:
        arb.on_trade(exchange, pair, price, ts_ms)

        # En spring detection:
        arb.on_btc_spring(btc_price)

        # Para healthcheck:
        summary = arb.summary()
    """

    def __init__(self, production: bool = False, cross_arb_real: bool = False) -> None:
        if not ARB_ENGINE_ENABLED:
            logger.info("[arb_engine] DESHABILITADO (ARB_ENGINE_ENABLED=False) — no se instancia nada")
            self._production = False
            self._funding = self._cross = self._lead_lag = self._triangular = self._bitso = None
            return
        self._production   = production
        cross_prod = production or cross_arb_real
        self._funding      = FundingArbEngine(production=production)
        self._cross        = None  # CrossExchangeArb deshabilitado
        self._lead_lag     = None  # LeadLagArb deshabilitado
        self._triangular   = TriangularArb(production=cross_prod)
        self._bitso        = BitsoArb()

        mode = "REAL" if production else "PAPEL"
        cross_mode = "REAL" if cross_prod else "PAPEL"
        logger.info("[arb_engine] Iniciado en modo {} con 5 estrategias (cross_arb={}, bitso=monitor)", mode, cross_mode)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tarea de fondo: arranca todas las subtareas de arbitraje."""
        if not ARB_ENGINE_ENABLED:
            logger.info("[arb_engine] DESHABILITADO (ARB_ENGINE_ENABLED=False) — no iniciando subtareas")
            return
        asyncio.create_task(self._triangular.startup_check(), name="cross_arb_startup")
        tasks = [self._funding.run(), self._bitso.run(), self._periodic_summary()]
        if self._lead_lag is not None:
            tasks.append(self._lead_lag.run())
        await asyncio.gather(*tasks)

    # ── Price routing ─────────────────────────────────────────────────────────

    def on_trade(self, exchange: str, pair: str, price: float, ts_ms: float) -> None:
        """
        Enrutar cada trade tick recibido del aggregator.
        Llamar con cada Trade object que llega del stream.
        """
        if not ARB_ENGINE_ENABLED:
            return
        # Cross-exchange: necesita precios por exchange
        self._cross.update_price(exchange, pair, price, ts_ms)

        # Lead-Lag: necesita precios de altcoins
        if pair in ("ETHUSDT", "SOLUSDT", "BNBUSDT"):
            self._lead_lag.update_price(pair, price)

        # Triangular cross-exchange: rutear por exchange para los 3 pares
        is_bybit = exchange in ("bybit", "bybit_spot", "")
        is_okx   = exchange in ("okx", "okx_spot")

        if pair == "BTCUSDT":
            if is_bybit or (not is_okx):
                self._triangular.update_btc_usdt(price)
                self._bitso.update_bybit(price)
            else:
                self._triangular.update_btc_usdt_okx(price)
                self._bitso.update_okx(price)
        elif pair == "ETHUSDT":
            if is_okx:
                self._triangular.update_eth_usdt_okx(price)
            else:
                self._triangular.update_eth_usdt(price)
        elif pair == "SOLUSDT":
            if is_okx:
                self._triangular.update_sol_usdt_okx(price)
            else:
                self._triangular.update_sol_usdt(price)

    def on_eth_btc_price(self, price: float) -> None:
        """
        Reservado por compatibilidad — la estrategia triangular ahora usa
        cross-exchange BTC/USDT (Bybit vs OKX), no requiere ETH/BTC.
        """
        self._triangular.update_eth_btc(price)

    # ── Spring signal ─────────────────────────────────────────────────────────

    def on_btc_spring(self, btc_price: float) -> None:
        """
        Llamar cuando el spring detector de BTC emite senal.
        Activa el lead-lag arb en altcoins.
        """
        if not ARB_ENGINE_ENABLED or self._lead_lag is None:
            return
        self._lead_lag.on_btc_spring(btc_price)

    # ── Snapshot ──────────────────────────────────────────────────────────────

    def summary(self) -> ArbSummary:
        if not ARB_ENGINE_ENABLED:
            return ArbSummary()
        f  = self._funding.snapshot()   if self._funding   is not None else None
        c  = self._cross.snapshot()     if self._cross     is not None else None
        ll = self._lead_lag.snapshot()  if self._lead_lag  is not None else None
        t  = self._triangular.snapshot() if self._triangular is not None else None
        b  = self._bitso.snapshot()     if self._bitso     is not None else None

        total_pnl = f.total_pnl_usd + c.pnl_total_usd + ll.pnl_total_usd + t.pnl_total_usd

        return ArbSummary(
            funding_rate_pct    = f.rate_pct,
            funding_open_trades = f.open_trades,
            funding_pnl_usd     = f.total_pnl_usd,

            cross_opps_1h       = c.opportunities_1h,
            cross_trades_total  = c.trades_total,
            cross_pnl_usd       = c.pnl_total_usd,
            cross_best_pair     = c.best_spread_pair,
            cross_best_spread   = c.best_spread_pct,

            lead_active_trades  = ll.active_trades,
            lead_triggers_1h    = ll.triggers_1h,
            lead_pnl_usd        = ll.pnl_total_usd,

            tri_spread_pct      = t.spread_pct,
            tri_opps_1h         = t.opportunities_1h,
            tri_pnl_usd         = t.pnl_total_usd,

            bitso_btc_usd       = b.btc_usd,
            bitso_usd_mxn       = b.usd_mxn,
            bitso_opps          = b.opportunities,
            bitso_avg_spread    = b.avg_spread_pct,

            total_pnl_usd       = round(total_pnl, 4),
        )

    def detailed_summary(self) -> dict:
        """Para el comando Telegram /arb."""
        s = self.summary()
        return {
            "funding":   {
                "rate_pct":    s.funding_rate_pct,
                "open_trades": s.funding_open_trades,
                "pnl_usd":     s.funding_pnl_usd,
                "positions":   self._funding.active_summary(),
            },
            "cross_exchange": {
                "opps_1h":     s.cross_opps_1h,
                "trades":      s.cross_trades_total,
                "pnl_usd":     s.cross_pnl_usd,
                "best_pair":   s.cross_best_pair,
                "best_spread": s.cross_best_spread,
                "recent":      self._cross.active_summary(),
            },
            "lead_lag": {
                "active":       s.lead_active_trades,
                "triggers_1h":  s.lead_triggers_1h,
                "pnl_usd":      s.lead_pnl_usd,
                "recent":       self._lead_lag.active_summary(),
            },
            "triangular": {
                "spread_pct":   s.tri_spread_pct,
                "opps_1h":      s.tri_opps_1h,
                "pnl_usd":      s.tri_pnl_usd,
                "recent":       self._triangular.active_summary(),
            },
            "bitso": {
                "btc_usd":      s.bitso_btc_usd,
                "usd_mxn":      s.bitso_usd_mxn,
                "opportunities": s.bitso_opps,
                "avg_spread":   s.bitso_avg_spread,
                "snapshot":     self._bitso.snapshot().__dict__,
            },
            "total_pnl_usd": s.total_pnl_usd,
        }

    # ── Periodic Telegram summary ─────────────────────────────────────────────

    async def _periodic_summary(self) -> None:
        """Envia resumen de arb cada 6 horas por Telegram."""
        await asyncio.sleep(3600)   # primera vez despues de 1h
        while True:
            await self._send_summary()
            await asyncio.sleep(6 * 3600)

    async def _send_summary(self) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        s    = self.summary()
        mode = "REAL" if self._production else "PAPEL"
        msg  = (
            f"📊 [ARB ENGINE] Resumen 6h ({mode})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Funding Rate: {s.funding_rate_pct:+.4f}% | "
            f"Trades: {s.funding_open_trades} | P&L: ${s.funding_pnl_usd:.4f}\n"
            f"⚡ Cross-Exch:  Opps 1h: {s.cross_opps_1h} | "
            f"P&L: ${s.cross_pnl_usd:.4f}\n"
            f"🔗 Lead-Lag:    Triggers: {s.lead_triggers_1h}/h | "
            f"P&L: ${s.lead_pnl_usd:.4f}\n"
            f"🔺 Triangular:  Spread: {s.tri_spread_pct:.4f}% | "
            f"P&L: ${s.tri_pnl_usd:.4f}\n"
            f"🇲🇽 Bitso:       BTC=${s.bitso_btc_usd:,.0f} | "
            f"Opps: {s.bitso_opps} | Spread prom: {s.bitso_avg_spread:.2f}%\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💵 Total P&L: ${s.total_pnl_usd:.4f}"
        )
        try:
            async with aiohttp.ClientSession() as sess:
                await sess.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception as exc:
            logger.warning("[arb_engine] Telegram summary error: {}", exc)
