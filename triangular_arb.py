# -*- coding: utf-8 -*-
"""
triangular_arb.py -- Whale Follower Bot
Arbitraje Triangular: BTC/USDT × ETH/BTC ≠ ETH/USDT

Estrategia:
  Con los precios en tiempo real de BTC y ETH (ambos en USDT),
  se calcula el tipo implicito ETH/BTC = ETH_USDT / BTC_USDT.

  Si el tipo real en Bybit difiere del tipo implicito por > 0.05%
  (despues de fees 0.02% x 3 patas = 0.06%):
    Triangulo A (implicito < real):
      1. Comprar ETH con USDT   (ETHUSDT)
      2. Vender ETH por BTC     (ETHBTC)
      3. Vender BTC por USDT    (BTCUSDT)
    Triangulo B (implicito > real):
      Ruta inversa

  En la practica capturamos la diferencia entre el tipo implicito
  y el tipo observable directamente desde los WebSockets.

En papel: simula las tres patas y registra spread capturado.
En real:  ejecuta ordenes simultaneas en Bybit (tres pares).
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT    = 0.06    # > fees (0.02% x 3 patas = 0.06%)
_MAX_SPREAD_PCT    = 1.5     # spread > 1.5% = dato anomalo
_POSITION_SIZE_USD = 300.0   # USD por ciclo triangular en papel
_TAKER_FEE_PCT     = 0.02    # fee por pata (Bybit taker)
_COOLDOWN_SECS     = 10      # cooldown entre arbs del mismo triangulo
_PRICE_STALE_MS    = 2000    # datos stale si > 2s


@dataclass
class TriPrice:
    btc_usdt:  float = 0.0   # BTC/USDT (eg. 65000)
    eth_usdt:  float = 0.0   # ETH/USDT (eg. 3000)
    eth_btc:   float = 0.0   # ETH/BTC real (eg. 0.04615) — desde WebSocket directo
    ts_ms:     float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class TriArbTrade:
    trade_id:       str
    route:          str       # "A" | "B"
    btc_usdt:       float
    eth_usdt:       float
    eth_btc_real:   float
    eth_btc_impl:   float
    spread_pct:     float
    size_usd:       float
    gross_pnl:      float
    net_pnl:        float
    opened_at:      float = field(default_factory=time.time)
    production:     bool  = False


@dataclass
class TriArbSnapshot:
    eth_btc_implied:   float = 0.0
    eth_btc_real:      float = 0.0
    spread_pct:        float = 0.0
    opportunities_1h:  int   = 0
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    stale:             bool  = True


class TriangularArb:
    """
    Detecta arbitraje triangular usando precios BTC/USDT, ETH/USDT y ETH/BTC
    en tiempo real desde el aggregator y WebSocket Bybit.

    Uso:
        arb = TriangularArb(production=False)
        arb.update_btc_usdt(65000.0)
        arb.update_eth_usdt(3000.0)
        arb.update_eth_btc(0.04550)  # precio real ETH/BTC desde Bybit
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._prices: TriPrice = TriPrice()
        self._trades: List[TriArbTrade] = []
        self._opportunities: Deque[float] = deque(maxlen=500)
        self._last_check: float = 0.0

        mode = "REAL" if production else "PAPEL"
        logger.info("[tri_arb] Iniciado en modo {} | min_spread={}%", mode, _MIN_SPREAD_PCT)

    # ── Public API ─────────────────────────────────────────────────────────────

    def update_btc_usdt(self, price: float) -> None:
        self._prices.btc_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_arb()

    def update_eth_usdt(self, price: float) -> None:
        self._prices.eth_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_arb()

    def update_eth_btc(self, price: float) -> None:
        """Precio ETH/BTC desde mercado spot (distinto del implicito)."""
        self._prices.eth_btc = price
        self._prices.ts_ms   = time.time() * 1000
        self._check_arb()

    def snapshot(self) -> TriArbSnapshot:
        now    = time.time()
        p      = self._prices
        impl   = self._implied_eth_btc()
        spread = self._spread_pct(impl, p.eth_btc) if (impl and p.eth_btc) else 0.0
        stale  = (time.time() * 1000 - p.ts_ms) > _PRICE_STALE_MS
        ops_1h = sum(1 for t in self._opportunities if now - t < 3600)
        return TriArbSnapshot(
            eth_btc_implied  = round(impl, 8) if impl else 0.0,
            eth_btc_real     = round(p.eth_btc, 8),
            spread_pct       = round(spread, 4),
            opportunities_1h = ops_1h,
            trades_total     = len(self._trades),
            pnl_total_usd    = round(sum(t.net_pnl for t in self._trades), 4),
            stale            = stale,
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":          t.trade_id[:8],
                "route":       t.route,
                "spread_pct":  t.spread_pct,
                "net_pnl":     round(t.net_pnl, 4),
            }
            for t in self._trades[-10:]
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _implied_eth_btc(self) -> Optional[float]:
        if self._prices.btc_usdt <= 0 or self._prices.eth_usdt <= 0:
            return None
        return self._prices.eth_usdt / self._prices.btc_usdt

    @staticmethod
    def _spread_pct(impl: float, real: float) -> float:
        if real <= 0:
            return 0.0
        return abs(impl - real) / real * 100

    def _check_arb(self) -> None:
        now = time.time()
        if now - self._last_check < _COOLDOWN_SECS:
            return

        # Necesitamos los tres precios
        p = self._prices
        if not p.btc_usdt or not p.eth_usdt or not p.eth_btc:
            return

        # Datos stale?
        if (time.time() * 1000 - p.ts_ms) > _PRICE_STALE_MS:
            return

        impl   = self._implied_eth_btc()
        if impl is None:
            return

        spread = self._spread_pct(impl, p.eth_btc)

        if spread < _MIN_SPREAD_PCT or spread > _MAX_SPREAD_PCT:
            return

        self._last_check = now
        self._opportunities.append(now)

        # Determinar ruta: si real < implicito -> ruta A (comprar ETH/BTC barato)
        route     = "A" if p.eth_btc < impl else "B"
        gross_pnl = _POSITION_SIZE_USD * spread / 100
        fees      = _POSITION_SIZE_USD * _TAKER_FEE_PCT / 100 * 3   # 3 patas
        net_pnl   = gross_pnl - fees

        if net_pnl <= 0:
            return

        logger.info(
            "[tri_arb] OPORTUNIDAD ruta={} spread={:.4f}% impl={:.8f} real={:.8f} net=+${:.4f}",
            route, spread, impl, p.eth_btc, net_pnl,
        )

        trade = TriArbTrade(
            trade_id      = str(uuid.uuid4()),
            route         = route,
            btc_usdt      = p.btc_usdt,
            eth_usdt      = p.eth_usdt,
            eth_btc_real  = p.eth_btc,
            eth_btc_impl  = round(impl, 8),
            spread_pct    = round(spread, 4),
            size_usd      = _POSITION_SIZE_USD,
            gross_pnl     = round(gross_pnl, 4),
            net_pnl       = round(net_pnl, 4),
            production    = self._production,
        )

        if self._production:
            asyncio.create_task(self._execute_real(trade))
        else:
            self._trades.append(trade)

        asyncio.create_task(self._alert(trade))

    # ── Real execution ────────────────────────────────────────────────────────

    async def _execute_real(self, trade: TriArbTrade) -> None:
        """
        Para activar con dinero real:
          Ruta A: Buy ETHUSDT → Sell ETHBTC → Sell BTCUSDT
          Ruta B: Buy BTCUSDT → Buy ETHBTC  → Sell ETHUSDT
          Ejecutar las tres patas con asyncio.gather() para minimizar slippage.

        Requiere:
          - BYBIT_API_KEY real configurada en .env
          - El par ETHBTC disponible en Bybit spot

        Por ahora registra sin ejecutar.
        """
        logger.warning(
            "[tri_arb] REAL execution pendiente — "
            "requiere BYBIT_API_KEY real + par ETHBTC activo en Bybit spot."
        )
        # Cuando este configurado:
        # if trade.route == "A":
        #     await asyncio.gather(
        #         bybit_executor.market_order("ETHUSDT", "Buy",  trade.size_usd),
        #         bybit_executor.market_order("ETHBTC",  "Sell", trade.size_usd),
        #         bybit_executor.market_order("BTCUSDT", "Sell", trade.size_usd),
        #     )
        self._trades.append(trade)

    # ── Telegram alert ────────────────────────────────────────────────────────

    async def _alert(self, trade: TriArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        mode  = "REAL" if trade.production else "PAPEL"
        route_desc = (
            "BUY ETHUSDT → SELL ETHBTC → SELL BTCUSDT"
            if trade.route == "A"
            else "BUY BTCUSDT → BUY ETHBTC → SELL ETHUSDT"
        )
        msg = (
            f"[TRI ARB] Oportunidad ({mode})\n"
            f"Ruta: {trade.route} — {route_desc}\n"
            f"ETH/BTC implicito: {trade.eth_btc_impl:.8f}\n"
            f"ETH/BTC real:      {trade.eth_btc_real:.8f}\n"
            f"Spread: {trade.spread_pct:.4f}%\n"
            f"P&L neto: +${trade.net_pnl:.4f}"
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
