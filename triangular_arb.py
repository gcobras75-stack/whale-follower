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

import config

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT    = 0.06    # > fees (0.02% x 3 patas = 0.06%)
_MAX_SPREAD_PCT    = 1.5     # spread > 1.5% = dato anomalo
_POSITION_SIZE_USD = 300.0   # USD por ciclo triangular en papel
_REAL_POSITION_USD = 30.0    # USD por ciclo en REAL (capped para $90 Bybit)
_MIN_NET_PNL       = 0.01    # minimo P&L neto para ejecutar ($0.01)
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

        if net_pnl < _MIN_NET_PNL:
            return

        logger.info(
            "[tri_arb] OPORTUNIDAD ruta={} spread={:.4f}% impl={:.8f} real={:.8f} net=+${:.4f}",
            route, spread, impl, p.eth_btc, net_pnl,
        )
        try:
            import alerts as _alerts
            _alerts.record_arb_opportunity()
        except Exception:
            pass

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
        Ejecuta arbitraje triangular real en Bybit Spot:
          Ruta A: Buy ETHUSDT → Sell ETHBTC → Sell BTCUSDT
          Ruta B: Buy BTCUSDT → Buy ETHBTC  → Sell ETHUSDT
        """
        try:
            from pybit.unified_trading import HTTP as BybitHTTP
        except ImportError:
            logger.error("[tri_arb] pybit no disponible")
            self._trades.append(trade)
            return

        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            logger.warning("[tri_arb] Faltan BYBIT_API_KEY/BYBIT_API_SECRET reales")
            self._trades.append(trade)
            return

        size_usd  = min(trade.size_usd, _REAL_POSITION_USD)
        eth_price = trade.eth_usdt
        eth_qty   = round(size_usd / eth_price, 4) if eth_price > 0 else 0
        btc_qty   = round(eth_qty * trade.eth_btc_real, 6)

        if eth_qty <= 0 or btc_qty <= 0:
            logger.warning("[tri_arb] qty <= 0, saltando")
            self._trades.append(trade)
            return

        logger.info(
            "[tri_arb] REAL INICIO ruta={} size=${:.0f} eth_qty={} btc_qty={}",
            trade.route, size_usd, eth_qty, btc_qty,
        )

        try:
            session = BybitHTTP(
                testnet=False,
                api_key=config.BYBIT_API_KEY,
                api_secret=config.BYBIT_API_SECRET,
            )
            loop = asyncio.get_event_loop()

            if trade.route == "A":
                # Buy ETHUSDT → Sell ETHBTC → Sell BTCUSDT
                _eth_qty  = eth_qty
                _btc_qty  = btc_qty
                leg1 = lambda: session.place_order(category="spot", symbol="ETHUSDT", side="Buy",  orderType="Market", qty=str(_eth_qty))
                leg2 = lambda: session.place_order(category="spot", symbol="ETHBTC",  side="Sell", orderType="Market", qty=str(_eth_qty))
                leg3 = lambda: session.place_order(category="spot", symbol="BTCUSDT", side="Sell", orderType="Market", qty=str(_btc_qty))
            else:
                # Buy BTCUSDT → Buy ETHBTC → Sell ETHUSDT
                _eth_qty  = eth_qty
                _btc_qty  = btc_qty
                leg1 = lambda: session.place_order(category="spot", symbol="BTCUSDT", side="Buy",  orderType="Market", qty=str(_btc_qty))
                leg2 = lambda: session.place_order(category="spot", symbol="ETHBTC",  side="Buy",  orderType="Market", qty=str(_eth_qty))
                leg3 = lambda: session.place_order(category="spot", symbol="ETHUSDT", side="Sell", orderType="Market", qty=str(_eth_qty))

            results = await asyncio.gather(
                loop.run_in_executor(None, leg1),
                loop.run_in_executor(None, leg2),
                loop.run_in_executor(None, leg3),
                return_exceptions=True,
            )

            all_ok = all(
                isinstance(r, dict) and r.get("retCode") == 0
                for r in results
            )

            if all_ok:
                logger.info(
                    "[tri_arb] ✅ REAL EJECUTADO ruta={} net=+${:.4f} spread={:.4f}%",
                    trade.route, trade.net_pnl, trade.spread_pct,
                )
                trade.production = True
                try:
                    import alerts as _alerts
                    _alerts.record_arb_executed(trade.net_pnl)
                except Exception:
                    pass
            else:
                for i, r in enumerate(results):
                    if isinstance(r, Exception):
                        logger.error("[tri_arb] Pata {} exception: {}", i + 1, r)
                    elif isinstance(r, dict) and r.get("retCode") != 0:
                        logger.error(
                            "[tri_arb] Pata {} error retCode={} msg={}",
                            i + 1, r.get("retCode"), r.get("retMsg"),
                        )

        except Exception as exc:
            logger.error("[tri_arb] _execute_real exception: {}", exc)

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
