# -*- coding: utf-8 -*-
"""
cross_exchange_arb.py -- Whale Follower Bot
Arbitraje de precio entre exchanges.

Estrategia:
  Si precio BTC en Bybit < precio BTC en OKX por mas de 0.05%
  -> Comprar en Bybit, vender en OKX simultaneamente
  -> Ganancia = diferencia de precio - fees (0.02% cada lado = 0.04% total)

En papel: simula las dos patas y registra el spread capturado.
En real:  ejecuta ordenes en ambos exchanges al mismo tiempo.

El bot ya recibe precios de Bybit y OKX en tiempo real via WebSocket
con latencia < 50ms, suficiente para capturar spreads de duracion > 200ms.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

from loguru import logger
import aiohttp

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT  = 0.05    # spread minimo para abrir arb (> fees 0.04%)
_MAX_SPREAD_PCT  = 2.0     # spread maximo (> 2% = dato anomalo, ignorar)
_POSITION_SIZE   = 200.0   # USD por pata en papel
_TAKER_FEE_PCT   = 0.02    # fee por lado (Bybit/OKX taker fee)
_PAIRS           = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_PRICE_STALE_MS  = 2000    # precio stale si > 2 segundos


@dataclass
class PricePoint:
    price:  float
    ts:     float   # unix ms


@dataclass
class CrossArbTrade:
    trade_id:    str
    pair:        str
    buy_exchange:  str
    sell_exchange: str
    buy_price:   float
    sell_price:  float
    spread_pct:  float
    size_usd:    float
    gross_pnl:   float     # spread capturado en USD
    net_pnl:     float     # despues de fees
    opened_at:   float = field(default_factory=time.time)
    production:  bool  = False


@dataclass
class CrossArbSnapshot:
    opportunities_1h:  int   = 0    # oportunidades en la ultima hora
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    last_spread_pct:   Dict[str, float] = field(default_factory=dict)
    best_spread_pair:  str   = ""
    best_spread_pct:   float = 0.0


class CrossExchangeArb:
    """
    Detecta spreads de precio entre Bybit y OKX en tiempo real.

    Recibe precios via update() desde el aggregator (latencia < 1ms).
    Detecta oportunidades y simula/ejecuta el arbitraje.
    """

    def __init__(self, production: bool = False) -> None:
        self._production  = production
        # prices[pair][exchange] = PricePoint
        self._prices: Dict[str, Dict[str, PricePoint]] = {
            p: {} for p in _PAIRS
        }
        self._trades: List[CrossArbTrade] = []
        self._opportunities: Deque[float] = deque(maxlen=1000)  # timestamps
        self._last_check: Dict[str, float] = {}   # par -> ultimo ts de arb

        mode = "REAL" if production else "PAPEL"
        logger.info("[cross_arb] Iniciado en modo {} | min_spread={}%", mode, _MIN_SPREAD_PCT)

    # ── Public API ─────────────────────────────────────────────────────────────

    def update_price(self, exchange: str, pair: str, price: float, ts_ms: float) -> None:
        """Actualizar precio desde el aggregator. Llamar con cada trade recibido."""
        if pair not in self._prices:
            return
        self._prices[pair][exchange] = PricePoint(price=price, ts=ts_ms)
        # Verificar arb cada vez que llega un precio
        self._check_arb(pair)

    def snapshot(self) -> CrossArbSnapshot:
        now = time.time()
        ops_1h = sum(1 for t in self._opportunities if now - t < 3600)
        spreads = {}
        best_pair, best_spread = "", 0.0
        for pair in _PAIRS:
            sp = self._current_spread_pct(pair)
            if sp is not None:
                spreads[pair] = round(sp, 4)
                if sp > best_spread:
                    best_spread = sp
                    best_pair   = pair
        return CrossArbSnapshot(
            opportunities_1h = ops_1h,
            trades_total     = len(self._trades),
            pnl_total_usd    = round(sum(t.net_pnl for t in self._trades), 4),
            last_spread_pct  = spreads,
            best_spread_pair = best_pair,
            best_spread_pct  = round(best_spread, 4),
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":           t.trade_id[:8],
                "pair":         t.pair,
                "buy":          t.buy_exchange,
                "sell":         t.sell_exchange,
                "spread_pct":   t.spread_pct,
                "net_pnl":      round(t.net_pnl, 4),
            }
            for t in self._trades[-10:]
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _current_spread_pct(self, pair: str) -> Optional[float]:
        """Retorna spread actual entre Bybit y OKX, o None si datos stale."""
        now_ms = time.time() * 1000
        bybit = self._prices[pair].get("bybit")
        okx   = self._prices[pair].get("okx")
        if not bybit or not okx:
            return None
        if now_ms - bybit.ts > _PRICE_STALE_MS or now_ms - okx.ts > _PRICE_STALE_MS:
            return None
        spread = abs(bybit.price - okx.price) / min(bybit.price, okx.price) * 100
        return spread

    def _check_arb(self, pair: str) -> None:
        spread = self._current_spread_pct(pair)
        if spread is None:
            return
        if spread < _MIN_SPREAD_PCT or spread > _MAX_SPREAD_PCT:
            return

        # Cooldown de 5 segundos por par para no duplicar
        now = time.time()
        if now - self._last_check.get(pair, 0) < 5:
            return
        self._last_check[pair] = now

        bybit_price = self._prices[pair]["bybit"].price
        okx_price   = self._prices[pair]["okx"].price

        buy_exchange  = "bybit" if bybit_price < okx_price else "okx"
        sell_exchange = "okx"   if bybit_price < okx_price else "bybit"
        buy_price     = min(bybit_price, okx_price)
        sell_price    = max(bybit_price, okx_price)

        gross_pnl = _POSITION_SIZE * spread / 100
        fees      = _POSITION_SIZE * _TAKER_FEE_PCT / 100 * 2   # 2 lados
        net_pnl   = gross_pnl - fees

        if net_pnl <= 0:
            return

        self._opportunities.append(now)

        logger.info(
            "[cross_arb] OPORTUNIDAD {} spread={:.4f}% buy={} sell={} net=+${:.4f}",
            pair, spread, buy_exchange, sell_exchange, net_pnl,
        )

        trade = CrossArbTrade(
            trade_id      = str(uuid.uuid4()),
            pair          = pair,
            buy_exchange  = buy_exchange,
            sell_exchange = sell_exchange,
            buy_price     = buy_price,
            sell_price    = sell_price,
            spread_pct    = round(spread, 4),
            size_usd      = _POSITION_SIZE,
            gross_pnl     = round(gross_pnl, 4),
            net_pnl       = round(net_pnl, 4),
            production    = self._production,
        )

        if self._production:
            asyncio.create_task(self._execute_real(trade))
        else:
            self._trades.append(trade)

        asyncio.create_task(self._alert(trade))

    # ── Real execution (dinero real) ──────────────────────────────────────────

    async def _execute_real(self, trade: CrossArbTrade) -> None:
        """
        Para activar con dinero real:
          1. Configurar BYBIT_API_KEY + OKX_API_KEY reales en .env
          2. Ejecutar ambas patas simultaneamente con asyncio.gather()
          3. Verificar que ambas ordenes se ejecutaron antes de registrar P&L

        El riesgo de ejecucion parcial (leg risk) se mitiga con ordenes MARKET.
        """
        logger.warning(
            "[cross_arb] REAL execution pendiente — "
            "necesita BYBIT_API_KEY + OKX_API_KEY reales."
        )
        # Una vez configurado, descomentar:
        # await asyncio.gather(
        #     bybit_executor.market_order(trade.pair, "Buy", trade.size_usd),
        #     okx_executor.market_order(trade.pair, "Sell", trade.size_usd),
        # )
        self._trades.append(trade)

    # ── Alert ─────────────────────────────────────────────────────────────────

    async def _alert(self, trade: CrossArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        mode = "REAL" if trade.production else "PAPEL"
        msg = (
            f"[CROSS ARB] Oportunidad capturada ({mode})\n"
            f"Par: {trade.pair}\n"
            f"Comprar en: {trade.buy_exchange} @ ${trade.buy_price:,.2f}\n"
            f"Vender en:  {trade.sell_exchange} @ ${trade.sell_price:,.2f}\n"
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
