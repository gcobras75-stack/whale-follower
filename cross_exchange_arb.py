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

import config

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT  = config.CROSS_ARB_MIN_SPREAD_PCT   # default 0.08%
_MAX_SPREAD_PCT  = 2.0     # spread maximo (> 2% = dato anomalo, ignorar)
_POSITION_SIZE   = config.CROSS_ARB_MAX_SIZE_USD     # default $50
_TAKER_FEE_PCT   = 0.02    # fee por lado (Bybit/OKX taker fee)
_PAIRS           = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
_PRICE_STALE_MS  = 2000    # precio stale si > 2 segundos
_CLOSE_AFTER_SECS = 60     # cerrar ambas patas despues de N segundos
_MIN_BALANCE_USD  = config.CROSS_ARB_MIN_BALANCE_USD  # default $10


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

        # Real executors (lazy init)
        self._okx_exec = None
        self._bybit_session = None
        self._balance_ok = True        # flip to False if balance < threshold
        self._last_balance_check: float = 0.0

        if production:
            self._init_real_executors()

        mode = "REAL" if production else "PAPEL"
        logger.info(
            "[cross_arb] Iniciado en modo {} | min_spread={}% | max_size=${}",
            mode, _MIN_SPREAD_PCT, _POSITION_SIZE,
        )

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

    # ── Real executors init ───────────────────────────────────────────────────

    def _init_real_executors(self) -> None:
        """Initialize real exchange connections."""
        # OKX
        try:
            from okx_executor import OKXExecutor
            self._okx_exec = OKXExecutor()
            if not self._okx_exec.enabled:
                logger.warning("[cross_arb] OKX executor not enabled — missing credentials")
                self._okx_exec = None
        except Exception as exc:
            logger.warning("[cross_arb] OKX executor init failed: {}", exc)
            self._okx_exec = None

        # Bybit (using pybit)
        try:
            from pybit.unified_trading import HTTP as BybitHTTP
            if config.BYBIT_API_KEY and config.BYBIT_API_SECRET:
                self._bybit_session = BybitHTTP(
                    testnet=False,
                    api_key=config.BYBIT_API_KEY,
                    api_secret=config.BYBIT_API_SECRET,
                )
                logger.info("[cross_arb] Bybit real session initialized")
            else:
                logger.warning("[cross_arb] Bybit real keys missing")
        except Exception as exc:
            logger.warning("[cross_arb] Bybit session init failed: {}", exc)

    def _can_execute_real(self) -> bool:
        """Check if both exchanges are ready for real execution."""
        return (
            self._production
            and self._okx_exec is not None
            and self._bybit_session is not None
            and self._balance_ok
        )

    # ── Balance check ────────────────────────────────────────────────────────

    async def _check_balances(self) -> bool:
        """Verify OKX has enough balance. Cache for 60s."""
        now = time.time()
        if now - self._last_balance_check < 60:
            return self._balance_ok
        self._last_balance_check = now

        if not self._okx_exec:
            self._balance_ok = False
            return False

        okx_bal = await self._okx_exec.get_balance()
        if okx_bal < _MIN_BALANCE_USD:
            if self._balance_ok:  # only log on state change
                logger.warning(
                    "[cross_arb] OKX balance ${:.2f} < min ${:.0f} — PAUSING arb",
                    okx_bal, _MIN_BALANCE_USD,
                )
            self._balance_ok = False
        else:
            if not self._balance_ok:
                logger.info("[cross_arb] OKX balance OK ${:.2f} — RESUMING arb", okx_bal)
            self._balance_ok = True

        return self._balance_ok

    # ── Real execution (dinero real) ──────────────────────────────────────────

    async def _execute_real(self, trade: CrossArbTrade) -> None:
        """
        Execute cross-exchange arb with real orders on both Bybit and OKX.

        1. Check balances
        2. Place simultaneous market orders on both exchanges
        3. Schedule auto-close after _CLOSE_AFTER_SECS
        4. Persist to Supabase
        """
        if not self._can_execute_real():
            logger.warning("[cross_arb] REAL execution skipped — executors not ready")
            self._trades.append(trade)  # still record as paper
            return

        # Balance check
        if not await self._check_balances():
            logger.warning("[cross_arb] REAL execution skipped — balance too low")
            self._trades.append(trade)
            return

        buy_price  = trade.buy_price
        sell_price = trade.sell_price
        pair       = trade.pair
        size_usd   = min(trade.size_usd, config.CROSS_ARB_MAX_SIZE_USD)

        logger.info(
            "[cross_arb] REAL EXECUTION {} buy@{} sell@{} size=${:.0f}",
            pair, trade.buy_exchange, trade.sell_exchange, size_usd,
        )

        # Execute both legs simultaneously
        buy_result  = None
        sell_result = None

        try:
            if trade.buy_exchange == "bybit":
                buy_result, sell_result = await asyncio.gather(
                    self._bybit_market_order(pair, "Buy", size_usd, buy_price),
                    self._okx_market_order(pair, "sell", size_usd, sell_price),
                    return_exceptions=True,
                )
            else:
                buy_result, sell_result = await asyncio.gather(
                    self._okx_market_order(pair, "buy", size_usd, buy_price),
                    self._bybit_market_order(pair, "Sell", size_usd, sell_price),
                    return_exceptions=True,
                )
        except Exception as exc:
            logger.error("[cross_arb] REAL execution error: {}", exc)

        # Check results
        buy_ok  = isinstance(buy_result, dict) and buy_result is not None
        sell_ok = isinstance(sell_result, dict) and sell_result is not None

        if buy_ok and sell_ok:
            logger.info(
                "[cross_arb] BOTH LEGS FILLED {} spread={:.4f}% net=+${:.4f}",
                pair, trade.spread_pct, trade.net_pnl,
            )
            trade.production = True
            self._trades.append(trade)

            # Persist to Supabase
            asyncio.create_task(self._save_to_supabase(trade, buy_result, sell_result))

            # Schedule auto-close
            asyncio.create_task(self._auto_close(trade, _CLOSE_AFTER_SECS))

        elif buy_ok and not sell_ok:
            logger.error(
                "[cross_arb] LEG RISK — buy filled but sell FAILED on {} — closing buy",
                pair,
            )
            # Emergency: close the buy leg
            asyncio.create_task(self._emergency_close(trade.buy_exchange, pair, "Sell", buy_price))
            trade.net_pnl = 0.0
            self._trades.append(trade)

        elif not buy_ok and sell_ok:
            logger.error(
                "[cross_arb] LEG RISK — sell filled but buy FAILED on {} — closing sell",
                pair,
            )
            asyncio.create_task(self._emergency_close(trade.sell_exchange, pair, "Buy", sell_price))
            trade.net_pnl = 0.0
            self._trades.append(trade)

        else:
            logger.warning("[cross_arb] BOTH LEGS FAILED {} — no action needed", pair)

    # ── Exchange order helpers ────────────────────────────────────────────────

    async def _bybit_market_order(
        self, pair: str, side: str, size_usd: float, price: float
    ) -> Optional[Dict]:
        """Place a market order on Bybit SPOT via pybit."""
        if not self._bybit_session:
            return None

        # Calculate qty in base asset
        qty = size_usd / price if price > 0 else 0
        # Round to appropriate precision
        if pair == "BTCUSDT":
            qty = round(qty, 3)     # min 0.001 BTC
        elif pair == "ETHUSDT":
            qty = round(qty, 2)     # min 0.01 ETH
        elif pair == "SOLUSDT":
            qty = round(qty, 1)     # min 0.1 SOL
        else:
            qty = round(qty, 3)

        if qty <= 0:
            logger.warning("[cross_arb] Bybit qty too small for {} (${:.0f})", pair, size_usd)
            return None

        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._bybit_session.place_order(
                    category="spot",
                    symbol=pair,
                    side=side,
                    orderType="Market",
                    qty=str(qty),
                ),
            )

            ret_code = result.get("retCode", -1)
            if ret_code != 0:
                logger.error(
                    "[cross_arb] Bybit order FAILED {}: {}",
                    pair, result.get("retMsg", "unknown"),
                )
                return None

            order_id = result.get("result", {}).get("orderId", "")
            logger.info("[cross_arb] Bybit {} {} {} qty={} orderId={}", side, pair, "OK", qty, order_id)
            return {
                "exchange": "bybit",
                "order_id": order_id,
                "pair": pair,
                "side": side,
                "qty": qty,
                "price_hint": price,
            }

        except Exception as exc:
            logger.error("[cross_arb] Bybit order exception: {}", exc)
            return None

    async def _okx_market_order(
        self, pair: str, side: str, size_usd: float, price: float
    ) -> Optional[Dict]:
        """Place a market order on OKX real (USDT perp) via okx_executor."""
        if not self._okx_exec:
            return None
        return await self._okx_exec.market_order(pair, side, size_usd, price)

    # ── Auto-close ───────────────────────────────────────────────────────────

    async def _auto_close(self, trade: CrossArbTrade, delay_secs: int) -> None:
        """Close both arb legs after a delay (spread should have converged)."""
        await asyncio.sleep(delay_secs)
        logger.info("[cross_arb] Auto-closing arb {} after {}s", trade.pair, delay_secs)
        await self._close_both_legs(trade)

    async def _close_both_legs(self, trade: CrossArbTrade) -> None:
        """Close positions on both exchanges."""
        pair = trade.pair

        # Close Bybit leg (opposite side)
        buy_close_side  = "Sell" if trade.buy_exchange == "bybit" else "Buy"
        sell_close_side = "Buy"  if trade.sell_exchange == "bybit" else "Sell"

        tasks = []
        if trade.buy_exchange == "bybit":
            tasks.append(self._bybit_market_order(pair, "Sell", trade.size_usd, trade.buy_price))
            if self._okx_exec:
                tasks.append(self._okx_exec.close_position(pair))
        else:
            tasks.append(self._bybit_market_order(pair, "Buy", trade.size_usd, trade.sell_price))
            if self._okx_exec:
                tasks.append(self._okx_exec.close_position(pair))

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("[cross_arb] Both legs closed for {}", pair)
        except Exception as exc:
            logger.error("[cross_arb] Error closing legs: {}", exc)

    async def _emergency_close(
        self, exchange: str, pair: str, side: str, price: float
    ) -> None:
        """Emergency close a single leg after partial fill (leg risk)."""
        logger.warning("[cross_arb] EMERGENCY close {} on {} side={}", pair, exchange, side)
        try:
            if exchange == "bybit":
                await self._bybit_market_order(pair, side, _POSITION_SIZE, price)
            elif exchange == "okx" and self._okx_exec:
                await self._okx_exec.close_position(pair)
        except Exception as exc:
            logger.error("[cross_arb] Emergency close FAILED: {}", exc)

    # ── Supabase persistence ─────────────────────────────────────────────────

    async def _save_to_supabase(self, trade: CrossArbTrade, buy_result: Dict, sell_result: Dict) -> None:
        """Persist real arb trade to Supabase."""
        try:
            from db_writer import save_cross_arb_trade
            await save_cross_arb_trade(
                trade_id=trade.trade_id,
                pair=trade.pair,
                buy_exchange=trade.buy_exchange,
                sell_exchange=trade.sell_exchange,
                buy_price=trade.buy_price,
                sell_price=trade.sell_price,
                spread_pct=trade.spread_pct,
                size_usd=trade.size_usd,
                gross_pnl=trade.gross_pnl,
                net_pnl=trade.net_pnl,
                production=True,
            )
        except Exception as exc:
            logger.warning("[cross_arb] Supabase save error: {}", exc)

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
