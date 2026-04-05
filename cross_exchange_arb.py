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
_ETH_MIN_USD      = 15.0   # inventario mínimo ETH en USD por exchange — replenish si cae aquí
_ETH_REPLENISH_USD = 20.0  # cuánto ETH comprar al replenish
_INVENTORY_CHECK_SECS = 300  # verificar inventario cada 5 min


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
        self._okx_exec    = None
        self._bybit_ready = False      # True cuando keys OK — sin pybit SDK
        self._balance_ok  = True       # flip to False if balance < threshold
        self._last_balance_check: float = 0.0

        # Inventario ETH
        self._replenishing: set  = set()   # exchanges con replenish en curso
        self._inventory_started: bool = False  # arranca loop en primer _check_arb

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

    def _get_price(self, pair: str, exchange: str) -> float:
        """Precio actual de un par en un exchange. 0.0 si no disponible."""
        pp = self._prices.get(pair, {}).get(exchange)
        return pp.price if pp else 0.0

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
        # Arrancar loop de inventario en el primer _check_arb en producción
        if self._production and not self._inventory_started:
            self._inventory_started = True
            asyncio.create_task(self._startup_inventory_check())
            asyncio.create_task(self._inventory_loop())

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
        """Initialize real exchange connections. Logs exact reason on failure."""
        # OKX
        try:
            from okx_executor import OKXExecutor
            self._okx_exec = OKXExecutor()
            if not self._okx_exec.enabled:
                logger.error(
                    "[cross_arb] OKX executor DESHABILITADO — "
                    "verificar OKX_API_KEY={} OKX_SECRET={} OKX_PASSPHRASE={}",
                    bool(config.OKX_API_KEY), bool(config.OKX_SECRET), bool(config.OKX_PASSPHRASE),
                )
                self._okx_exec = None
            else:
                logger.info("[cross_arb] OKX executor OK ✅")
        except Exception as exc:
            logger.error("[cross_arb] OKX executor init ERROR: {}", exc)
            self._okx_exec = None

        # Bybit — via aiohttp directo a api.bytick.com (sin pybit SDK)
        if config.BYBIT_API_KEY and config.BYBIT_API_SECRET:
            self._bybit_ready = True
            logger.info("[cross_arb] Bybit keys OK ✅ (key={}****)", config.BYBIT_API_KEY[:4])
        else:
            self._bybit_ready = False
            logger.error(
                "[cross_arb] Bybit keys FALTANTES — "
                "BYBIT_API_KEY={} BYBIT_API_SECRET={}",
                bool(config.BYBIT_API_KEY), bool(config.BYBIT_API_SECRET),
            )

    def _can_execute_real(self) -> bool:
        """Check if both exchanges are ready for real execution."""
        return (
            self._production
            and self._okx_exec is not None
            and self._bybit_ready
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
            # Reintentar init si algún executor es None
            if self._okx_exec is None or not self._bybit_ready:
                logger.warning("[cross_arb] Executors no listos — reintentando init...")
                self._init_real_executors()
            if not self._can_execute_real():
                logger.error(
                    "[cross_arb] REAL skipped — okx_exec={} bybit_ready={} balance_ok={}",
                    self._okx_exec is not None,
                    self._bybit_ready,
                    self._balance_ok,
                )
                self._trades.append(trade)  # still record as paper
                return

        # Balance check
        if config.BYBIT_ORDERS_BLOCKED:
            logger.info("[cross_arb] BYBIT_ORDERS_BLOCKED=true — ejecución real cancelada, modo papel")
            self._trades.append(trade)
            return

        if not await self._check_balances():
            logger.warning("[cross_arb] REAL execution skipped — balance too low")
            self._trades.append(trade)
            return

        buy_price  = trade.buy_price
        sell_price = trade.sell_price
        pair       = trade.pair
        coin       = pair.replace("USDT", "")   # ETHUSDT → ETH

        # ── Verificar inventario ETH en el lado vendedor ──────────────────────
        if trade.sell_exchange == "okx" and self._okx_exec:
            eth_bal = await self._okx_exec.get_coin_balance(coin)
        elif trade.sell_exchange == "bybit" and self._bybit_ready:
            eth_bal = await self._bybit_coin_balance(coin)
        else:
            eth_bal = 0.0

        eth_usd  = eth_bal * sell_price
        size_usd = self._calc_size(coin, eth_usd)

        if size_usd < 10.0:
            logger.warning(
                "[cross_arb] ⚠️ Inventario {} insuficiente en {} "
                "(${:.2f} = {:.6f} {}) — activando reabastecimiento",
                coin, trade.sell_exchange, eth_usd, eth_bal, coin,
            )
            # Replenish asíncrono: compra ETH para el próximo arb
            if trade.sell_exchange not in self._replenishing:
                asyncio.create_task(self._replenish_eth(trade.sell_exchange, coin))
            self._trades.append(trade)
            return

        # Si inventario está por debajo del mínimo saludable, replenish en bg
        if eth_usd < _ETH_MIN_USD and trade.sell_exchange not in self._replenishing:
            asyncio.create_task(self._replenish_eth(trade.sell_exchange, coin))

        trade.size_usd = size_usd

        logger.info(
            "[cross_arb] REAL EXECUTION {} buy@{} sell@{} size=${:.2f} "
            "(inventario {}={:.4f} ≈ ${:.2f})",
            pair, trade.buy_exchange, trade.sell_exchange, size_usd,
            coin, eth_bal, eth_usd,
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

    # ── Dynamic size ──────────────────────────────────────────────────────────

    @staticmethod
    def _calc_size(coin: str, available_balance_usd: float) -> float:
        """Calcula size dinámico: 80% del balance disponible, entre $10 y $50."""
        max_size = config.CROSS_ARB_MAX_SIZE  # configurable via env CROSS_ARB_MAX_SIZE
        min_size = 10.0
        dynamic  = available_balance_usd * 0.80
        size     = max(min_size, min(max_size, dynamic))
        logger.info(
            "[cross_arb] {} size dinámico: balance=${:.2f} → size=${:.2f}",
            coin, available_balance_usd, size,
        )
        return size

    # ── Inventario ETH ────────────────────────────────────────────────────────

    async def _startup_inventory_check(self) -> None:
        """Verifica inventario ETH en arranque y alerta si está bajo."""
        await asyncio.sleep(10)   # esperar a que los precios lleguen vía WS
        logger.info("[cross_arb] 🔍 Verificando inventario ETH inicial...")
        for coin in ["ETH"]:
            pair  = f"{coin}USDT"
            for exchange in ["bybit", "okx"]:
                if exchange == "bybit" and not self._bybit_ready:
                    continue
                if exchange == "okx" and not self._okx_exec:
                    continue
                try:
                    if exchange == "bybit":
                        bal = await self._bybit_coin_balance(coin)
                    else:
                        bal = await self._okx_exec.get_coin_balance(coin)
                    price   = self._get_price(pair, exchange)
                    eth_usd = bal * price
                    if eth_usd < _ETH_MIN_USD:
                        logger.warning(
                            "[cross_arb] ⚠️ Inventario bajo en {} — {}={:.4f} (${:.2f}) "
                            "mínimo=${:.0f} — comprando ${:.0f} ETH",
                            exchange, coin, bal, eth_usd, _ETH_MIN_USD, _ETH_REPLENISH_USD,
                        )
                        asyncio.create_task(self._replenish_eth(exchange, coin))
                    else:
                        logger.info(
                            "[cross_arb] ✅ Inventario {} {} {:.4f} ≈ ${:.2f}",
                            exchange, coin, bal, eth_usd,
                        )
                except Exception as exc:
                    logger.warning("[cross_arb] inventory check {}/{} error: {}", exchange, coin, exc)

    async def _inventory_loop(self) -> None:
        """Loop de fondo: verifica inventario ETH cada 5 minutos."""
        while True:
            await asyncio.sleep(_INVENTORY_CHECK_SECS)
            await self._check_and_replenish_inventory()

    async def _check_and_replenish_inventory(self) -> None:
        """Revisa inventario ETH en ambos exchanges y replenish si < mínimo."""
        for coin in ["ETH"]:
            pair = f"{coin}USDT"
            for exchange in ["bybit", "okx"]:
                if exchange == "bybit" and not self._bybit_ready:
                    continue
                if exchange == "okx" and not self._okx_exec:
                    continue
                if exchange in self._replenishing:
                    continue
                try:
                    if exchange == "bybit":
                        bal = await self._bybit_coin_balance(coin)
                    else:
                        bal = await self._okx_exec.get_coin_balance(coin)
                    price   = self._get_price(pair, exchange)
                    eth_usd = bal * price
                    if eth_usd < _ETH_MIN_USD:
                        logger.info(
                            "[cross_arb] 📦 Inventario {} {} ${:.2f} < ${:.0f} mínimo — reabasteciendo",
                            exchange, coin, eth_usd, _ETH_MIN_USD,
                        )
                        asyncio.create_task(self._replenish_eth(exchange, coin))
                except Exception as exc:
                    logger.warning("[cross_arb] _check_inventory {}/{}: {}", exchange, coin, exc)

    async def _replenish_eth(self, exchange: str, coin: str) -> None:
        """Compra ETH en el exchange indicado usando USDT para reponer inventario."""
        if exchange in self._replenishing:
            return
        self._replenishing.add(exchange)
        try:
            pair  = f"{coin}USDT"
            price = self._get_price(pair, exchange)
            if price <= 0:
                logger.warning(
                    "[cross_arb] _replenish_eth: sin precio para {} en {} — abortando",
                    pair, exchange,
                )
                return
            logger.info(
                "[cross_arb] 🛒 Comprando ${:.0f} {} en {} para inventario (precio=${:.2f})",
                _ETH_REPLENISH_USD, coin, exchange, price,
            )
            if exchange == "bybit":
                result = await self._bybit_market_order(pair, "Buy", _ETH_REPLENISH_USD, price)
            elif exchange == "okx" and self._okx_exec:
                result = await self._okx_market_order(pair, "buy", _ETH_REPLENISH_USD, price)
            else:
                result = None

            if result:
                logger.info("[cross_arb] ✅ Inventario {} replenish OK — compraron ${:.0f} {}", exchange, _ETH_REPLENISH_USD, coin)
            else:
                logger.error("[cross_arb] ❌ Inventario {} replenish FALLÓ", exchange)
        except Exception as exc:
            logger.error("[cross_arb] _replenish_eth {} error: {}", exchange, exc)
        finally:
            self._replenishing.discard(exchange)

    # ── Exchange order helpers ────────────────────────────────────────────────

    async def _bybit_market_order(
        self, pair: str, side: str, size_usd: float, price: float
    ) -> Optional[Dict]:
        """Place a market order on Bybit SPOT via aiohttp (sin pybit)."""
        if not self._bybit_ready:
            return None

        # Calculate qty in base asset
        qty = size_usd / price if price > 0 else 0
        if pair == "BTCUSDT":
            qty = round(qty, 3)
        elif pair == "ETHUSDT":
            qty = round(qty, 2)
        elif pair == "SOLUSDT":
            qty = round(qty, 1)
        else:
            qty = round(qty, 3)

        if qty <= 0:
            logger.warning("[cross_arb] Bybit qty too small for {} (${:.0f})", pair, size_usd)
            return None

        from bybit_utils import place_spot_order
        result   = await place_spot_order(pair, side, qty, caller="cross_arb")
        ret_code = result.get("retCode", -1)
        if ret_code != 0:
            return None

        order_id = result.get("result", {}).get("orderId", "")
        return {
            "exchange":   "bybit",
            "order_id":   order_id,
            "pair":       pair,
            "side":       side,
            "qty":        qty,
            "price_hint": price,
        }

    async def _bybit_coin_balance(self, coin: str) -> float:
        """Fetch available balance of a specific coin on Bybit (aiohttp, sin pybit)."""
        from bybit_utils import get_bybit_coin_balance
        return await get_bybit_coin_balance(coin, caller="cross_arb")

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
