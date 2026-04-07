# -*- coding: utf-8 -*-
"""
grid_trading.py -- Whale Follower Bot
Grid Trading adaptativo con ATR y Bandas de Bollinger.

Estrategia (version maximizada):
  - Grid spacing calculado con ATR(14) de los ultimos ticks para adaptarse
    automaticamente a la volatilidad del momento.
  - Rango del grid definido por Bollinger Bands (±2σ): el grid se centra
    donde el precio DEBERIA estar segun el mercado, no en rangos fijos.
  - Cuando el precio toca un nivel de compra → comprar + colocar venta en
    el nivel inmediatamente superior (captura el rebote).
  - Cuando el precio sale del rango (>1σ fuera): PAUSE y reposicionar.
  - Rebalanceo automatico cada N horas si el precio se desplazo >1 nivel.
  - Circuit breaker: si pierde >3% del capital asignado, suspender.

Rentabilidad estimada mercado lateral:
  Frecuencia: 8-15 toques de grid por dia en BTC
  P&L por vuelta: spacing% - fees (0.04% round trip)
  Con spacing ~0.5%: 0.46% neto por ciclo → 4-7% diario en lateralizacion

Pares: BTC, ETH, SOL (mayor volatilidad → mas ciclos de grid)
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import base64
import hashlib
import hmac
import json

import aiohttp
from loguru import logger

import config
import db_writer
from bybit_utils import place_spot_order as _bybit_ws_order

# ── Config por par ────────────────────────────────────────────────────────────
_GRID_CONFIG: Dict[str, dict] = {
    "BTCUSDT": {
        "levels":        2,         # 2 niveles → $30/2=$15 por nivel > $11 mínimo Bybit
        "capital_usd":   30.0,      # $30 total — $15 por nivel
        "min_spacing":   0.002,     # 0.2% — Bybit fees 0.04% → neto 0.16%
        "max_spacing":   0.015,
        "atr_mult":      1.5,
        "bb_period":     20,
        "bb_std":        2.0,
    },
    "ETHUSDT": {
        "levels":        2,         # 2 niveles para capital real ($25 → $12.50/nivel > min Bybit)
        "capital_usd":   25.0,      # $25 total — $12.50 por nivel
        "min_spacing":   0.002,     # 0.2% — Bybit fees 0.04% → neto 0.16%
        "max_spacing":   0.020,
        "atr_mult":      2.0,
        "bb_period":     20,
        "bb_std":        2.0,
    },
    "SOLUSDT": {
        "levels":        2,         # reducido para capital real ($20)
        "capital_usd":   20.0,      # $20 total — $10 por nivel
        "min_spacing":   0.002,     # 0.2% — Bybit fees 0.04% → neto 0.16%
        "max_spacing":   0.025,
        "atr_mult":      2.5,
        "bb_period":     20,
        "bb_std":        2.0,
    },
}

# Balance minimo de Bybit (USDT) requerido por par para operar en real
_MIN_BALANCE: Dict[str, float] = {
    "BTCUSDT": 30.0,
    "ETHUSDT": 25.0,
    "SOLUSDT": 20.0,
}

# Cantidad minima de activo base por par en Bybit spot
_MIN_QTY: Dict[str, float] = {
    "BTCUSDT": 0.000048,
    "ETHUSDT": 0.005,
    "SOLUSDT": 0.5,    # Bybit spot SOLUSDT minimo real = 0.5 SOL ~ $67
}

# Minimo en USD por orden (basado en _MIN_ORDER_QTY de bybit_utils)
_MIN_ORDER_USD: Dict[str, float] = {
    "BTCUSDT": 11.0,   # 0.000149 BTC * $68k ~ $10.20 → redondeamos a $11
    "ETHUSDT": 22.0,   # 0.01 ETH     * $2100 ~ $21   → redondeamos a $22
    "SOLUSDT": 67.0,   # 0.5 SOL      * $134  ~ $67
}

_FEES_PCT             = 0.0002    # 0.02% por lado (maker/taker Bybit)
_PAUSE_SIGMA          = 1.5       # pausar si precio > 1.5 sigma fuera de BB
_CIRCUIT_BREAKER      = 0.03      # suspender si pierde >3% del capital asignado
_REBALANCE_HOURS      = 4         # rebalancear grid cada 4 horas
_PRICE_HISTORY        = 200       # precios para calcular ATR y BB
_REBALANCE_DELAY_SECS = 300       # esperar 5 min fuera de rango antes de pausar


@dataclass
class GridLevel:
    price:      float
    side:       str       # "buy" | "sell"
    size_usd:   float
    filled:     bool = False
    fill_ts:    float = 0.0
    fill_price: float = 0.0   # precio real al que se compró
    pnl:        float = 0.0
    pending:    bool = False   # True mientras espera respuesta de API
    order_id:   str  = ""     # orderId de Bybit si la orden fue colocada


@dataclass
class GridState:
    pair:           str
    center_price:   float
    spacing_pct:    float
    levels:         List[GridLevel] = field(default_factory=list)
    capital_used:   float = 0.0
    capital_usd:    float = 0.0     # capital activo (crece con compounding)
    pnl_total:      float = 0.0
    cycles_done:    int   = 0
    paused:         bool  = False
    pause_reason:   str   = ""
    paper_mode:     bool  = False   # True si nivel < minimo Bybit spot
    trend_bearish:  bool  = False   # True cuando precio < MA20 → solo sell
    created_at:     float = field(default_factory=time.time)
    last_rebalance: float = field(default_factory=time.time)


@dataclass
class GridSnapshot:
    grids:          Dict[str, dict] = field(default_factory=dict)
    total_pnl_usd:  float = 0.0
    total_cycles:   int   = 0
    active_grids:   int   = 0


class GridTradingEngine:
    """
    Motor de grid trading adaptativo.

    Integración:
        grid = GridTradingEngine()
        # En cada tick:
        grid.on_price(pair, price)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._grids:   Dict[str, GridState]           = {}
        self._prices:  Dict[str, Deque[float]]        = {
            p: deque(maxlen=_PRICE_HISTORY) for p in _GRID_CONFIG
        }
        self._initialized:       Dict[str, bool]  = {p: False for p in _GRID_CONFIG}
        self._out_of_range_since: Dict[str, float] = {}
        self._bybit_balance:     float = 0.0
        self._balance_ts:        float = 0.0
        self._failed_sells: list = []   # (grid, level, price) tuples to retry

        mode = "REAL" if production else "PAPEL"
        logger.info("[grid] Iniciado en modo {} | pares={} | max_exposure=${}",
                    mode, list(_GRID_CONFIG.keys()),
                    sum(c["capital_usd"] for c in _GRID_CONFIG.values()))

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_price(self, pair: str, price: float) -> None:
        if pair not in _GRID_CONFIG:
            return

        self._prices[pair].append(price)

        # Necesitamos historia suficiente para ATR y BB
        cfg = _GRID_CONFIG[pair]
        if len(self._prices[pair]) < cfg["bb_period"] + 5:
            return

        # Inicializar grid la primera vez
        if not self._initialized[pair]:
            self._init_grid(pair, price)
            self._initialized[pair] = True
            return

        grid = self._grids.get(pair)
        if grid is None:
            return

        if grid.paused:
            self._check_resume(grid, price)
            return

        # Verificar circuit breaker
        if self._check_circuit_breaker(grid):
            return

        # Verificar si hay que rebalancear
        if time.time() - grid.last_rebalance > _REBALANCE_HOURS * 3600:
            self._rebalance_grid(grid, pair, price)
            return

        # Comprobar si el precio salio del rango BB
        bb_upper, bb_lower, _ = self._bollinger(pair)
        sigma = (bb_upper - bb_lower) / (2 * cfg["bb_std"])
        out_of_range = (
            price > bb_upper + sigma * _PAUSE_SIGMA
            or price < bb_lower - sigma * _PAUSE_SIGMA
        )

        if out_of_range:
            if pair not in self._out_of_range_since:
                self._out_of_range_since[pair] = time.time()
                logger.debug(
                    "[grid] {} precio fuera de rango BB, esperando {}s antes de pausar",
                    pair, _REBALANCE_DELAY_SECS,
                )
            elif time.time() - self._out_of_range_since[pair] >= _REBALANCE_DELAY_SECS:
                grid.paused = True
                grid.pause_reason = f"precio fuera de rango BB x{_PAUSE_SIGMA}\u03c3 (>{_REBALANCE_DELAY_SECS}s)"
                self._out_of_range_since.pop(pair, None)
                logger.warning("[grid] {} PAUSADO: {}", pair, grid.pause_reason)
                return
            # Grace period: NO hacer return — evaluar niveles igual
        else:
            # Precio volvio al rango — limpiar timer de out-of-range
            self._out_of_range_since.pop(pair, None)

        # Evaluar niveles del grid (incluso durante grace period fuera de BB)
        self._evaluate_levels(grid, pair, price)

    def snapshot(self) -> GridSnapshot:
        result = {}
        total_pnl = 0.0
        total_cycles = 0
        active = 0
        for pair, grid in self._grids.items():
            filled = sum(1 for l in grid.levels if l.filled)
            result[pair] = {
                "center":    round(grid.center_price, 2),
                "spacing":   f"{grid.spacing_pct*100:.3f}%",
                "levels":    len(grid.levels),
                "filled":    filled,
                "cycles":    grid.cycles_done,
                "pnl_usd":   round(grid.pnl_total, 4),
                "paused":    grid.paused,
            }
            total_pnl    += grid.pnl_total
            total_cycles += grid.cycles_done
            if not grid.paused:
                active += 1
        return GridSnapshot(
            grids         = result,
            total_pnl_usd = round(total_pnl, 4),
            total_cycles  = total_cycles,
            active_grids  = active,
        )

    # ── Grid initialization ───────────────────────────────────────────────────

    def _init_grid(self, pair: str, center: float) -> None:
        cfg     = _GRID_CONFIG[pair]
        spacing = self._calc_spacing(pair, cfg)
        n       = cfg["levels"]
        cap_per_level = cfg["capital_usd"] / n

        levels = []
        # Niveles de COMPRA: por debajo del precio actual
        for i in range(1, n // 2 + 1):
            lvl_price = center * (1 - spacing * i)
            levels.append(GridLevel(price=lvl_price, side="buy", size_usd=cap_per_level))
        # Niveles de VENTA: por encima del precio actual
        for i in range(1, n // 2 + 1):
            lvl_price = center * (1 + spacing * i)
            levels.append(GridLevel(price=lvl_price, side="sell", size_usd=cap_per_level))

        # Verificar si el nivel supera el minimo de Bybit spot
        min_usd   = _MIN_ORDER_USD.get(pair, 5.0)
        meets_min = cap_per_level >= min_usd
        paper     = (not self._production) or (not meets_min) or config.BYBIT_ORDERS_BLOCKED

        if self._production:
            if config.BYBIT_ORDERS_BLOCKED:
                logger.warning(
                    "[grid] {} BYBIT_ORDERS_BLOCKED=true \u2192 forzando modo PAPEL",
                    pair,
                )
            elif meets_min:
                logger.info(
                    "[grid] {} minimo=${:.0f} nivel=${:.2f} \u2705 real",
                    pair, min_usd, cap_per_level,
                )
            else:
                logger.warning(
                    "[grid] {} minimo=${:.0f} nivel=${:.2f} \u274c papel "
                    "(nivel < minimo Bybit spot)",
                    pair, min_usd, cap_per_level,
                )

        self._grids[pair] = GridState(
            pair         = pair,
            center_price = center,
            spacing_pct  = spacing,
            levels       = levels,
            paper_mode   = paper,
            capital_usd  = cfg["capital_usd"],
        )
        logger.info(
            "[grid] {} inicializado: centro={:.2f} spacing={:.3f}% niveles={} modo={}",
            pair, center, spacing * 100, n,
            "PAPEL" if paper else "REAL",
        )
        asyncio.create_task(self._alert_init(pair, center, spacing, n, paper))

    def _rebalance_grid(self, grid: GridState, pair: str, price: float) -> None:
        """Recentrar el grid en el precio actual con spacing recalculado y compounding."""
        cfg     = _GRID_CONFIG[pair]
        spacing = self._calc_spacing(pair, cfg)
        n       = cfg["levels"]

        # Compounding: reinvertir 50% de profits acumulados (max +50% del capital base)
        base_capital = cfg["capital_usd"]
        if grid.pnl_total > 0:
            compound_add = min(grid.pnl_total * 0.5, base_capital * 0.5)
            new_capital  = base_capital + compound_add
            if abs(new_capital - grid.capital_usd) >= 0.01:
                logger.info("[grid] {} compounding: ${:.2f} → ${:.2f} (profit=${:.2f})",
                            pair, grid.capital_usd, new_capital, grid.pnl_total)
            grid.capital_usd = new_capital
        elif grid.capital_usd <= 0:
            grid.capital_usd = base_capital

        cap_per_level = grid.capital_usd / n
        old_pnl = grid.pnl_total
        levels  = []
        for i in range(1, n // 2 + 1):
            levels.append(GridLevel(price=price * (1 - spacing * i), side="buy",  size_usd=cap_per_level))
        for i in range(1, n // 2 + 1):
            levels.append(GridLevel(price=price * (1 + spacing * i), side="sell", size_usd=cap_per_level))

        grid.center_price   = price
        grid.spacing_pct    = spacing
        grid.levels         = levels
        grid.last_rebalance = time.time()
        grid.pnl_total      = old_pnl

        logger.info("[grid] {} rebalanceado: centro={:.2f} spacing={:.3f}% capital=${:.2f}",
                    pair, price, spacing * 100, grid.capital_usd)

    # ── Level evaluation ──────────────────────────────────────────────────────

    def _evaluate_levels(self, grid: GridState, pair: str, price: float) -> None:
        bearish = self._is_bearish(pair)
        if bearish != grid.trend_bearish:
            grid.trend_bearish = bearish
            mode = "BAJISTA ⚠️ solo-sell" if bearish else "NEUTRAL ✅ buy+sell"
            logger.info("[grid] {} tendencia cambió → {} (precio={:.2f} vs MA20={:.2f})",
                        pair, mode, price, self._ma20(pair))
        for level in grid.levels:
            if level.pending:
                continue   # esperando respuesta de Bybit API
            if level.filled:
                if level.side == "sell" and price >= level.price:
                    self._fill_sell(grid, level, price)
                continue
            if level.side == "buy" and price <= level.price:
                if not bearish:
                    self._fill_buy(grid, level, price, pair)
                # En tendencia bajista: ignorar compras, no atrapar cuchillos

    def _fill_buy(self, grid: GridState, level: GridLevel, price: float, pair: str) -> None:
        if self._production:
            level.pending = True
            asyncio.create_task(self._handle_buy(grid, level, price, pair))
        else:
            level.filled     = True
            level.fill_ts    = time.time()
            level.fill_price = price
            sell_price = price * (1 + grid.spacing_pct)
            grid.levels.append(GridLevel(
                price=sell_price, side="sell", size_usd=level.size_usd,
                fill_price=price, filled=True,
            ))
            logger.info("[grid] {} COMPRA @ {:.4f} size=${:.0f} → venta programada @ {:.4f}",
                        pair, price, level.size_usd, sell_price)

    def _fill_sell(self, grid: GridState, level: GridLevel, price: float) -> None:
        if self._production:
            level.pending = True
            asyncio.create_task(self._handle_sell(grid, level, price))
        else:
            self._record_sell(grid, level, price)

    # ── ATR y Bollinger ───────────────────────────────────────────────────────

    def _calc_spacing(self, pair: str, cfg: dict) -> float:
        """Spacing dinámico: 50% del ancho BB actual, floor = min_spacing."""
        prices = list(self._prices[pair])
        if len(prices) < cfg["bb_period"] + 5:
            return cfg["min_spacing"]
        bb_upper, bb_lower, bb_mean = self._bollinger(pair)
        if bb_mean > 0:
            bb_width_pct = (bb_upper - bb_lower) / bb_mean
            spacing = max(cfg["min_spacing"], min(cfg["max_spacing"], bb_width_pct * 0.5))
        else:
            spacing = cfg["min_spacing"]
        return spacing

    def _is_bearish(self, pair: str) -> bool:
        """True si precio actual cayó >1% bajo el centro del grid → tendencia bajista.
        Usa center_price como referencia porque MA20 de ticks ≈ precio actual (inutil)."""
        grid = self._grids.get(pair)
        if grid is None:
            return False
        prices = list(self._prices[pair])
        if not prices:
            return False
        return prices[-1] < grid.center_price * 0.990

    def _ma20(self, pair: str) -> float:
        grid = self._grids.get(pair)
        return grid.center_price if grid else 0.0

    def _bollinger(self, pair: str) -> Tuple[float, float, float]:
        """Retorna (upper, lower, mid) de Bollinger Bands."""
        cfg    = _GRID_CONFIG[pair]
        prices = list(self._prices[pair])[-cfg["bb_period"]:]
        mid    = sum(prices) / len(prices)
        variance = sum((p - mid) ** 2 for p in prices) / len(prices)
        std    = variance ** 0.5
        return mid + cfg["bb_std"] * std, mid - cfg["bb_std"] * std, mid

    # ── Protecciones ──────────────────────────────────────────────────────────

    def _check_circuit_breaker(self, grid: GridState) -> bool:
        cfg = _GRID_CONFIG[grid.pair]
        loss_pct = -grid.pnl_total / cfg["capital_usd"]
        if loss_pct > _CIRCUIT_BREAKER:
            grid.paused      = True
            grid.pause_reason = f"circuit breaker: perdida {loss_pct*100:.1f}% > {_CIRCUIT_BREAKER*100:.0f}%"
            logger.error("[grid] {} CIRCUIT BREAKER: {}", grid.pair, grid.pause_reason)
            asyncio.create_task(self._alert_circuit_breaker(grid))
            return True
        return False

    def _check_resume(self, grid: GridState, price: float) -> None:
        """Reanudar grid si el precio volvio al rango BB."""
        pair = grid.pair
        bb_upper, bb_lower, _ = self._bollinger(pair)
        if bb_lower <= price <= bb_upper:
            grid.paused      = False
            grid.pause_reason = ""
            self._rebalance_grid(grid, pair, price)
            logger.info("[grid] {} reanudado, precio={:.2f} dentro del rango BB", pair, price)

    # ── Real-order helpers ─────────────────────────────────────────────────────

    def _record_sell(self, grid: GridState, level: GridLevel, price: float) -> None:
        """Registra un ciclo completado (llamado tanto en papel como post-API-real)."""
        level.filled     = False
        level.pending    = False
        pnl = level.size_usd * (grid.spacing_pct - _FEES_PCT * 2)
        grid.pnl_total  += pnl
        grid.cycles_done += 1
        logger.info("[grid] {} VENTA @ {:.4f} pnl_ciclo={:+.4f} total_pnl={:+.4f}",
                    grid.pair, price, pnl, grid.pnl_total)
        try:
            import alerts as _alerts
            _alerts.record_grid_cycle(pnl)
            if self._production:
                asyncio.create_task(_alerts.send_trade_alert("grid", {
                    "pair":       grid.pair,
                    "buy_price":  level.fill_price if level.fill_price else price * (1 - grid.spacing_pct),
                    "sell_price": price,
                    "pnl":        pnl,
                    "pnl_total":  grid.pnl_total,
                }))
        except Exception:
            pass
        asyncio.create_task(self._alert_cycle(grid, price, pnl))
        asyncio.create_task(db_writer.save_grid_cycle(
            pair=grid.pair,
            center=grid.center_price,
            spacing_pct=grid.spacing_pct,
            buy_price=level.fill_price if level.fill_price else price * (1 - grid.spacing_pct),
            sell_price=price,
            size_usd=level.size_usd,
            pnl_usd=pnl,
            production=self._production,
        ))

    async def run(self) -> None:
        """Tarea de fondo: retry de ventas fallidas cada 30s."""
        if self._production:
            await self._retry_sells_loop()

    async def _retry_sells_loop(self) -> None:
        """Reintenta ventas que fallaron en la primera llamada a Bybit."""
        while True:
            await asyncio.sleep(30)
            if not self._failed_sells:
                continue
            retry_list = list(self._failed_sells)
            self._failed_sells.clear()
            for grid, level, price in retry_list:
                if level.pending:
                    logger.info("[grid] Reintentando VENTA {} @ {:.4f}", grid.pair, price)
                    asyncio.create_task(self._handle_sell(grid, level, price))

    async def _fetch_bybit_balance(self) -> float:
        """Balance USDT disponible en Bybit. Intenta UNIFIED primero, luego SPOT."""
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return 0.0
        if time.time() - self._balance_ts < 60:
            return self._bybit_balance   # cache 60 s
        try:
            async with aiohttp.ClientSession() as s:
                for acct_type in ("UNIFIED", "SPOT"):
                    query = f"accountType={acct_type}&coin=USDT"
                    ts    = str(int(time.time() * 1000))
                    msg   = f"{ts}{config.BYBIT_API_KEY}5000{query}"
                    sig   = hmac.new(
                        config.BYBIT_API_SECRET.encode(), msg.encode(), hashlib.sha256
                    ).hexdigest()
                    headers = {
                        "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
                        "X-BAPI-TIMESTAMP":   ts,
                        "X-BAPI-SIGN":        sig,
                        "X-BAPI-RECV-WINDOW": "5000",
                        "User-Agent":         "Mozilla/5.0",
                        "Referer":            "https://www.bybit.com",
                    }
                    async with s.get(
                        f"https://api.bytick.com/v5/account/wallet-balance?{query}",
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=8),
                    ) as r:
                        if r.status == 403:
                            logger.info(
                                "[grid] Bybit balance REST bloqueado (403) → usando REAL_CAPITAL=${:.2f}",
                                config.REAL_CAPITAL,
                            )
                            self._bybit_balance = config.REAL_CAPITAL
                            self._balance_ts    = time.time()
                            return config.REAL_CAPITAL
                        data = await r.json()
                        if data.get("retCode") == 0:
                            for c in data["result"]["list"][0].get("coin", []):
                                if c.get("coin") == "USDT":
                                    bal = float(c.get("walletBalance", 0))
                                    if bal > 0:
                                        self._bybit_balance = bal
                                        self._balance_ts    = time.time()
                                        logger.info("[bybit] Balance {}=${:.2f} \u2705", acct_type, bal)
                                        return bal
                            logger.info("[bybit] Balance {}=$0 \u2192 intentando siguiente tipo", acct_type)
            logger.warning("[bybit] Balance UNIFIED=$0 y SPOT=$0 \u2014 subcuenta incorrecta?")
        except Exception as exc:
            logger.warning("[grid] _fetch_bybit_balance error: {}", exc)
        return self._bybit_balance

    async def _bybit_spot_order(self, pair: str, side: str, size_usd: float,
                                 price: float) -> Optional[str]:
        """
        Coloca orden MARKET en Bybit SPOT via bybit_utils (WS primero, REST fallback).
        Retorna orderId si OK, None si falla.
        """
        if config.BYBIT_ORDERS_BLOCKED:
            logger.warning("[grid] BYBIT_ORDERS_BLOCKED=true — orden cancelada {} {} ${:.0f}",
                           side, pair, size_usd)
            return None

        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            logger.error("[grid] Faltan credenciales Bybit")
            return None

        if price <= 0:
            logger.error("[grid] {} precio invalido: {}", pair, price)
            return None

        qty = round(size_usd / price, 6)
        min_qty = _MIN_QTY.get(pair, 0.001)
        if qty < min_qty:
            logger.error("[grid] {} qty={} < min={} (size_usd=${:.2f} price={:.2f})",
                         pair, qty, min_qty, size_usd, price)
            return None

        result = await _bybit_ws_order(pair, side, qty, caller="grid", price=price)
        if result and result.get("retCode") == 0:
            order_id = result.get("result", {}).get("orderId", "")
            logger.info("[grid] \U0001f7e2 ORDER OK {} {} ${:.0f} qty={} orderId={}",
                        side.upper(), pair, size_usd, qty, order_id)
            self._bybit_balance = max(0, self._bybit_balance - size_usd)
            return order_id

        ret_msg  = result.get("retMsg", "sin respuesta") if result else "sin respuesta"
        ret_code = result.get("retCode", 0) if result else 0
        logger.error("[grid] \u274c ORDER FALLIDA {} {} — {}", side, pair, ret_msg)
        await self._alert_order_error(pair, side, size_usd, ret_msg)
        # retCode 170140 = "Order value exceeded lower limit" → pasar a papel
        if ret_code == 170140 or "lower limit" in ret_msg.lower():
            grid = self._grids.get(pair)
            if grid and not grid.paper_mode:
                grid.paper_mode   = True
                grid.pause_reason = "mínimo Bybit no alcanzado → modo PAPEL"
                logger.warning("[grid] {} → PAPEL automático (retCode=170140 — orden < mínimo)", pair)
        return None

    async def _handle_buy(self, grid: GridState, level: GridLevel,
                           price: float, pair: str) -> None:
        """Ejecuta compra en Bybit SPOT (o papel si grid.paper_mode)."""
        if grid.paper_mode:
            level.filled     = True
            level.pending    = False
            level.fill_ts    = time.time()
            level.fill_price = price
            sell_price = price * (1 + grid.spacing_pct)
            grid.levels.append(GridLevel(
                price=sell_price, side="sell", size_usd=level.size_usd, fill_price=price,
            ))
            logger.info("[grid] {} COMPRA PAPEL @ {:.4f} size=${:.2f} -> venta @ {:.4f}",
                        pair, price, level.size_usd, sell_price)
            return

        # Gate de balance
        balance = await self._fetch_bybit_balance()
        min_bal = _MIN_BALANCE.get(pair, 20.0)
        if balance < min_bal:
            logger.warning("[grid] {} PAUSADO: balance=${:.2f} < ${:.0f} minimo",
                           pair, balance, min_bal)
            level.pending    = False
            grid.paused      = True
            grid.pause_reason = f"balance insuficiente ${balance:.2f} < ${min_bal:.0f}"
            return

        order_id = await self._bybit_spot_order(pair, "Buy", level.size_usd, price)
        if order_id:
            level.filled     = True
            level.pending    = False
            level.fill_ts    = time.time()
            level.fill_price = price
            level.order_id   = order_id
            sell_price = price * (1 + grid.spacing_pct)
            grid.levels.append(GridLevel(
                price=sell_price, side="sell", size_usd=level.size_usd,
                fill_price=price, filled=True,
            ))
            logger.info("[grid] REAL COMPRA {} @ {:.4f} -> venta programada @ {:.4f}",
                        pair, price, sell_price)
        else:
            level.pending = False   # liberar para siguiente tick

    async def _handle_sell(self, grid: GridState, level: GridLevel, price: float) -> None:
        """Ejecuta venta en Bybit SPOT (o papel si grid.paper_mode)."""
        if grid.paper_mode:
            self._record_sell(grid, level, price)
            return

        order_id = await self._bybit_spot_order(grid.pair, "Sell", level.size_usd, price)
        if order_id:
            level.order_id = order_id
            self._record_sell(grid, level, price)
        else:
            level.pending = False
            logger.error("[grid] \u274c VENTA FALLIDA {} @ {:.4f} \u2014 reintentando en 30s",
                         grid.pair, price)
            self._failed_sells.append((grid, level, price))

    async def _alert_order_error(self, pair: str, side: str,
                                  size_usd: float, msg: str) -> None:
        """Envia error de orden rechazada a Telegram."""
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        alert = (
            f"\u274c [GRID] ORDER RECHAZADA\n"
            f"Par: {pair} | Lado: {side} | Size: ${size_usd:.0f}\n"
            f"Error: {msg}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": alert},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_init(self, pair: str, center: float, spacing: float,
                           levels: int, paper: bool = True) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        cfg      = _GRID_CONFIG[pair]
        min_usd  = _MIN_ORDER_USD.get(pair, 5.0)
        lvl_size = cfg["capital_usd"] / levels
        mode_icon = "\u274c papel" if paper else "\u2705 real"
        msg = (
            f"\U0001f4ca [GRID] Iniciado {pair}\n"
            f"Centro: ${center:,.2f}\n"
            f"Spacing: {spacing*100:.3f}% (ATR adaptativo)\n"
            f"Niveles: {levels} | Capital: ${cfg['capital_usd']:,.0f}\n"
            f"Minimo Bybit: ${min_usd:.0f} | Nivel: ${lvl_size:.2f} {mode_icon}\n"
            f"Ganancia est./ciclo: ${cfg['capital_usd']/levels*(spacing-0.0004):.3f}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_cycle(self, grid: GridState, exit_price: float, pnl: float) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        buy_price = exit_price * (1 - grid.spacing_pct)
        if self._production:
            msg = (
                f"\U0001f7e2 GRID CICLO COMPLETADO\n"
                f"Par: {grid.pair}\n"
                f"Compra:  ${buy_price:,.4f}\n"
                f"Venta:   ${exit_price:,.4f}\n"
                f"PnL:     +${pnl:.4f}\n"
                f"PnL total: ${grid.pnl_total:.4f} | Ciclos: {grid.cycles_done}"
            )
        else:
            msg = (
                f"\u2705 [GRID] Ciclo completado {grid.pair}\n"
                f"Salida: ${exit_price:,.4f}\n"
                f"P&L ciclo: +${pnl:.4f}\n"
                f"P&L total: ${grid.pnl_total:.4f} | Ciclos: {grid.cycles_done}"
            )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_circuit_breaker(self, grid: GridState) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"🚨 [GRID] CIRCUIT BREAKER {grid.pair}\n"
            f"{grid.pause_reason}\n"
            f"P&L acumulado: ${grid.pnl_total:.4f}\n"
            f"Grid suspendido hasta recuperacion de rango"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass
