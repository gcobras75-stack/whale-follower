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

import aiohttp
from loguru import logger

import db_writer

# ── Config por par ────────────────────────────────────────────────────────────
_GRID_CONFIG: Dict[str, dict] = {
    "BTCUSDT": {
        "levels":        12,        # niveles de grid (6 arriba, 6 abajo)
        "capital_usd":   600.0,     # capital total asignado
        "min_spacing":   0.003,     # spacing minimo 0.3%
        "max_spacing":   0.010,     # spacing maximo 1.0%
        "atr_mult":      1.5,       # spacing = ATR * multiplicador
        "bb_period":     20,        # periodo Bollinger Bands
        "bb_std":        2.0,       # desviaciones estandar
    },
    "ETHUSDT": {
        "levels":        10,
        "capital_usd":   400.0,
        "min_spacing":   0.004,
        "max_spacing":   0.015,
        "atr_mult":      2.0,
        "bb_period":     20,
        "bb_std":        2.0,
    },
    "SOLUSDT": {
        "levels":        8,
        "capital_usd":   200.0,
        "min_spacing":   0.006,
        "max_spacing":   0.020,
        "atr_mult":      2.5,
        "bb_period":     20,
        "bb_std":        2.0,
    },
}

_FEES_PCT        = 0.0002    # 0.02% por lado (maker/taker Bybit)
_PAUSE_SIGMA     = 1.5       # pausar si precio > 1.5 sigma fuera de BB
_CIRCUIT_BREAKER = 0.03      # suspender si pierde >3% del capital asignado
_REBALANCE_HOURS = 4         # rebalancear grid cada 4 horas
_PRICE_HISTORY   = 200       # precios para calcular ATR y BB


@dataclass
class GridLevel:
    price:      float
    side:       str       # "buy" | "sell"
    size_usd:   float
    filled:     bool = False
    fill_ts:    float = 0.0
    fill_price: float = 0.0   # precio real al que se compró
    pnl:        float = 0.0


@dataclass
class GridState:
    pair:           str
    center_price:   float
    spacing_pct:    float
    levels:         List[GridLevel] = field(default_factory=list)
    capital_used:   float = 0.0
    pnl_total:      float = 0.0
    cycles_done:    int   = 0
    paused:         bool  = False
    pause_reason:   str   = ""
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
        self._initialized: Dict[str, bool] = {p: False for p in _GRID_CONFIG}

        mode = "REAL" if production else "PAPEL"
        logger.info("[grid] Iniciado en modo {} | pares={}", mode, list(_GRID_CONFIG.keys()))

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

        # Comprobar si el precio salio del rango BB → pausar
        bb_upper, bb_lower, _ = self._bollinger(pair)
        sigma = (bb_upper - bb_lower) / (2 * cfg["bb_std"])
        if price > bb_upper + sigma * _PAUSE_SIGMA or price < bb_lower - sigma * _PAUSE_SIGMA:
            grid.paused = True
            grid.pause_reason = f"precio fuera de rango BB x{_PAUSE_SIGMA}σ"
            logger.warning("[grid] {} PAUSADO: {}", pair, grid.pause_reason)
            return

        # Evaluar niveles del grid
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

        self._grids[pair] = GridState(
            pair         = pair,
            center_price = center,
            spacing_pct  = spacing,
            levels       = levels,
        )
        logger.info(
            "[grid] {} inicializado: centro={:.2f} spacing={:.3f}% niveles={}",
            pair, center, spacing * 100, n,
        )
        asyncio.create_task(self._alert_init(pair, center, spacing, n))

    def _rebalance_grid(self, grid: GridState, pair: str, price: float) -> None:
        """Recentrar el grid en el precio actual con spacing recalculado."""
        cfg     = _GRID_CONFIG[pair]
        spacing = self._calc_spacing(pair, cfg)
        n       = cfg["levels"]
        cap_per_level = cfg["capital_usd"] / n

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
        grid.pnl_total      = old_pnl   # preservar P&L acumulado

        logger.info("[grid] {} rebalanceado: nuevo centro={:.2f} spacing={:.3f}%",
                    pair, price, spacing * 100)

    # ── Level evaluation ──────────────────────────────────────────────────────

    def _evaluate_levels(self, grid: GridState, pair: str, price: float) -> None:
        for level in grid.levels:
            if level.filled:
                # Venta ya colocada — verificar si se ejecuta
                if level.side == "sell" and price >= level.price:
                    self._fill_sell(grid, level, price)
                continue

            # Nivel de COMPRA: precio baja hasta el nivel
            if level.side == "buy" and price <= level.price:
                self._fill_buy(grid, level, price, pair)

    def _fill_buy(self, grid: GridState, level: GridLevel, price: float, pair: str) -> None:
        level.filled     = True
        level.fill_ts    = time.time()
        level.fill_price = price

        # Convertir nivel de compra en nivel de venta en el siguiente grid arriba
        sell_price = price * (1 + grid.spacing_pct)
        sell_level = GridLevel(
            price=sell_price, side="sell", size_usd=level.size_usd,
            fill_price=price,   # guardar buy_price en el sell_level para el writer
        )
        grid.levels.append(sell_level)

        logger.info("[grid] {} COMPRA @ {:.4f} size=${:.0f} → venta programada @ {:.4f}",
                    pair, price, level.size_usd, sell_price)

    def _fill_sell(self, grid: GridState, level: GridLevel, price: float) -> None:
        level.filled = False   # resetear para poder usarse de nuevo

        # P&L de este ciclo: spacing - fees * 2
        pnl = level.size_usd * (grid.spacing_pct - _FEES_PCT * 2)
        grid.pnl_total  += pnl
        grid.cycles_done += 1

        logger.info("[grid] {} VENTA @ {:.4f} pnl_ciclo={:+.4f} total_pnl={:+.4f}",
                    grid.pair, price, pnl, grid.pnl_total)
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

    # ── ATR y Bollinger ───────────────────────────────────────────────────────

    def _calc_spacing(self, pair: str, cfg: dict) -> float:
        """Spacing = ATR(14) * multiplicador, dentro de [min, max]."""
        prices = list(self._prices[pair])
        if len(prices) < 15:
            return cfg["min_spacing"]

        # ATR aproximado con precios de cierre (ticks)
        changes = [abs(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, 15)]
        atr_pct = sum(changes) / len(changes)
        spacing = atr_pct * cfg["atr_mult"]
        return max(cfg["min_spacing"], min(cfg["max_spacing"], spacing))

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

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_init(self, pair: str, center: float, spacing: float, levels: int) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        cfg = _GRID_CONFIG[pair]
        msg = (
            f"📊 [GRID] Iniciado {pair}\n"
            f"Centro: ${center:,.2f}\n"
            f"Spacing: {spacing*100:.3f}% (ATR adaptativo)\n"
            f"Niveles: {levels} | Capital: ${cfg['capital_usd']:,.0f}\n"
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
        msg = (
            f"✅ [GRID] Ciclo completado {grid.pair}\n"
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
