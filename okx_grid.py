# -*- coding: utf-8 -*-
"""
okx_grid.py — Grid Trading solo OKX (modo temporal)

Estrategia de grid en OKX mientras Railway IPs están bloqueadas por Bybit.
Usa OKX REST API que NO está afectada por el bloqueo.

Pares activos: ETH/USDT, SOL/USDT
Capital: ETH $25, SOL $20 desde balance OKX
Exchange: OKX SPOT — mínimo $1/orden (vs Bybit mínimo $5-$67)
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

import aiohttp
from loguru import logger

import config

# ── Configuración por par ─────────────────────────────────────────────────────
_GRID_CONFIG: Dict[str, dict] = {
    "ETHUSDT": {
        "levels":      4,
        "capital_usd": 25.0,    # $25 OKX — $6.25/nivel
        "min_spacing": 0.008,
        "max_spacing": 0.018,
        "atr_mult":    1.5,
        "bb_period":   20,
    },
    "SOLUSDT": {
        "levels":      4,
        "capital_usd": 20.0,    # $20 OKX — $5/nivel
        "min_spacing": 0.010,
        "max_spacing": 0.022,
        "atr_mult":    2.0,
        "bb_period":   20,
    },
}

_MIN_ORDER_OKX     = 1.0      # OKX SPOT mínimo ~$1 USD
_FEES_PCT          = 0.001    # 0.1% OKX taker fee
_PRICE_HISTORY     = 100
_CIRCUIT_BREAKER   = 0.04     # suspender si pierde >4% del capital asignado
_REBALANCE_HOURS   = 6        # recentrar grid cada 6h


@dataclass
class OKXLevel:
    price:      float
    side:       str       # "buy" | "sell"
    size_usd:   float
    filled:     bool  = False
    fill_price: float = 0.0
    fill_ts:    float = 0.0
    order_id:   str   = ""
    pending:    bool  = False
    pnl:        float = 0.0


@dataclass
class OKXGridState:
    pair:           str
    center_price:   float
    spacing_pct:    float
    capital_usd:    float
    levels:         List[OKXLevel] = field(default_factory=list)
    pnl_total:      float = 0.0
    cycles_done:    int   = 0
    paper_mode:     bool  = False
    paused:         bool  = False
    pause_reason:   str   = ""
    created_at:     float = field(default_factory=time.time)
    last_rebalance: float = field(default_factory=time.time)


class OKXGridEngine:
    """
    Grid trader en OKX SPOT.

    Integración en main.py:
        okx_grid = OKXGridEngine(production=prod)
        # En cada tick de precio:
        okx_grid.on_price(trade.pair, trade.price)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._grids:       Dict[str, OKXGridState] = {}
        self._prices:      Dict[str, Deque[float]] = {
            p: deque(maxlen=_PRICE_HISTORY) for p in _GRID_CONFIG
        }
        self._initialized: Dict[str, bool] = {p: False for p in _GRID_CONFIG}
        self._okx_exec = None

        if production:
            self._init_okx()

        mode = "REAL" if production else "PAPEL"
        logger.info(
            "[okx_grid] Iniciado modo {} | pares={} | capital_total=${}",
            mode, list(_GRID_CONFIG.keys()),
            sum(c["capital_usd"] for c in _GRID_CONFIG.values()),
        )

    def _init_okx(self) -> None:
        try:
            from okx_executor import OKXExecutor
            self._okx_exec = OKXExecutor()
            logger.info("[okx_grid] OKXExecutor listo ✅")
        except Exception as exc:
            logger.error("[okx_grid] Error iniciando OKXExecutor: {}", exc)

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_price(self, pair: str, price: float) -> None:
        """Llamar en cada tick de precio (desde trading_loop en main.py)."""
        if pair not in _GRID_CONFIG:
            return

        self._prices[pair].append(price)

        cfg = _GRID_CONFIG[pair]
        if len(self._prices[pair]) < cfg["bb_period"] + 5:
            return

        if not self._initialized[pair]:
            self._init_grid(pair, price)
            self._initialized[pair] = True
            return

        grid = self._grids.get(pair)
        if grid is None or grid.paused:
            return

        if self._check_circuit_breaker(grid):
            return

        if time.time() - grid.last_rebalance > _REBALANCE_HOURS * 3600:
            self._rebalance_grid(grid, pair, price)
            return

        self._evaluate_levels(grid, pair, price)

    def snapshot(self) -> dict:
        result = {}
        total_pnl = 0.0
        for pair, grid in self._grids.items():
            active = sum(1 for l in grid.levels if not l.filled and not l.pending)
            result[pair] = {
                "center":        round(grid.center_price, 2),
                "spacing":       f"{grid.spacing_pct*100:.2f}%",
                "cycles":        grid.cycles_done,
                "pnl_usd":       round(grid.pnl_total, 4),
                "active_levels": active,
                "paused":        grid.paused,
                "mode":          "PAPEL" if grid.paper_mode else "REAL",
            }
            total_pnl += grid.pnl_total
        return {
            "grids":     result,
            "total_pnl": round(total_pnl, 4),
            "exchange":  "OKX",
            "mode":      "REAL" if self._production else "PAPEL",
        }

    # ── Grid init / rebalance ─────────────────────────────────────────────────

    def _init_grid(self, pair: str, center: float) -> None:
        cfg         = _GRID_CONFIG[pair]
        spacing     = self._calc_spacing(pair, cfg)
        n           = cfg["levels"]
        cap         = cfg["capital_usd"]
        cap_per_lvl = cap / n

        meets_min = cap_per_lvl >= _MIN_ORDER_OKX
        paper = (not self._production) or (not meets_min) or (self._okx_exec is None)

        levels = []
        for i in range(1, n // 2 + 1):
            levels.append(OKXLevel(
                price    = center * (1 - spacing * i),
                side     = "buy",
                size_usd = cap_per_lvl,
            ))
        for i in range(1, n // 2 + 1):
            levels.append(OKXLevel(
                price    = center * (1 + spacing * i),
                side     = "sell",
                size_usd = cap_per_lvl,
            ))

        self._grids[pair] = OKXGridState(
            pair         = pair,
            center_price = center,
            spacing_pct  = spacing,
            capital_usd  = cap,
            levels       = levels,
            paper_mode   = paper,
        )
        logger.info(
            "[okx_grid] {} grid: centro={:.2f} spacing={:.2f}% "
            "niveles={} ${:.1f}/lvl modo={}",
            pair, center, spacing * 100, n, cap_per_lvl,
            "PAPEL" if paper else "REAL OKX",
        )

    def _rebalance_grid(self, grid: OKXGridState, pair: str, price: float) -> None:
        cfg         = _GRID_CONFIG[pair]
        spacing     = self._calc_spacing(pair, cfg)
        n           = cfg["levels"]
        cap_per_lvl = grid.capital_usd / n
        old_pnl     = grid.pnl_total

        levels = []
        for i in range(1, n // 2 + 1):
            levels.append(OKXLevel(price=price * (1 - spacing * i), side="buy",  size_usd=cap_per_lvl))
        for i in range(1, n // 2 + 1):
            levels.append(OKXLevel(price=price * (1 + spacing * i), side="sell", size_usd=cap_per_lvl))

        grid.center_price   = price
        grid.spacing_pct    = spacing
        grid.levels         = levels
        grid.pnl_total      = old_pnl
        grid.last_rebalance = time.time()

        logger.info("[okx_grid] {} rebalanceado: centro={:.2f} spacing={:.2f}%",
                    pair, price, spacing * 100)

    # ── Level evaluation ──────────────────────────────────────────────────────

    def _evaluate_levels(self, grid: OKXGridState, pair: str, price: float) -> None:
        for level in grid.levels:
            if level.pending:
                continue
            if level.filled:
                if level.side == "sell" and price >= level.price:
                    self._fill_sell(grid, level, price)
                continue
            if level.side == "buy" and price <= level.price:
                self._fill_buy(grid, level, price, pair)

    def _fill_buy(self, grid: OKXGridState, level: OKXLevel,
                  price: float, pair: str) -> None:
        if self._production and not grid.paper_mode:
            level.pending = True
            asyncio.create_task(self._handle_buy_real(grid, level, price, pair))
        else:
            level.filled     = True
            level.fill_price = price
            level.fill_ts    = time.time()
            sell_price = price * (1 + grid.spacing_pct)
            grid.levels.append(OKXLevel(
                price=sell_price, side="sell", size_usd=level.size_usd, fill_price=price,
            ))
            logger.info("[okx_grid] {} COMPRA PAPEL @ {:.4f} → venta @ {:.4f}",
                        pair, price, sell_price)

    def _fill_sell(self, grid: OKXGridState, level: OKXLevel, price: float) -> None:
        if self._production and not grid.paper_mode:
            level.pending = True
            asyncio.create_task(self._handle_sell_real(grid, level, price))
        else:
            self._record_sell(grid, level, price)

    def _record_sell(self, grid: OKXGridState, level: OKXLevel, price: float) -> None:
        pnl = level.size_usd * grid.spacing_pct - level.size_usd * _FEES_PCT * 2
        level.filled     = True
        level.fill_price = price
        level.fill_ts    = time.time()
        level.pnl        = pnl
        grid.pnl_total  += pnl
        grid.cycles_done += 1
        buy_price = price * (1 - grid.spacing_pct)
        grid.levels.append(OKXLevel(
            price=buy_price, side="buy", size_usd=level.size_usd,
        ))
        logger.info("[okx_grid] {} VENTA @ {:.4f} pnl_ciclo=${:.3f} total=${:.4f}",
                    grid.pair, price, pnl, grid.pnl_total)

    # ── Real execution via OKX ────────────────────────────────────────────────

    async def _handle_buy_real(self, grid: OKXGridState, level: OKXLevel,
                                price: float, pair: str) -> None:
        try:
            result = await self._okx_exec.market_order(pair, "buy", level.size_usd, price)
            if result:
                level.filled     = True
                level.fill_price = price
                level.fill_ts    = time.time()
                level.order_id   = result.get("order_id", "")
                sell_price = price * (1 + grid.spacing_pct)
                grid.levels.append(OKXLevel(
                    price=sell_price, side="sell", size_usd=level.size_usd, fill_price=price,
                ))
                logger.info("[okx_grid] 🟢 REAL BUY {} @ {:.4f} → sell @ {:.4f}",
                            pair, price, sell_price)
                await self._alert(f"🟢 [OKX GRID] BUY {pair} @ {price:.4f} → sell @ {sell_price:.4f} | ${level.size_usd:.0f}")
            else:
                logger.error("[okx_grid] ❌ BUY FAILED {} @ {:.4f}", pair, price)
        except Exception as exc:
            logger.error("[okx_grid] _handle_buy_real error: {}", exc)
        finally:
            level.pending = False

    async def _handle_sell_real(self, grid: OKXGridState, level: OKXLevel,
                                 price: float) -> None:
        try:
            result = await self._okx_exec.market_order(grid.pair, "sell", level.size_usd, price)
            if result:
                level.order_id = result.get("order_id", "")
                self._record_sell(grid, level, price)
                logger.info("[okx_grid] 🟢 REAL SELL {} @ {:.4f}", grid.pair, price)
                await self._alert(
                    f"🟢 [OKX GRID] SELL {grid.pair} @ {price:.4f} "
                    f"pnl=${level.pnl:.3f} total=${grid.pnl_total:.3f}"
                )
            else:
                logger.error("[okx_grid] ❌ SELL FAILED {} @ {:.4f}", grid.pair, price)
        except Exception as exc:
            logger.error("[okx_grid] _handle_sell_real error: {}", exc)
        finally:
            level.pending = False

    # ── Utils ─────────────────────────────────────────────────────────────────

    def _calc_spacing(self, pair: str, cfg: dict) -> float:
        prices = list(self._prices[pair])
        if len(prices) < 15:
            return cfg["min_spacing"]
        changes = [abs(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, 15)]
        atr_pct = sum(changes) / len(changes)
        spacing = atr_pct * cfg["atr_mult"]
        return max(cfg["min_spacing"], min(cfg["max_spacing"], spacing))

    def _check_circuit_breaker(self, grid: OKXGridState) -> bool:
        if grid.capital_usd > 0 and grid.pnl_total < -(grid.capital_usd * _CIRCUIT_BREAKER):
            grid.paused      = True
            grid.pause_reason = f"circuit breaker: pnl=${grid.pnl_total:.2f}"
            logger.warning("[okx_grid] {} CIRCUIT BREAKER: pnl=${:.2f}", grid.pair, grid.pnl_total)
            return True
        return False

    async def _alert(self, msg: str) -> None:
        import os
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=8),
                )
        except Exception:
            pass
