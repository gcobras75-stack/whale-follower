"""
OKX Futures Grid — Grid trading en perpetuos USDT-M de OKX.

Ventajas vs spot:
  • Fees maker: 0.02% (vs 0.10% spot) → 5x más barato
  • Fees taker: 0.05% (vs 0.10% spot)
  • Net profit por ciclo: 0.3% - 0.04% fees = 0.26% (vs 0.10% en spot)

Activar en real: ENABLE_OKX_FUTURES=true + capital OKX >= $200

Pares soportados:
  ETH-USDT-SWAP  (1 contrato = 0.01 ETH ~ $21)
  DOGE-USDT-SWAP (1 contrato = 10 DOGE ~ $0.80)
"""

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

import aiohttp
from loguru import logger

import config

# ── Configuración ──────────────────────────────────────────────────────────────
_FUTURES_CONFIG: Dict[str, dict] = {
    "ETHUSDT": {
        "instId":      "ETH-USDT-SWAP",
        "ct_val":      0.01,           # 1 contrato = 0.01 ETH
        "levels":      4,
        "capital_usd": 25.0,           # $25 — ~1 contrato/nivel @ $21/contrato
        "min_spacing": 0.003,          # 0.3% — fees 0.04% → neto 0.26%
        "max_spacing": 0.020,
        "bb_period":   20,
    },
    "DOGEUSDT": {
        "instId":      "DOGE-USDT-SWAP",
        "ct_val":      10.0,           # 1 contrato = 10 DOGE
        "levels":      4,
        "capital_usd": 15.0,           # $15 — múltiples contratos/nivel
        "min_spacing": 0.003,
        "max_spacing": 0.030,
        "bb_period":   20,
    },
}

_FEES_MAKER        = 0.0002   # 0.02% maker OKX futures
_FEES_TAKER        = 0.0005   # 0.05% taker OKX futures
_PRICE_HISTORY     = 150
_CIRCUIT_BREAKER   = 0.04     # pausar si pierde >4% del capital
_REBALANCE_HOURS   = 6
_MIN_CAPITAL_REAL  = 200.0    # capital mínimo OKX para activar en real


@dataclass
class FuturesLevel:
    price:      float
    side:       str       # "buy" | "sell"
    size_usd:   float
    filled:     bool  = False
    fill_price: float = 0.0
    fill_ts:    float = 0.0
    pnl:        float = 0.0
    pending:    bool  = False


@dataclass
class FuturesGrid:
    pair:           str
    inst_id:        str
    center_price:   float
    spacing_pct:    float
    capital_usd:    float
    levels:         List[FuturesLevel] = field(default_factory=list)
    pnl_total:      float = 0.0
    cycles_done:    int   = 0
    paused:         bool  = False
    paper_mode:     bool  = True
    last_rebalance: float = field(default_factory=time.time)


class OKXFuturesGrid:
    """
    Grid trading en OKX perpetuos USDT-M.
    Modo paper por defecto; activa real con ENABLE_OKX_FUTURES=true.
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._real_enabled = (
            production
            and os.getenv("ENABLE_OKX_FUTURES", "false").lower() == "true"
        )
        self._prices:       Dict[str, Deque[float]] = {
            p: deque(maxlen=_PRICE_HISTORY) for p in _FUTURES_CONFIG
        }
        self._grids:        Dict[str, FuturesGrid] = {}
        self._initialized:  Dict[str, bool] = {p: False for p in _FUTURES_CONFIG}

        mode = "REAL" if self._real_enabled else "PAPEL"
        logger.info(
            "[okx_fut] Iniciado modo {} | pares={} | capital=${}",
            mode, list(_FUTURES_CONFIG.keys()),
            sum(c["capital_usd"] for c in _FUTURES_CONFIG.values()),
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_price(self, pair: str, price: float) -> None:
        if pair not in _FUTURES_CONFIG:
            return

        self._prices[pair].append(price)
        cfg = _FUTURES_CONFIG[pair]

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
            result[pair] = {
                "inst_id":  grid.inst_id,
                "center":   round(grid.center_price, 4),
                "spacing":  f"{grid.spacing_pct*100:.2f}%",
                "cycles":   grid.cycles_done,
                "pnl_usd":  round(grid.pnl_total, 4),
                "paused":   grid.paused,
                "mode":     "PAPEL" if grid.paper_mode else "REAL",
            }
            total_pnl += grid.pnl_total
        return {
            "grids":     result,
            "total_pnl": round(total_pnl, 4),
            "exchange":  "OKX-FUTURES",
            "fees":      "0.02% maker / 0.05% taker",
        }

    # ── Init / Rebalance ───────────────────────────────────────────────────────

    def _init_grid(self, pair: str, center: float) -> None:
        cfg     = _FUTURES_CONFIG[pair]
        spacing = self._calc_spacing(pair, cfg)
        n       = cfg["levels"]
        cap_per = cfg["capital_usd"] / n

        paper = not self._real_enabled

        levels = []
        for i in range(1, n // 2 + 1):
            levels.append(FuturesLevel(price=center * (1 - spacing * i), side="buy",  size_usd=cap_per))
        for i in range(1, n // 2 + 1):
            levels.append(FuturesLevel(price=center * (1 + spacing * i), side="sell", size_usd=cap_per))

        self._grids[pair] = FuturesGrid(
            pair         = pair,
            inst_id      = cfg["instId"],
            center_price = center,
            spacing_pct  = spacing,
            capital_usd  = cfg["capital_usd"],
            levels       = levels,
            paper_mode   = paper,
        )
        logger.info(
            "[okx_fut] {} SWAP grid: centro={:.4f} spacing={:.2f}% "
            "${:.1f}/lvl fees=0.02% modo={}",
            pair, center, spacing * 100, cap_per,
            "PAPEL" if paper else "REAL OKX-SWAP",
        )

    def _rebalance_grid(self, grid: FuturesGrid, pair: str, price: float) -> None:
        cfg     = _FUTURES_CONFIG[pair]
        spacing = self._calc_spacing(pair, cfg)
        n       = cfg["levels"]

        # Compounding: reinvertir 50% de profits
        base_capital = cfg["capital_usd"]
        if grid.pnl_total > 0:
            compound_add = min(grid.pnl_total * 0.5, base_capital * 0.5)
            new_capital  = base_capital + compound_add
            if abs(new_capital - grid.capital_usd) >= 0.01:
                logger.info("[okx_fut] {} compounding: ${:.2f} → ${:.2f}",
                            pair, grid.capital_usd, new_capital)
            grid.capital_usd = new_capital

        cap_per = grid.capital_usd / n
        levels  = []
        for i in range(1, n // 2 + 1):
            levels.append(FuturesLevel(price=price * (1 - spacing * i), side="buy",  size_usd=cap_per))
        for i in range(1, n // 2 + 1):
            levels.append(FuturesLevel(price=price * (1 + spacing * i), side="sell", size_usd=cap_per))

        old_pnl             = grid.pnl_total
        grid.center_price   = price
        grid.spacing_pct    = spacing
        grid.levels         = levels
        grid.pnl_total      = old_pnl
        grid.last_rebalance = time.time()

        logger.info("[okx_fut] {} rebalanceado: centro={:.4f} spacing={:.2f}% capital=${:.2f}",
                    pair, price, spacing * 100, grid.capital_usd)

    # ── Level evaluation ───────────────────────────────────────────────────────

    def _evaluate_levels(self, grid: FuturesGrid, pair: str, price: float) -> None:
        for level in grid.levels:
            if level.pending:
                continue
            if level.filled:
                if level.side == "sell" and price >= level.price:
                    self._fill_sell(grid, level, price)
                continue
            if level.side == "buy" and price <= level.price:
                self._fill_buy(grid, level, price, pair)

    def _fill_buy(self, grid: FuturesGrid, level: FuturesLevel, price: float, pair: str) -> None:
        if self._real_enabled and not grid.paper_mode:
            level.pending = True
            asyncio.create_task(self._handle_buy_real(grid, level, price, pair))
        else:
            level.filled     = True
            level.fill_price = price
            level.fill_ts    = time.time()
            sell_price = price * (1 + grid.spacing_pct)
            grid.levels.append(FuturesLevel(
                price=sell_price, side="sell", size_usd=level.size_usd,
                fill_price=price, filled=True,
            ))
            logger.info("[okx_fut] {} COMPRA PAPEL @ {:.4f} → venta @ {:.4f}",
                        pair, price, sell_price)

    def _fill_sell(self, grid: FuturesGrid, level: FuturesLevel, price: float) -> None:
        if self._real_enabled and not grid.paper_mode:
            level.pending = True
            asyncio.create_task(self._handle_sell_real(grid, level, price))
        else:
            self._record_sell(grid, level, price)

    def _record_sell(self, grid: FuturesGrid, level: FuturesLevel, price: float) -> None:
        fees_rt  = _FEES_MAKER * 2      # round-trip maker fees
        pnl      = level.size_usd * grid.spacing_pct - level.size_usd * fees_rt
        level.filled     = False
        level.pending    = False
        level.fill_price = price
        level.pnl        = pnl
        grid.pnl_total  += pnl
        grid.cycles_done += 1

        buy_price = price * (1 - grid.spacing_pct)
        grid.levels.append(FuturesLevel(price=buy_price, side="buy", size_usd=level.size_usd))

        logger.info("[okx_fut] {} VENTA @ {:.4f} pnl=${:.3f} total=${:.4f} fees=0.02%%",
                    grid.pair, price, pnl, grid.pnl_total)
        try:
            import alerts as _alerts
            _alerts.record_strategy_pnl("okx_futures", pnl)
        except Exception:
            pass

    # ── Real order execution ───────────────────────────────────────────────────

    async def _handle_buy_real(self, grid: FuturesGrid, level: FuturesLevel,
                                price: float, pair: str) -> None:
        try:
            cfg         = _FUTURES_CONFIG[pair]
            ct_val      = cfg["ct_val"]
            n_contracts = max(1, round(level.size_usd / (price * ct_val)))
            result      = await self._place_swap_order(
                grid.inst_id, "buy", n_contracts, price
            )
            if result:
                level.filled     = True
                level.fill_price = price
                level.fill_ts    = time.time()
                sell_price       = price * (1 + grid.spacing_pct)
                grid.levels.append(FuturesLevel(
                    price=sell_price, side="sell", size_usd=level.size_usd,
                    fill_price=price, filled=True,
                ))
                logger.info("[okx_fut] 🟢 REAL BUY {} @ {:.4f} contracts={}", pair, price, n_contracts)
                await self._alert(f"🟢 [OKX SWAP] BUY {pair} @ {price:.4f} → sell @ {sell_price:.4f}")
            else:
                logger.error("[okx_fut] ❌ BUY FAILED {} @ {:.4f}", pair, price)
        except Exception as exc:
            logger.error("[okx_fut] _handle_buy_real error: {}", exc)
        finally:
            level.pending = False

    async def _handle_sell_real(self, grid: FuturesGrid, level: FuturesLevel,
                                 price: float) -> None:
        try:
            cfg         = _FUTURES_CONFIG[grid.pair]
            ct_val      = cfg["ct_val"]
            n_contracts = max(1, round(level.size_usd / (price * ct_val)))
            result      = await self._place_swap_order(
                grid.inst_id, "sell", n_contracts, price
            )
            if result:
                self._record_sell(grid, level, price)
                logger.info("[okx_fut] 🟢 REAL SELL {} @ {:.4f}", grid.pair, price)
            else:
                logger.error("[okx_fut] ❌ SELL FAILED {} @ {:.4f}", grid.pair, price)
        except Exception as exc:
            logger.error("[okx_fut] _handle_sell_real error: {}", exc)
        finally:
            level.pending = False

    async def _place_swap_order(self, inst_id: str, side: str,
                                 sz: int, price: float) -> Optional[dict]:
        """Coloca orden market en OKX SWAP (perpetuo)."""
        import base64, hashlib, hmac, json, datetime

        api_key    = os.getenv("OKX_API_KEY", "")
        secret_key = os.getenv("OKX_SECRET", "")
        passphrase = os.getenv("OKX_PASSPHRASE", "")

        if not api_key:
            logger.error("[okx_fut] Sin credenciales OKX")
            return None

        ts   = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        body = json.dumps({
            "instId":  inst_id,
            "tdMode":  "cross",
            "side":    side,
            "ordType": "market",
            "sz":      str(sz),
        })
        msg  = ts + "POST" + "/api/v5/trade/order" + body
        sig  = base64.b64encode(
            hmac.new(secret_key.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()

        headers = {
            "OK-ACCESS-KEY":        api_key,
            "OK-ACCESS-SIGN":       sig,
            "OK-ACCESS-TIMESTAMP":  ts,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type":         "application/json",
        }
        url = "https://www.okx.com/api/v5/trade/order"
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, headers=headers, data=body,
                                  timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json()
                    if data.get("code") == "0":
                        return data["data"][0]
                    logger.error("[okx_fut] ORDER ERROR {}: {}", inst_id, data.get("msg", ""))
                    return None
        except Exception as exc:
            logger.error("[okx_fut] HTTP error: {}", exc)
            return None

    # ── Utils ──────────────────────────────────────────────────────────────────

    def _calc_spacing(self, pair: str, cfg: dict) -> float:
        """Spacing dinámico: 50% del BB_width, floor = min_spacing."""
        prices = list(self._prices[pair])
        if len(prices) < cfg["bb_period"] + 5:
            return cfg["min_spacing"]
        prices_sl = prices[-cfg["bb_period"]:]
        mid       = sum(prices_sl) / len(prices_sl)
        variance  = sum((p - mid) ** 2 for p in prices_sl) / len(prices_sl)
        std       = variance ** 0.5
        if mid > 0:
            bb_width = (4 * std) / mid
            return max(cfg["min_spacing"], min(cfg["max_spacing"], bb_width * 0.5))
        return cfg["min_spacing"]

    def _check_circuit_breaker(self, grid: FuturesGrid) -> bool:
        if grid.capital_usd > 0 and grid.pnl_total < -(grid.capital_usd * _CIRCUIT_BREAKER):
            grid.paused = True
            logger.warning("[okx_fut] {} CIRCUIT BREAKER pnl=${:.2f}", grid.pair, grid.pnl_total)
            return True
        return False

    async def _alert(self, msg: str) -> None:
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
