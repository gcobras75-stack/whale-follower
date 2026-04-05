# -*- coding: utf-8 -*-
"""
triangular_arb.py -- Whale Follower Bot
Arbitraje Cross-Exchange: BTC/USDT en Bybit Spot vs OKX Spot

Estrategia:
  Compara el precio de BTC/USDT en Bybit y OKX en tiempo real.
  Si la diferencia supera el coste de fees (0.10% x 2 patas = 0.20%):

    Ruta A (Bybit mas barato):
      1. Comprar BTC/USDT en Bybit Spot
      2. Vender BTC/USDT en OKX Spot

    Ruta B (OKX mas barato):
      1. Comprar BTC/USDT en OKX Spot
      2. Vender BTC/USDT en Bybit Spot

  Umbral minimo: net P&L > $0.01 por operacion.
  Tamano real: hasta $30 por ciclo.

En papel: simula las dos patas y registra spread capturado.
En real:  ejecuta ordenes simultaneas en Bybit (pybit) y OKX (REST API v5).
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

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_SPREAD_PCT    = 0.05    # spread minimo para detectar oportunidad (0.05%)
_MAX_SPREAD_PCT    = 1.5     # spread > 1.5% = dato anomalo
_POSITION_SIZE_USD = 300.0   # USD por ciclo en papel
_REAL_POSITION_USD = 30.0    # USD por ciclo en REAL (max $30)
_MIN_NET_PNL       = 0.01    # minimo P&L neto para EJECUTAR ($0.01)
_TAKER_FEE_PCT     = 0.10    # fee por pata en spot (Bybit/OKX taker 0.10%)
_COOLDOWN_SECS     = 10      # cooldown minimo entre ejecuciones
_PRICE_STALE_MS    = 2000    # datos stale si > 2s
_LOG_INTERVAL_SECS = 30      # frecuencia del log periodico de spread


@dataclass
class TriPrice:
    btc_usdt:     float = 0.0   # BTC/USDT en Bybit
    eth_usdt:     float = 0.0   # reservado
    eth_btc:      float = 0.0   # reservado
    btc_usdt_okx: float = 0.0   # BTC/USDT en OKX
    ts_ms:        float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class TriArbTrade:
    trade_id:       str
    route:          str       # "A" (buy Bybit/sell OKX) | "B" (buy OKX/sell Bybit)
    btc_usdt:       float     # precio Bybit
    eth_usdt:       float     # precio OKX (campo reutilizado)
    eth_btc_real:   float     # precio en exchange de compra
    eth_btc_impl:   float     # precio en exchange de venta
    spread_pct:     float
    size_usd:       float
    gross_pnl:      float
    net_pnl:        float
    opened_at:      float = field(default_factory=time.time)
    production:     bool  = False


@dataclass
class TriArbSnapshot:
    eth_btc_implied:   float = 0.0   # precio BTC en Bybit
    eth_btc_real:      float = 0.0   # precio BTC en OKX
    spread_pct:        float = 0.0
    opportunities_1h:  int   = 0
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    stale:             bool  = True


class TriangularArb:
    """
    Detecta y ejecuta arbitraje cross-exchange BTC/USDT (Bybit vs OKX).

    Uso:
        arb = TriangularArb(production=True)
        arb.update_btc_usdt(83500.0)       # precio BTC en Bybit
        arb.update_btc_usdt_okx(83600.0)   # precio BTC en OKX
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._prices: TriPrice = TriPrice()
        self._trades: List[TriArbTrade] = []
        self._opportunities: Deque[float] = deque(maxlen=500)
        self._last_check: float = 0.0
        self._last_log:   float = 0.0

        mode = "REAL" if production else "PAPEL"
        logger.info("[cross_arb] Iniciado en modo {} | deteccion={}% | min_net=${}",
                    mode, _MIN_SPREAD_PCT, _MIN_NET_PNL)

    # ── Public API ─────────────────────────────────────────────────────────────

    def update_btc_usdt(self, price: float) -> None:
        self._prices.btc_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_arb()

    def update_eth_usdt(self, price: float) -> None:
        self._prices.eth_usdt = price
        self._prices.ts_ms    = time.time() * 1000

    def update_eth_btc(self, price: float) -> None:
        """Reservado por compatibilidad — no usado en estrategia cross-exchange."""
        self._prices.eth_btc = price
        self._prices.ts_ms   = time.time() * 1000

    def update_btc_usdt_okx(self, price: float) -> None:
        """Precio BTC/USDT en OKX — dispara la deteccion de arbitraje."""
        self._prices.btc_usdt_okx = price
        self._prices.ts_ms        = time.time() * 1000
        self._check_arb()

    def snapshot(self) -> TriArbSnapshot:
        now    = time.time()
        p      = self._prices
        spread = self._cross_spread(p.btc_usdt, p.btc_usdt_okx)
        stale  = (time.time() * 1000 - p.ts_ms) > _PRICE_STALE_MS
        ops_1h = sum(1 for t in self._opportunities if now - t < 3600)
        return TriArbSnapshot(
            eth_btc_implied  = round(p.btc_usdt, 2),
            eth_btc_real     = round(p.btc_usdt_okx, 2),
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

    @staticmethod
    def _cross_spread(bybit: float, okx: float) -> float:
        """Spread porcentual entre los dos exchanges."""
        if bybit <= 0 or okx <= 0:
            return 0.0
        ref = min(bybit, okx)
        return abs(bybit - okx) / ref * 100

    def _check_arb(self) -> None:
        now = time.time()
        p   = self._prices

        if not p.btc_usdt or not p.btc_usdt_okx:
            return

        if (time.time() * 1000 - p.ts_ms) > _PRICE_STALE_MS:
            return

        spread = self._cross_spread(p.btc_usdt, p.btc_usdt_okx)

        # ─ Log periodico de estado (cada 30s) ────────────────────────────────────
        if now - self._last_log >= _LOG_INTERVAL_SECS:
            self._last_log = now
            if spread >= _MIN_SPREAD_PCT:
                sign = "+" if p.btc_usdt_okx >= p.btc_usdt else "-"
                logger.info(
                    "[cross_arb] 📊 Bybit={:.2f} OKX={:.2f} spread={}{:.4f}%",
                    p.btc_usdt, p.btc_usdt_okx, sign, spread,
                )
            else:
                logger.info(
                    "[cross_arb] ❌ spread insuficiente {:.4f}% < {}%",
                    spread, _MIN_SPREAD_PCT,
                )

        # ─ Deteccion: spread minimo 0.05% ──────────────────────────────────
        if spread < _MIN_SPREAD_PCT or spread > _MAX_SPREAD_PCT:
            return

        # ─ Cooldown entre ejecuciones ─────────────────────────────────────
        if now - self._last_check < _COOLDOWN_SECS:
            return

        # ─ Calcular P&L neto ────────────────────────────────────────────
        if p.btc_usdt <= p.btc_usdt_okx:
            route      = "A"   # Compra Bybit, Venta OKX
            buy_price  = p.btc_usdt
            sell_price = p.btc_usdt_okx
        else:
            route      = "B"   # Compra OKX, Venta Bybit
            buy_price  = p.btc_usdt_okx
            sell_price = p.btc_usdt

        size_real  = _REAL_POSITION_USD if self._production else _POSITION_SIZE_USD
        gross_pnl  = size_real * spread / 100
        fees       = size_real * _TAKER_FEE_PCT / 100 * 2
        net_pnl    = gross_pnl - fees

        if net_pnl < _MIN_NET_PNL:
            logger.debug(
                "[cross_arb] spread={:.4f}% net=${:.4f} < ${} — no ejecutar",
                spread, net_pnl, _MIN_NET_PNL,
            )
            return

        self._last_check = now
        self._opportunities.append(now)

        buy_ex  = "Bybit" if route == "A" else "OKX"
        sell_ex = "OKX"   if route == "A" else "Bybit"
        logger.info(
            "[cross_arb] 📊 Bybit={:.2f} OKX={:.2f} spread=+{:.4f}% → Compra {} + Venta {}",
            p.btc_usdt, p.btc_usdt_okx, spread, buy_ex, sell_ex,
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
            eth_usdt      = p.btc_usdt_okx,
            eth_btc_real  = buy_price,
            eth_btc_impl  = sell_price,
            spread_pct    = round(spread, 4),
            size_usd      = size_real,
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
        Ejecuta arbitraje cross-exchange real:
          Ruta A: Buy BTC en Bybit Spot + Sell BTC en OKX Spot
          Ruta B: Buy BTC en OKX Spot  + Sell BTC en Bybit Spot
        """
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            logger.warning("[cross_arb] Faltan BYBIT_API_KEY/BYBIT_API_SECRET")
            self._trades.append(trade)
            return
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            logger.warning("[cross_arb] Faltan credenciales OKX")
            self._trades.append(trade)
            return

        try:
            from pybit.unified_trading import HTTP as BybitHTTP
        except ImportError:
            logger.error("[cross_arb] pybit no disponible")
            self._trades.append(trade)
            return

        size_usd  = min(trade.size_usd, _REAL_POSITION_USD)
        btc_price = trade.eth_btc_real
        btc_qty   = round(size_usd / btc_price, 6) if btc_price > 0 else 0

        if btc_qty <= 0:
            logger.warning("[cross_arb] btc_qty <= 0, saltando")
            self._trades.append(trade)
            return

        buy_ex  = "Bybit" if trade.route == "A" else "OKX"
        sell_ex = "OKX"   if trade.route == "A" else "Bybit"
        logger.info(
            "[cross_arb] Ejecutando: Compra {} ${:.0f} @ {:.2f} + Venta {} @ {:.2f}",
            buy_ex, size_usd, trade.eth_btc_real, sell_ex, trade.eth_btc_impl,
        )

        session = BybitHTTP(testnet=False, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
        loop    = asyncio.get_event_loop()
        _qty    = btc_qty

        if trade.route == "A":
            bybit_fn = lambda: session.place_order(category="spot", symbol="BTCUSDT", side="Buy",  orderType="Market", qty=str(_qty))
            okx_side = "sell"
        else:
            bybit_fn = lambda: session.place_order(category="spot", symbol="BTCUSDT", side="Sell", orderType="Market", qty=str(_qty))
            okx_side = "buy"

        try:
            bybit_result, okx_result = await asyncio.gather(
                loop.run_in_executor(None, bybit_fn),
                self._okx_spot_order("BTC-USDT", okx_side, _qty),
                return_exceptions=True,
            )

            bybit_ok = isinstance(bybit_result, dict) and bybit_result.get("retCode") == 0

            if bybit_ok and okx_result is True:
                logger.info(
                    "[cross_arb] ✅ EJECUTADO Compra {} + Venta {} net=+${:.4f} spread={:.4f}%",
                    buy_ex, sell_ex, trade.net_pnl, trade.spread_pct,
                )
                trade.production = True
                try:
                    import alerts as _alerts
                    _alerts.record_arb_executed(trade.net_pnl)
                except Exception:
                    pass
            else:
                if not bybit_ok:
                    logger.error("[cross_arb] Bybit leg fallido: {}", bybit_result)
                if okx_result is not True:
                    logger.error("[cross_arb] OKX leg fallido: {}", okx_result)

        except Exception as exc:
            logger.error("[cross_arb] _execute_real exception: {}", exc)

        self._trades.append(trade)

    async def _okx_spot_order(self, inst_id: str, side: str, sz: float) -> bool:
        """Coloca una orden de mercado en OKX Spot. Retorna True si exitosa."""
        from datetime import datetime, timezone
        ts      = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        payload = {
            "instId":  inst_id,
            "tdMode": "cash",
            "side":    side,
            "ordType": "market",
            "sz":      str(round(sz, 6)),
        }
        body    = json.dumps(payload)
        prehash = ts + "POST" + "/api/v5/trade/order" + body
        sig     = base64.b64encode(
            hmac.new(config.OKX_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
        ).decode()
        headers = {
            "OK-ACCESS-KEY":        config.OKX_API_KEY,
            "OK-ACCESS-SIGN":       sig,
            "OK-ACCESS-TIMESTAMP":  ts,
            "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
            "Content-Type":         "application/json",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://www.okx.com/api/v5/trade/order",
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if data.get("code") == "0":
                        return True
                    logger.error("[cross_arb] OKX orden error: {}", data)
                    return False
        except Exception as exc:
            logger.error("[cross_arb] OKX request exception: {}", exc)
            return False

    # ── Telegram alert ────────────────────────────────────────────────────────

    async def _alert(self, trade: TriArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        mode = "REAL" if trade.production else "PAPEL"
        if trade.route == "A":
            route_desc = f"BUY BTC Bybit @ {trade.btc_usdt:.2f} → SELL BTC OKX @ {trade.eth_usdt:.2f}"
        else:
            route_desc = f"BUY BTC OKX @ {trade.eth_usdt:.2f} → SELL BTC Bybit @ {trade.btc_usdt:.2f}"
        msg = (
            f"[CROSS ARB] Oportunidad ({mode})\n"
            f"Ruta: {trade.route} — {route_desc}\n"
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
