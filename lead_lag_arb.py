# -*- coding: utf-8 -*-
"""
lead_lag_arb.py -- Whale Follower Bot
Lead-Lag Arbitrage: BTC spring predice movimiento en altcoins.

Estrategia:
  Cuando BTC detecta un spring (senial de inversion alcista),
  abrir LONG en ETH/SOL/BNB con probabilidad historica:
    ETH: 73% de seguir a BTC en < 60s
    SOL: 61% de seguir a BTC en < 90s
    BNB: 58% de seguir a BTC en < 90s

  Las posiciones se cierran automaticamente:
    - Take profit: entrada + lead_ratio * BTC_move
    - Stop loss:   entrada - 0.3% (riesgo minimo)

En papel: simula posiciones y calcula P&L al cierre.
En real:  ejecuta ordenes Bybit al detectar la senal.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import base64
import hashlib
import hmac

import aiohttp
from loguru import logger

import config

# ── Config ────────────────────────────────────────────────────────────────────
_LEAD_LAG_PAIRS: Dict[str, dict] = {
    "ETHUSDT": {"prob": 0.73, "window_secs": 60,  "lead_ratio": 0.85, "size_pct": 0.40},
    "SOLUSDT": {"prob": 0.61, "window_secs": 90,  "lead_ratio": 0.70, "size_pct": 0.30},
    "BNBUSDT": {"prob": 0.58, "window_secs": 90,  "lead_ratio": 0.60, "size_pct": 0.30},
}
_STOP_LOSS_PCT   = 0.003    # 0.3% stop loss
_MIN_BTC_MOVE    = 0.002    # BTC debe moverse >= 0.2% para que haya trailing TP
_COOLDOWN_SECS   = 120      # no abrir otro lote por 2 min
_MIN_CAPITAL_USD = 200.0    # capital minimo para operar en real
_REAL_SIZE_PCT   = 0.10     # 10% del capital real, repartido entre pares
# size_pct de _LEAD_LAG_PAIRS = fraccion del 10% real asignado a ese par
# Papel: tamanios fijos equivalentes
_PAPER_SIZES: Dict[str, float] = {"ETHUSDT": 300.0, "SOLUSDT": 150.0, "BNBUSDT": 150.0}


@dataclass
class LeadLagTrade:
    trade_id:     str
    pair:         str
    entry_price:  float
    stop_loss:    float
    take_profit:  float
    size_usd:     float
    btc_entry:    float
    opened_at:    float = field(default_factory=time.time)
    closed_at:    Optional[float] = None
    exit_price:   float = 0.0
    pnl_usd:      float = 0.0
    status:       str   = "open"   # "open" | "closed"
    production:   bool  = False
    order_id:     str   = ""       # Bybit orderId si se ejecuto en real


@dataclass
class LeadLagSnapshot:
    active_trades:     int   = 0
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    last_trigger_ts:   float = 0.0
    triggers_1h:       int   = 0


class LeadLagArb:
    """
    Escucha seniales BTC spring del main loop y abre posiciones en altcoins.

    Uso:
        arb = LeadLagArb(production=False)
        # Cada vez que BTC detecta spring:
        arb.on_btc_spring(btc_price=65000.0)
        # Cada tick de precio:
        arb.update_price("ETHUSDT", 3100.0)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._trades: List[LeadLagTrade] = []
        self._last_trigger: float = 0.0
        self._current_prices: Dict[str, float] = {}
        self._triggers: List[float] = []
        self._real_capital_usd: float = 0.0
        self._paper_mode: bool = True

        mode = "REAL" if production else "PAPEL"
        logger.info("[lead_lag] Iniciado modo {} | min_capital=${:.0f} size={}% pares={}",
                    mode, _MIN_CAPITAL_USD, int(_REAL_SIZE_PCT * 100),
                    list(_LEAD_LAG_PAIRS.keys()))

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tarea de fondo: verificar capital real cada hora."""
        await self._check_capital()
        while True:
            await asyncio.sleep(3600)
            await self._check_capital()

    async def _check_capital(self) -> None:
        if not self._production:
            self._paper_mode = True
            self._real_capital_usd = config.PAPER_CAPITAL
            logger.info("[lead_lag] Capital=${:.0f} < ${:.0f} \u2192 papel (PRODUCTION=false)",
                        config.PAPER_CAPITAL, _MIN_CAPITAL_USD)
            return
        try:
            total = await self._fetch_real_capital()
            self._real_capital_usd = total
            if total < _MIN_CAPITAL_USD:
                self._paper_mode = True
                logger.warning("[lead_lag] Capital=${:.2f} < ${:.0f} \u2192 papel \U0001f512",
                               total, _MIN_CAPITAL_USD)
            else:
                prev = self._paper_mode
                self._paper_mode = False
                size = total * _REAL_SIZE_PCT
                logger.info("[lead_lag] Capital=${:.2f} >= ${:.0f} \u2192 REAL activado \u2705 size=${:.2f}",
                            total, _MIN_CAPITAL_USD, size)
                if prev:
                    asyncio.create_task(self._alert_capital_activated(total))
        except Exception as exc:
            logger.warning("[lead_lag] _check_capital error: {} \u2014 mantener papel", exc)

    async def _fetch_real_capital(self) -> float:
        b = await self._fetch_bybit_balance()
        o = await self._fetch_okx_balance()
        return b + o

    async def _fetch_bybit_balance(self) -> float:
        from bybit_utils import fetch_usdt_balance
        return await fetch_usdt_balance(caller="lead_lag")

    async def _fetch_okx_balance(self) -> float:
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return 0.0
        try:
            from datetime import datetime, timezone as _tz
            ts = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path = "/api/v5/asset/balances?ccy=USDT"
            sig  = base64.b64encode(
                hmac.new(config.OKX_SECRET.encode(),
                         (ts + "GET" + path).encode(), hashlib.sha256).digest()
            ).decode()
            headers = {"OK-ACCESS-KEY": config.OKX_API_KEY, "OK-ACCESS-SIGN": sig,
                       "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE}
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://www.okx.com{path}", headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    data = await r.json()
                    if data.get("code") == "0":
                        for item in data.get("data", []):
                            if item.get("ccy") == "USDT":
                                return float(item.get("availBal", 0))
        except Exception as exc:
            logger.warning("[lead_lag] OKX balance error: {}", exc)
        return 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_btc_spring(self, btc_price: float) -> None:
        """Llamar cuando el spring detector de BTC emite senal."""
        now = time.time()
        if now - self._last_trigger < _COOLDOWN_SECS:
            return

        self._last_trigger = now
        self._triggers.append(now)
        logger.info("[lead_lag] BTC spring @ {:.2f} — abriendo altcoins", btc_price)

        is_real   = self._production and not self._paper_mode
        total_size = (self._real_capital_usd * _REAL_SIZE_PCT if is_real
                      else sum(_PAPER_SIZES.values()))

        for pair, cfg in _LEAD_LAG_PAIRS.items():
            entry = self._current_prices.get(pair)
            if not entry:
                logger.debug("[lead_lag] Sin precio para {}, skip", pair)
                continue

            sl      = entry * (1 - _STOP_LOSS_PCT)
            tp      = entry * (1 + cfg["lead_ratio"] * _MIN_BTC_MOVE * 3)
            size    = (total_size * cfg["size_pct"] if is_real
                       else _PAPER_SIZES[pair])

            trade = LeadLagTrade(
                trade_id    = str(uuid.uuid4()),
                pair        = pair,
                entry_price = entry,
                stop_loss   = sl,
                take_profit = tp,
                size_usd    = size,
                btc_entry   = btc_price,
                production  = is_real,
            )

            if is_real:
                asyncio.create_task(self._execute_real(trade))
            else:
                label = "PAPEL" if not self._production else f"PAPEL (capital ${self._real_capital_usd:.0f} < ${_MIN_CAPITAL_USD:.0f})"
                logger.info(
                    "[lead_lag] {} {} LONG ${:.0f} entry={:.4f} sl={:.4f} tp={:.4f}",
                    label, pair, size, entry, sl, tp,
                )
                self._trades.append(trade)

            # Programar cierre automatico por ventana de tiempo
            asyncio.create_task(self._auto_close(trade, cfg["window_secs"]))

    def update_price(self, pair: str, price: float) -> None:
        """Recibir tick de precio para gestionar SL/TP."""
        self._current_prices[pair] = price
        self._check_sl_tp(pair, price)

    def snapshot(self) -> LeadLagSnapshot:
        now = time.time()
        open_trades  = [t for t in self._trades if t.status == "open"]
        total_pnl    = sum(t.pnl_usd for t in self._trades if t.status == "closed")
        triggers_1h  = sum(1 for ts in self._triggers if now - ts < 3600)
        return LeadLagSnapshot(
            active_trades   = len(open_trades),
            trades_total    = len(self._trades),
            pnl_total_usd   = round(total_pnl, 4),
            last_trigger_ts = self._last_trigger,
            triggers_1h     = triggers_1h,
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":          t.trade_id[:8],
                "pair":        t.pair,
                "entry":       t.entry_price,
                "sl":          round(t.stop_loss, 4),
                "tp":          round(t.take_profit, 4),
                "size_usd":    t.size_usd,
                "status":      t.status,
                "pnl":         round(t.pnl_usd, 4),
            }
            for t in self._trades[-10:]
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _check_sl_tp(self, pair: str, price: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue
            if price <= trade.stop_loss:
                self._close_trade(trade, price, "stop_loss")
            elif price >= trade.take_profit:
                self._close_trade(trade, price, "take_profit")

    def _close_trade(self, trade: LeadLagTrade, exit_price: float, reason: str) -> None:
        if trade.status != "open":
            return
        trade.status    = "closed"
        trade.closed_at = time.time()
        trade.exit_price = exit_price
        pnl_pct  = (exit_price - trade.entry_price) / trade.entry_price
        trade.pnl_usd = trade.size_usd * pnl_pct

        logger.info(
            "[lead_lag] CIERRE {} {} exit={:.4f} pnl={:+.4f} USD ({})",
            trade.pair, trade.trade_id[:8], exit_price, trade.pnl_usd, reason,
        )
        asyncio.create_task(self._alert_close(trade, reason))
        if trade.production and trade.order_id:
            asyncio.create_task(self._close_real(trade, exit_price))

    async def _auto_close(self, trade: LeadLagTrade, window_secs: float) -> None:
        await asyncio.sleep(window_secs)
        if trade.status != "open":
            return
        price = self._current_prices.get(trade.pair, trade.entry_price)
        self._close_trade(trade, price, "timeout")

    # ── Real execution ────────────────────────────────────────────────────────

    async def _execute_real(self, trade: LeadLagTrade) -> None:
        """Ejecuta BUY spot en Bybit para el altcoin cuando capital >= $200."""
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            logger.error("[lead_lag] Faltan credenciales Bybit — abortando real")
            self._trades.append(trade)
            return

        pair  = trade.pair
        price = trade.entry_price
        if price <= 0:
            logger.error("[lead_lag] Precio invalido para {}: {}", pair, price)
            self._trades.append(trade)
            return

        qty = round(trade.size_usd / price, 6)
        min_qty = {"ETHUSDT": 0.005, "SOLUSDT": 0.1, "BNBUSDT": 0.01}.get(pair, 0.01)
        if qty < min_qty:
            logger.warning("[lead_lag] {} qty={} < min={} (${:.2f}) — papel",
                           pair, qty, min_qty, trade.size_usd)
            self._trades.append(trade)
            return

        ts       = str(int(__import__('time').time() * 1000))
        body_dict = {"category": "spot", "symbol": pair, "side": "Buy",
                     "orderType": "Market", "qty": str(qty), "timeInForce": "IOC"}
        import json as _json
        body_str = _json.dumps(body_dict, separators=(",", ":"))
        payload  = f"{ts}{config.BYBIT_API_KEY}5000{body_str}"
        sig      = hmac.new(config.BYBIT_API_SECRET.encode(),
                             payload.encode(), hashlib.sha256).hexdigest()
        headers  = {"X-BAPI-API-KEY": config.BYBIT_API_KEY, "X-BAPI-TIMESTAMP": ts,
                    "X-BAPI-SIGN": sig, "X-BAPI-RECV-WINDOW": "5000",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0", "Referer": "https://www.bybit.com"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://api.bybit.com/v5/order/create",
                    headers=headers, data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        trade.order_id  = data.get("result", {}).get("orderId", "")
                        trade.production = True
                        logger.info("[lead_lag] \U0001f7e2 REAL BUY {} ${:.0f} qty={} orderId={}",
                                    pair, trade.size_usd, qty, trade.order_id)
                    else:
                        logger.error("[lead_lag] Bybit ORDER RECHAZADA {} retCode={} msg={}",
                                     pair, data.get("retCode"), data.get("retMsg"))
                        trade.production = False
        except Exception as exc:
            logger.error("[lead_lag] _execute_real exception {}: {}", pair, exc)
            trade.production = False

        self._trades.append(trade)

    async def _close_real(self, trade: LeadLagTrade, exit_price: float) -> None:
        """Ejecuta SELL spot en Bybit para cerrar la posicion real."""
        if not trade.production or not trade.order_id:
            return
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return

        pair  = trade.pair
        price = exit_price if exit_price > 0 else trade.entry_price
        qty   = round(trade.size_usd / trade.entry_price, 6)
        min_qty = {"ETHUSDT": 0.005, "SOLUSDT": 0.1, "BNBUSDT": 0.01}.get(pair, 0.01)
        if qty < min_qty:
            logger.warning("[lead_lag] _close_real {} qty={} < min={} — skip", pair, qty, min_qty)
            return

        ts       = str(int(__import__('time').time() * 1000))
        body_dict = {"category": "spot", "symbol": pair, "side": "Sell",
                     "orderType": "Market", "qty": str(qty), "timeInForce": "IOC"}
        import json as _json
        body_str = _json.dumps(body_dict, separators=(",", ":"))
        payload  = f"{ts}{config.BYBIT_API_KEY}5000{body_str}"
        sig      = hmac.new(config.BYBIT_API_SECRET.encode(),
                             payload.encode(), hashlib.sha256).hexdigest()
        headers  = {"X-BAPI-API-KEY": config.BYBIT_API_KEY, "X-BAPI-TIMESTAMP": ts,
                    "X-BAPI-SIGN": sig, "X-BAPI-RECV-WINDOW": "5000",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0", "Referer": "https://www.bybit.com"}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://api.bybit.com/v5/order/create",
                    headers=headers, data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        close_id = data.get("result", {}).get("orderId", "")
                        logger.info("[lead_lag] \U0001f534 REAL SELL {} qty={} orderId={}",
                                    pair, qty, close_id)
                    else:
                        logger.error("[lead_lag] Bybit CLOSE RECHAZADA {} retCode={} msg={}",
                                     pair, data.get("retCode"), data.get("retMsg"))
        except Exception as exc:
            logger.error("[lead_lag] _close_real exception {}: {}", pair, exc)

    # ── Telegram alert ────────────────────────────────────────────────────────

    async def _alert_capital_activated(self, capital: float) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        size = capital * _REAL_SIZE_PCT
        msg = (
            f"\u2705 [LEAD-LAG ARB] activada en REAL\n"
            f"Capital: ${capital:.2f} >= ${_MIN_CAPITAL_USD:.0f} m\u00ednimo\n"
            f"Tama\u00f1o total: ${size:.2f} (10% capital)"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_close(self, trade: LeadLagTrade, reason: str) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        icon  = "\u2705" if trade.production else "\U0001f4ca"
        mode  = "REAL EJECUTADO" if trade.production else "Simulado"
        emoji = "\u2705" if trade.pnl_usd >= 0 else "\u274c"
        msg = (
            f"{emoji} [LEAD-LAG ARB] Cierre ({mode})\n"
            f"Par: {trade.pair}\n"
            f"Entrada: ${trade.entry_price:,.4f}\n"
            f"Salida:  ${trade.exit_price:,.4f}\n"
            f"Motivo: {reason}\n"
            f"P&L: {'+' if trade.pnl_usd >= 0 else ''}{trade.pnl_usd:.4f} USD"
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
