# -*- coding: utf-8 -*-
"""
funding_arb.py -- Whale Follower Bot
Funding Rate Arbitrage: captura el pago de funding sin riesgo direccional.

Estrategia:
  Funding > +0.03% cada 8h  -> LONG perpetuo + SHORT spot (neutraliza direccion)
  Funding < -0.03% cada 8h  -> SHORT perpetuo + LONG spot

En papel: simula ambas patas y calcula P&L del funding puro.
En real:  ejecuta ordenes en Bybit (perpetuo) + OKX (spot) simultaneamente.

Ganancia tipica: 0.03-0.10% cada 8h = 0.09-0.30% diario = ~3-9% mensual
sin riesgo de mercado (posicion delta-neutral).
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

import base64
import hashlib
import hmac

import aiohttp
from loguru import logger

import config

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_FUNDING_PCT    = 0.03    # % minimo para abrir arb (cubre fees ~0.02%)
_POSITION_SIZE_USD  = 500.0   # tamano de cada pata en USD (papel)
_FUNDING_INTERVAL   = 8 * 3600  # cada 8 horas
_MIN_CAPITAL_USD    = 300.0   # capital minimo para operar en real
_REAL_SIZE_PCT      = 0.15    # 15% del capital real por pata
_BYBIT_FUNDING_URL  = (
    "https://api.bytick.com/v5/market/funding/history"
    "?category=linear&symbol=BTCUSDT&limit=1"
)
_SYMBOLS = ["BTCUSDT", "ETHUSDT"]


@dataclass
class FundingArbTrade:
    trade_id:      str
    symbol:        str
    direction:     str        # "long_perp" o "short_perp"
    funding_rate:  float      # % que se cobra por posicion
    size_usd:      float
    opened_at:     float = field(default_factory=time.time)
    status:        str   = "open"
    payments:      List[float] = field(default_factory=list)
    total_pnl:     float = 0.0
    production:    bool  = False   # True = dinero real


@dataclass
class FundingArbSnapshot:
    rate_pct:       float = 0.0
    signal:         str   = "neutral"   # "long_arb" | "short_arb" | "neutral"
    open_trades:    int   = 0
    total_pnl_usd:  float = 0.0
    next_settlement: float = 0.0       # unix timestamp del proximo pago
    stale:          bool  = True


class FundingArbEngine:
    """
    Detecta y simula arbitraje de funding rate.

    En modo papel: rastrea P&L como si las ordenes se ejecutaran.
    En modo real:  llama a executor_real para abrir posiciones reales.
    """

    def __init__(self, production: bool = False) -> None:
        self._production   = production
        self._trades: List[FundingArbTrade] = []
        self._last_rate:   float = 0.0
        self._last_update: float = 0.0
        self._next_settlement: float = 0.0
        self._real_capital_usd: float = 0.0
        self._paper_mode: bool = True

        mode = "REAL" if production else "PAPEL"
        logger.info("[funding_arb] Iniciado modo {} | min_funding={}% min_capital=${:.0f}",
                    mode, _MIN_FUNDING_PCT, _MIN_CAPITAL_USD)

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tarea de fondo: monitorea funding cada 5 minutos y capital cada hora."""
        await self._check_capital()
        _cap_counter = 0
        while True:
            await self._check_funding()
            await self._settle_payments()
            await asyncio.sleep(300)   # revisar cada 5 min
            _cap_counter += 1
            if _cap_counter >= 12:     # cada 12 ciclos = 1 hora
                await self._check_capital()
                _cap_counter = 0

    async def _check_capital(self) -> None:
        if not self._production:
            self._paper_mode = True
            self._real_capital_usd = config.PAPER_CAPITAL
            logger.info("[funding_arb] Capital=${:.0f} < ${:.0f} \u2192 papel (PRODUCTION=false)",
                        config.PAPER_CAPITAL, _MIN_CAPITAL_USD)
            return
        try:
            total = await self._fetch_real_capital()
            self._real_capital_usd = total
            if total < _MIN_CAPITAL_USD:
                self._paper_mode = True
                logger.warning("[funding_arb] Capital=${:.2f} < ${:.0f} \u2192 papel \U0001f512",
                               total, _MIN_CAPITAL_USD)
            else:
                prev = self._paper_mode
                self._paper_mode = False
                size = total * _REAL_SIZE_PCT
                logger.info("[funding_arb] Capital=${:.2f} >= ${:.0f} \u2192 REAL activado \u2705 tama\u00f1o=${:.2f}",
                            total, _MIN_CAPITAL_USD, size)
                if prev:
                    asyncio.create_task(self._alert_capital_activated(total))
        except Exception as exc:
            logger.warning("[funding_arb] _check_capital error: {} \u2014 mantener papel", exc)

    async def _fetch_real_capital(self) -> float:
        b = await self._fetch_bybit_balance()
        o = await self._fetch_okx_balance()
        return b + o

    async def _fetch_bybit_balance(self) -> float:
        from bybit_utils import fetch_usdt_balance
        return await fetch_usdt_balance(caller="funding_arb")

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
            logger.warning("[funding_arb] OKX balance error: {}", exc)
        return 0.0

    def snapshot(self) -> FundingArbSnapshot:
        open_trades = [t for t in self._trades if t.status == "open"]
        total_pnl   = sum(t.total_pnl for t in self._trades)
        return FundingArbSnapshot(
            rate_pct        = self._last_rate,
            signal          = self._signal(),
            open_trades     = len(open_trades),
            total_pnl_usd   = round(total_pnl, 4),
            next_settlement = self._next_settlement,
            stale           = (time.time() - self._last_update) > 600,
        )

    def active_summary(self) -> List[dict]:
        """Para el dashboard y comando /trades."""
        return [
            {
                "id":          t.trade_id[:8],
                "symbol":      t.symbol,
                "direction":   t.direction,
                "funding_pct": t.funding_rate,
                "size_usd":    t.size_usd,
                "payments":    len(t.payments),
                "pnl_usd":     round(t.total_pnl, 4),
                "status":      t.status,
            }
            for t in self._trades[-10:]
        ]

    # ── Core logic ────────────────────────────────────────────────────────────

    async def _check_funding(self) -> None:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(_BYBIT_FUNDING_URL, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status != 200:
                        return
                    data = await r.json(content_type=None)

            entries = data.get("result", {}).get("list", [])
            if not entries:
                return

            rate_raw  = float(entries[0].get("fundingRate", 0))
            rate_pct  = round(rate_raw * 100, 4)
            next_ts   = int(entries[0].get("nextFundingTime", 0)) / 1000

            self._last_rate       = rate_pct
            self._last_update     = time.time()
            self._next_settlement = next_ts

            logger.info("[funding_arb] BTCUSDT funding={:.4f}% next={}min",
                        rate_pct, int((next_ts - time.time()) / 60))

            signal = self._signal()
            if signal != "neutral":
                await self._open_arb("BTCUSDT", rate_pct, signal)

        except Exception as exc:
            logger.warning("[funding_arb] fetch error: {}", exc)

    def _signal(self) -> str:
        if self._last_rate >= _MIN_FUNDING_PCT:
            return "long_arb"    # funding positivo: longs pagan -> recibir siendo long
        if self._last_rate <= -_MIN_FUNDING_PCT:
            return "short_arb"   # funding negativo: shorts pagan -> recibir siendo short
        return "neutral"

    async def _open_arb(self, symbol: str, rate_pct: float, signal: str) -> None:
        # No abrir si ya hay un trade abierto para este simbolo
        open_symbols = {t.symbol for t in self._trades if t.status == "open"}
        if symbol in open_symbols:
            return

        direction = "long_perp" if signal == "long_arb" else "short_perp"
        is_real   = self._production and not self._paper_mode
        size      = (self._real_capital_usd * _REAL_SIZE_PCT if is_real
                     else _POSITION_SIZE_USD)

        trade = FundingArbTrade(
            trade_id     = str(uuid.uuid4()),
            symbol       = symbol,
            direction    = direction,
            funding_rate = rate_pct,
            size_usd     = size,
            production   = is_real,
        )

        if is_real:
            success = await self._execute_real(trade)
            if not success:
                return
        else:
            label = "PAPEL" if not self._production else f"PAPEL (capital ${self._real_capital_usd:.0f} < ${_MIN_CAPITAL_USD:.0f})"
            logger.info("[funding_arb] {}: abriendo {} {} ${:.0f} funding={:.4f}%",
                        label, direction, symbol, size, rate_pct)

        self._trades.append(trade)
        await self._alert_opened(trade)

    async def _settle_payments(self) -> None:
        """Cada 8h calcula el pago de funding recibido."""
        now = time.time()
        for trade in self._trades:
            if trade.status != "open":
                continue
            age_hours = (now - trade.opened_at) / 3600
            expected_payments = int(age_hours / 8)
            if expected_payments <= len(trade.payments):
                continue

            # Calcular pago: size_usd * funding_rate (como receptor)
            payment = trade.size_usd * abs(trade.funding_rate) / 100
            trade.payments.append(payment)
            trade.total_pnl += payment

            logger.info("[funding_arb] Pago funding {} {}: +${:.4f} (total: +${:.4f})",
                        trade.symbol, trade.trade_id[:8], payment, trade.total_pnl)

            # Cerrar si el funding rate ya no es favorable
            if abs(self._last_rate) < _MIN_FUNDING_PCT * 0.5:
                trade.status = "closed"
                logger.info("[funding_arb] Cerrando {} — funding ya no rentable", trade.symbol)
                await self._alert_closed(trade)

    # ── Real execution (para cuando sea dinero real) ──────────────────────────

    async def _execute_real(self, trade: FundingArbTrade) -> bool:
        """
        Placeholder para ejecucion con dinero real.
        Implementar cuando se configure capital real:
          1. Abrir LONG/SHORT perpetuo en Bybit
          2. Abrir posicion opuesta en spot (hedge)
        Por ahora solo loguea y retorna False para no ejecutar en real accidentalmente.
        """
        logger.warning(
            "[funding_arb] REAL execution no implementada aun — "
            "configurar BYBIT_API_KEY real y activar en produccion."
        )
        return False

    # ── Telegram alerts ───────────────────────────────────────────────────────

    async def _alert_capital_activated(self, capital: float) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        size = capital * _REAL_SIZE_PCT
        msg = (
            f"\u2705 [FUNDING ARB] activada en REAL\n"
            f"Capital: ${capital:.2f} >= ${_MIN_CAPITAL_USD:.0f} m\u00ednimo\n"
            f"Tama\u00f1o por pata: ${size:.2f}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_opened(self, trade: FundingArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        icon = "\u2705" if trade.production else "\U0001f4ca"
        mode = "REAL EJECUTADO" if trade.production else "Simulado"
        msg = (
            f"{icon} [FUNDING ARB] {mode}\n"
            f"Par: {trade.symbol}\n"
            f"Tipo: {trade.direction}\n"
            f"Funding: {trade.funding_rate:+.4f}% cada 8h\n"
            f"Tama\u00f1o: ${trade.size_usd:,.0f}\n"
            f"Ganancia esperada: ${trade.size_usd * abs(trade.funding_rate) / 100:.4f} / 8h\n"
            f"Ganancia mensual est.: ${trade.size_usd * abs(trade.funding_rate) / 100 * 90:.2f}"
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

    async def _alert_closed(self, trade: FundingArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"[FUNDING ARB] Posicion cerrada\n"
            f"Par: {trade.symbol} | {trade.direction}\n"
            f"Pagos recibidos: {len(trade.payments)}\n"
            f"P&L total: +${trade.total_pnl:.4f}"
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
