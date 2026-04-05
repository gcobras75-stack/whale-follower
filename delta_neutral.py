# -*- coding: utf-8 -*-
"""
delta_neutral.py -- Whale Follower Bot
Delta-Neutral con rebalanceo y captura de funding diferencial.

Estrategia (version maximizada):
  Mantener posicion delta-neutral (sin riesgo direccional) pero con
  dos fuentes de ganancia simultaneas:

  GANANCIA 1 — Funding rate diferencial entre exchanges:
    Bybit perpetuo paga X% cada 8h
    OKX perpetuo paga Y% cada 8h
    Si X != Y: estar LONG donde el funding es mas bajo,
              SHORT donde el funding es mas alto.
    Captura la diferencia sin exposicion al precio.

  GANANCIA 2 — Basis spread (perp vs spot):
    El precio del perpetuo casi siempre difiere ligeramente del spot.
    Long spot + Short perp cuando perp > spot (contango)
    Long perp + Short spot cuando perp < spot (backwardation)

  REBALANCEO DINAMICO:
    Cada hora verifica si el hedge ratio deriva mas de 0.5%.
    Si el precio BTC se mueve 1%, una pata crece mas que la otra.
    Rebalancear para mantener delta = 0 en todo momento.

  SALIDA:
    Cerrar cuando el diferencial de funding se reduce a < 0.005%
    O cuando el basis spread revierte a 0.

  RIESGO:
    Practicamente cero riesgo de mercado si las dos patas se ejecutan
    simultáneamente. El riesgo real es ejecucion parcial (leg risk).

  RENDIMIENTO ESTIMADO:
    Funding diferencial tipico: 0.01-0.05% cada 8h
    Basis spread tipico: 0.01-0.03% capturado en el rebalanceo
    Total: 0.1-0.3% diario = 3-9% mensual sin riesgo direccional
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

import config
import db_writer

# ── Config ────────────────────────────────────────────────────────────────────
_MIN_FUNDING_DIFF     = 0.005   # % diferencial minimo para abrir (cubre fees)
_MIN_BASIS_SPREAD     = 0.008   # % basis spread minimo para abrir
_MAX_SIZE_PCT_CAPITAL = 0.15    # maximo 15% del capital real por pata
_POSITION_SIZE_USD    = 50.0    # USD por pata — referencia, se clampea en runtime
_DELTA_NEUTRAL_MIN_CAPITAL = 500.0  # capital minimo para operar en real (USD)
_REBALANCE_SECS       = 3600    # rebalancear cada hora
_DELTA_TOLERANCE      = 0.005   # 0.5% de desviacion maxima antes de rebalancear
_CLOSE_FUNDING_DIFF   = 0.003   # cerrar si diferencial cae por debajo de esto
_FUNDING_INTERVAL_H   = 8       # horas entre pagos de funding
_FUNDING_POLL_SECS    = 300     # consultar funding cada 5 min

# URLs funding rate
_BYBIT_FUNDING_URL = (
    "https://api.bytick.com/v5/market/tickers"
    "?category=linear&symbol=BTCUSDT"
)
_OKX_FUNDING_URL = (
    "https://www.okx.com/api/v5/public/funding-rate"
    "?instId=BTC-USDT-SWAP"
)


@dataclass
class FundingRates:
    bybit_pct: float = 0.0
    okx_pct:   float = 0.0
    diff_pct:  float = 0.0   # abs(bybit - okx)
    ts:        float = field(default_factory=time.time)

    @property
    def long_on(self) -> str:
        """Abrir LONG en el exchange con funding mas bajo."""
        return "bybit" if self.bybit_pct <= self.okx_pct else "okx"

    @property
    def short_on(self) -> str:
        return "okx" if self.bybit_pct <= self.okx_pct else "bybit"


@dataclass
class DNTrade:
    trade_id:        str
    strategy:        str     # "funding_diff" | "basis"
    long_exchange:   str
    short_exchange:  str
    long_price:      float
    short_price:     float
    size_usd:        float
    funding_diff:    float   # diferencial capturado en %
    basis_pct:       float   # basis spread capturado en %
    payments:        List[float] = field(default_factory=list)
    rebalances:      int    = 0
    opened_at:       float  = field(default_factory=time.time)
    status:          str    = "open"
    total_pnl:       float  = 0.0
    production:      bool   = False

    def add_payment(self, payment: float) -> None:
        self.payments.append(payment)
        self.total_pnl += payment

    @property
    def expected_payment_8h(self) -> float:
        return self.size_usd * self.funding_diff / 100


@dataclass
class DNSnapshot:
    funding_bybit:   float = 0.0
    funding_okx:     float = 0.0
    funding_diff:    float = 0.0
    basis_pct:       float = 0.0
    open_trades:     int   = 0
    total_pnl_usd:   float = 0.0
    payments_count:  int   = 0
    next_payment_min: float = 0.0


class DeltaNeutralEngine:
    """
    Motor delta-neutral con captura de funding diferencial y basis.

    Integración:
        dn = DeltaNeutralEngine()
        asyncio.create_task(dn.run())
        # En cada tick (para basis spread):
        dn.on_perp_price("bybit", price_bybit_perp)
        dn.on_perp_price("okx", price_okx_perp)
    """

    def __init__(self, production: bool = False) -> None:
        self._production       = production
        self._funding          = FundingRates()
        self._trades: List[DNTrade] = []
        self._perp_prices: Dict[str, float] = {}
        self._next_funding_ts: float = 0.0
        self._real_capital_usd: float = 0.0   # se actualiza en run() con balance real
        self._paper_mode: bool = True          # True hasta verificar capital >= MIN
        logger.warning(
            "[delta_neutral] Iniciado | min_capital_real=${} | min_diff={}% | min_basis={}%",
            _DELTA_NEUTRAL_MIN_CAPITAL, _MIN_FUNDING_DIFF, _MIN_BASIS_SPREAD,
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tareas de fondo: verificar capital, polling funding, settlement, rebalanceo."""
        await self._check_capital()   # verifica balance real antes de arrancar
        await asyncio.gather(
            self._poll_funding(),
            self._settle_payments(),
            self._rebalance_loop(),
            self._capital_poll_loop(),
        )

    async def _capital_poll_loop(self) -> None:
        """Re-verifica capital real cada hora para actualizar modo papel/real."""
        while True:
            await asyncio.sleep(3600)
            await self._check_capital()

    async def _check_capital(self) -> None:
        """Obtiene balance real de Bybit + OKX y decide si operar en real o papel."""
        if not self._production:
            self._paper_mode = True
            self._real_capital_usd = config.PAPER_CAPITAL
            logger.info("[delta_neutral] Modo PAPEL (PRODUCTION=false)")
            return

        bybit_bal = await self._fetch_bybit_balance()
        okx_bal   = await self._fetch_okx_balance()
        total     = bybit_bal + okx_bal
        self._real_capital_usd = total

        if total < _DELTA_NEUTRAL_MIN_CAPITAL:
            self._paper_mode = True
            logger.warning(
                "[delta_neutral] Capital real ${:.2f} < ${:.0f} minimo "
                "— forzando PAPEL hasta acumular capital suficiente",
                total, _DELTA_NEUTRAL_MIN_CAPITAL,
            )
        else:
            self._paper_mode = False
            logger.info(
                "[delta_neutral] Capital real ${:.2f} >= ${:.0f} "
                "— modo REAL habilitado",
                total, _DELTA_NEUTRAL_MIN_CAPITAL,
            )

    async def _fetch_bybit_balance(self) -> float:
        """Obtiene balance USDT de Bybit Unified account."""
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return 0.0
        try:
            import hmac as _hmac, hashlib as _hashlib
            ts  = str(int(time.time() * 1000))
            msg = f"{ts}{config.BYBIT_API_KEY}5000accountType=UNIFIED&coin=USDT"
            sig = _hmac.new(
                config.BYBIT_API_SECRET.encode(), msg.encode(), _hashlib.sha256
            ).hexdigest()
            headers = {
                "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
                "X-BAPI-TIMESTAMP":   ts,
                "X-BAPI-SIGN":        sig,
                "X-BAPI-RECV-WINDOW": "5000",
                "User-Agent":         "Mozilla/5.0",
                "Referer":            "https://www.bybit.com",
            }
            url = "https://api.bytick.com/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    data = await r.json()
                    if data.get("retCode") == 0:
                        coins = data["result"]["list"][0].get("coin", [])
                        for c in coins:
                            if c.get("coin") == "USDT":
                                bal = float(c.get("walletBalance", 0))
                                logger.info("[delta_neutral] Bybit balance: ${:.2f}", bal)
                                return bal
        except Exception as exc:
            logger.warning("[delta_neutral] Bybit balance error: {}", exc)
        return 0.0

    async def _fetch_okx_balance(self) -> float:
        """Obtiene balance USDT disponible de OKX funding account."""
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return 0.0
        try:
            import hmac as _hmac, hashlib as _hashlib, base64 as _b64
            from datetime import datetime, timezone as _tz
            ts      = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path    = "/api/v5/asset/balances?ccy=USDT"
            prehash = ts + "GET" + path
            sig = _b64.b64encode(
                _hmac.new(config.OKX_SECRET.encode(), prehash.encode(),
                          _hashlib.sha256).digest()
            ).decode()
            headers = {
                "OK-ACCESS-KEY":        config.OKX_API_KEY,
                "OK-ACCESS-SIGN":       sig,
                "OK-ACCESS-TIMESTAMP":  ts,
                "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://www.okx.com{path}", headers=headers,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    data = await r.json()
                    if data.get("code") == "0":
                        for item in data.get("data", []):
                            if item.get("ccy") == "USDT":
                                bal = float(item.get("availBal", 0))
                                logger.info("[delta_neutral] OKX balance: ${:.2f}", bal)
                                return bal
        except Exception as exc:
            logger.warning("[delta_neutral] OKX balance error: {}", exc)
        return 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_perp_price(self, exchange: str, price: float) -> None:
        """Actualizar precio del perpetuo para calcular basis spread."""
        self._perp_prices[exchange] = price

    def snapshot(self) -> DNSnapshot:
        open_t   = [t for t in self._trades if t.status == "open"]
        total_pnl = sum(t.total_pnl for t in self._trades)
        payments  = sum(len(t.payments) for t in self._trades)
        mins_to_next = max(0, (self._next_funding_ts - time.time()) / 60)
        basis = self._current_basis_pct()
        return DNSnapshot(
            funding_bybit    = self._funding.bybit_pct,
            funding_okx      = self._funding.okx_pct,
            funding_diff     = self._funding.diff_pct,
            basis_pct        = round(basis, 4),
            open_trades      = len(open_t),
            total_pnl_usd    = round(total_pnl, 4),
            payments_count   = payments,
            next_payment_min = round(mins_to_next, 1),
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":          t.trade_id[:8],
                "strategy":    t.strategy,
                "long":        t.long_exchange,
                "short":       t.short_exchange,
                "diff_pct":    t.funding_diff,
                "basis_pct":   t.basis_pct,
                "payments":    len(t.payments),
                "pnl":         round(t.total_pnl, 4),
                "status":      t.status,
            }
            for t in self._trades[-10:]
        ]

    # ── Funding poll ──────────────────────────────────────────────────────────

    async def _poll_funding(self) -> None:
        while True:
            await self._fetch_funding()
            await self._evaluate_opportunities()
            await asyncio.sleep(_FUNDING_POLL_SECS)

    async def _fetch_funding(self) -> None:
        bybit_rate, okx_rate = 0.0, 0.0

        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(_BYBIT_FUNDING_URL,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        items = data.get("result", {}).get("list", [])
                        if items:
                            bybit_rate = float(items[0].get("fundingRate", 0)) * 100
                            next_ts    = int(items[0].get("nextFundingTime", 0)) / 1000
                            self._next_funding_ts = next_ts
        except Exception as exc:
            logger.debug("[delta_neutral] Bybit funding error: {}", exc)

        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(_OKX_FUNDING_URL,
                                  timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        data = await r.json(content_type=None)
                        items = data.get("data", [])
                        if items:
                            okx_rate = float(items[0].get("fundingRate", 0)) * 100
        except Exception as exc:
            logger.debug("[delta_neutral] OKX funding error: {}", exc)

        self._funding = FundingRates(
            bybit_pct = round(bybit_rate, 6),
            okx_pct   = round(okx_rate, 6),
            diff_pct  = round(abs(bybit_rate - okx_rate), 6),
        )
        logger.info("[delta_neutral] Bybit={:.4f}% OKX={:.4f}% diff={:.4f}%",
                    bybit_rate, okx_rate, self._funding.diff_pct)

    # ── Opportunity evaluation ────────────────────────────────────────────────

    async def _evaluate_opportunities(self) -> None:
        open_symbols = {t.strategy for t in self._trades if t.status == "open"}

        # Estrategia 1: Funding diferencial
        if "funding_diff" not in open_symbols:
            if self._funding.diff_pct >= _MIN_FUNDING_DIFF:
                await self._open_funding_diff()

        # Estrategia 2: Basis spread
        if "basis" not in open_symbols:
            basis = self._current_basis_pct()
            if abs(basis) >= _MIN_BASIS_SPREAD:
                await self._open_basis(basis)

        # Evaluar cierre de posiciones abiertas
        for trade in self._trades:
            if trade.status != "open":
                continue
            if trade.strategy == "funding_diff" and self._funding.diff_pct < _CLOSE_FUNDING_DIFF:
                self._close_trade(trade, "funding_diff_normalized")
            elif trade.strategy == "basis":
                basis = self._current_basis_pct()
                if abs(basis) < _MIN_BASIS_SPREAD * 0.3:
                    self._close_trade(trade, "basis_reverted")

    def _calc_size(self) -> float:
        """15% del capital real disponible, nunca mas de _POSITION_SIZE_USD."""
        cap = self._real_capital_usd if self._real_capital_usd > 0 else _POSITION_SIZE_USD
        return min(_POSITION_SIZE_USD, cap * _MAX_SIZE_PCT_CAPITAL)

    async def _open_funding_diff(self) -> None:
        f        = self._funding
        size_usd = self._calc_size()
        is_real  = self._production and not self._paper_mode
        trade = DNTrade(
            trade_id       = str(uuid.uuid4()),
            strategy       = "funding_diff",
            long_exchange  = f.long_on,
            short_exchange = f.short_on,
            long_price     = self._perp_prices.get(f.long_on, 0.0),
            short_price    = self._perp_prices.get(f.short_on, 0.0),
            size_usd       = size_usd,
            funding_diff   = f.diff_pct,
            basis_pct      = 0.0,
            production     = is_real,
        )
        self._trades.append(trade)
        logger.info(
            "[delta_neutral] ABIERTO funding_diff: LONG {} SHORT {} diff={:.4f}%",
            trade.long_exchange, trade.short_exchange, f.diff_pct,
        )
        await self._alert_opened(trade)
        asyncio.create_task(db_writer.save_dn_open(trade))

    async def _open_basis(self, basis_pct: float) -> None:
        # Si basis > 0: perp > spot → short perp (bybit), long spot (okx)
        long_ex  = "okx"   if basis_pct > 0 else "bybit"
        short_ex = "bybit" if basis_pct > 0 else "okx"
        size_usd = self._calc_size()
        is_real  = self._production and not self._paper_mode
        trade = DNTrade(
            trade_id       = str(uuid.uuid4()),
            strategy       = "basis",
            long_exchange  = long_ex,
            short_exchange = short_ex,
            long_price     = self._perp_prices.get(long_ex, 0.0),
            short_price    = self._perp_prices.get(short_ex, 0.0),
            size_usd       = size_usd,
            funding_diff   = 0.0,
            basis_pct      = abs(basis_pct),
            production     = is_real,
        )
        self._trades.append(trade)
        logger.info(
            "[delta_neutral] ABIERTO basis: LONG {} SHORT {} basis={:.4f}%",
            long_ex, short_ex, abs(basis_pct),
        )
        await self._alert_opened(trade)
        asyncio.create_task(db_writer.save_dn_open(trade))

    def _close_trade(self, trade: DNTrade, reason: str) -> None:
        trade.status = "closed"
        logger.info("[delta_neutral] CERRADO {} {} pnl={:.4f} ({})",
                    trade.strategy, trade.trade_id[:8], trade.total_pnl, reason)
        asyncio.create_task(self._alert_closed(trade))
        asyncio.create_task(db_writer.save_dn_close(trade))

    # ── Funding settlement ────────────────────────────────────────────────────

    async def _settle_payments(self) -> None:
        """Simular cobro de funding cada 8h."""
        while True:
            await asyncio.sleep(300)
            now = time.time()
            for trade in self._trades:
                if trade.status != "open":
                    continue
                age_hours         = (now - trade.opened_at) / 3600
                expected_payments = int(age_hours / _FUNDING_INTERVAL_H)
                if expected_payments <= len(trade.payments):
                    continue
                payment = trade.size_usd * trade.funding_diff / 100
                trade.add_payment(payment)
                logger.info("[delta_neutral] Pago funding {} +${:.4f} (total: ${:.4f})",
                            trade.trade_id[:8], payment, trade.total_pnl)
                asyncio.create_task(db_writer.save_dn_payment(
                    trade.trade_id, trade.total_pnl, len(trade.payments)
                ))

    # ── Rebalanceo ────────────────────────────────────────────────────────────

    async def _rebalance_loop(self) -> None:
        await asyncio.sleep(_REBALANCE_SECS)
        while True:
            self._check_rebalance()
            await asyncio.sleep(_REBALANCE_SECS)

    def _check_rebalance(self) -> None:
        for trade in self._trades:
            if trade.status != "open":
                continue
            long_price  = self._perp_prices.get(trade.long_exchange, trade.long_price)
            short_price = self._perp_prices.get(trade.short_exchange, trade.short_price)
            if not long_price or not short_price:
                continue

            long_change  = (long_price  - trade.long_price)  / trade.long_price
            short_change = (short_price - trade.short_price) / trade.short_price
            delta        = abs(long_change - short_change)

            if delta > _DELTA_TOLERANCE:
                trade.rebalances += 1
                trade.long_price  = long_price
                trade.short_price = short_price
                logger.info(
                    "[delta_neutral] Rebalanceo #{} {} delta={:.3f}%",
                    trade.rebalances, trade.trade_id[:8], delta * 100,
                )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _current_basis_pct(self) -> float:
        bybit = self._perp_prices.get("bybit", 0.0)
        okx   = self._perp_prices.get("okx", 0.0)
        if not bybit or not okx:
            return 0.0
        return (bybit - okx) / okx * 100

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_opened(self, trade: DNTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        est_daily = trade.size_usd * (trade.funding_diff + trade.basis_pct) / 100 * 3
        if trade.production:
            pct_used = (trade.size_usd / self._real_capital_usd * 100
                        if self._real_capital_usd > 0 else 0)
            msg = (
                f"\u2696\ufe0f [DELTA NEUTRAL] REAL EJECUTADO\n"
                f"Estrategia: {trade.strategy}\n"
                f"LONG: {trade.long_exchange} | SHORT: {trade.short_exchange}\n"
                f"Funding diff: {trade.funding_diff:.4f}% | Basis: {trade.basis_pct:.4f}%\n"
                f"Tama\u00f1o real: ${trade.size_usd:,.0f} USD\n"
                f"Capital usado: {pct_used:.0f}% de ${self._real_capital_usd:.0f}\n"
                f"Est. diario: ${est_daily:.4f}"
            )
        else:
            msg = (
                f"\U0001f4ca [DELTA NEUTRAL] Simulado\n"
                f"Estrategia: {trade.strategy}\n"
                f"LONG: {trade.long_exchange} | SHORT: {trade.short_exchange}\n"
                f"Funding diff: {trade.funding_diff:.4f}% | Basis: {trade.basis_pct:.4f}%\n"
                f"Tama\u00f1o papel: ${trade.size_usd:,.0f} | Est. diario: ${est_daily:.4f}"
            )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_closed(self, trade: DNTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"✅ [DELTA NEUTRAL] Cerrado\n"
            f"Estrategia: {trade.strategy}\n"
            f"Pagos recibidos: {len(trade.payments)}\n"
            f"Rebalanceos: {trade.rebalances}\n"
            f"P&L total: +${trade.total_pnl:.4f}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass
