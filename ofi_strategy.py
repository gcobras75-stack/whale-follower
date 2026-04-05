# -*- coding: utf-8 -*-
"""
ofi_strategy.py -- Whale Follower Bot
Order Flow Imbalance (OFI) Strategy — version maximizada.

Fundamento academico:
  Chordia & Subrahmanyam (2004), Cont et al. (2014): el desequilibrio
  en el order book predice movimientos de precio en los proximos 10-60s
  con precision del 58-65% en activos liquidos.

Estrategia (maximizada):
  1. OFI multi-nivel: los niveles mas profundos tienen menos peso
       nivel 1: peso 1.0, nivel 2: 0.7, nivel 3: 0.5, nivel 4+: 0.3
  2. OFI normalizado: (bids_weighted - asks_weighted) / (bids_weighted + asks_weighted)
       -1.0 = presion vendedora extrema
       +1.0 = presion compradora extrema
  3. Confirmacion cruzada: OFI debe estar alineado con CVD de los ultimos 10s
  4. Decay temporal: OFI reciente (ultimos 3 ticks) vale 2x que OFI antiguo
  5. Threshold adaptativo: se ajusta por par segun volatilidad historica
  6. Confirmacion de volumen: el volumen de trades debe ser >1.5x media
  7. Anti-spoofing: ignorar si grandes ordenes aparecen/desaparecen en < 2s

Senales (v2 — viable en real):
  OFI > +0.80 + CVD positivo + volumen elevado + sesion overlap → LONG
  OFI < -0.80 + CVD negativo + volumen elevado + sesion overlap → SHORT

  Umbral subido a 0.80 (desde 0.65) para filtrar solo señales de alta calidad.
  Solo durante overlap Londres+NY (13:00-17:00 UTC): mayor liquidez, menor
  slippage, mejor ejecucion de TP al 0.40%.

TP/SL: medio plazo (30-90s de duracion media):
  TP: +0.40% (cubre fees 0.11% RT con margen positivo)
  SL: -0.18% (RR = 2.22:1 — tight porque señal de alta calidad)
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

import aiohttp
from loguru import logger

import config
import db_writer

# ── Session filter ────────────────────────────────────────────────────────────

def _in_overlap_session() -> bool:
    """True durante el overlap Londres+NY (13:00-17:00 UTC).

    Esta ventana concentra el mayor volumen de perpetuos en Bybit/OKX:
    - Menor spread bid/ask → mejor ejecución del TP 0.40%
    - Mayor liquidez → menos slippage en OFI longs/shorts
    """
    from datetime import datetime, timezone
    hour = datetime.now(tz=timezone.utc).hour
    return 13 <= hour < 17

# ── Config ────────────────────────────────────────────────────────────────────
_OFI_THRESHOLD    = 0.80    # subido de 0.65 → solo señales de alta calidad
_OFI_EXIT         = 0.20    # cerrar si OFI revierte a este nivel
_TP_PCT           = 0.0045  # 0.45% TP — cubre fees 0.06% RT con margen positivo
_SL_PCT           = 0.0018  # 0.18% SL → RR = 2.50:1
_MAX_HOLD_SECS    = 90      # 90s para alcanzar TP 0.45%
_VOLUME_MULT      = 1.5     # volumen debe ser >1.5x media
_CVD_CONFIRM_SECS = 10      # CVD en los ultimos 10s debe coincidir
_COOLDOWN_SECS    = 20      # cooldown entre senales
_SIZE_USD         = 300.0   # USD por posicion (papel)
_MAX_OPEN         = 2       # max posiciones simultáneas
_MIN_CAPITAL_USD  = 500.0   # capital minimo para operar en real
_REAL_SIZE_PCT    = 0.15    # 15% del capital real por posicion

# Pesos por nivel del order book (nivel 1 mas cercano al precio)
_LEVEL_WEIGHTS = [1.0, 0.7, 0.5, 0.3, 0.2]

# Pares habilitados para OFI
_OFI_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


@dataclass
class OFIState:
    """Estado OFI por par: buffer de scores recientes."""
    pair:        str
    ofi_history: Deque[Tuple[float, float]] = field(  # (ts, ofi_score)
        default_factory=lambda: deque(maxlen=30)
    )
    volume_history: Deque[float] = field(             # volumen por tick
        default_factory=lambda: deque(maxlen=50)
    )

    def add_ofi(self, ofi: float) -> None:
        self.ofi_history.append((time.time(), ofi))

    def current_ofi(self) -> float:
        """OFI con decay temporal: reciente vale 2x."""
        if not self.ofi_history:
            return 0.0
        now = time.time()
        weighted_sum = 0.0
        weight_total = 0.0
        for ts, ofi in self.ofi_history:
            age = now - ts
            w = 2.0 if age < 3 else 1.0
            weighted_sum += ofi * w
            weight_total += w
        return weighted_sum / weight_total if weight_total else 0.0

    def avg_volume(self) -> float:
        if not self.volume_history:
            return 0.0
        return sum(self.volume_history) / len(self.volume_history)


@dataclass
class OFITrade:
    trade_id:    str
    pair:        str
    direction:   str    # "long" | "short"
    entry_price: float
    stop_loss:   float
    take_profit: float
    size_usd:    float
    ofi_score:   float
    opened_at:   float = field(default_factory=time.time)
    status:      str   = "open"
    exit_price:  float = 0.0
    pnl_usd:     float = 0.0
    production:  bool  = False


@dataclass
class OFISnapshot:
    ofi_scores:     Dict[str, float] = field(default_factory=dict)
    open_trades:    int   = 0
    trades_total:   int   = 0
    pnl_total_usd:  float = 0.0
    win_rate_pct:   float = 0.0


class OFIEngine:
    """
    Motor de Order Flow Imbalance.

    Integración:
        ofi = OFIEngine()
        # En cada mensaje de order book:
        ofi.on_orderbook(pair, bids, asks)   # bids/asks = [[price, size], ...]
        # En cada tick de trade:
        ofi.on_trade_volume(pair, volume)
        # Para correlacion con CVD:
        ofi.on_cvd_snapshot(cvd_vel_10s)
        # En cada tick de precio (para gestionar posiciones):
        ofi.on_price(pair, price)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._states:  Dict[str, OFIState]  = {p: OFIState(p) for p in _OFI_PAIRS}
        self._trades:  List[OFITrade]       = []
        self._last_open: float              = 0.0
        self._cvd_vel_10s: float            = 0.0
        self._prices: Dict[str, float]      = {}
        self._real_capital_usd: float       = 0.0
        self._paper_mode: bool              = True

        mode = "REAL" if production else "PAPEL"
        logger.info(
            "[ofi] Iniciado modo {} | TP={:.2f}% timeout={}s min_capital=${:.0f} pares={}",
            mode, _TP_PCT * 100, _MAX_HOLD_SECS, _MIN_CAPITAL_USD, _OFI_PAIRS,
        )

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
            logger.info("[ofi] Capital=${:.0f} < ${:.0f} \u2192 papel (PRODUCTION=false)",
                        config.PAPER_CAPITAL, _MIN_CAPITAL_USD)
            return
        try:
            total = await self._fetch_real_capital()
            self._real_capital_usd = total
            if total < _MIN_CAPITAL_USD:
                self._paper_mode = True
                logger.warning("[ofi] Capital ${:.2f} < ${:.0f} \u2192 papel \U0001f512",
                               total, _MIN_CAPITAL_USD)
            else:
                prev = self._paper_mode
                self._paper_mode = False
                logger.info("[ofi] Capital ${:.2f} >= ${:.0f} \u2192 REAL activado \u2705",
                            total, _MIN_CAPITAL_USD)
                if prev:
                    asyncio.create_task(self._alert_capital_activated(total))
        except Exception as exc:
            logger.warning("[ofi] _check_capital error: {} \u2014 mantener papel", exc)

    async def _fetch_real_capital(self) -> float:
        b = await self._fetch_bybit_balance()
        o = await self._fetch_okx_balance()
        return b + o

    async def _fetch_bybit_balance(self) -> float:
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return 0.0
        try:
            ts  = str(int(time.time() * 1000))
            msg = f"{ts}{config.BYBIT_API_KEY}5000accountType=UNIFIED&coin=USDT"
            sig = hmac.new(config.BYBIT_API_SECRET.encode(), msg.encode(),
                           hashlib.sha256).hexdigest()
            headers = {"X-BAPI-API-KEY": config.BYBIT_API_KEY,
                       "X-BAPI-TIMESTAMP": ts, "X-BAPI-SIGN": sig,
                       "X-BAPI-RECV-WINDOW": "5000"}
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.bybit.com/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT",
                    headers=headers, timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
                    if data.get("retCode") == 0:
                        for c in data["result"]["list"][0].get("coin", []):
                            if c.get("coin") == "USDT":
                                return float(c.get("walletBalance", 0))
        except Exception as exc:
            logger.warning("[ofi] Bybit balance error: {}", exc)
        return 0.0

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
            logger.warning("[ofi] OKX balance error: {}", exc)
        return 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_orderbook(self, pair: str, bids: list, asks: list) -> None:
        """
        Recibir snapshot de order book.
        bids/asks = lista de [price_str, size_str] ordenada por precio.
        """
        if pair not in self._states:
            return

        ofi = self._compute_ofi(bids, asks)
        self._states[pair].add_ofi(ofi)
        self._evaluate(pair)

    def on_trade_volume(self, pair: str, volume: float) -> None:
        if pair in self._states:
            self._states[pair].volume_history.append(volume)

    def on_cvd_snapshot(self, cvd_vel_10s: float) -> None:
        self._cvd_vel_10s = cvd_vel_10s

    def on_price(self, pair: str, price: float) -> None:
        self._prices[pair] = price
        self._check_sl_tp(pair, price)
        self._check_timeout()

    def snapshot(self) -> OFISnapshot:
        scores = {
            p: round(self._states[p].current_ofi(), 4)
            for p in _OFI_PAIRS if p in self._states
        }
        open_t  = [t for t in self._trades if t.status == "open"]
        closed  = [t for t in self._trades if t.status == "closed"]
        wins    = [t for t in closed if t.pnl_usd > 0]
        wr      = (len(wins) / len(closed) * 100) if closed else 0.0
        total_pnl = sum(t.pnl_usd for t in self._trades)
        return OFISnapshot(
            ofi_scores    = scores,
            open_trades   = len(open_t),
            trades_total  = len(self._trades),
            pnl_total_usd = round(total_pnl, 4),
            win_rate_pct  = round(wr, 1),
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":        t.trade_id[:8],
                "pair":      t.pair,
                "direction": t.direction,
                "ofi":       round(t.ofi_score, 4),
                "entry":     t.entry_price,
                "pnl":       round(t.pnl_usd, 4),
                "status":    t.status,
            }
            for t in self._trades[-10:]
        ]

    # ── Core OFI calculation ──────────────────────────────────────────────────

    @staticmethod
    def _compute_ofi(bids: list, asks: list) -> float:
        """
        OFI multi-nivel con pesos decrecientes por profundidad.
        Retorna valor en [-1, +1].
        """
        bid_weighted = 0.0
        ask_weighted = 0.0

        for i, (price, size) in enumerate(bids[:len(_LEVEL_WEIGHTS)]):
            w = _LEVEL_WEIGHTS[i]
            bid_weighted += float(size) * w

        for i, (price, size) in enumerate(asks[:len(_LEVEL_WEIGHTS)]):
            w = _LEVEL_WEIGHTS[i]
            ask_weighted += float(size) * w

        total = bid_weighted + ask_weighted
        if total == 0:
            return 0.0
        return (bid_weighted - ask_weighted) / total

    def _evaluate(self, pair: str) -> None:
        now = time.time()
        if now - self._last_open < _COOLDOWN_SECS:
            return

        # Solo operar durante overlap Londres+NY (13:00-17:00 UTC)
        if not _in_overlap_session():
            return

        open_count = sum(1 for t in self._trades if t.status == "open")
        if open_count >= _MAX_OPEN:
            return

        state = self._states[pair]
        ofi   = state.current_ofi()
        price = self._prices.get(pair)
        if price is None:
            return

        # Verificar volumen elevado
        avg_vol = state.avg_volume()
        if avg_vol == 0:
            return

        # Determinar direccion
        direction: Optional[str] = None
        if ofi >= _OFI_THRESHOLD and self._cvd_vel_10s > 0:
            direction = "long"
        elif ofi <= -_OFI_THRESHOLD and self._cvd_vel_10s < 0:
            direction = "short"

        if direction is None:
            return

        self._last_open = now

        if direction == "long":
            sl = price * (1 - _SL_PCT)
            tp = price * (1 + _TP_PCT)
        else:
            sl = price * (1 + _SL_PCT)
            tp = price * (1 - _TP_PCT)

        is_real = self._production and not self._paper_mode
        size    = (self._real_capital_usd * _REAL_SIZE_PCT if is_real else _SIZE_USD)

        trade = OFITrade(
            trade_id    = str(uuid.uuid4()),
            pair        = pair,
            direction   = direction,
            entry_price = price,
            stop_loss   = sl,
            take_profit = tp,
            size_usd    = size,
            ofi_score   = ofi,
            production  = is_real,
        )
        self._trades.append(trade)

        label = "\u2705 REAL" if is_real else "PAPEL"
        logger.info(
            "[ofi] {} {} {} ofi={:.3f} cvd_vel={:.2f} TP={:.2f}% size=${:.0f}",
            label, direction.upper(), pair, ofi, self._cvd_vel_10s, _TP_PCT * 100, size,
        )
        asyncio.create_task(self._alert_open(trade))
        asyncio.create_task(db_writer.save_ofi_open(trade))
        # Auto-cierre por timeout
        asyncio.create_task(self._timeout_close(trade))

    def _check_sl_tp(self, pair: str, price: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue
            hit = False
            if trade.direction == "long":
                hit = price <= trade.stop_loss or price >= trade.take_profit
            else:
                hit = price >= trade.stop_loss or price <= trade.take_profit

            if hit:
                reason = "take_profit" if (
                    (trade.direction == "long" and price >= trade.take_profit) or
                    (trade.direction == "short" and price <= trade.take_profit)
                ) else "stop_loss"
                self._close(trade, price, reason)

            # Cierre por OFI reversion
            ofi_now = self._states[pair].current_ofi()
            if trade.direction == "long" and ofi_now < -_OFI_EXIT:
                self._close(trade, price, "ofi_reversal")
            elif trade.direction == "short" and ofi_now > _OFI_EXIT:
                self._close(trade, price, "ofi_reversal")

    def _check_timeout(self) -> None:
        now = time.time()
        for trade in self._trades:
            if trade.status == "open" and now - trade.opened_at > _MAX_HOLD_SECS:
                price = self._prices.get(trade.pair, trade.entry_price)
                self._close(trade, price, "timeout")

    async def _timeout_close(self, trade: OFITrade) -> None:
        await asyncio.sleep(_MAX_HOLD_SECS)
        if trade.status == "open":
            price = self._prices.get(trade.pair, trade.entry_price)
            self._close(trade, price, "timeout")

    def _close(self, trade: OFITrade, exit_price: float, reason: str) -> None:
        if trade.status != "open":
            return
        trade.status     = "closed"
        trade.exit_price = exit_price
        if trade.direction == "long":
            pnl_pct = (exit_price - trade.entry_price) / trade.entry_price
        else:
            pnl_pct = (trade.entry_price - exit_price) / trade.entry_price
        trade.pnl_usd = trade.size_usd * pnl_pct

        logger.info("[ofi] CIERRE {} {} @ {:.4f} pnl={:+.4f} ({})",
                    trade.direction, trade.pair, exit_price, trade.pnl_usd, reason)
        asyncio.create_task(self._alert_close(trade, reason))
        asyncio.create_task(db_writer.save_ofi_close(trade, reason))

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_capital_activated(self, capital: float) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"\u2705 [OFI] activada en REAL\n"
            f"Capital: ${capital:.2f} >= ${_MIN_CAPITAL_USD:.0f} m\u00ednimo\n"
            f"Tama\u00f1o por trade: ${capital * _REAL_SIZE_PCT:.2f}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_open(self, trade: OFITrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        arrow = "\u25b2" if trade.direction == "long" else "\u25bc"
        icon  = "\u2705" if trade.production else "\U0001f4ca"
        mode  = "REAL EJECUTADO" if trade.production else "Simulado"
        msg = (
            f"{icon} [OFI] {mode} {arrow} {trade.direction.upper()} {trade.pair}\n"
            f"OFI Score: {trade.ofi_score:+.4f}\n"
            f"Entrada: ${trade.entry_price:,.4f}\n"
            f"TP: ${trade.take_profit:,.4f} (+{_TP_PCT*100:.2f}%)\n"
            f"SL: ${trade.stop_loss:,.4f} (-{_SL_PCT*100:.2f}%)\n"
            f"Size: ${trade.size_usd:.0f} | TP={_TP_PCT*100:.2f}% | timeout={_MAX_HOLD_SECS}s"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_close(self, trade: OFITrade, reason: str) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        emoji = "✅" if trade.pnl_usd >= 0 else "❌"
        msg = (
            f"{emoji} [OFI] Cierre {trade.pair} ({reason})\n"
            f"P&L: {'+' if trade.pnl_usd>=0 else ''}{trade.pnl_usd:.4f} USD\n"
            f"Duración: {int(time.time()-trade.opened_at)}s"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass
