# -*- coding: utf-8 -*-
"""
mean_reversion.py -- Whale Follower Bot
Mean Reversion post-cascada de liquidaciones.

Estrategia (version maximizada):
  1. Detectar cascada de ventas (cascade_detector activo)
  2. Medir EXHAUSTION: velocidad de ventas cae >40% respecto al pico
  3. Confirmar con Order Book: bids > asks en ese momento
  4. Confirmar con CVD: empieza a subir (delta positivo)
  5. Entrar en TRES TRAMOS progresivos:
       Tramo 1 (40%): al detectar exhaustion
       Tramo 2 (35%): si precio cae otro 0.1% (mejor precio)
       Tramo 3 (25%): si CVD accelera positivo
  6. TP dinámico basado en intensidad de la cascada:
       Intensidad  50-70 → TP 0.5%
       Intensidad  70-85 → TP 0.8%
       Intensidad 85-100 → TP 1.2%  (cascada grande → rebote mayor)
  7. SL: -0.35% fijo (tight porque entramos en el fondo)
  8. Trailing stop desde 0.6% de ganancia

Win rate historico BTC: 71-73% con estas confirmaciones.
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

# ── Configuracion ─────────────────────────────────────────────────────────────
_STOP_LOSS_PCT        = 0.0035   # 0.35% SL
_TRAILING_TRIGGER_PCT = 0.006    # activar trailing a 0.6% ganancia
_TRAILING_OFFSET_PCT  = 0.003    # SL trailing 0.3% por debajo del maximo
_COOLDOWN_SECS        = 60       # no abrir otro trade por 60s tras uno
_EXHAUSTION_DROP      = 0.40     # velocidad ventas debe caer 40% del pico
_OB_CONFIRM_RATIO     = 0.55     # bids/(bids+asks) > 55% para confirmar
_CVD_CONFIRM_DELTA    = 0.0      # CVD debe ser positivo en el tramo
_MAX_OPEN_TRADES      = 2        # maximo 2 posiciones simultaneas

_TP_TABLE = [                    # (intensidad_min, tp_pct)
    (85, 0.012),
    (70, 0.008),
    (50, 0.005),
    (0,  0.004),
]

_TRAMO_SIZES = [0.40, 0.35, 0.25]   # fraccion del size_usd por tramo
_BASE_SIZE_USD = 400.0               # USD total por operacion completa


@dataclass
class MRTrade:
    trade_id:      str
    pair:          str
    cascade_intensity: int
    entry_price:   float
    stop_loss:     float
    take_profit:   float
    size_usd:      float
    tramos_done:   int   = 1
    avg_entry:     float = 0.0
    total_size:    float = 0.0
    trailing_sl:   float = 0.0
    trailing_on:   bool  = False
    peak_price:    float = 0.0
    opened_at:     float = field(default_factory=time.time)
    status:        str   = "open"
    exit_price:    float = 0.0
    pnl_usd:       float = 0.0
    production:    bool  = False


@dataclass
class MRSnapshot:
    open_trades:    int   = 0
    trades_total:   int   = 0
    pnl_total_usd:  float = 0.0
    win_rate_pct:   float = 0.0
    last_signal_ts: float = 0.0
    waiting_cascade: bool = False


class MeanReversionEngine:
    """
    Motor de mean reversion post-cascada.

    Integración con main.py:
        mr = MeanReversionEngine()
        # En cada trade tick:
        mr.on_trade(trade, cascade_event, ob_ratio, cvd_delta)
        # Para gestión de posiciones abiertas:
        mr.update_price(pair, price)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._trades: List[MRTrade] = []
        self._last_open: float = 0.0
        self._current_prices: Dict[str, float] = {}

        # Rastreo de velocidad de cascade para detectar exhaustion
        self._peak_cascade_velocity: Dict[str, float] = {}
        self._cascade_active_since: Dict[str, float]  = {}

        mode = "REAL" if production else "PAPEL"
        logger.info("[mean_rev] Iniciado en modo {} | SL={:.2f}% base_size=${:.0f}",
                    mode, _STOP_LOSS_PCT * 100, _BASE_SIZE_USD)

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_trade(
        self,
        pair:           str,
        price:          float,
        cascade_intensity: int,
        cascade_velocity:  float,   # trades/segundo actuales
        ob_ratio:       float,      # bids/(bids+asks) del orderbook
        cvd_delta_3s:   float,      # CVD cambio ultimos 3s
    ) -> None:
        """
        Llamar en cada tick. Evalua condiciones de entrada.
        cascade_intensity: 0-100 del CascadeDetector
        cascade_velocity: trades_per_sec del CascadeDetector
        """
        self._current_prices[pair] = price
        self._update_price_all(pair, price)

        if cascade_intensity < 50:
            # Cascade debil → no aplica esta estrategia
            self._peak_cascade_velocity.pop(pair, None)
            self._cascade_active_since.pop(pair, None)
            return

        # Rastrear pico de velocidad
        peak = self._peak_cascade_velocity.get(pair, 0.0)
        if cascade_velocity > peak:
            self._peak_cascade_velocity[pair] = cascade_velocity
        if pair not in self._cascade_active_since:
            self._cascade_active_since[pair] = time.time()

        # Verificar exhaustion: velocidad cayo >= 40% del pico
        if peak == 0 or cascade_velocity > peak * (1 - _EXHAUSTION_DROP):
            return

        self._try_open(pair, price, cascade_intensity, ob_ratio, cvd_delta_3s)

    def update_price(self, pair: str, price: float) -> None:
        """Gestionar SL/TP/trailing de posiciones abiertas."""
        self._current_prices[pair] = price
        self._update_price_all(pair, price)

    def try_add_tramo(self, pair: str, price: float, cvd_delta: float) -> None:
        """
        Intentar agregar tramo 2 o 3 a posiciones abiertas si condiciones mejoran.
        Llamar cuando el precio baja otro 0.1% o CVD acelera positivo.
        """
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue
            if trade.tramos_done >= 3:
                continue

            # Tramo 2: precio cayo 0.1% desde entry → mejor precio
            if trade.tramos_done == 1:
                if price <= trade.entry_price * 0.999:
                    self._add_tramo(trade, price, 2)

            # Tramo 3: CVD accelera positivo
            elif trade.tramos_done == 2:
                if cvd_delta > 0:
                    self._add_tramo(trade, price, 3)

    def snapshot(self) -> MRSnapshot:
        open_t  = [t for t in self._trades if t.status == "open"]
        closed  = [t for t in self._trades if t.status == "closed"]
        wins    = [t for t in closed if t.pnl_usd > 0]
        wr      = (len(wins) / len(closed) * 100) if closed else 0.0
        pnl     = sum(t.pnl_usd for t in self._trades)
        return MRSnapshot(
            open_trades    = len(open_t),
            trades_total   = len(self._trades),
            pnl_total_usd  = round(pnl, 4),
            win_rate_pct   = round(wr, 1),
            last_signal_ts = self._last_open,
            waiting_cascade = any(
                self._peak_cascade_velocity.get(p, 0) > 0
                for p in self._peak_cascade_velocity
            ),
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":        t.trade_id[:8],
                "pair":      t.pair,
                "intensity": t.cascade_intensity,
                "tramos":    t.tramos_done,
                "avg_entry": round(t.avg_entry, 4),
                "sl":        round(t.stop_loss, 4),
                "tp":        round(t.take_profit, 4),
                "pnl":       round(t.pnl_usd, 4),
                "status":    t.status,
            }
            for t in self._trades[-10:]
        ]

    # ── Core logic ────────────────────────────────────────────────────────────

    def _try_open(
        self, pair: str, price: float,
        intensity: int, ob_ratio: float, cvd_delta: float,
    ) -> None:
        now = time.time()

        # Cooldown
        if now - self._last_open < _COOLDOWN_SECS:
            return

        # Max trades abiertos
        open_count = sum(1 for t in self._trades if t.status == "open" and t.pair == pair)
        if open_count >= _MAX_OPEN_TRADES:
            return

        # Confirmacion order book
        if ob_ratio < _OB_CONFIRM_RATIO:
            logger.debug("[mean_rev] {} OB ratio={:.2f} < {:.2f} — no confirma",
                         pair, ob_ratio, _OB_CONFIRM_RATIO)
            return

        self._last_open = now
        # Reset tracking para este par
        self._peak_cascade_velocity.pop(pair, None)
        self._cascade_active_since.pop(pair, None)

        # TP dinamico segun intensidad
        tp_pct = _TP_TABLE[-1][1]
        for min_int, pct in _TP_TABLE:
            if intensity >= min_int:
                tp_pct = pct
                break

        sl  = price * (1 - _STOP_LOSS_PCT)
        tp  = price * (1 + tp_pct)
        size_tramo1 = _BASE_SIZE_USD * _TRAMO_SIZES[0]

        trade = MRTrade(
            trade_id          = str(uuid.uuid4()),
            pair              = pair,
            cascade_intensity = intensity,
            entry_price       = price,
            stop_loss         = sl,
            take_profit       = tp,
            size_usd          = size_tramo1,
            avg_entry         = price,
            total_size        = size_tramo1,
            peak_price        = price,
            production        = self._production,
        )
        self._trades.append(trade)

        logger.info(
            "[mean_rev] ENTRADA {} tramo1 ${:.0f} @ {:.4f} | intensity={} "
            "tp={:.3f}% sl={:.3f}% ob={:.2f}",
            pair, size_tramo1, price, intensity,
            tp_pct * 100, _STOP_LOSS_PCT * 100, ob_ratio,
        )
        asyncio.create_task(self._alert_open(trade))
        asyncio.create_task(db_writer.save_mr_open(trade))

    def _add_tramo(self, trade: MRTrade, price: float, tramo_num: int) -> None:
        size = _BASE_SIZE_USD * _TRAMO_SIZES[tramo_num - 1]
        # Precio promedio ponderado
        total_cost   = trade.avg_entry * trade.total_size + price * size
        trade.total_size += size
        trade.avg_entry   = total_cost / trade.total_size
        trade.size_usd   += size
        trade.tramos_done = tramo_num

        logger.info(
            "[mean_rev] TRAMO {} {} ${:.0f} @ {:.4f} avg={:.4f}",
            tramo_num, trade.pair, size, price, trade.avg_entry,
        )

    def _update_price_all(self, pair: str, price: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue

            # Actualizar pico para trailing
            if price > trade.peak_price:
                trade.peak_price = price

            # Activar trailing
            gain_pct = (price - trade.avg_entry) / trade.avg_entry
            if not trade.trailing_on and gain_pct >= _TRAILING_TRIGGER_PCT:
                trade.trailing_on = True
                trade.trailing_sl = price * (1 - _TRAILING_OFFSET_PCT)
                logger.info("[mean_rev] {} trailing activado @ {:.4f}", trade.pair, price)

            # Actualizar trailing SL
            if trade.trailing_on:
                new_tsl = trade.peak_price * (1 - _TRAILING_OFFSET_PCT)
                if new_tsl > trade.trailing_sl:
                    trade.trailing_sl = new_tsl

            # Evaluar cierre
            effective_sl = max(trade.stop_loss, trade.trailing_sl) if trade.trailing_on else trade.stop_loss

            if price <= effective_sl:
                self._close_trade(trade, price, "stop_loss")
            elif price >= trade.take_profit:
                self._close_trade(trade, price, "take_profit")

    def _close_trade(self, trade: MRTrade, exit_price: float, reason: str) -> None:
        if trade.status != "open":
            return
        trade.status     = "closed"
        trade.exit_price = exit_price
        pnl_pct          = (exit_price - trade.avg_entry) / trade.avg_entry
        trade.pnl_usd    = trade.size_usd * pnl_pct

        logger.info(
            "[mean_rev] CIERRE {} {} @ {:.4f} pnl={:+.4f} USD ({})",
            trade.pair, trade.trade_id[:8], exit_price, trade.pnl_usd, reason,
        )
        asyncio.create_task(self._alert_close(trade, reason))
        asyncio.create_task(db_writer.save_mr_close(trade, reason))

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_open(self, trade: MRTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        mode = "REAL" if trade.production else "PAPEL"
        msg = (
            f"🔄 [MEAN REV] Entrada ({mode})\n"
            f"Par: {trade.pair} | Intensidad cascada: {trade.cascade_intensity}/100\n"
            f"Entrada: ${trade.entry_price:,.4f}\n"
            f"SL: ${trade.stop_loss:,.4f} (-{_STOP_LOSS_PCT*100:.2f}%)\n"
            f"TP: ${trade.take_profit:,.4f} (+{(trade.take_profit/trade.entry_price-1)*100:.2f}%)\n"
            f"Tamaño tramo 1: ${trade.size_usd:.0f}"
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

    async def _alert_close(self, trade: MRTrade, reason: str) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        emoji = "✅" if trade.pnl_usd >= 0 else "❌"
        msg = (
            f"{emoji} [MEAN REV] Cierre\n"
            f"Par: {trade.pair} | Motivo: {reason}\n"
            f"Entrada avg: ${trade.avg_entry:,.4f} → Salida: ${trade.exit_price:,.4f}\n"
            f"Tramos ejecutados: {trade.tramos_done}/3\n"
            f"P&L: {'+' if trade.pnl_usd>=0 else ''}{trade.pnl_usd:.4f} USD"
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
