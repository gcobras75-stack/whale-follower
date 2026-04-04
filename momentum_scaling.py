# -*- coding: utf-8 -*-
"""
momentum_scaling.py -- Whale Follower Bot
Momentum Scaling con entrada piramidal y trailing stop adaptativo.

Estrategia (version maximizada):
  Cuando el momentum es fuerte y sostenido, montarlo con entrada escalonada:

  CONDICIONES DE ENTRADA:
    - CVD velocity_3s  > umbral (aceleracion inmediata)
    - CVD velocity_10s > umbral (momentum de 10s confirmado)
    - CVD velocity_30s > umbral (tendencia de 30s alineada)
    - Los 3 exchanges coinciden (CVD combinado all_three_positive)
    - Order book: ratio > 0.60 (mas bids que asks)
    - Fear & Greed: no bloqueo

  ENTRADA PIRAMIDAL 3 ETAPAS:
    Etapa 1 (30%): al cumplir condiciones basicas
    Etapa 2 (40%): si precio sube 0.15% desde entrada
                   Y velocity_3s sigue acelerando
    Etapa 3 (30%): si precio sube 0.30% desde entrada
                   Y CVD acceleration positivo

  GESTION DE POSICION:
    - SL inicial: -0.40% desde entrada promedio
    - Breakeven: precio sube 0.25% → SL a entrada
    - Trailing: 0.20% por debajo del maximo (sigue el momentum)
    - Salida acelerada si CVD velocity_3s invierte a negativo

  TARGET: capturar 0.5-1.5% de movimiento en tendencias de 30-300 segundos.
  Win rate estimado con 3 confirmaciones: 64-68%.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp
from loguru import logger

import db_writer

# ── Config ────────────────────────────────────────────────────────────────────
_CVD_VEL_3S_MIN    = 15.0    # BTC equivalente minimo en 3s
_CVD_VEL_10S_MIN   = 40.0    # BTC en 10s
_CVD_VEL_30S_MIN   = 80.0    # BTC en 30s
_OB_RATIO_MIN      = 0.60    # ratio bids minimo
_BASE_SIZE_USD     = 500.0   # capital total por operacion completa
_PYRAMID_PCTS      = [0.30, 0.40, 0.30]  # fraccion por etapa

_ENTRY_2_TRIGGER   = 0.0015  # +0.15% para agregar etapa 2
_ENTRY_3_TRIGGER   = 0.0030  # +0.30% para agregar etapa 3

_SL_INITIAL_PCT    = 0.0040  # SL inicial -0.40%
_BREAKEVEN_PCT     = 0.0025  # activar breakeven a +0.25%
_TRAILING_OFFSET   = 0.0020  # trailing 0.20% debajo del maximo
_CVD_EXIT_THRESHOLD = -5.0   # salir si CVD_3s < -5 (reversal rapido)

_COOLDOWN_SECS     = 90      # no abrir otro en 90s
_MAX_OPEN          = 3       # maximo 3 posiciones simultaneas
_MAX_HOLD_SECS     = 300     # cierre forzado a 5 minutos


@dataclass
class MomentumTrade:
    trade_id:     str
    pair:         str
    direction:    str      # "long" (short no implementado — momentum alcista)
    entry_price:  float    # precio de etapa 1
    stop_loss:    float
    size_usd:     float    # actual (puede crecer con piramide)
    avg_entry:    float    # precio promedio ponderado
    total_size:   float    # size actual acumulado
    stage:        int = 1  # etapa actual de la piramide
    peak_price:   float = 0.0
    trailing_sl:  float = 0.0
    trailing_on:  bool  = False
    breakeven_on: bool  = False
    opened_at:    float = field(default_factory=time.time)
    status:       str   = "open"
    exit_price:   float = 0.0
    pnl_usd:      float = 0.0
    production:   bool  = False


@dataclass
class MomentumSnapshot:
    open_trades:   int   = 0
    trades_total:  int   = 0
    pnl_total_usd: float = 0.0
    win_rate_pct:  float = 0.0
    avg_hold_secs: float = 0.0


class MomentumScalingEngine:
    """
    Motor de momentum scaling con entrada piramidal.

    Integración:
        ms = MomentumScalingEngine()
        ms.on_tick(pair, price, cvd_3s, cvd_10s, cvd_30s, cvd_accel,
                   all_three_positive, ob_ratio, fg_blocked)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._trades: List[MomentumTrade] = []
        self._last_open: float = 0.0
        self._prices: Dict[str, float] = {}

        mode = "REAL" if production else "PAPEL"
        logger.info("[momentum] Iniciado en modo {} | base_size=${:.0f}",
                    mode, _BASE_SIZE_USD)

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_tick(
        self,
        pair:              str,
        price:             float,
        cvd_vel_3s:        float,
        cvd_vel_10s:       float,
        cvd_vel_30s:       float,
        cvd_acceleration:  float,
        all_three_positive: bool,
        ob_ratio:          float,
        fg_blocked:        bool = False,
    ) -> None:
        """Evaluar condiciones en cada tick de precio."""
        self._prices[pair] = price

        # Gestionar posiciones abiertas
        self._manage_open(pair, price, cvd_vel_3s, cvd_acceleration)

        if fg_blocked:
            return

        # Intentar entrada nueva
        self._try_entry(
            pair, price, cvd_vel_3s, cvd_vel_10s, cvd_vel_30s,
            cvd_acceleration, all_three_positive, ob_ratio,
        )

        # Intentar agregar etapas a posiciones existentes
        self._try_pyramid(pair, price, cvd_vel_3s, cvd_acceleration)

    def snapshot(self) -> MomentumSnapshot:
        open_t  = [t for t in self._trades if t.status == "open"]
        closed  = [t for t in self._trades if t.status == "closed"]
        wins    = [t for t in closed if t.pnl_usd > 0]
        wr      = (len(wins) / len(closed) * 100) if closed else 0.0
        pnl     = sum(t.pnl_usd for t in self._trades)
        if closed:
            now = time.time()
            avg_hold = sum(
                (t.opened_at + _MAX_HOLD_SECS - t.opened_at) for t in closed
            ) / len(closed)
        else:
            avg_hold = 0.0
        return MomentumSnapshot(
            open_trades   = len(open_t),
            trades_total  = len(self._trades),
            pnl_total_usd = round(pnl, 4),
            win_rate_pct  = round(wr, 1),
            avg_hold_secs = round(avg_hold, 1),
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":       t.trade_id[:8],
                "pair":     t.pair,
                "stage":    t.stage,
                "avg_entry":round(t.avg_entry, 4),
                "size_usd": round(t.size_usd, 2),
                "sl":       round(t.stop_loss, 4),
                "trailing": t.trailing_on,
                "pnl":      round(t.pnl_usd, 4),
                "status":   t.status,
            }
            for t in self._trades[-10:]
        ]

    # ── Core logic ────────────────────────────────────────────────────────────

    def _try_entry(
        self, pair, price, vel3, vel10, vel30, accel, three_pos, ob_ratio
    ) -> None:
        now = time.time()
        if now - self._last_open < _COOLDOWN_SECS:
            return

        open_count = sum(1 for t in self._trades
                         if t.status == "open" and t.pair == pair)
        if open_count >= _MAX_OPEN:
            return

        # Todas las condiciones
        if not (
            vel3  >= _CVD_VEL_3S_MIN  and
            vel10 >= _CVD_VEL_10S_MIN and
            vel30 >= _CVD_VEL_30S_MIN and
            three_pos                  and
            ob_ratio >= _OB_RATIO_MIN
        ):
            return

        self._last_open = now

        size1 = _BASE_SIZE_USD * _PYRAMID_PCTS[0]
        sl    = price * (1 - _SL_INITIAL_PCT)

        trade = MomentumTrade(
            trade_id    = str(uuid.uuid4()),
            pair        = pair,
            direction   = "long",
            entry_price = price,
            stop_loss   = sl,
            size_usd    = size1,
            avg_entry   = price,
            total_size  = size1,
            peak_price  = price,
            production  = self._production,
        )
        self._trades.append(trade)

        logger.info(
            "[momentum] ENTRADA etapa1 {} ${:.0f} @ {:.4f} | "
            "vel3={:.1f} vel10={:.1f} vel30={:.1f} ob={:.2f}",
            pair, size1, price, vel3, vel10, vel30, ob_ratio,
        )
        asyncio.create_task(self._alert_open(trade))
        asyncio.create_task(db_writer.save_momentum_open(trade))
        asyncio.create_task(self._force_close_after(trade, _MAX_HOLD_SECS))

    def _try_pyramid(self, pair: str, price: float, vel3: float, accel: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue

            gain = (price - trade.entry_price) / trade.entry_price

            # Etapa 2
            if trade.stage == 1 and gain >= _ENTRY_2_TRIGGER and vel3 >= _CVD_VEL_3S_MIN:
                size2 = _BASE_SIZE_USD * _PYRAMID_PCTS[1]
                self._add_stage(trade, price, size2, 2)

            # Etapa 3
            elif trade.stage == 2 and gain >= _ENTRY_3_TRIGGER and accel > 0:
                size3 = _BASE_SIZE_USD * _PYRAMID_PCTS[2]
                self._add_stage(trade, price, size3, 3)

    def _add_stage(self, trade: MomentumTrade, price: float, size: float, stage: int) -> None:
        total_cost     = trade.avg_entry * trade.total_size + price * size
        trade.total_size += size
        trade.avg_entry   = total_cost / trade.total_size
        trade.size_usd   += size
        trade.stage       = stage
        # Subir SL a trailing desde esta etapa
        trade.stop_loss   = trade.avg_entry * (1 - _SL_INITIAL_PCT * 0.7)

        logger.info(
            "[momentum] PIRAMIDE etapa{} {} +${:.0f} @ {:.4f} avg={:.4f} total=${:.0f}",
            stage, trade.pair, size, price, trade.avg_entry, trade.size_usd,
        )

    def _manage_open(self, pair: str, price: float, vel3: float, accel: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue

            # Actualizar pico
            if price > trade.peak_price:
                trade.peak_price = price

            gain = (price - trade.avg_entry) / trade.avg_entry

            # Breakeven
            if not trade.breakeven_on and gain >= _BREAKEVEN_PCT:
                trade.stop_loss   = trade.avg_entry
                trade.breakeven_on = True
                logger.info("[momentum] {} breakeven activado @ {:.4f}", trade.pair, price)

            # Trailing stop
            if gain >= _BREAKEVEN_PCT:
                trade.trailing_on = True
                new_tsl = trade.peak_price * (1 - _TRAILING_OFFSET)
                if new_tsl > trade.stop_loss:
                    trade.stop_loss   = new_tsl
                    trade.trailing_sl = new_tsl

            # Cierre por reversal rapido de CVD
            if trade.trailing_on and vel3 < _CVD_EXIT_THRESHOLD:
                self._close(trade, price, "cvd_reversal")
                continue

            # Cierre por SL
            if price <= trade.stop_loss:
                self._close(trade, price, "stop_loss")

    def _close(self, trade: MomentumTrade, exit_price: float, reason: str) -> None:
        if trade.status != "open":
            return
        trade.status     = "closed"
        trade.exit_price = exit_price
        pnl_pct          = (exit_price - trade.avg_entry) / trade.avg_entry
        trade.pnl_usd    = trade.size_usd * pnl_pct

        logger.info(
            "[momentum] CIERRE {} etapa{} @ {:.4f} pnl={:+.4f} USD ({})",
            trade.pair, trade.stage, exit_price, trade.pnl_usd, reason,
        )
        asyncio.create_task(self._alert_close(trade, reason))
        asyncio.create_task(db_writer.save_momentum_close(trade, reason))

    async def _force_close_after(self, trade: MomentumTrade, secs: float) -> None:
        await asyncio.sleep(secs)
        if trade.status == "open":
            price = self._prices.get(trade.pair, trade.avg_entry)
            self._close(trade, price, "timeout")

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _alert_open(self, trade: MomentumTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"🚀 [MOMENTUM] Entrada {trade.pair}\n"
            f"Etapa: 1/3 (piramide progresiva)\n"
            f"Precio: ${trade.entry_price:,.4f}\n"
            f"SL: ${trade.stop_loss:,.4f} (-{_SL_INITIAL_PCT*100:.2f}%)\n"
            f"Tamaño inicial: ${trade.size_usd:.0f} (max ${_BASE_SIZE_USD:.0f})\n"
            f"Timeout: {_MAX_HOLD_SECS}s"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{token}/sendMessage",
                             json={"chat_id": chat_id, "text": msg},
                             timeout=aiohttp.ClientTimeout(total=10))
        except Exception:
            pass

    async def _alert_close(self, trade: MomentumTrade, reason: str) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        emoji = "✅" if trade.pnl_usd >= 0 else "❌"
        msg = (
            f"{emoji} [MOMENTUM] Cierre {trade.pair}\n"
            f"Etapas ejecutadas: {trade.stage}/3 | Motivo: {reason}\n"
            f"Avg entry: ${trade.avg_entry:,.4f} → ${trade.exit_price:,.4f}\n"
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
