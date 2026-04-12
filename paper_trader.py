# -*- coding: utf-8 -*-
"""
paper_trader.py — Whale Follower Bot
Paper trading paralelo con threshold bajo (35) para acumular datos ML.

Corre junto al bot real. Evalua TODAS las senales que pasan el pre-filtro
(score >= 5 en multi_pair) y abre trades simulados con threshold=35.
Cada trade cerrado alimenta ml_model.record_outcome().

No toca dinero real. No envía alertas Telegram (solo reporte diario).
Persiste en Supabase paper_trades con source="paper_parallel".
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from loguru import logger

import config


@dataclass
class PaperTrade:
    pair:       str
    score:      int
    entry:      float
    sl:         float
    tp:         float
    size_usd:   float
    regime:     str
    status:     str   = "open"    # open | won | lost
    pnl_usd:   float = 0.0
    created_at: float = field(default_factory=time.time)
    closed_at:  float = 0.0
    features:   Optional[dict] = None


class PaperTrader:
    """Paper trader paralelo — threshold bajo, ML rápido."""

    def __init__(self, threshold: int = 35, capital: float = 10_000.0) -> None:
        self._threshold = threshold
        self._capital   = capital
        self._risk_pct  = 0.01       # 1% por trade
        self._max_open  = 10         # max trades abiertos simultáneos
        self._trades: List[PaperTrade] = []
        self._ml_model  = None

        # Stats diarias
        self._day_start   = time.time()
        self._day_trades  = 0
        self._day_wins    = 0
        self._day_losses  = 0
        self._day_pnl     = 0.0

        logger.info(
            "[paper] Iniciado | threshold={} capital=${:,.0f} risk={}%",
            threshold, capital, self._risk_pct * 100,
        )

    # ── API pública ──────────────────────────────────────────────────────────

    def evaluate_signal(
        self,
        pair: str,
        score: int,
        entry: float,
        sl: float,
        tp: float,
        regime: str,
        features: Optional[dict] = None,
    ) -> Optional[PaperTrade]:
        """Evalúa una señal con threshold bajo. Retorna trade si se abre."""
        if score < self._threshold:
            return None

        open_count = sum(1 for t in self._trades if t.status == "open")
        if open_count >= self._max_open:
            return None

        # No duplicar: si ya hay trade abierto en el mismo par, skip
        if any(t.pair == pair and t.status == "open" for t in self._trades):
            return None

        size_usd = self._capital * self._risk_pct
        trade = PaperTrade(
            pair=pair, score=score, entry=entry,
            sl=sl, tp=tp, size_usd=size_usd,
            regime=regime, features=features,
        )
        self._trades.append(trade)

        logger.info(
            "[paper] Trade abierto: {} score={} entry={:.2f} "
            "SL={:.2f} TP={:.2f} size=${:.0f}",
            pair, score, entry, sl, tp, size_usd,
        )

        # Guardar en Supabase
        asyncio.create_task(self._save_open(trade))
        return trade

    def check_prices(self, current_prices: Dict[str, float]) -> List[PaperTrade]:
        """Revisa trades abiertos contra precios actuales. Retorna cerrados."""
        closed = []
        for trade in self._trades:
            if trade.status != "open":
                continue

            price = current_prices.get(trade.pair)
            if not price:
                continue

            if price >= trade.tp:
                tp_pct = (trade.tp - trade.entry) / trade.entry
                trade.pnl_usd  = trade.size_usd * tp_pct
                trade.status   = "won"
                trade.closed_at = time.time()
                self._day_wins += 1
            elif price <= trade.sl:
                sl_pct = (trade.entry - trade.sl) / trade.entry
                trade.pnl_usd  = -trade.size_usd * sl_pct
                trade.status   = "lost"
                trade.closed_at = time.time()
                self._day_losses += 1

            if trade.status != "open":
                self._day_pnl += trade.pnl_usd
                self._day_trades += 1
                closed.append(trade)

                logger.info(
                    "[paper] {} {} | entry={:.2f} exit={:.2f} PnL=${:+.2f} score={}",
                    trade.status.upper(), trade.pair,
                    trade.entry, price, trade.pnl_usd, trade.score,
                )

                # Alimentar ML
                self._feed_ml(trade)

                # Guardar cierre en Supabase
                asyncio.create_task(self._save_close(trade))

        return closed

    def daily_stats(self) -> dict:
        """Estadísticas del día actual."""
        total = self._day_wins + self._day_losses
        wr = (self._day_wins / total * 100) if total > 0 else 0
        return {
            "threshold":  self._threshold,
            "trades":     total,
            "wins":       self._day_wins,
            "losses":     self._day_losses,
            "win_rate":   round(wr, 1),
            "pnl":        round(self._day_pnl, 4),
            "open":       sum(1 for t in self._trades if t.status == "open"),
        }

    def reset_daily(self) -> dict:
        """Reset stats diarias y retorna las anteriores."""
        stats = self.daily_stats()
        self._day_start  = time.time()
        self._day_trades = 0
        self._day_wins   = 0
        self._day_losses = 0
        self._day_pnl    = 0.0
        return stats

    # ── ML feeding ───────────────────────────────────────────────────────────

    def _feed_ml(self, trade: PaperTrade) -> None:
        """Alimenta ml_model.record_outcome con el resultado."""
        try:
            if self._ml_model is None:
                from ml_model import MLModel
                self._ml_model = MLModel()
            if trade.features:
                self._ml_model.record_outcome(trade.features, trade.status == "won")
        except Exception as exc:
            logger.debug("[paper] ML feed error: {}", exc)

    # ── Supabase ─────────────────────────────────────────────────────────────

    async def _save_open(self, trade: PaperTrade) -> None:
        try:
            from db_writer import _client, _run, _now_ts
            await _run(lambda: _client().table("paper_trades").insert({
                "strategy":    "wyckoff",
                "pair":        trade.pair,
                "side":        "Buy",
                "entry_price": trade.entry,
                "stop_loss":   trade.sl,
                "take_profit": trade.tp,
                "size_usd":    trade.size_usd,
                "score":       trade.score,
                "status":      "open",
                "source":      "paper_parallel",
                "created_at":  _now_ts(),
            }).execute())
        except Exception as exc:
            logger.debug("[paper] Supabase save_open error: {}", exc)

    async def _save_close(self, trade: PaperTrade) -> None:
        try:
            from db_writer import _client, _run, _now_ts
            # Actualizar el trade más reciente con ese par y source
            await _run(lambda: _client().table("paper_trades")
                .update({
                    "status":     trade.status,
                    "pnl_usd":    trade.pnl_usd,
                    "closed_at":  _now_ts(),
                })
                .eq("pair", trade.pair)
                .eq("source", "paper_parallel")
                .eq("status", "open")
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
        except Exception as exc:
            logger.debug("[paper] Supabase save_close error: {}", exc)

    # ── Reporte diario ───────────────────────────────────────────────────────

    async def send_daily_report(self, real_stats: Optional[dict] = None) -> None:
        """Envía reporte comparativo papel vs real a Telegram."""
        stats = self.daily_stats()

        lines = [
            "\U0001f4c4 Paper Trading (24h)",
            f"Threshold: {stats['threshold']} | Trades: {stats['trades']}",
            f"Ganados: {stats['wins']} | Perdidos: {stats['losses']}",
            f"Win Rate: {stats['win_rate']}%",
            f"PnL simulado: ${stats['pnl']:+.2f}",
            f"Abiertos: {stats['open']}",
        ]

        if real_stats:
            lines.extend([
                "",
                "vs Bot Real:",
                f"Threshold: {real_stats.get('threshold', '?')} | "
                f"Trades: {real_stats.get('trades', 0)}",
                f"Win Rate: {real_stats.get('win_rate', 0)}%",
                f"PnL real: ${real_stats.get('pnl', 0):+.2f}",
            ])

        msg = "\n".join(lines)

        try:
            import alerts
            await alerts._send_telegram(msg, priority="normal")
        except Exception as exc:
            logger.debug("[paper] Telegram report error: {}", exc)

        # Reset para el día siguiente
        self.reset_daily()

    # ── Background loop ──────────────────────────────────────────────────────

    async def run_check_loop(self, price_getter) -> None:
        """Tarea de fondo: revisa trades abiertos cada 30s."""
        while True:
            try:
                prices = price_getter()
                if prices:
                    self.check_prices(prices)
            except Exception as exc:
                logger.debug("[paper] check_loop error: {}", exc)
            await asyncio.sleep(30)

    async def run_daily_report_loop(self, real_stats_getter=None) -> None:
        """Tarea de fondo: reporte diario cada 24h."""
        await asyncio.sleep(600)  # esperar 10 min al arrancar
        while True:
            try:
                real = real_stats_getter() if real_stats_getter else None
                await self.send_daily_report(real)
            except Exception as exc:
                logger.debug("[paper] daily_report error: {}", exc)
            await asyncio.sleep(86400)  # cada 24h
