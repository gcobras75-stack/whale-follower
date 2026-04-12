# -*- coding: utf-8 -*-
"""
threshold_optimizer.py — Whale Follower Bot
Auto-optimiza el threshold de score basándose en resultados históricos.

Corre cada 6 horas. Lee trades de Supabase, analiza win rate y ratio de
ejecución, y ajusta el threshold por régimen. Persiste en tabla bot_config.

Límites de seguridad:
  - threshold_minimo = 35
  - threshold_maximo = 80
  - máx ±5 puntos por sesión (cada 6h)
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional

import aiohttp
from loguru import logger

import config


# ── Constantes ───────────────────────────────────────────────────────────────

_THRESHOLD_MIN       = 35
_THRESHOLD_MAX       = 80
_MAX_ADJ_PER_SESSION = 3     # máximo ±3 pts por ciclo (era 5)
_MIN_TRADES_TO_ADJUST = 5

# Límites por régimen — evita que el optimizer suba demasiado en lateral
_REGIME_MAX: dict = {
    "LATERAL":       65,
    "TRENDING_UP":   75,
    "TRENDING_DOWN": 80,
    "HIGH_VOL":      80,
}
_INTERVAL_SECS       = 6 * 3600   # cada 6 horas

_REGIMES = ["LATERAL", "TRENDING_UP", "TRENDING_DOWN", "HIGH_VOL"]


@dataclass
class OptResult:
    regime:       str
    old_threshold: int
    new_threshold: int
    reason:       str
    win_rate:     Optional[float]
    trades_count: int
    exec_ratio:   Optional[float]


class ThresholdOptimizer:
    """Lee resultados de Supabase y ajusta thresholds por régimen."""

    def __init__(self) -> None:
        # Thresholds activos en memoria, inicializados desde SPRING_PARAMS
        self._thresholds: Dict[str, int] = {
            r: config.SPRING_PARAMS.get(r, config.SPRING_PARAMS["LATERAL"])["score_min"]
            for r in _REGIMES
        }
        # Contadores de señales (detectadas vs ejecutadas) — alimentados externamente
        self._signals_detected:  int = 0
        self._signals_executed:  int = 0
        self._last_run: float = 0.0

    # ── API pública ──────────────────────────────────────────────────────────

    def get_threshold(self, regime: str) -> int:
        return self._thresholds.get(regime, self._thresholds.get("LATERAL", 55))

    def record_signal_detected(self) -> None:
        self._signals_detected += 1

    def record_signal_executed(self) -> None:
        self._signals_executed += 1

    async def load_from_supabase(self) -> None:
        """Carga thresholds persistidos al arrancar."""
        try:
            from db_writer import _client, _run
            for regime in _REGIMES:
                key = f"score_threshold_{regime.lower()}"
                result = await _run(
                    lambda k=key: _client().table("bot_config")
                    .select("value")
                    .eq("key", k)
                    .execute()
                )
                if result and result.data:
                    val = int(result.data[0]["value"])
                    val = max(_THRESHOLD_MIN, min(_THRESHOLD_MAX, val))
                    self._thresholds[regime] = val
                    logger.info(
                        "[optimizer] Cargado {}: threshold={} (Supabase)",
                        regime, val,
                    )
        except Exception as exc:
            logger.warning("[optimizer] Error cargando thresholds: {}", exc)

    async def run_loop(self) -> None:
        """Tarea de fondo: optimizar cada 6 horas."""
        # Esperar 10 min al arrancar para acumular datos
        await asyncio.sleep(600)
        while True:
            try:
                await self._optimize()
            except Exception as exc:
                logger.warning("[optimizer] Error en ciclo: {}", exc)
            await asyncio.sleep(_INTERVAL_SECS)

    # ── Lógica de optimización ───────────────────────────────────────────────

    async def _optimize(self) -> None:
        stats = await self._fetch_trade_stats()
        if stats is None:
            logger.info("[optimizer] No hay datos suficientes para optimizar")
            return

        trades      = stats["trades"]
        wins        = stats["wins"]
        losses      = stats["losses"]
        avg_score_w = stats["avg_score_winners"]
        avg_score_l = stats["avg_score_losers"]
        total       = wins + losses

        # Ratio de ejecución
        exec_ratio = None
        if self._signals_detected > 0:
            exec_ratio = self._signals_executed / self._signals_detected

        for regime in _REGIMES:
            old = self._thresholds[regime]
            adj = 0
            reason = ""

            # REGLA 1 — Muy pocas señales ejecutadas
            if exec_ratio is not None and exec_ratio < 0.05 and self._signals_detected >= 20:
                adj = -2
                reason = f"pocas señales ejecutadas ({exec_ratio*100:.1f}%)"

            # REGLA 2 — Win rate bajo
            elif total >= 10 and (wins / total) < 0.45:
                adj = 3
                reason = f"win rate bajo ({wins/total*100:.1f}%)"

            # REGLA 3 — Win rate excelente
            elif total >= 20 and (wins / total) >= 0.70:
                adj = -1
                reason = f"win rate excelente ({wins/total*100:.1f}%)"

            # REGLA 4 — Score de ganadores muy por encima del threshold
            elif avg_score_w is not None and avg_score_w > old + 15 and total >= 10:
                adj = 2
                reason = f"ganadores con score alto ({avg_score_w:.0f} vs thr={old})"

            if adj == 0:
                continue

            # Sin trades reales → solo permitir BAJAR (no subir)
            if total < _MIN_TRADES_TO_ADJUST and adj > 0:
                logger.info(
                    "[optimizer] {} ignorando subida (+{}) — solo {} trades (min {})",
                    regime, adj, total, _MIN_TRADES_TO_ADJUST,
                )
                continue

            # Aplicar límites de seguridad
            adj = max(-_MAX_ADJ_PER_SESSION, min(_MAX_ADJ_PER_SESSION, adj))
            regime_max = _REGIME_MAX.get(regime, _THRESHOLD_MAX)
            new = max(_THRESHOLD_MIN, min(regime_max, old + adj))

            if new == old:
                continue

            self._thresholds[regime] = new
            logger.info(
                "[optimizer] {} threshold: {} → {} | razón: {}",
                regime, old, new, reason,
            )

            # Persistir en Supabase
            await self._save_threshold(regime, new)

            # Alerta Telegram
            wr_str = f"{wins/total*100:.1f}%" if total >= 5 else "sin datos suficientes"
            await self._alert_change(OptResult(
                regime=regime,
                old_threshold=old,
                new_threshold=new,
                reason=reason,
                win_rate=wins / total if total > 0 else None,
                trades_count=total,
                exec_ratio=exec_ratio,
            ))

        # Reset contadores para el próximo ciclo
        self._signals_detected = 0
        self._signals_executed = 0
        self._last_run = time.time()

    async def _fetch_trade_stats(self) -> Optional[dict]:
        """Lee últimos 20 trades cerrados de paper_trades."""
        try:
            from db_writer import _client, _run
            result = await _run(
                lambda: _client().table("paper_trades")
                .select("pnl_usd, score, status")
                .eq("status", "closed")
                .order("created_at", desc=True)
                .limit(20)
                .execute()
            )
            if not result or not result.data:
                return None

            trades = result.data
            if len(trades) < _MIN_TRADES_TO_ADJUST:
                return None

            wins   = sum(1 for t in trades if (t.get("pnl_usd") or 0) > 0)
            losses = sum(1 for t in trades if (t.get("pnl_usd") or 0) <= 0)
            scores_w = [t["score"] for t in trades if (t.get("pnl_usd") or 0) > 0 and t.get("score")]
            scores_l = [t["score"] for t in trades if (t.get("pnl_usd") or 0) <= 0 and t.get("score")]

            return {
                "trades":             len(trades),
                "wins":               wins,
                "losses":             losses,
                "avg_score_winners":  sum(scores_w) / len(scores_w) if scores_w else None,
                "avg_score_losers":   sum(scores_l) / len(scores_l) if scores_l else None,
            }
        except Exception as exc:
            logger.warning("[optimizer] Error leyendo trades: {}", exc)
            return None

    async def _save_threshold(self, regime: str, value: int) -> None:
        """Upsert en bot_config."""
        try:
            from db_writer import _client, _run
            key = f"score_threshold_{regime.lower()}"
            await _run(
                lambda: _client().table("bot_config")
                .upsert({
                    "key":        key,
                    "value":      str(value),
                    "updated_at": "now()",
                }, on_conflict="key")
                .execute()
            )
        except Exception as exc:
            logger.warning("[optimizer] Error guardando threshold: {}", exc)

    async def _alert_change(self, r: OptResult) -> None:
        """Telegram alert cuando cambia el threshold."""
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        wr_str = f"{r.win_rate*100:.1f}%" if r.win_rate is not None else "sin datos suficientes"
        exec_str = f"{r.exec_ratio*100:.1f}%" if r.exec_ratio is not None else "N/A"

        msg = (
            f"\U0001f3af Threshold auto-ajustado\n"
            f"R\u00e9gimen: {r.regime}\n"
            f"Anterior: {r.old_threshold} \u2192 Nuevo: {r.new_threshold}\n"
            f"Raz\u00f3n: {r.reason}\n"
            f"Win rate: {wr_str} ({r.trades_count} trades)\n"
            f"Ejecuci\u00f3n: {exec_str}"
        )
        try:
            import tg_sender
            await tg_sender.send(msg)
        except Exception:
            pass


# ── Singleton ────────────────────────────────────────────────────────────────

_instance: Optional[ThresholdOptimizer] = None


def get_optimizer() -> ThresholdOptimizer:
    global _instance
    if _instance is None:
        _instance = ThresholdOptimizer()
    return _instance
