# -*- coding: utf-8 -*-
"""
strategy_manager.py — Whale Follower Bot — Sprint 6
Agente autónomo que monitorea el mercado y activa/pausa estrategias
según las condiciones detectadas. Evaluación cada 60 segundos.

Reglas:
  grid_okx       → SIEMPRE activo (spacing dinámico maneja tendencias)
  range_trader   → Solo cuando régimen LATERAL en al menos 1 par
  wyckoff_spring → SIEMPRE activo (score > 65 filtra internamente)
  momentum       → Solo cuando régimen TRENDING en al menos 1 par
  ofi            → SIEMPRE activo (OFI score filtra internamente)
  mean_reversion → Solo cuando liquidaciones LONG > $3M (cascada detectada)
  delta_neutral  → Solo cuando funding_diff >= 0.004%
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from loguru import logger

import config

# ── Estrategias controladas ──────────────────────────────────────────────────
STRATEGIES = [
    "grid_okx",
    "range_trader",
    "wyckoff_spring",
    "momentum",
    "ofi",
    "mean_reversion",
    "delta_neutral",
]

# ── Estado global — consultable desde cualquier módulo ───────────────────────
_active: Dict[str, bool] = {s: True for s in STRATEGIES}


def is_active(strategy: str) -> bool:
    """Devuelve True si la estrategia está activa según condiciones de mercado."""
    return _active.get(strategy, True)


# ── Thresholds de activación ─────────────────────────────────────────────────
_LIQ_MEAN_REV_USD_M   = 3.0    # liquidaciones LONG mínimas para mean_reversion ($M)
_FUNDING_DELTA_MIN    = 0.004  # diferencial de funding mínimo para delta_neutral (%)
_EVAL_INTERVAL_SECS   = 60     # evaluación cada 60 segundos
_STARTUP_DELAY_SECS   = 45     # esperar al inicio para que los monitores se estabilicen


@dataclass
class MarketConditions:
    """Snapshot de condiciones de mercado para la evaluación."""
    regimes:          Dict[str, str] = field(default_factory=dict)
    liq_long_m:       float = 0.0     # liquidaciones LONG acumuladas ~1h ($M)
    liq_short_m:      float = 0.0     # liquidaciones SHORT acumuladas ~1h ($M)
    liq_long_blocked: bool  = False   # True si cascada LONG reciente bloqueante
    funding_diff_pct: float = 0.0     # |diferencial| funding Bybit-OKX (%)
    btc_dom_pct:      float = 0.0     # % dominancia BTC
    btc_dom_signal:   str   = "stable"
    eval_ts:          float = field(default_factory=time.time)

    # Helpers derivados
    @property
    def has_lateral(self) -> bool:
        return any(r == "LATERAL" for r in self.regimes.values())

    @property
    def has_trending(self) -> bool:
        return any(r in ("TRENDING_UP", "TRENDING_DOWN") for r in self.regimes.values())

    @property
    def lateral_pairs(self) -> List[str]:
        return [p for p, r in self.regimes.items() if r == "LATERAL"]

    @property
    def trending_pairs(self) -> List[str]:
        return [p for p, r in self.regimes.items() if r in ("TRENDING_UP", "TRENDING_DOWN")]


class StrategyManager:
    """
    Agente autónomo de monitoreo y control de estrategias.
    Corre como tarea asyncio de fondo.
    """

    def __init__(
        self,
        regime_detector=None,
        liq_monitor=None,
        delta_neutral_engine=None,
        btc_dominance_monitor=None,
    ) -> None:
        self._regime  = regime_detector
        self._liq     = liq_monitor
        self._dn      = delta_neutral_engine
        self._btc_dom = btc_dominance_monitor

        self._prev_states: Dict[str, bool] = {s: True for s in STRATEGIES}
        self._eval_count:  int   = 0
        self._last_cond:   Optional[MarketConditions] = None

        logger.info(
            "[strategy_mgr] Iniciado — eval cada {}s | {} estrategias monitoreadas",
            _EVAL_INTERVAL_SECS, len(STRATEGIES),
        )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tarea de fondo principal."""
        await asyncio.sleep(_STARTUP_DELAY_SECS)
        while True:
            try:
                await self._evaluate()
            except Exception as exc:
                logger.warning("[strategy_mgr] Error en ciclo de evaluación: {}", exc)
            await asyncio.sleep(_EVAL_INTERVAL_SECS)

    # ── Evaluación ────────────────────────────────────────────────────────────

    async def _evaluate(self) -> None:
        self._eval_count += 1
        cond = self._gather_conditions()
        self._last_cond = cond
        new_states = self._compute_states(cond)

        changes: List[str] = []
        for strategy, active_now in new_states.items():
            prev = _active.get(strategy, True)
            _active[strategy] = active_now
            if active_now != prev:
                emoji  = "✅" if active_now else "⏸️"
                reason = self._reason(strategy, cond, active_now)
                changes.append(f"{emoji} *{strategy}*: {'ACTIVADA' if active_now else 'PAUSADA'}\n  _{reason}_")
                logger.info(
                    "[strategy_mgr] {} → {} | {}",
                    strategy, "ON" if active_now else "OFF", reason,
                )

        if changes:
            await self._notify_telegram(changes, cond)
        elif self._eval_count % 10 == 0:
            self._log_summary(cond)

    def _gather_conditions(self) -> MarketConditions:
        """Recopila condiciones del mercado desde los monitores disponibles."""
        pairs = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        regimes: Dict[str, str] = {}
        if self._regime:
            for p in pairs:
                try:
                    regimes[p] = self._regime.regime(p).name
                except Exception:
                    regimes[p] = "UNKNOWN"

        liq_long_m = liq_short_m = 0.0
        liq_blocked = False
        if self._liq:
            try:
                snap        = self._liq.snapshot()
                liq_long_m  = snap.liq_long_m
                liq_short_m = snap.liq_short_m
                liq_blocked = snap.long_blocked
            except Exception:
                pass

        funding_diff = 0.0
        if self._dn:
            try:
                funding_diff = abs(self._dn.snapshot().funding_diff)
            except Exception:
                pass

        dom_pct    = 0.0
        dom_signal = "stable"
        if self._btc_dom:
            try:
                d          = self._btc_dom.snapshot()
                dom_pct    = d.get("dominance_pct", 0.0)
                dom_signal = d.get("signal", "stable")
            except Exception:
                pass

        return MarketConditions(
            regimes          = regimes,
            liq_long_m       = liq_long_m,
            liq_short_m      = liq_short_m,
            liq_long_blocked = liq_blocked,
            funding_diff_pct = funding_diff,
            btc_dom_pct      = dom_pct,
            btc_dom_signal   = dom_signal,
        )

    def _compute_states(self, c: MarketConditions) -> Dict[str, bool]:
        """Calcula el estado óptimo de cada estrategia según las condiciones."""
        return {
            # ── Siempre activas (su lógica interna filtra las entradas) ────────
            "grid_okx":       True,
            "wyckoff_spring": True,
            "ofi":            True,

            # ── Range Trader: solo en mercado lateral ─────────────────────────
            "range_trader":   c.has_lateral,

            # ── Momentum: solo en tendencia clara ─────────────────────────────
            # Si el mercado está en lateral puro, el momentum pierde
            "momentum":       c.has_trending or not c.has_lateral,

            # ── Mean Reversion: activar tras cascada de liquidaciones ──────────
            # Sin cascada no hay "reversión" que capturar
            "mean_reversion": c.liq_long_m >= _LIQ_MEAN_REV_USD_M or c.liq_short_m >= _LIQ_MEAN_REV_USD_M,

            # ── Delta Neutral: solo cuando diferencial de funding es rentable ──
            "delta_neutral":  c.funding_diff_pct >= _FUNDING_DELTA_MIN,
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _reason(self, strategy: str, c: MarketConditions, active: bool) -> str:
        reasons = {
            "range_trader": (
                f"lateral en {c.lateral_pairs}" if active
                else f"mercado trending {c.trending_pairs} — esperando lateral"
            ),
            "momentum": (
                f"tendencia detectada {c.trending_pairs}" if active
                else "mercado lateral — momentum pausado"
            ),
            "mean_reversion": (
                f"cascada detectada liq_long=${c.liq_long_m:.1f}M liq_short=${c.liq_short_m:.1f}M" if active
                else f"sin cascada (liq=${c.liq_long_m:.1f}M < ${_LIQ_MEAN_REV_USD_M}M)"
            ),
            "delta_neutral": (
                f"funding_diff={c.funding_diff_pct:.4f}% >= {_FUNDING_DELTA_MIN}%" if active
                else f"funding_diff={c.funding_diff_pct:.4f}% < {_FUNDING_DELTA_MIN}% — sin ventaja"
            ),
        }
        return reasons.get(strategy, "condición automática")

    def _log_summary(self, c: MarketConditions) -> None:
        on  = [s for s, a in _active.items() if a]
        off = [s for s, a in _active.items() if not a]
        logger.info(
            "[strategy_mgr] #{} | Regímenes: {} | Liq: L=${:.1f}M S=${:.1f}M | "
            "Funding: {:.4f}% | ON={} OFF={}",
            self._eval_count,
            {p: r[:3] for p, r in c.regimes.items()},
            c.liq_long_m, c.liq_short_m,
            c.funding_diff_pct,
            on, off,
        )

    # ── Telegram ──────────────────────────────────────────────────────────────

    async def _notify_telegram(self, changes: List[str], c: MarketConditions) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        try:
            import aiohttp as _aiohttp
            on_list  = ", ".join(s for s, a in _active.items() if a)
            off_list = ", ".join(s for s, a in _active.items() if not a) or "ninguna"
            text = (
                "🤖 *Strategy Manager — Cambio de estado*\n\n"
                + "\n".join(changes)
                + f"\n\n📊 *Condiciones:*\n"
                + f"Regímenes: {dict((p, r[:3]) for p, r in c.regimes.items())}\n"
                + f"Liq LONG: ${c.liq_long_m:.1f}M | SHORT: ${c.liq_short_m:.1f}M\n"
                + f"Funding diff: {c.funding_diff_pct:.4f}%\n"
                + f"\n✅ Activas: {on_list}\n"
                + f"⏸️ Pausadas: {off_list}"
            )
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            async with _aiohttp.ClientSession() as s:
                await s.post(url, json={
                    "chat_id":    chat_id,
                    "text":       text,
                    "parse_mode": "Markdown",
                }, timeout=_aiohttp.ClientTimeout(total=8))
        except Exception as exc:
            logger.warning("[strategy_mgr] Telegram error: {}", exc)

    # ── API pública extra ─────────────────────────────────────────────────────

    def status(self) -> Dict[str, bool]:
        """Snapshot del estado actual de todas las estrategias."""
        return dict(_active)

    def conditions(self) -> Optional[MarketConditions]:
        """Última evaluación de condiciones del mercado."""
        return self._last_cond
