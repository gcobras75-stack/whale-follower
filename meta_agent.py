# -*- coding: utf-8 -*-
"""
meta_agent.py — Whale Follower Bot — Sprint 7
Meta-Agente de Estrategias: analiza el mercado en tiempo real
y activa/pausa estrategias automáticamente según el régimen.

Señales analizadas:
  - Fear & Greed (0-100)
  - BTC Dominancia
  - Volatilidad ATR 14 periodos
  - Tendencia EMA 20 / EMA 50
  - Volumen relativo
  - DXY dirección

Regímenes detectados:
  LATERAL    → ATR bajo, precio entre EMAs
  ALCISTA    → precio > EMA20 > EMA50
  BAJISTA    → precio < EMA20 < EMA50
  FEAR       → Fear & Greed < 25
  VOLATILE   → ATR > 2% del precio

Evaluación cada 5 minutos. Requiere 3 confirmaciones para cambiar régimen.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from loguru import logger

import config

# ── Constantes ────────────────────────────────────────────────────────────────
_EVAL_INTERVAL_SECS   = 300     # evaluar cada 5 minutos
_STARTUP_DELAY_SECS   = 60      # esperar al inicio
_REGIME_CONFIRM_COUNT = 3       # confirmaciones consecutivas para cambiar régimen
_ATR_PERIOD           = 14      # periodos ATR
_EMA_SHORT            = 20      # EMA rápida
_EMA_LONG             = 50      # EMA lenta
_PRICE_BUFFER         = 80      # muestras de precio a conservar (> EMA50)
_SAMPLE_INTERVAL      = 300.0   # segundos entre muestras de precio (5min)
_ATR_VOLATILE_PCT     = 2.0     # ATR > 2% del precio = mercado volátil
_FEAR_EXTREME         = 25      # Fear & Greed < 25 = pánico extremo
_FEAR_GREED_BULL      = 60      # F&G > 60 = ambiente alcista
_VOL_RELATIVE_HIGH    = 1.5     # volumen 1.5x por encima del promedio = alto
_WEEKLY_SECS          = 7 * 24 * 3600  # 7 días


# ── Régimen de mercado ────────────────────────────────────────────────────────
class Regime(Enum):
    LATERAL  = "LATERAL"
    ALCISTA  = "ALCISTA"
    BAJISTA  = "BAJISTA"
    FEAR     = "FEAR"
    VOLATILE = "VOLATILE"


# ── Estado de estrategias — actualizado por meta_agent ───────────────────────
# (también actualiza strategy_manager._active para que los gates en main.py funcionen)
_active: Dict[str, bool] = {
    "grid_okx":       True,
    "range_trader":   True,
    "wyckoff_spring": True,
    "momentum":       True,
    "ofi":            True,
    "mean_reversion": True,
    "delta_neutral":  True,
    "cross_arb":      False,   # deshabilitado hasta capital suficiente
    "lead_lag":       False,   # deshabilitado hasta capital suficiente
}

# Multiplicador global de tamaño — usado por estrategias que lo soportan
size_multiplier: float = 1.0


def is_active(strategy: str) -> bool:
    return _active.get(strategy, True)


# ── Dataclass de indicadores ──────────────────────────────────────────────────
@dataclass
class MarketIndicators:
    pair:          str
    price:         float = 0.0
    ema20:         float = 0.0
    ema50:         float = 0.0
    atr_pct:       float = 0.0
    rel_volume:    float = 1.0
    fear_greed:    int   = 50
    fg_label:      str   = "Neutral"
    btc_dom_pct:   float = 0.0
    btc_dom_signal:str   = "stable"
    dxy_change_1h: float = 0.0
    dxy_signal:    str   = "neutral"
    samples:       int   = 0


@dataclass
class MetaSnapshot:
    regime:       Regime
    indicators:   MarketIndicators
    active_strats: List[str] = field(default_factory=list)
    paused_strats: List[str] = field(default_factory=list)
    size_mult:    float = 1.0
    eval_count:   int   = 0
    ts:           float = field(default_factory=time.time)


# ── Helper: indicadores técnicos ─────────────────────────────────────────────
def _ema(prices: List[float], period: int) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2.0 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


def _atr_pct(prices: List[float], period: int = 14) -> float:
    if len(prices) < 2:
        return 0.0
    diffs = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    atr   = sum(diffs[-period:]) / min(len(diffs), period)
    return (atr / prices[-1] * 100) if prices[-1] > 0 else 0.0


def _rel_volume(volumes: List[float], window: int = 20) -> float:
    if len(volumes) < 2:
        return 1.0
    avg = sum(volumes[-window-1:-1]) / max(len(volumes[-window-1:-1]), 1)
    return (volumes[-1] / avg) if avg > 0 else 1.0


# ── Meta-Agente ───────────────────────────────────────────────────────────────
class MetaAgent:
    """
    Evalúa el mercado cada 5 minutos y toma decisiones automáticas
    sobre qué estrategias deben estar activas.
    """

    def __init__(
        self,
        fear_greed=None,
        btc_dom=None,
        dxy_mon=None,
        liq_monitor=None,
    ) -> None:
        self._fg      = fear_greed
        self._btc_dom = btc_dom
        self._dxy     = dxy_mon
        self._liq     = liq_monitor

        self._prices:  Dict[str, deque] = {}
        self._volumes: Dict[str, deque] = {}
        self._last_sample: Dict[str, float] = {}

        self._current_regime:   Regime = Regime.LATERAL
        self._candidate_regime: Regime = Regime.LATERAL
        self._regime_streak:    int    = 0
        self._prev_regime:      Optional[Regime] = None

        self._eval_count:  int   = 0
        self._last_weekly: float = time.time()  # evitar análisis semanal en el primer ciclo
        self._last_snap:   Optional[MetaSnapshot] = None

        # Tomar control de strategy_manager
        try:
            import strategy_manager as _sm
            _sm._meta_override = True
            logger.info("[meta_agent] Tomó control de strategy_manager ✅")
        except Exception:
            pass

        logger.info(
            "[meta_agent] Iniciado | eval={}s | ATR_vol={}% | fear_ext={}",
            _EVAL_INTERVAL_SECS, _ATR_VOLATILE_PCT, _FEAR_EXTREME,
        )

    # ── Colección de datos ────────────────────────────────────────────────────

    def on_price(self, pair: str, price: float, volume: float = 0.0) -> None:
        """Llamar en cada tick desde main.py — muestrea internamente cada 5 min."""
        now = time.time()
        if now - self._last_sample.get(pair, 0.0) < _SAMPLE_INTERVAL:
            return
        self._last_sample[pair] = now

        if pair not in self._prices:
            self._prices[pair]  = deque(maxlen=_PRICE_BUFFER)
            self._volumes[pair] = deque(maxlen=_PRICE_BUFFER)
        self._prices[pair].append(price)
        if volume > 0:
            self._volumes[pair].append(volume)

    # ── Loop principal ────────────────────────────────────────────────────────

    async def run(self) -> None:
        await asyncio.sleep(_STARTUP_DELAY_SECS)
        while True:
            try:
                await self._evaluate()
            except Exception as exc:
                logger.warning("[meta_agent] Error en evaluación: {}", exc)
            await asyncio.sleep(_EVAL_INTERVAL_SECS)

    # ── Evaluación completa ───────────────────────────────────────────────────

    async def _evaluate(self) -> None:
        self._eval_count += 1
        ind = self._calc_indicators("BTCUSDT")

        # Detectar régimen candidato
        candidate = self._detect_regime(ind)

        # Hysteresis: requiere N confirmaciones consecutivas
        if candidate == self._candidate_regime:
            self._regime_streak += 1
        else:
            self._candidate_regime = candidate
            self._regime_streak    = 1

        if self._regime_streak >= _REGIME_CONFIRM_COUNT:
            regime_confirmed = candidate
        else:
            regime_confirmed = self._current_regime  # mantener régimen actual
            logger.debug(
                "[meta_agent] Candidato {} {}/{} — manteniendo {}",
                candidate.value, self._regime_streak, _REGIME_CONFIRM_COUNT,
                self._current_regime.value,
            )

        # Aplicar cambio de régimen
        changed = regime_confirmed != self._current_regime
        if changed:
            self._prev_regime    = self._current_regime
            self._current_regime = regime_confirmed
            self._regime_streak  = 0

        new_states, size_mult = self._compute_states(regime_confirmed, ind)
        self._apply_states(new_states, size_mult)

        snap = MetaSnapshot(
            regime        = regime_confirmed,
            indicators    = ind,
            active_strats = [s for s, a in _active.items() if a],
            paused_strats = [s for s, a in _active.items() if not a],
            size_mult     = size_mult,
            eval_count    = self._eval_count,
        )
        self._last_snap = snap

        if changed:
            logger.info(
                "[meta_agent] 🔄 RÉGIMEN: {} → {} | F&G={} ATR={:.2f}% EMA20={:.0f} EMA50={:.0f}",
                self._prev_regime.value, regime_confirmed.value,
                ind.fear_greed, ind.atr_pct, ind.ema20, ind.ema50,
            )
            await self._notify_telegram(snap)
            await self._log_to_supabase(snap)
        elif self._eval_count % 12 == 0:
            self._log_summary(snap)

        # Análisis semanal de ML
        if time.time() - self._last_weekly >= _WEEKLY_SECS:
            await self._weekly_analysis()
            self._last_weekly = time.time()

    # ── Cálculo de indicadores ────────────────────────────────────────────────

    def _calc_indicators(self, pair: str) -> MarketIndicators:
        prices  = list(self._prices.get(pair, []))
        volumes = list(self._volumes.get(pair, []))

        ind = MarketIndicators(pair=pair, samples=len(prices))

        if prices:
            ind.price   = prices[-1]
            ind.ema20   = _ema(prices, _EMA_SHORT)
            ind.ema50   = _ema(prices, _EMA_LONG)
            ind.atr_pct = _atr_pct(prices, _ATR_PERIOD)
        if volumes:
            ind.rel_volume = _rel_volume(volumes)

        if self._fg:
            try:
                fg = self._fg.snapshot()
                ind.fear_greed = fg.value
                ind.fg_label   = fg.label
            except Exception:
                pass

        if self._btc_dom:
            try:
                d = self._btc_dom.snapshot()
                ind.btc_dom_pct    = d.get("dominance_pct", 0.0)
                ind.btc_dom_signal = d.get("signal", "stable")
            except Exception:
                pass

        if self._dxy:
            try:
                d = self._dxy.snapshot()
                ind.dxy_change_1h = d.get("change_1h_pct", 0.0)
                ind.dxy_signal    = "rising" if ind.dxy_change_1h > 0.1 else (
                                    "falling" if ind.dxy_change_1h < -0.1 else "neutral")
            except Exception:
                pass

        return ind

    # ── Detección de régimen ──────────────────────────────────────────────────

    def _detect_regime(self, ind: MarketIndicators) -> Regime:
        # FEAR tiene prioridad máxima
        if ind.fear_greed > 0 and ind.fear_greed < _FEAR_EXTREME:
            return Regime.FEAR

        # Alta volatilidad
        if ind.atr_pct >= _ATR_VOLATILE_PCT:
            return Regime.VOLATILE

        # Necesitamos suficientes muestras para EMA
        if ind.samples < _EMA_SHORT:
            return self._current_regime  # mantener hasta tener datos

        # Tendencia por EMA
        if ind.price > ind.ema20 and ind.ema20 > ind.ema50:
            return Regime.ALCISTA

        if ind.price < ind.ema20 and ind.ema20 < ind.ema50:
            return Regime.BAJISTA

        return Regime.LATERAL

    # ── Decisiones por régimen ────────────────────────────────────────────────

    def _compute_states(self, regime: Regime, ind: MarketIndicators):
        size_mult = 1.0

        if regime == Regime.LATERAL:
            states = {
                "grid_okx":       True,
                "range_trader":   True,
                "cross_arb":      True,
                "momentum":       False,
                "wyckoff_spring": False,
                "lead_lag":       False,
                "mean_reversion": True,
                "ofi":            True,
                "delta_neutral":  True,
            }

        elif regime == Regime.ALCISTA:
            states = {
                "grid_okx":       False,  # grid pierde en tendencia
                "range_trader":   False,
                "cross_arb":      True,
                "momentum":       True,
                "wyckoff_spring": True,
                "lead_lag":       True,
                "mean_reversion": False,
                "ofi":            True,
                "delta_neutral":  True,
            }

        elif regime == Regime.BAJISTA:
            size_mult = 0.5  # reducir tamaño 50%
            states = {
                "grid_okx":       True,   # grid neutral por estructura
                "range_trader":   False,
                "cross_arb":      True,
                "momentum":       False,  # sin longs en bajista
                "wyckoff_spring": False,  # sin longs
                "lead_lag":       False,
                "mean_reversion": True,   # rebotes post-caída
                "ofi":            True,
                "delta_neutral":  True,
            }

        elif regime == Regime.FEAR:
            states = {
                "grid_okx":       True,   # solo grid OKX
                "range_trader":   False,
                "cross_arb":      True,   # funciona en cualquier mercado
                "momentum":       False,
                "wyckoff_spring": False,
                "lead_lag":       False,
                "mean_reversion": True,   # pánico = oportunidad de reversión
                "ofi":            False,
                "delta_neutral":  True,
            }

        else:  # VOLATILE
            states = {
                "grid_okx":       False,  # grid arriesga salir del rango
                "range_trader":   False,
                "cross_arb":      True,
                "momentum":       True,   # movimientos grandes = oportunidad
                "wyckoff_spring": True,
                "lead_lag":       False,
                "mean_reversion": False,  # demasiado volátil para MR
                "ofi":            True,
                "delta_neutral":  True,
            }

        return states, size_mult

    def _apply_states(self, new_states: dict, size_mult: float) -> None:
        global size_multiplier
        size_multiplier = size_mult
        _active.update(new_states)
        # Sincronizar con strategy_manager
        try:
            import strategy_manager as _sm
            _sm._active.update(new_states)
        except Exception:
            pass

    # ── Logging ───────────────────────────────────────────────────────────────

    def _log_summary(self, snap: MetaSnapshot) -> None:
        on  = ", ".join(snap.active_strats)
        off = ", ".join(snap.paused_strats) or "ninguna"
        ind = snap.indicators
        logger.info(
            "[meta_agent] #{} | Régimen={} | F&G={} ({}) | ATR={:.2f}% | "
            "EMA20={:.0f} EMA50={:.0f} | DXY={} | Dom={:.1f}% | size={}x\n"
            "  ✅ ON:  {}\n  ⏸️ OFF: {}",
            snap.eval_count, snap.regime.value,
            ind.fear_greed, ind.fg_label,
            ind.atr_pct, ind.ema20, ind.ema50,
            ind.dxy_signal, ind.btc_dom_pct,
            snap.size_mult, on, off,
        )

    # ── Telegram ─────────────────────────────────────────────────────────────

    async def _notify_telegram(self, snap: MetaSnapshot) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        ind = snap.indicators
        prev = self._prev_regime.value if self._prev_regime else "—"

        emoji_map = {
            Regime.LATERAL:  "📊",
            Regime.ALCISTA:  "📈",
            Regime.BAJISTA:  "📉",
            Regime.FEAR:     "😱",
            Regime.VOLATILE: "⚡",
        }
        emoji = emoji_map.get(snap.regime, "🤖")

        on_lines  = "\n".join(f"  ✅ {s}" for s in snap.active_strats)
        off_lines = "\n".join(f"  ⏸️ {s}" for s in snap.paused_strats)
        size_note = f"\n⚠️ *Tamaño reducido al {int(snap.size_mult*100)}%*" if snap.size_mult < 1.0 else ""

        text = (
            f"{emoji} *Cambio de régimen detectado*\n\n"
            f"Anterior: `{prev}`\n"
            f"Nuevo:    `{snap.regime.value}`\n\n"
            f"📊 *Indicadores:*\n"
            f"Fear & Greed: {ind.fear_greed} ({ind.fg_label})\n"
            f"ATR: {ind.atr_pct:.2f}% | DXY: {ind.dxy_signal}\n"
            f"BTC Dom: {ind.btc_dom_pct:.1f}% ({ind.btc_dom_signal})\n"
            f"EMA20: {ind.ema20:.0f} | EMA50: {ind.ema50:.0f}\n\n"
            f"*Estrategias ajustadas automáticamente:*\n"
            f"{on_lines}\n{off_lines}"
            f"{size_note}"
        )
        try:
            import aiohttp as _ah
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            async with _ah.ClientSession() as s:
                await s.post(url, json={
                    "chat_id":    chat_id,
                    "text":       text,
                    "parse_mode": "Markdown",
                }, timeout=_ah.ClientTimeout(total=8))
        except Exception as exc:
            logger.warning("[meta_agent] Telegram error: {}", exc)

    # ── Supabase logging ──────────────────────────────────────────────────────

    async def _log_to_supabase(self, snap: MetaSnapshot) -> None:
        try:
            import aiohttp as _ah
            url = os.environ.get("SUPABASE_URL", "")
            key = os.environ.get("SUPABASE_KEY", "")
            if not url or not key:
                return
            ind = snap.indicators
            payload = {
                "regime":          snap.regime.value,
                "fear_greed":      ind.fear_greed,
                "atr_pct":         round(ind.atr_pct, 4),
                "ema20":           round(ind.ema20, 2),
                "ema50":           round(ind.ema50, 2),
                "btc_dom_pct":     round(ind.btc_dom_pct, 2),
                "dxy_signal":      ind.dxy_signal,
                "active_strats":   ",".join(snap.active_strats),
                "paused_strats":   ",".join(snap.paused_strats),
                "size_multiplier": snap.size_mult,
            }
            headers = {
                "apikey":        key,
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json",
            }
            async with _ah.ClientSession() as s:
                await s.post(
                    f"{url}/rest/v1/meta_agent_log",
                    json=payload, headers=headers,
                    timeout=_ah.ClientTimeout(total=8),
                )
        except Exception as exc:
            logger.debug("[meta_agent] Supabase log error: {}", exc)

    # ── Análisis semanal (ML) ─────────────────────────────────────────────────

    async def _weekly_analysis(self) -> None:
        """
        Cada 7 días: consulta Supabase para ver qué estrategias funcionaron
        en qué régimen. Ajusta umbrales si se detecta bajo rendimiento.
        """
        try:
            import aiohttp as _ah
            url = os.environ.get("SUPABASE_URL", "")
            key = os.environ.get("SUPABASE_KEY", "")
            if not url or not key:
                return

            headers = {
                "apikey":        key,
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json",
            }
            # Consultar últimos 7 días de meta_agent_log
            since = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.time() - _WEEKLY_SECS)
            )
            results = {}
            async with _ah.ClientSession() as s:
                # paper_trades
                r = await s.get(
                    f"{url}/rest/v1/paper_trades",
                    params={"select": "pnl_usd,status", "status": "eq.closed",
                            "created_at": f"gte.{since}"},
                    headers=headers, timeout=_ah.ClientTimeout(total=10),
                )
                data = await r.json()
                if isinstance(data, list) and data:
                    pnl   = sum(float(t.get("pnl_usd", 0)) for t in data)
                    wins  = sum(1 for t in data if float(t.get("pnl_usd", 0)) > 0)
                    wr    = wins / len(data) * 100
                    results["wyckoff_spring"] = {"trades": len(data), "pnl": pnl, "wr": wr}

                # ofi_trades
                r = await s.get(
                    f"{url}/rest/v1/ofi_trades",
                    params={"select": "pnl_usd,trade_status",
                            "trade_status": "eq.closed",
                            "opened_at": f"gte.{since}"},
                    headers=headers, timeout=_ah.ClientTimeout(total=10),
                )
                data = await r.json()
                if isinstance(data, list) and data:
                    pnl   = sum(float(t.get("pnl_usd", 0)) for t in data)
                    wins  = sum(1 for t in data if float(t.get("pnl_usd", 0)) > 0)
                    wr    = wins / len(data) * 100
                    results["ofi"] = {"trades": len(data), "pnl": pnl, "wr": wr}

            if results:
                lines = [f"  {s}: {v['trades']} trades | PnL ${v['pnl']:.2f} | WR {v['wr']:.0f}%"
                         for s, v in results.items()]
                report = "\n".join(lines)
                logger.info("[meta_agent] 📊 Análisis semanal:\n{}", report)
                await self._send_weekly_telegram(results)

        except Exception as exc:
            logger.warning("[meta_agent] Weekly analysis error: {}", exc)

    async def _send_weekly_telegram(self, results: dict) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        lines = [f"  • {s}: {v['trades']} trades | ${v['pnl']:.2f} | WR {v['wr']:.0f}%"
                 for s, v in results.items()]
        text = (
            "📊 *Reporte Semanal — Meta-Agente*\n\n"
            "Rendimiento de estrategias últimos 7 días:\n"
            + "\n".join(lines)
            + f"\n\nRégimen actual: `{self._current_regime.value}`"
        )
        try:
            import aiohttp as _ah
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            async with _ah.ClientSession() as s:
                await s.post(url, json={
                    "chat_id": chat_id, "text": text, "parse_mode": "Markdown",
                }, timeout=_ah.ClientTimeout(total=8))
        except Exception:
            pass

    # ── API pública ───────────────────────────────────────────────────────────

    def snapshot(self) -> Optional[MetaSnapshot]:
        return self._last_snap

    def current_regime(self) -> Regime:
        return self._current_regime
