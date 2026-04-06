# -*- coding: utf-8 -*-
"""
position_sizer.py — Whale Follower Bot
Adaptive Position Sizing basado en volatilidad, régimen y capital real.

Integra:
  - ATR% y régimen desde meta_agent
  - Multiplicadores por tipo de estrategia
  - Reducción automática en drawdown
  - Límites máximos de asignación por estrategia
"""
from __future__ import annotations

import time
from typing import Dict, Optional

from loguru import logger

# ── Asignación máxima del capital por estrategia ──────────────────────────────
_MAX_ALLOC: Dict[str, float] = {
    "grid":           0.30,   # Bybit grid: máx 30%
    "okx_grid":       0.30,   # OKX grid: máx 30%
    "mean_reversion": 0.20,
    "ofi":            0.15,
    "delta_neutral":  0.40,
    "wyckoff":        0.10,
    "arb":            0.10,
}

# Multiplicador de riesgo base por estrategia
_STRAT_RISK_MULT: Dict[str, float] = {
    "grid":           0.8,   # Grid más estable → menor riesgo unitario
    "okx_grid":       0.8,
    "mean_reversion": 1.0,
    "ofi":            1.2,
    "delta_neutral":  0.5,
    "wyckoff":        1.0,
    "arb":            0.7,
}

# Multiplicador por régimen de mercado
_REGIME_MULT: Dict[str, float] = {
    "LATERAL":  1.2,   # Ideal para grid
    "ALCISTA":  1.0,
    "BAJISTA":  0.6,   # Conservador en tendencia bajista
    "FEAR":     0.8,
    "VOLATILE": 0.7,
}

_BASE_RISK_PCT  = 0.01    # 1% del capital por operación (base)
_DD_WARN_PCT    = 5.0     # alerta drawdown si > 5%
_DD_PAUSE_PCT   = 15.0    # pausa parcial si > 15%


class AdaptivePositionSizer:
    """
    Calcula tamaños de posición adaptativos por estrategia.

    Uso rápido:
        from position_sizer import get_sizer
        sz = get_sizer()
        sz.update_capital(432.0)
        result = sz.calculate("grid")
        print(result["position_size_usd"])
    """

    def __init__(self) -> None:
        self._total_capital: float = 0.0
        self._peak_capital:  float = 0.0
        self._last_update:   float = 0.0
        logger.info("[position_sizer] AdaptivePositionSizer iniciado ✅")

    # ── Capital ───────────────────────────────────────────────────────────────

    def update_capital(self, total: float) -> None:
        """Actualizar capital total. Actualiza el peak para tracking de drawdown."""
        if total <= 0:
            return
        self._total_capital = total
        if total > self._peak_capital:
            self._peak_capital = total
        self._last_update = time.time()

    def get_drawdown_pct(self) -> float:
        """Drawdown actual como % del capital peak."""
        if self._peak_capital <= 0:
            return 0.0
        return max(0.0, (self._peak_capital - self._total_capital) / self._peak_capital * 100)

    # ── Datos de mercado desde meta_agent ─────────────────────────────────────

    def _regime(self) -> str:
        try:
            import meta_agent as _ma
            return _ma._current_regime.value
        except Exception:
            return "LATERAL"

    def _atr_pct(self) -> float:
        try:
            import meta_agent as _ma
            if _ma._last_snap and _ma._last_snap.indicators:
                return _ma._last_snap.indicators.atr_pct
        except Exception:
            pass
        return 0.5

    def _size_mult(self) -> float:
        try:
            import meta_agent as _ma
            return float(_ma.size_multiplier)
        except Exception:
            return 1.0

    # ── Cálculo principal ─────────────────────────────────────────────────────

    def calculate(
        self,
        strategy:        str,
        entry_price:     float = 0.0,
        stop_loss_price: float = 0.0,
    ) -> Dict:
        """
        Retorna dict con:
          position_size_usd, risk_usd, risk_percent,
          regime, atr_pct, drawdown_pct, ...
        """
        if self._total_capital <= 0:
            return {
                "position_size_usd": 0.0, "risk_usd": 0.0,
                "risk_percent": 0.0, "regime": "?",
            }

        regime      = self._regime()
        atr_pct     = self._atr_pct()
        size_mult   = self._size_mult()
        dd          = self.get_drawdown_pct()

        # Riesgo base en USD
        risk_usd = self._total_capital * _BASE_RISK_PCT

        # Ajuste por volatilidad: más ATR → posición más chica
        vol_adj = 1.0 / (1.0 + atr_pct * 5) if atr_pct > 0 else 1.0

        # Multiplicadores
        regime_mult = _REGIME_MULT.get(regime, 1.0)
        strat_mult  = _STRAT_RISK_MULT.get(strategy, 1.0)

        # Reducción por drawdown excesivo
        dd_mult = 1.0
        if dd > _DD_WARN_PCT:
            dd_mult = max(0.3, 1.0 - (dd - _DD_WARN_PCT) / 100 * 5)

        # Riesgo ajustado
        adj_risk = risk_usd * vol_adj * regime_mult * strat_mult * size_mult * dd_mult

        # Convertir a tamaño USD
        if entry_price > 0 and stop_loss_price > 0 and entry_price != stop_loss_price:
            risk_per_unit = abs(entry_price - stop_loss_price)
            units         = adj_risk / risk_per_unit
            position_usd  = units * entry_price
        else:
            # Sin SL explícito: fracción fija con todos los multiplicadores
            position_usd = self._total_capital * 0.10 * regime_mult * strat_mult * size_mult * dd_mult

        # Límite máximo por estrategia
        max_usd      = self._total_capital * _MAX_ALLOC.get(strategy, 0.20)
        position_usd = min(position_usd, max_usd)

        return {
            "strategy":          strategy,
            "position_size_usd": round(position_usd, 2),
            "risk_usd":          round(adj_risk, 2),
            "risk_percent":      round((adj_risk / self._total_capital) * 100, 3),
            "regime":            regime,
            "atr_pct":           round(atr_pct, 3),
            "regime_mult":       regime_mult,
            "strat_mult":        strat_mult,
            "size_mult":         size_mult,
            "dd_mult":           round(dd_mult, 2),
            "drawdown_pct":      round(dd, 2),
            "total_capital":     round(self._total_capital, 2),
        }

    def allocation_summary(self) -> Dict[str, float]:
        """Retorna tamaños máximos permitidos por estrategia en USD."""
        if self._total_capital <= 0:
            return {}
        return {
            strat: round(self._total_capital * pct, 2)
            for strat, pct in _MAX_ALLOC.items()
        }

    def log_summary(self) -> None:
        logger.info(
            "[position_sizer] Capital=${:.0f} | Peak=${:.0f} | DD={:.1f}% | "
            "Régimen={} | ATR={:.2f}% | size_mult={:.1f}x",
            self._total_capital, self._peak_capital, self.get_drawdown_pct(),
            self._regime(), self._atr_pct(), self._size_mult(),
        )


# ── Singleton global ──────────────────────────────────────────────────────────
_sizer: Optional[AdaptivePositionSizer] = None


def get_sizer() -> AdaptivePositionSizer:
    """Singleton — importar y usar desde cualquier estrategia."""
    global _sizer
    if _sizer is None:
        _sizer = AdaptivePositionSizer()
    return _sizer
