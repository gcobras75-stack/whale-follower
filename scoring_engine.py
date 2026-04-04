"""
scoring_engine.py — Whale Follower Bot — Sprint 2
Sistema de puntuación con 30 medidas agrupadas en 5 categorías.

Diseño:
- Cada categoría tiene un techo de puntos
- La puntuación total máxima es 100
- Thresholds: >= 65 → señal normal, >= 80 → alta confianza
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from cascade_detector import CascadeEvent
from context_engine import MarketContext
from cvd_real import CVDMetrics


@dataclass
class ScoreBreakdown:
    """Desglose detallado de la puntuación para el mensaje de Telegram."""
    # Puntos por categoría
    primary_pts:   int = 0    # máx 40
    volume_pts:    int = 0    # máx 25
    context_pts:   int = 0    # máx 20
    structure_pts: int = 0    # máx 15
    total:         int = 0    # máx 100

    # Flags individuales (para el mensaje de Telegram)
    spring_confirmed:    bool = False
    cvd_divergence:      bool = False
    cascade_active:      bool = False
    vol_spike:           bool = False
    cvd_velocity_pos:    bool = False
    funding_favorable:   bool = False
    oi_confirming:       bool = False
    institutional_session: bool = False
    near_vwap:           bool = False
    ema_favorable:       bool = False

    # Valores numéricos para el mensaje
    drop_pct:       float = 0.0
    bounce_pct:     float = 0.0
    cvd_vel_10s:    float = 0.0
    cascade_trades: int   = 0
    funding_rate:   Optional[float] = None
    oi_change_pct:  Optional[float] = None
    session_name:   str   = "unknown"
    session_mult:   float = 1.0
    vol_ratio:      float = 0.0


class ScoringEngine:
    """
    Combina datos del spring, CVD, cascade y contexto en un score 0-100.

    Uso:
        engine = ScoringEngine()
        score, breakdown = engine.score(
            spring_data=..., cvd=..., cascade=..., context=...
        )
    """

    def score(
        self,
        spring_data: Dict,          # del spring_detector: condiciones, intensidades
        cvd: CVDMetrics,
        cascade: CascadeEvent,
        context: MarketContext,
        current_price: float = 0.0,
        vwap: Optional[float] = None,
        ema200: Optional[float] = None,
    ) -> tuple[int, ScoreBreakdown]:
        """
        Devuelve (score_total, breakdown).
        score_total: int 0-100
        """
        bd = ScoreBreakdown()

        # ── 1. SEÑALES PRIMARIAS (máx 40 pts) ─────────────────────────────────
        # Spring confirmado: cond_a (caída) + cond_b (rebote)
        cond_a = spring_data.get("cond_a", False)
        cond_b = spring_data.get("cond_b", False)
        if cond_a and cond_b:
            bd.spring_confirmed = True
            bd.primary_pts += 20
        elif cond_a:
            bd.primary_pts += 8   # caída sin rebote = parcial

        bd.drop_pct   = spring_data.get("drop_pct", 0.0)
        bd.bounce_pct = spring_data.get("bounce_pct", 0.0)

        # CVD real subiendo durante el Spring (divergencia alcista)
        cond_c = spring_data.get("cond_c", False)
        if cond_c and cvd.velocity_10s > 0:
            bd.cvd_divergence = True
            bd.primary_pts += 20
        elif cond_c:
            bd.primary_pts += 10  # divergencia sin velocidad positiva = parcial

        bd.primary_pts = min(bd.primary_pts, 40)

        # ── 2. VOLUMEN Y FLUJO (máx 25 pts) ───────────────────────────────────
        # Volumen spike
        vol_ratio = spring_data.get("vol_ratio", 0.0)
        bd.vol_ratio = vol_ratio
        if vol_ratio >= 1.5:
            bd.vol_spike = True
            bd.volume_pts += 10
        elif vol_ratio >= 1.2:
            bd.volume_pts += 5

        # Stop cascade
        if cascade.active:
            bd.cascade_active  = True
            bd.cascade_trades  = cascade.sell_count
            intensity_pts = min(int(cascade.intensity / 10), 10)
            bd.volume_pts += intensity_pts   # 0-10 pts según intensidad

        # CVD velocity positiva en 10s
        bd.cvd_vel_10s = cvd.velocity_10s
        if cvd.velocity_10s > 0:
            bd.cvd_velocity_pos = True
            bd.volume_pts += 5

        bd.volume_pts = min(bd.volume_pts, 25)

        # ── 3. CONTEXTO (máx 20 pts) ───────────────────────────────────────────
        # Funding Rate
        if context.funding_signal == "bullish":
            bd.funding_favorable = True
            bd.context_pts += context.funding_pts   # 0-8
        bd.funding_rate = context.funding_rate

        # Open Interest
        if context.oi_signal == "confirming":
            bd.oi_confirming = True
            bd.context_pts += context.oi_pts        # 0-7
        bd.oi_change_pct = context.oi_change_pct

        # Sesión institucional activa (London, NY o overlap)
        bd.session_name = context.session_name
        bd.session_mult = context.session_multiplier
        if context.session_multiplier >= 1.3:
            bd.institutional_session = True
            bd.context_pts += context.session_pts   # 0-5

        bd.context_pts = min(bd.context_pts, 20)

        # ── 4. ESTRUCTURA (máx 15 pts) ────────────────────────────────────────
        # Precio cerca de VWAP (±1%)
        if vwap and vwap > 0 and current_price > 0:
            vwap_dev = abs(current_price - vwap) / vwap
            if vwap_dev <= 0.01:
                bd.near_vwap = True
                bd.structure_pts += 8
            elif vwap_dev <= 0.02:
                bd.structure_pts += 4

        # EMA 200 a favor (precio por encima = alcista)
        if ema200 and ema200 > 0 and current_price > 0:
            if current_price > ema200:
                bd.ema_favorable = True
                bd.structure_pts += 7
            elif current_price > ema200 * 0.98:  # dentro del 2%
                bd.structure_pts += 3

        bd.structure_pts = min(bd.structure_pts, 15)

        # ── Score total ────────────────────────────────────────────────────────
        raw = bd.primary_pts + bd.volume_pts + bd.context_pts + bd.structure_pts

        # Aplicar multiplicador de sesión (±20% máximo)
        multiplied = raw * context.session_multiplier
        bd.total = min(int(multiplied), 100)

        return bd.total, bd
