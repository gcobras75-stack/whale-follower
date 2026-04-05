# -*- coding: utf-8 -*-
"""
scoring_engine.py -- Whale Follower Bot -- Sprint 4
Sistema de puntuacion con nuevas capas: CVD combinado, liquidaciones,
order book, correlacion BTC-ETH, on-chain, sesion historica.

Categorias y techos:
  Primarias          max 40   Spring + CVD combinado
  Volumen y Flujo    max 35   Vol, Cascade, CVD velocity/aceleracion, OB imbalance
  Contexto           max 35   Funding, OI, On-Chain, Liquidation zone
  Estructura         max 20   BTC-ETH correlation, VWAP, Session

Total teorico max: 130 -> capped a 100
Filtros que BLOQUEAN (score -> 0): Fear&Greed extremo contrario,
                                   noticia importante, ML prob < 0.65
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from loguru import logger
from cascade_detector import CascadeEvent
from context_engine import MarketContext
from cvd_real import CVDMetrics


# ── Extended context from Sprint 4 modules ────────────────────────────────────

@dataclass
class ExtendedContext:
    """Signals from the 10 new Sprint 4 layers.

    All fields have safe defaults so the scoring engine degrades gracefully
    if any module is unavailable or returns stale data.
    """
    # Capa 1 — CVD Combinado
    cvd_all_positive: bool  = False   # 3/3 exchanges positivos -> +20
    cvd_two_positive: bool  = False   # 2/3 exchanges positivos -> +10
    cvd_weighted_vel: float = 0.0

    # Capa 2 — Liquidation Map
    liq_zone_active:    bool  = False
    liq_extra_pts:      int   = 0     # 0 o 15
    liq_zone_distance:  float = 1.0   # fraccion del precio

    # Capa 3 — Fear & Greed
    fear_greed_value:       int   = 50
    fear_greed_multiplier:  float = 1.0
    fear_greed_block_long:  bool  = False
    fear_greed_block_short: bool  = False

    # Capa 4 — News Filter
    news_blocked:  bool = False
    news_reason:   str  = ""

    # Capa 5 — On-Chain
    onchain_signal: str = "neutral"   # "bullish" | "bearish" | "neutral"
    onchain_pts:    int = 0           # +10, -10 o 0

    # Capa 6 — CVD Acceleration (ya en CVDMetrics.acceleration, re-expuesto aqui)
    cvd_acceleration_strong: bool  = False   # acceleration > umbral -> +8
    cvd_acceleration_val:    float = 0.0

    # Capa 7 — Order Book Imbalance
    ob_imbalance:   float = 0.5
    ob_favorable:   bool  = False     # imbalance > 0.6 -> +8
    ob_pts:         int   = 0

    # Capa 8 — Correlacion BTC-ETH
    btc_eth_signal: str = "neutral"   # "confirming" | "diverging" | "neutral"
    btc_eth_pts:    int = 0           # +10 | -10 | 0

    # Capa 9 — Sesion Volumen Historico
    vol_unusual:        bool  = False
    vol_hist_ratio:     float = 1.0
    vol_hist_pts:       int   = 0     # 0 o +8

    # Capa 10 — ML Model
    ml_probability: float = 1.0      # 1.0 si modelo no disponible
    ml_block:       bool  = False    # True si prob < 0.65

    # Capa 11 — Deribit Options Flow
    options_pcr:           float = 1.0    # put/call ratio (>1.5 bearish, <0.5 bullish)
    options_iv_spike:      bool  = False  # IV spike > 30% vs promedio → pause
    options_bullish_sweep: bool  = False  # large call sweep → +8 pts
    options_bearish_sweep: bool  = False  # large put sweep → -8 pts
    options_pts:           int   = 0      # ajuste final calculado

    # Capa 12 — DXY Macro Correlation
    dxy_strong_up: bool = False   # DXY +0.30% en 5m → multiplicador 0.85x en Long


# ── Score breakdown ────────────────────────────────────────────────────────────

@dataclass
class ScoreBreakdown:
    """Desglose detallado para mensajes Telegram y diagnostico."""
    # Puntos por categoria
    primary_pts:   int = 0    # max 40
    volume_pts:    int = 0    # max 35
    context_pts:   int = 0    # max 35
    structure_pts: int = 0    # max 20
    total:         int = 0    # max 100

    # Flags individuales (para Telegram)
    spring_confirmed:    bool = False
    cvd_divergence:      bool = False   # legacy: True si CVD sube durante spring
    cvd_all_exchanges:   bool = False   # True si los 3 exchanges tienen CVD positivo
    cascade_active:      bool = False
    vol_spike:           bool = False
    cvd_velocity_pos:    bool = False
    cvd_accel_strong:    bool = False
    ob_favorable:        bool = False
    funding_favorable:   bool = False
    oi_confirming:       bool = False
    onchain_bullish:     bool = False
    liq_zone:            bool = False
    btc_eth_confirming:  bool = False
    btc_eth_diverging:   bool = False
    vol_unusual:         bool = False
    near_vwap:           bool = False
    ema_favorable:       bool = False
    institutional_session: bool = False

    # Filtros activos
    blocked_fear_greed: bool = False
    blocked_news:       bool = False
    blocked_ml:         bool = False
    block_reason:       str  = ""

    # Valores numericos
    drop_pct:       float         = 0.0
    bounce_pct:     float         = 0.0
    cvd_vel_10s:    float         = 0.0
    cascade_trades: int           = 0
    funding_rate:   Optional[float] = None
    oi_change_pct:  Optional[float] = None
    session_name:   str           = "unknown"
    session_mult:   float         = 1.0
    vol_ratio:      float         = 0.0
    ml_prob:        float         = 1.0
    fear_greed_val: int           = 50


# ── Scoring engine ────────────────────────────────────────────────────────────

class ScoringEngine:
    """
    Combina todas las capas en un score 0-100.

    Uso:
        engine = ScoringEngine()
        score, bd = engine.score(
            spring_data=..., cvd=..., cascade=..., context=...,
            ext=ExtendedContext(...)  # opcional
        )
    """

    # Umbrales para CVD acceleration
    _CVD_ACCEL_THRESHOLD = 0.002   # acceleration > 0.002 BTC/10s = fuerte

    def score(
        self,
        spring_data: Dict,
        cvd: CVDMetrics,
        cascade: CascadeEvent,
        context: MarketContext,
        current_price: float = 0.0,
        vwap: Optional[float] = None,
        ema200: Optional[float] = None,
        ext: Optional[ExtendedContext] = None,
    ) -> tuple[int, ScoreBreakdown]:
        """
        Devuelve (score_total, breakdown).
        Si score_total == 0 y bd.block_reason != "" -> señal bloqueada por filtro.
        """
        if ext is None:
            ext = ExtendedContext()

        bd = ScoreBreakdown(
            ml_prob=ext.ml_probability,
            fear_greed_val=ext.fear_greed_value,
        )

        # ── FILTROS QUE BLOQUEAN ANTES DE CALCULAR ────────────────────────────
        # Options IV spike → mercado impredecible, pausar
        if ext.options_iv_spike:
            bd.block_reason = "options_iv_spike: volatilidad implícita disparada"
            return 0, bd

        # Fear & Greed: solo bloquear LONGs (el bot solo hace longs ahora)
        if ext.fear_greed_block_long:
            bd.blocked_fear_greed = True
            bd.block_reason = f"Fear&Greed extremo bajo ({ext.fear_greed_value})"
            return 0, bd

        # Noticia importante
        if ext.news_blocked:
            bd.blocked_news = True
            bd.block_reason = f"Noticia: {ext.news_reason}"
            return 0, bd

        # ML model
        if ext.ml_block:
            bd.blocked_ml = True
            bd.block_reason = f"ML prob={ext.ml_probability:.2f} < 0.65"
            return 0, bd

        # ── 1. SENALES PRIMARIAS (max 40) ──────────────────────────────────────
        cond_a = spring_data.get("cond_a", False)
        cond_b = spring_data.get("cond_b", False)
        if cond_a and cond_b:
            bd.spring_confirmed = True
            bd.primary_pts += 20
        elif cond_a:
            bd.primary_pts += 8

        bd.drop_pct   = spring_data.get("drop_pct", 0.0)
        bd.bounce_pct = spring_data.get("bounce_pct", 0.0)

        # CVD combinado 3 exchanges (Capa 1) — reemplaza CVD divergencia simple
        if ext.cvd_all_positive:
            bd.cvd_all_exchanges = True
            bd.cvd_divergence    = True
            bd.primary_pts      += 20
        elif ext.cvd_two_positive:
            bd.cvd_divergence   = True
            bd.primary_pts     += 10
        else:
            # Fallback: CVD legacy del exchange individual
            cond_c = spring_data.get("cond_c", False)
            if cond_c and cvd.velocity_10s > 0:
                bd.cvd_divergence = True
                bd.primary_pts   += 10   # menos puntos que combined

        bd.primary_pts = min(bd.primary_pts, 40)

        # ── 2. VOLUMEN Y FLUJO (max 35) ────────────────────────────────────────
        # Volumen spike
        vol_ratio = spring_data.get("vol_ratio", 0.0)
        bd.vol_ratio = vol_ratio
        if vol_ratio >= 1.3:
            bd.vol_spike = True
            bd.volume_pts += 8
        elif vol_ratio >= 1.1:
            bd.volume_pts += 4

        # Stop cascade
        if cascade.active:
            bd.cascade_active = True
            bd.cascade_trades = cascade.sell_count
            bd.volume_pts    += min(int(cascade.intensity / 10), 10)

        # CVD velocity 10s positiva
        bd.cvd_vel_10s = cvd.velocity_10s
        if cvd.velocity_10s > 0:
            bd.cvd_velocity_pos = True
            bd.volume_pts      += 5

        # CVD Acceleration (Capa 6)
        accel = ext.cvd_acceleration_val if ext.cvd_acceleration_val != 0.0 else cvd.acceleration
        bd.cvd_acceleration_val = accel
        if accel > self._CVD_ACCEL_THRESHOLD:
            bd.cvd_accel_strong = True
            bd.volume_pts      += 8

        # Order Book Imbalance (Capa 7)
        bd.ob_imbalance = ext.ob_imbalance
        if ext.ob_favorable or ext.ob_pts > 0:
            bd.ob_favorable = True
            bd.volume_pts  += 8

        bd.volume_pts = min(bd.volume_pts, 35)

        # ── 3. CONTEXTO INSTITUCIONAL (max 35) ────────────────────────────────
        # Funding Rate
        if context.funding_signal == "bullish":
            bd.funding_favorable = True
            bd.context_pts      += context.funding_pts
        bd.funding_rate = context.funding_rate

        # Open Interest
        if context.oi_signal == "confirming":
            bd.oi_confirming = True
            bd.context_pts  += context.oi_pts
        bd.oi_change_pct = context.oi_change_pct

        # On-Chain (Capa 5)
        if ext.onchain_signal == "bullish":
            bd.onchain_bullish = True
            bd.context_pts    += 10
        elif ext.onchain_signal == "bearish":
            bd.context_pts    -= 10   # penalizar

        # Liquidation Map (Capa 2)
        if ext.liq_zone_active and ext.liq_extra_pts > 0:
            bd.liq_zone    = True
            bd.context_pts += ext.liq_extra_pts

        bd.context_pts = min(bd.context_pts, 35)

        # ── 4. CORRELACION Y ESTRUCTURA (max 20) ──────────────────────────────
        # Sesion institucional
        bd.session_name = context.session_name
        bd.session_mult = context.session_multiplier
        if context.session_multiplier >= 1.3:
            bd.institutional_session = True
            bd.structure_pts        += context.session_pts

        # Precio cerca VWAP
        if vwap and vwap > 0 and current_price > 0:
            vwap_dev = abs(current_price - vwap) / vwap
            if vwap_dev <= 0.01:
                bd.near_vwap     = True
                bd.structure_pts += 5
            elif vwap_dev <= 0.02:
                bd.structure_pts += 2

        # EMA 200
        if ema200 and ema200 > 0 and current_price > 0:
            if current_price > ema200:
                bd.ema_favorable  = True
                bd.structure_pts += 5
            elif current_price > ema200 * 0.98:
                bd.structure_pts += 2

        # BTC-ETH Correlacion (Capa 8)
        if ext.btc_eth_signal == "confirming":
            bd.btc_eth_confirming = True
            bd.structure_pts     += 10
        elif ext.btc_eth_signal == "diverging":
            bd.btc_eth_diverging = True
            bd.structure_pts    -= 10

        # Sesion Volumen Historico (Capa 9)
        if ext.vol_unusual:
            bd.vol_unusual   = True
            bd.structure_pts += 8

        bd.structure_pts = min(bd.structure_pts, 20)
        bd.structure_pts = max(bd.structure_pts, 0)   # no negativo

        # ── Capa 11 — Options Flow (fuera de caps de categoría) ───────────────
        if ext.options_bullish_sweep:
            ext.options_pts += 8
        if ext.options_bearish_sweep:
            ext.options_pts -= 8
        if ext.options_pcr < 0.5:
            ext.options_pts += 10    # ratio muy bajo = mucho más call que put = bullish
        elif ext.options_pcr > 1.5:
            ext.options_pts -= 10    # ratio muy alto = mucho más put que call = bearish

        # ── Score total ────────────────────────────────────────────────────────
        raw = (bd.primary_pts + bd.volume_pts +
               bd.context_pts + bd.structure_pts + ext.options_pts)

        # Aplicar Fear & Greed multiplier (0.8 o 1.2)
        raw_adj = raw * ext.fear_greed_multiplier

        # Capa 12 — DXY Macro: si el dólar sube fuerte, reducir score Long
        if ext.dxy_strong_up:
            raw_adj *= 0.85
            logger.info(
                "[scoring] DXY alcista → 0.85x Long score ({:.1f} → {:.1f})",
                raw_adj / 0.85, raw_adj,
            )

        # Aplicar session multiplier (max +20%)
        final = raw_adj * min(context.session_multiplier, 1.2)

        bd.total = min(int(final), 100)
        return bd.total, bd
