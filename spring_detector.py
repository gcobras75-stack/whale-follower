"""
spring_detector.py — Whale Follower Bot — Sprint 2
Detecta el patrón Spring de Wyckoff en la ventana deslizante de precios.
En Sprint 2 devuelve spring_data dict que el ScoringEngine procesa.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple

from loguru import logger

import config
from aggregator import Trade


@dataclass
class PricePoint:
    price:     float
    timestamp: float
    cvd:       float
    exchange:  str
    volume:    float


class VolumeTracker:
    """Volumen rodante por exchange (1 minuto)."""

    def __init__(self) -> None:
        self._windows: Dict[str, Deque[Tuple[float, float]]] = {}

    def add(self, exchange: str, volume: float, ts: float) -> None:
        if exchange not in self._windows:
            self._windows[exchange] = deque()
        self._windows[exchange].append((ts, volume))
        self._prune(exchange, ts)

    def total_last_minute(self, ts: float) -> float:
        total = 0.0
        for exc, dq in self._windows.items():
            self._prune(exc, ts)
            total += sum(v for _, v in dq)
        return total

    def strongest(self, ts: float) -> str:
        best_exc, best_vol = "", 0.0
        for exc, dq in self._windows.items():
            self._prune(exc, ts)
            vol = sum(v for _, v in dq)
            if vol > best_vol:
                best_vol, best_exc = vol, exc
        return best_exc or "unknown"

    def _prune(self, exchange: str, now: float) -> None:
        dq     = self._windows[exchange]
        cutoff = now - 60.0
        while dq and dq[0][0] < cutoff:
            dq.popleft()


class SpringDetector:
    """
    Detecta el patrón Spring (caída + rebote + divergencia CVD).
    Devuelve un dict con los datos crudos del spring para que el
    ScoringEngine calcule el score final.
    """

    def __init__(self) -> None:
        self._window:   Deque[PricePoint] = deque()
        self._vol_tracker                  = VolumeTracker()

        # Baseline de volumen: guarda snapshots del volumen-por-minuto cada 60s.
        # Se necesitan ≥ _BASELINE_MIN_SAMPLES para que el baseline sea confiable.
        self._vol_snapshots: Deque[float]  = deque(maxlen=30)   # últimos 30 minutos
        self._last_vol_snapshot_ts: float  = 0.0
        _BASELINE_SNAPSHOT_INTERVAL        = 60.0   # una muestra cada 60s
        self._baseline_snapshot_interval   = _BASELINE_SNAPSHOT_INTERVAL
        self._BASELINE_MIN_SAMPLES         = 5      # mínimo 5 minutos de datos

        self._last_signal_ts:  float = 0.0
        self._cooldown_secs:   float = 30.0

        # ── Telemetría temporal ───────────────────────────────────────────────
        self._telemetry_interval_secs: float = 30.0
        self._last_telemetry_ts:       float = 0.0
        self._best_drop_pct:           float = 0.0
        self._best_bounce_pct:         float = 0.0
        self._best_vol_ratio:          float = 0.0
        self._eval_count:              int   = 0

    # ── API pública ────────────────────────────────────────────────────────────

    def feed(
        self, trade: Trade, cvd_value: float, regime: str = "UNKNOWN"
    ) -> Optional[Dict]:
        """
        Alimenta un trade. Devuelve spring_data dict si detecta patrón,
        o None si no hay patrón o está en cooldown.

        *regime*: régimen de mercado actual (LATERAL, TRENDING_UP,
        TRENDING_DOWN, HIGH_VOL, UNKNOWN). Determina los umbrales
        de detección vía config.SPRING_PARAMS.
        """
        now = trade.timestamp
        pt  = PricePoint(
            price     = trade.price,
            timestamp = now,
            cvd       = cvd_value,
            exchange  = trade.exchange,
            volume    = trade.quantity,
        )
        self._window.append(pt)
        self._vol_tracker.add(trade.exchange, trade.quantity, now)
        self._update_baseline(trade.quantity, now)
        self._prune_window(now)

        if now - self._last_signal_ts < self._cooldown_secs:
            return None
        if len(self._window) < 10:
            return None

        return self._evaluate(now, cvd_value, regime)

    def strongest_exchange(self, ts: float) -> str:
        return self._vol_tracker.strongest(ts)

    def current_price(self) -> float:
        if self._window:
            return self._window[-1].price
        return 0.0

    # ── Evaluación ─────────────────────────────────────────────────────────────

    def _evaluate(self, now: float, cvd_now: float,
                  regime: str = "UNKNOWN") -> Optional[Dict]:
        pts           = list(self._window)
        current_price = pts[-1].price

        # ── Seleccionar umbrales adaptativos por régimen ─────────────────────
        params     = config.SPRING_PARAMS.get(regime, config.SPRING_PARAMS["LATERAL"])
        drop_thr   = params["drop_pct"]
        bounce_thr = params["bounce_pct"]
        vol_thr    = params["vol_mult"]

        # Condición A: caída >= drop_thr en ≤ SPRING_DROP_SECS
        drop_window     = [p for p in pts if now - p.timestamp <= config.SPRING_DROP_SECS]
        if not drop_window:
            return None
        high_before     = max(p.price for p in drop_window)
        spring_low      = min(p.price for p in drop_window)
        drop_pct        = (high_before - spring_low) / high_before
        cond_a          = drop_pct >= drop_thr
        drop_intensity  = min(drop_pct / (drop_thr * 2), 1.0)

        # Condición B: rebote >= bounce_thr desde el mínimo en ≤ SPRING_BOUNCE_SECS
        bounce_window   = [p for p in pts if now - p.timestamp <= config.SPRING_BOUNCE_SECS]
        bounce_low      = min((p.price for p in bounce_window), default=current_price)
        bounce_pct      = (current_price - bounce_low) / bounce_low if bounce_low > 0 else 0.0
        cond_b          = bounce_pct >= bounce_thr
        bounce_intensity = min(bounce_pct / (bounce_thr * 2), 1.0)

        # Condición C: CVD sube mientras precio baja (divergencia)
        # cvd_at_high = CVD en el momento del precio más alto de la ventana,
        # NO drop_window[0] que es simplemente el punto más antiguo.
        if len(drop_window) >= 2:
            high_point  = max(drop_window, key=lambda p: p.price)
            cvd_at_high = high_point.cvd
            cond_c      = (cvd_now > cvd_at_high) and cond_a
            if cond_a:
                logger.debug(
                    "[spring] CVD check: cvd_now={:.2f} cvd_at_high={:.2f} "
                    "diff={:+.2f} cond_c={}",
                    cvd_now, cvd_at_high, cvd_now - cvd_at_high, cond_c,
                )
        else:
            cvd_at_high = cvd_now
            cond_c = False

        # ── Snapshot de volumen-por-minuto para baseline ────────────────────
        # Cada 60s guardamos el volumen total del último minuto como una muestra.
        # vol_ratio = vol_actual / promedio_de_esas_muestras.
        if now - self._last_vol_snapshot_ts >= self._baseline_snapshot_interval:
            self._vol_snapshots.append(self._vol_tracker.total_last_minute(now))
            self._last_vol_snapshot_ts = now

        # Condición D: volumen combinado > vol_thr × promedio
        recent_vol    = self._vol_tracker.total_last_minute(now)
        baseline_avg  = self._baseline_avg()
        if baseline_avg > 0 and len(self._vol_snapshots) >= self._BASELINE_MIN_SAMPLES:
            vol_ratio = recent_vol / baseline_avg
        else:
            vol_ratio = 1.0   # no calcular hasta tener baseline real (≥5 min)
        cond_d        = vol_ratio >= vol_thr

        # ── Telemetría temporal ───────────────────────────────────────────────
        self._eval_count += 1
        if drop_pct   > self._best_drop_pct:   self._best_drop_pct   = drop_pct
        if bounce_pct > self._best_bounce_pct: self._best_bounce_pct = bounce_pct
        if vol_ratio  > self._best_vol_ratio:  self._best_vol_ratio  = vol_ratio

        if now - self._last_telemetry_ts >= self._telemetry_interval_secs:
            # Identifica exactamente qué condición falta para el best intento
            missing = []
            if self._best_drop_pct   < drop_thr:
                missing.append(
                    f"drop {self._best_drop_pct*100:.3f}%<{drop_thr*100:.3f}%"
                )
            if self._best_bounce_pct < bounce_thr:
                missing.append(
                    f"bounce {self._best_bounce_pct*100:.3f}%<{bounce_thr*100:.3f}%"
                )
            if self._best_vol_ratio  < vol_thr:
                missing.append(
                    f"vol {self._best_vol_ratio:.2f}x<{vol_thr:.2f}x"
                )
            if not missing:
                missing.append("(ninguna — debería haber disparado)")

            proxy_score = 0
            if self._best_drop_pct   >= drop_thr:   proxy_score += 8
            if self._best_bounce_pct >= bounce_thr:  proxy_score += 12
            if self._best_vol_ratio  >= vol_thr:     proxy_score += 8

            baseline_status = (
                f"ready({len(self._vol_snapshots)})"
                if len(self._vol_snapshots) >= self._BASELINE_MIN_SAMPLES
                else f"warmup({len(self._vol_snapshots)}/{self._BASELINE_MIN_SAMPLES})"
            )
            logger.info(
                "[spring] Vela analizada | régimen={} | evals={} | proxy_score={} | "
                "best: drop={:.3f}% bounce={:.3f}% vol={:.2f}x | "
                "umbrales: drop={:.3f}% bounce={:.3f}% vol={:.2f}x | "
                "vol_baseline={} | faltó: {}",
                regime, self._eval_count, proxy_score,
                self._best_drop_pct * 100,
                self._best_bounce_pct * 100,
                self._best_vol_ratio,
                drop_thr * 100,
                bounce_thr * 100,
                vol_thr,
                baseline_status,
                ", ".join(missing),
            )
            # Reset del intervalo
            self._last_telemetry_ts = now
            self._best_drop_pct     = 0.0
            self._best_bounce_pct   = 0.0
            self._best_vol_ratio    = 0.0
            self._eval_count        = 0

        # Si ninguna condición primaria → ignorar
        if not cond_a and not cond_b:
            return None

        logger.info(
            "[spring] \u2b50 SPRING DETECTADO | drop={:.3f}% bounce={:.3f}% "
            "cond_a={} cond_b={} cond_c={} cond_d={} vol_ratio={:.2f}x",
            drop_pct * 100, bounce_pct * 100,
            cond_a, cond_b, cond_c, cond_d, vol_ratio,
        )
        self._last_signal_ts = now

        return {
            "cond_a":          cond_a,
            "cond_b":          cond_b,
            "cond_c":          cond_c,
            "cond_d":          cond_d,
            "drop_pct":        round(drop_pct * 100, 3),
            "bounce_pct":      round(bounce_pct * 100, 3),
            "drop_intensity":  drop_intensity,
            "bounce_intensity": bounce_intensity,
            "spring_low":      round(spring_low, 2),
            "current_price":   round(current_price, 2),
            "vol_ratio":       round(vol_ratio, 2),
            "strongest_exchange": self._vol_tracker.strongest(now),
            "regime":          regime,
            "score_min":       params["score_min"],
        }

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _prune_window(self, now: float) -> None:
        cutoff = now - config.WINDOW_SECS
        while self._window and self._window[0].timestamp < cutoff:
            self._window.popleft()

    def _update_baseline(self, volume: float, ts: float) -> None:
        # Legacy — ya no se usa para cálculo de vol_ratio.
        # El VolumeTracker acumula volumen por exchange y
        # _vol_snapshots guarda muestras de volumen-por-minuto cada 60s.
        pass

    def _baseline_avg(self) -> float:
        """Promedio de volumen-por-minuto de los últimos 30 snapshots."""
        if not self._vol_snapshots:
            return 0.0
        return sum(self._vol_snapshots) / len(self._vol_snapshots)
