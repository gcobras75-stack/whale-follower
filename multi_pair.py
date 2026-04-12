"""
multi_pair.py — Whale Follower Bot — Sprint 3
One spring/CVD/cascade engine per pair. Routes trades and returns signals.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from loguru import logger

import config
from aggregator import Trade
from cascade_detector import CascadeDetector, CascadeEvent
from context_engine import MarketContext
from cvd_real import CVDEngine, CVDMetrics
from scoring_engine import ScoreBreakdown, ScoringEngine
from spring_detector import SpringDetector


@dataclass
class PairSignal:
    pair: str
    score: int
    breakdown: ScoreBreakdown
    spring_data: Dict
    entry_price: float
    stop_loss: float
    take_profit: float
    cvd_metrics: CVDMetrics
    cascade_event: CascadeEvent
    timestamp: float


@dataclass
class PairState:
    pair: str
    spring: SpringDetector
    cvd: CVDEngine
    cascade: CascadeDetector
    last_score: int = 0
    last_signal_ts: float = 0.0
    last_spring_data: Optional[Dict] = None
    price_history: deque = field(default_factory=lambda: deque(maxlen=300))


class MultiPairMonitor:
    """Routes incoming trades to per-pair engines and emits PairSignal when a
    spring passes the configured score threshold."""

    # Minimum seconds between signals for the same pair to avoid spam.
    _SIGNAL_COOLDOWN_SECS: float = 30.0

    def __init__(self, pairs: Optional[List[str]] = None,
                 regime_detector=None) -> None:
        target_pairs = pairs if pairs is not None else config.TRADING_PAIRS
        self._states: Dict[str, PairState] = {}
        self._scorer = ScoringEngine()
        self._btc_spring_ts: float = 0.0
        self._regime_det = regime_detector   # MarketRegimeDetector (o None)

        for pair in target_pairs:
            self._states[pair] = PairState(
                pair=pair,
                spring=SpringDetector(),
                cvd=CVDEngine(),
                cascade=CascadeDetector(
                    window_secs=config.CASCADE_SECS,
                    min_trades=config.CASCADE_SELL_COUNT,
                ),
            )
            logger.debug("MultiPairMonitor: initialized engine for {}", pair)

    # ── Public API ─────────────────────────────────────────────────────────────

    def process(self, trade: Trade, context: MarketContext) -> Optional[PairSignal]:
        """Route *trade* to the correct pair engine.

        Returns a PairSignal when a spring is detected and its score meets the
        threshold; returns None otherwise.
        """
        pair = getattr(trade, "pair", "BTCUSDT")
        state = self._states.get(pair)
        if state is None:
            logger.trace("MultiPairMonitor: unknown pair {}, skipping", pair)
            return None

        # Update CVD
        state.cvd.update(side=trade.side, quantity=trade.quantity, timestamp=trade.timestamp)
        cvd_metrics: CVDMetrics = state.cvd.metrics()

        # Acumular historial de precios para ATR
        state.price_history.append((trade.price, trade.timestamp))

        # Update cascade
        cascade_event: CascadeEvent = state.cascade.add(
            side=trade.side, quantity=trade.quantity, timestamp=trade.timestamp
        )

        # Obtener régimen actual del par para umbrales adaptativos
        regime_str = "UNKNOWN"
        if self._regime_det is not None:
            regime_str = self._regime_det.regime(pair).value

        # Feed spring detector con régimen → umbrales adaptativos
        spring_data: Optional[Dict] = state.spring.feed(
            trade, cvd_metrics.cumulative, regime=regime_str,
        )
        if spring_data is None:
            return None

        state.last_spring_data = spring_data
        current_price: float = spring_data.get("current_price", trade.price)

        # Score the spring
        score, breakdown = self._scorer.score(
            spring_data=spring_data,
            cvd=cvd_metrics,
            cascade=cascade_event,
            context=context,
            current_price=current_price,
        )

        state.last_score = score

        # Threshold adaptativo: usa score_min del régimen si disponible
        score_threshold = spring_data.get("score_min", config.SIGNAL_SCORE_THRESHOLD)
        if score < score_threshold:
            logger.debug(
                "MultiPairMonitor: {} spring score {} below threshold {} (régimen={})",
                pair, score, score_threshold, regime_str,
            )
            return None

        # Enforce per-pair cooldown
        now = time.time()
        if now - state.last_signal_ts < self._SIGNAL_COOLDOWN_SECS:
            logger.debug(
                "MultiPairMonitor: {} signal suppressed by cooldown ({:.1f}s remaining)",
                pair,
                self._SIGNAL_COOLDOWN_SECS - (now - state.last_signal_ts),
            )
            return None

        state.last_signal_ts = now

        # Compute entry / exits — ATR dinámico con fallback a config fijo
        stop_loss, take_profit = self._atr_exits(state, current_price)

        signal = PairSignal(
            pair=pair,
            score=score,
            breakdown=breakdown,
            spring_data=spring_data,
            entry_price=current_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            cvd_metrics=cvd_metrics,
            cascade_event=cascade_event,
            timestamp=now,
        )

        if pair == "BTCUSDT":
            self.mark_btc_spring(now)

        logger.info(
            "MultiPairMonitor: {} SIGNAL score={} entry={:.2f} sl={:.2f} tp={:.2f}",
            pair, score, current_price, stop_loss, take_profit,
        )
        return signal

    # ── ATR-based dynamic risk management ──────────────────────────────────────────────────

    def _atr_exits(self, state: PairState, entry: float) -> Tuple[float, float]:
        """SL = entry - 1.5*ATR, TP = entry + 1.5*ATR*RR (RR 3:1).
        Fallback a STOP_LOSS_PCT / RISK_REWARD fijos de config si no hay datos suficientes.
        """
        atr = self._compute_atr14(state)
        if atr and atr > 0:
            sl_dist = 1.5 * atr
            stop_loss   = entry - sl_dist
            take_profit = entry + sl_dist * config.RISK_REWARD
            logger.info(
                "MultiPairMonitor: {} ATR={:.4f} SL={:.4f} TP={:.4f} (dinámico 1.5×ATR)",
                state.pair, atr, stop_loss, take_profit,
            )
            return stop_loss, take_profit
        # Fallback: valores fijos de config.py
        stop_loss   = entry * (1.0 - config.STOP_LOSS_PCT)
        take_profit = entry + (entry - stop_loss) * config.RISK_REWARD
        logger.debug("MultiPairMonitor: {} SL fijo (ATR insuf. datos)", state.pair)
        return stop_loss, take_profit

    def _compute_atr14(self, state: PairState) -> Optional[float]:
        """ATR(14) calculado desde pseudo-velas de 1 minuto usando tick data."""
        if len(state.price_history) < 14:
            return None

        # Agrupar en pseudo-velas de 1 minuto
        buckets: Dict[int, Dict[str, float]] = {}
        for price, ts in state.price_history:
            bkt = int(ts // 60)
            if bkt not in buckets:
                buckets[bkt] = {"h": price, "l": price, "c": price}
            else:
                b = buckets[bkt]
                if price > b["h"]:
                    b["h"] = price
                if price < b["l"]:
                    b["l"] = price
                b["c"] = price

        sorted_b = sorted(buckets.items())
        if len(sorted_b) < 2:
            return None

        # True Range por cada vela
        trs: List[float] = []
        for i in range(1, len(sorted_b)):
            _, cur  = sorted_b[i]
            _, prev = sorted_b[i - 1]
            trs.append(max(
                cur["h"] - cur["l"],
                abs(cur["h"] - prev["c"]),
                abs(cur["l"] - prev["c"]),
            ))

        if not trs:
            return None

        # ATR = media de los últimos 14 TR disponibles
        recent = trs[-14:]
        return sum(recent) / len(recent)

    def get_all_scores(self) -> Dict[str, int]:
        """Return the most recent score for every pair."""
        return {pair: state.last_score for pair, state in self._states.items()}

    def get_pair_state(self, pair: str) -> Optional[PairState]:
        """Return the internal PairState for *pair*, or None if unknown."""
        return self._states.get(pair)

    def mark_btc_spring(self, ts: float) -> None:
        """Record the timestamp of the most recent BTC spring."""
        self._btc_spring_ts = ts
        logger.debug("MultiPairMonitor: BTC spring timestamp recorded at {:.3f}", ts)

    @property
    def btc_spring_ts(self) -> float:
        """Last BTC spring timestamp, or 0.0 if none recorded yet."""
        return self._btc_spring_ts
