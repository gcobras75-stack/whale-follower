"""
multi_pair.py — Whale Follower Bot — Sprint 3
One spring/CVD/cascade engine per pair. Routes trades and returns signals.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

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


class MultiPairMonitor:
    """Routes incoming trades to per-pair engines and emits PairSignal when a
    spring passes the configured score threshold."""

    # Minimum seconds between signals for the same pair to avoid spam.
    _SIGNAL_COOLDOWN_SECS: float = 30.0

    def __init__(self, pairs: Optional[List[str]] = None) -> None:
        target_pairs = pairs if pairs is not None else config.TRADING_PAIRS
        self._states: Dict[str, PairState] = {}
        self._scorer = ScoringEngine()
        self._btc_spring_ts: float = 0.0

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

        # Update cascade
        cascade_event: CascadeEvent = state.cascade.add(
            side=trade.side, quantity=trade.quantity, timestamp=trade.timestamp
        )

        # Feed spring detector
        spring_data: Optional[Dict] = state.spring.feed(trade, cvd_metrics.cumulative)
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

        if score < config.SIGNAL_SCORE_THRESHOLD:
            logger.debug(
                "MultiPairMonitor: {} spring score {} below threshold {}",
                pair, score, config.SIGNAL_SCORE_THRESHOLD,
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

        # Compute entry / exits
        stop_loss = current_price * (1.0 - config.STOP_LOSS_PCT)
        take_profit = current_price + (current_price - stop_loss) * config.RISK_REWARD

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
