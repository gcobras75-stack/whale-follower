"""
pair_selector.py — Whale Follower Bot — Sprint 3
Selects best trading pair(s) using score, volume, and BTC correlation.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from loguru import logger

import config
from multi_pair import PairSignal


@dataclass
class SelectionResult:
    selected_pairs: List[str]
    mode_used: str
    btc_correlation_active: bool
    scores: Dict[str, int]


class PairSelector:
    """Picks the best pair(s) to trade from a list of recent PairSignals.

    Selection criteria (applied in order):
      1. Highest score wins.
      2. If 2+ scores are within 5 points, the one with a higher vol_ratio wins.
      3. If a BTC spring fired within CORRELATION_WINDOW_SECS, ETH and SOL
         receive a flat +10 bonus to their adjusted score.
      4. Additionally, ETH score is multiplied by 1.15 when BTC correlation is
         active (73% historical follow-through).
    """

    _SCORE_TIE_BAND: int = 5        # points within which vol_ratio is the tiebreaker
    _ETH_FOLLOW_BOOST: float = 1.15  # multiplier reflecting ETH follow probability
    _CORR_FLAT_BONUS: int = 10       # flat bonus for ETH+SOL on BTC spring

    def select(
        self,
        signals: List[PairSignal],
        btc_spring_ts: float,
        all_scores: Dict[str, int],
    ) -> SelectionResult:
        """Return a SelectionResult identifying which pair(s) to enter.

        Parameters
        ----------
        signals:
            Recent PairSignals (may be empty).
        btc_spring_ts:
            Unix timestamp of the last BTC spring; 0.0 if none.
        all_scores:
            Mapping pair → last_score from MultiPairMonitor.get_all_scores().
        """
        if not signals:
            logger.debug("PairSelector: no signals to evaluate")
            return SelectionResult(
                selected_pairs=[],
                mode_used="none",
                btc_correlation_active=False,
                scores=dict(all_scores),
            )

        now = time.time()
        corr_window: int = getattr(config, "CORRELATION_WINDOW_SECS", 60)
        btc_correlation_active = (
            btc_spring_ts > 0.0 and (now - btc_spring_ts) <= corr_window
        )

        # Build adjusted score map from the provided signals
        adjusted: Dict[str, float] = {}
        vol_ratio_map: Dict[str, float] = {}

        for sig in signals:
            base_score: float = float(sig.score)
            vol_ratio_map[sig.pair] = float(sig.spring_data.get("vol_ratio", 1.0))

            if btc_correlation_active:
                if sig.pair in ("ETHUSDT", "SOLUSDT"):
                    base_score += self._CORR_FLAT_BONUS
                if sig.pair == "ETHUSDT":
                    base_score *= self._ETH_FOLLOW_BOOST

            adjusted[sig.pair] = base_score

        if not adjusted:
            return SelectionResult(
                selected_pairs=[],
                mode_used="none",
                btc_correlation_active=btc_correlation_active,
                scores=dict(all_scores),
            )

        # Sort pairs by adjusted score descending
        ranked: List[str] = sorted(adjusted, key=lambda p: adjusted[p], reverse=True)
        best_pair = ranked[0]
        best_score = adjusted[best_pair]

        logger.debug(
            "PairSelector: adjusted scores={} btc_corr={}",
            {p: round(adjusted[p], 1) for p in ranked},
            btc_correlation_active,
        )

        # Criterion 2: check if any other pair is within the tie band
        tied_pairs: List[str] = [
            p for p in ranked if best_score - adjusted[p] <= self._SCORE_TIE_BAND
        ]

        selected: List[str]
        mode_used: str

        if len(tied_pairs) > 1:
            # Break tie by vol_ratio
            tied_pairs.sort(key=lambda p: vol_ratio_map.get(p, 0.0), reverse=True)
            selected = [tied_pairs[0]]
            mode_used = "vol_ratio_tiebreak"
            logger.info(
                "PairSelector: tie between {} resolved by vol_ratio → {}",
                tied_pairs, selected[0],
            )
        else:
            selected = [best_pair]
            mode_used = "highest_score"

        if btc_correlation_active and "BTCUSDT" in adjusted:
            # Under BTC correlation, allow up to 2 pairs if ETH also qualified
            if "ETHUSDT" in adjusted and "ETHUSDT" not in selected:
                selected.append("ETHUSDT")
                mode_used = "btc_correlation"
                logger.info(
                    "PairSelector: BTC correlation active — adding ETHUSDT to selection"
                )

        logger.info(
            "PairSelector: selected={} mode={} btc_corr={}",
            selected, mode_used, btc_correlation_active,
        )
        return SelectionResult(
            selected_pairs=selected,
            mode_used=mode_used,
            btc_correlation_active=btc_correlation_active,
            scores=dict(all_scores),
        )
