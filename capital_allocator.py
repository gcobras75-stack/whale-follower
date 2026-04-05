"""
capital_allocator.py — Whale Follower Bot — Sprint 3
Distributes capital across multiple pair signals.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from loguru import logger

import config
from multi_pair import PairSignal


@dataclass
class TradeAllocation:
    pair: str
    entry_price: float
    stop_loss: float
    take_profit: float
    capital_fraction: float   # 0.0–1.0
    size_usd: float
    signal_score: int


@dataclass
class AllocationPlan:
    trades: List[TradeAllocation]
    mode: str
    total_risk_pct: float   # fraction of capital at risk across all trades
    description: str
    preferred_exchange: str = "bybit"   # exchange con más USDT libre al momento de la señal


class CapitalAllocator:
    """Computes position sizes for qualifying PairSignals across three modes.

    A: single best pair, 100% allocation, 1% risk cap.
    B: 2+ pairs score>=80 → 50/30/20 split (top 3).
    C: BTC score>=85 + BTC correlation → BTC=40%, ETH=35%, SOL=25% (BTC first).
    Total risk capped at 3% of capital in all modes.
    """

    # ── Asignación de exchange ───────────────────────────────────────────────
    @staticmethod
    def select_exchange(bybit_usdt: float, okx_usdt: float) -> str:
        """
        Retorna el exchange con mayor USDT libre para maximizar
        la probabilidad de ejecución del trade.
        """
        if okx_usdt > bybit_usdt:
            logger.info(
                "CapitalAllocator: OKX preferido (${:.2f}) > Bybit (${:.2f})",
                okx_usdt, bybit_usdt,
            )
            return "okx"
        logger.debug(
            "CapitalAllocator: Bybit preferido (${:.2f}) >= OKX (${:.2f})",
            bybit_usdt, okx_usdt,
        )
        return "bybit"

    _MAX_TOTAL_RISK_FRACTION: float = 0.03   # 3 %
    _MODE_A_RISK_FRACTION: float = 0.01      # 1 %
    _MODE_B_MIN_SCORE: int = 80
    _MODE_C_BTC_MIN_SCORE: int = 85

    # Pair order for Mode C (BTC first, then ETH, then SOL)
    _MODE_C_BASKET: List[tuple[str, float]] = [
        ("BTCUSDT", 0.40),
        ("ETHUSDT", 0.35),
        ("SOLUSDT", 0.25),
    ]

    def allocate(
        self,
        signals: List[PairSignal],
        available_capital: float,
        mode: str,
        btc_correlation_active: bool,
    ) -> AllocationPlan:
        """Return an AllocationPlan for the given signals and allocation mode."""
        if not signals or available_capital <= 0.0:
            logger.debug("CapitalAllocator: no signals or zero capital — empty plan")
            return AllocationPlan(trades=[], mode=mode, total_risk_pct=0.0, description="No signals")

        sig_map: Dict[str, PairSignal] = {s.pair: s for s in signals}
        btc_sig = sig_map.get("BTCUSDT")
        btc_score = btc_sig.score if btc_sig else 0
        effective_mode = mode

        if mode == "C" and btc_correlation_active and btc_score >= self._MODE_C_BTC_MIN_SCORE:
            return self._plan_mode_c(sig_map, available_capital)

        if mode == "B":
            qualifying = [s for s in signals if s.score >= self._MODE_B_MIN_SCORE]
            if len(qualifying) >= 2:
                return self._plan_mode_b(qualifying, available_capital)
            # Fall through to Mode A if fewer than 2 pairs qualify
            effective_mode = "A"

        # Mode A (default or fallback)
        best = max(signals, key=lambda s: s.score)
        return self._plan_mode_a(best, available_capital, effective_mode)

    def _plan_mode_a(
        self, signal: PairSignal, capital: float, mode_label: str
    ) -> AllocationPlan:
        risk_usd = capital * self._MODE_A_RISK_FRACTION
        size_usd = min(capital, self._risk_to_size(signal, risk_usd))
        alloc = self._build_allocation(signal, fraction=1.0, size_usd=size_usd)
        risk_pct = self._actual_risk_pct(signal, size_usd, capital)
        logger.info(
            "CapitalAllocator [{}]: {} ${:.0f} risk={:.2%}",
            mode_label, signal.pair, size_usd, risk_pct,
        )
        return AllocationPlan(
            trades=[alloc],
            mode=mode_label,
            total_risk_pct=min(risk_pct, self._MAX_TOTAL_RISK_FRACTION),
            description=f"Mode {mode_label}: single pair {signal.pair}",
        )

    def _plan_mode_b(
        self, qualifying: List[PairSignal], capital: float
    ) -> AllocationPlan:
        # Sort by score descending, take top 3
        ranked = sorted(qualifying, key=lambda s: s.score, reverse=True)[:3]
        fractions = [0.50, 0.30, 0.20][: len(ranked)]

        trades: List[TradeAllocation] = []
        total_risk_pct = 0.0

        for sig, frac in zip(ranked, fractions):
            alloc_capital = capital * frac
            risk_usd = alloc_capital * self._MODE_A_RISK_FRACTION
            size_usd = min(alloc_capital, self._risk_to_size(sig, risk_usd))
            alloc = self._build_allocation(sig, fraction=frac, size_usd=size_usd)
            trades.append(alloc)
            total_risk_pct += self._actual_risk_pct(sig, size_usd, capital)

        total_risk_pct = min(total_risk_pct, self._MAX_TOTAL_RISK_FRACTION)
        pairs_str = ", ".join(t.pair for t in trades)
        logger.info(
            "CapitalAllocator [B]: pairs={} total_risk={:.2%}", pairs_str, total_risk_pct
        )
        return AllocationPlan(
            trades=trades,
            mode="B",
            total_risk_pct=total_risk_pct,
            description=f"Mode B: multi-pair [{pairs_str}]",
        )

    def _plan_mode_c(
        self, sig_map: Dict[str, PairSignal], capital: float
    ) -> AllocationPlan:
        trades: List[TradeAllocation] = []
        total_risk_pct = 0.0

        for pair, frac in self._MODE_C_BASKET:
            sig = sig_map.get(pair)
            if sig is None:
                logger.debug("CapitalAllocator [C]: {} not in signals, skipping", pair)
                continue
            alloc_capital = capital * frac
            risk_usd = alloc_capital * self._MODE_A_RISK_FRACTION
            size_usd = min(alloc_capital, self._risk_to_size(sig, risk_usd))
            alloc = self._build_allocation(sig, fraction=frac, size_usd=size_usd)
            trades.append(alloc)
            total_risk_pct += self._actual_risk_pct(sig, size_usd, capital)

        total_risk_pct = min(total_risk_pct, self._MAX_TOTAL_RISK_FRACTION)
        pairs_str = ", ".join(t.pair for t in trades)
        logger.info(
            "CapitalAllocator [C]: basket={} total_risk={:.2%}", pairs_str, total_risk_pct
        )
        return AllocationPlan(
            trades=trades,
            mode="C",
            total_risk_pct=total_risk_pct,
            description=f"Mode C: BTC-correlated basket [{pairs_str}] — BTC first",
        )

    @staticmethod
    def _risk_to_size(signal: PairSignal, risk_usd: float) -> float:
        """USD position size that risks exactly risk_usd if stop is hit."""
        entry = signal.entry_price
        stop = signal.stop_loss
        if entry <= 0.0 or stop >= entry:
            return 0.0
        stop_pct = (entry - stop) / entry
        if stop_pct <= 0.0:
            return 0.0
        return risk_usd / stop_pct

    @staticmethod
    def _actual_risk_pct(signal: PairSignal, size_usd: float, capital: float) -> float:
        """Fraction of capital lost if stop is hit."""
        if capital <= 0.0 or signal.entry_price <= 0.0:
            return 0.0
        stop_pct = (signal.entry_price - signal.stop_loss) / signal.entry_price
        loss_usd = size_usd * stop_pct
        return loss_usd / capital

    @staticmethod
    def _build_allocation(
        signal: PairSignal, fraction: float, size_usd: float
    ) -> TradeAllocation:
        return TradeAllocation(
            pair=signal.pair,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit=signal.take_profit,
            capital_fraction=fraction,
            size_usd=round(size_usd, 2),
            signal_score=signal.score,
        )
