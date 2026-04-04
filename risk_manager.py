"""
risk_manager.py — Whale Follower Bot — Sprint 3
Risk limits, circuit breaker, and drawdown protection.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Tuple

from loguru import logger


class RiskManager:
    """Enforces per-session position limits and a daily drawdown circuit breaker.

    Rules
    -----
    - Hard limit: max 3 simultaneous open trades.
    - Score gate: a 3rd trade requires score >= 85.
    - Daily drawdown circuit breaker fires at -5 % of initial capital and
      blocks all new trades for the rest of the calendar day (UTC).
    """

    _MAX_OPEN_TRADES: int = 3
    _THIRD_TRADE_MIN_SCORE: int = 85
    _CIRCUIT_BREAKER_LOSS_PCT: float = 0.05   # 5 %

    def __init__(self, initial_capital: float) -> None:
        if initial_capital <= 0.0:
            raise ValueError(f"initial_capital must be positive, got {initial_capital}")

        self._initial_capital: float = initial_capital
        self._daily_pnl_usd: float = 0.0
        self._circuit_breaker: bool = False
        self._trade_count: int = 0          # total closed trades today
        self._current_day: str = self._utc_day()

        logger.info(
            "RiskManager: initialized capital={:.2f} circuit_breaker=off", initial_capital
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def can_open_trade(self, score: int, open_count: int) -> Tuple[bool, str]:
        """Check whether opening a new trade is permitted.

        Returns (allowed: bool, reason: str).
        """
        self.check_daily_reset()

        if self._circuit_breaker:
            return False, "circuit_breaker_active"

        if open_count >= self._MAX_OPEN_TRADES:
            return False, "max_trades_reached"

        if open_count >= 2 and score < self._THIRD_TRADE_MIN_SCORE:
            return (
                False,
                f"third_trade_requires_score>={self._THIRD_TRADE_MIN_SCORE} (got {score})",
            )

        daily_loss_pct = self._daily_loss_fraction()
        if daily_loss_pct >= self._CIRCUIT_BREAKER_LOSS_PCT:
            self._circuit_breaker = True
            logger.warning(
                "RiskManager: circuit breaker ACTIVATED — daily loss {:.2%}", daily_loss_pct
            )
            return False, "circuit_breaker_activated"

        return True, "ok"

    def record_trade_close(self, pnl_usd: float) -> None:
        """Update daily P&L after a trade closes.

        Also checks whether the circuit breaker threshold has been reached.
        """
        self.check_daily_reset()
        self._daily_pnl_usd += pnl_usd
        self._trade_count += 1

        daily_loss_pct = self._daily_loss_fraction()
        logger.info(
            "RiskManager: trade closed pnl={:+.2f} daily_pnl={:+.2f} ({:.2%} of capital) trades={}",
            pnl_usd, self._daily_pnl_usd, daily_loss_pct, self._trade_count,
        )

        if not self._circuit_breaker and daily_loss_pct >= self._CIRCUIT_BREAKER_LOSS_PCT:
            self._circuit_breaker = True
            logger.warning(
                "RiskManager: circuit breaker ACTIVATED after trade close — daily loss {:.2%}",
                daily_loss_pct,
            )

    def reset_daily(self) -> None:
        """Reset all daily accumulators. Call at midnight UTC."""
        logger.info(
            "RiskManager: daily reset — previous pnl={:+.2f} trades={} circuit_breaker={}",
            self._daily_pnl_usd, self._trade_count, self._circuit_breaker,
        )
        self._daily_pnl_usd = 0.0
        self._circuit_breaker = False
        self._trade_count = 0
        self._current_day = self._utc_day()

    def check_daily_reset(self) -> None:
        """Automatically reset if the UTC calendar day has rolled over."""
        today = self._utc_day()
        if today != self._current_day:
            logger.info("RiskManager: day changed {} → {}, triggering reset", self._current_day, today)
            self.reset_daily()

    def daily_summary(self) -> str:
        """Return a human-readable string summarising today's trading activity."""
        self.check_daily_reset()
        loss_pct = self._daily_loss_fraction()
        status = "BLOCKED" if self._circuit_breaker else "active"
        return (
            f"Daily Summary [{self._current_day}] | "
            f"P&L: {self._daily_pnl_usd:+.2f} USD ({loss_pct:.2%}) | "
            f"Trades: {self._trade_count} | "
            f"Status: {status}"
        )

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def daily_pnl_usd(self) -> float:
        """Current daily P&L in USD (negative = loss)."""
        return self._daily_pnl_usd

    @property
    def circuit_breaker_active(self) -> bool:
        """True if the daily drawdown limit has been hit."""
        return self._circuit_breaker

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _daily_loss_fraction(self) -> float:
        """Return the daily loss as a fraction of initial capital (positive = loss)."""
        if self._initial_capital <= 0.0:
            return 0.0
        loss = -self._daily_pnl_usd   # negative pnl → positive loss
        return max(loss / self._initial_capital, 0.0)

    @staticmethod
    def _utc_day() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
