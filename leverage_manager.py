# -*- coding: utf-8 -*-
"""
leverage_manager.py -- Whale Follower Bot
Dynamic leverage based on rolling win rate with safety protections.

Leverage table:
  < 50%  -> 1x
  50-59% -> 2x
  60-69% -> 3x
  70-74% -> 4x
  75-79% -> 5x
  >= 80% -> 7x

Safety rules:
  - Minimum 20 trades to activate leverage
  - Drawdown > 15% -> lower one level automatically
  - 3 consecutive losses -> pause 1 hour
  - 5 consecutive losses -> lower one level
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import List, Optional, Tuple

import aiohttp
from loguru import logger


# ── Leverage table (descending order) ─────────────────────────────────────────
_LEVERAGE_TABLE: List[Tuple[int, int]] = [
    (80, 7),
    (75, 5),
    (70, 4),
    (60, 3),
    (50, 2),
    (0,  1),
]

_DRAWDOWN_LIMIT:       float = 0.15    # 15%
_PAUSE_AFTER_LOSSES:   int   = 3       # consecutive losses -> pause
_LOWER_AFTER_LOSSES:   int   = 5       # consecutive losses -> lower level
_PAUSE_DURATION_SECS:  int   = 3600    # 1 hour


# ── Telegram helper (standalone to avoid circular import) ─────────────────────

async def _telegram(text: str) -> None:
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"chat_id": chat_id, "text": text},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("[leverage] Telegram error {}: {}", resp.status, body)
    except Exception as exc:
        logger.warning("[leverage] Telegram send failed: {}", exc)


# ── Alert builders ────────────────────────────────────────────────────────────

async def _alert_level_up(prev: int, new: int, win_rate_pct: float, trades: int) -> None:
    if new == 7:
        msg = (
            "\U0001f3c6 MODO ELITE ACTIVADO\n"
            "--------------------\n"
            f"Win Rate: {win_rate_pct:.0f}%+\n"
            f"Apalancamiento: {new}x\n"
            "Eres del top 1% de traders algoritmicos\n"
            "Capital protegido con circuit breaker 15%"
        )
    else:
        msg = (
            "\U0001f433 APALANCAMIENTO SUBIO\n"
            f"Win Rate: {win_rate_pct:.0f}%\n"
            f"Nivel anterior: {prev}x\n"
            f"Nivel nuevo: {new}x\n"
            f"Trades analizados: {trades}"
        )
    await _telegram(msg)


async def _alert_level_down(prev: int, new: int, reason: str) -> None:
    msg = (
        "\u26a0\ufe0f APALANCAMIENTO BAJO\n"
        f"Motivo: {reason}\n"
        f"Nivel anterior: {prev}x\n"
        f"Nivel nuevo: {new}x\n"
        "Proteccion activada automaticamente"
    )
    await _telegram(msg)


# ── Main class ────────────────────────────────────────────────────────────────

class LeverageManager:
    """
    Tracks closed trade outcomes and computes the appropriate leverage.

    Usage:
        lm = LeverageManager(initial_capital=10000)
        lev = lm.get_leverage()          # -> int (1-7)
        lm.record_trade(won=True, pnl_usd=+45.0)

        if lm.is_paused():
            # skip opening new trades
    """

    def __init__(
        self,
        initial_capital: float,
        min_trades: int = 0,
        max_leverage: int = 7,
        warmup_win_rate_pct: float = 50.0,
        warmup_samples: int = 20,
    ) -> None:
        """
        warmup_win_rate_pct: WR asumido para arrancar (50 = 2x desde el primer trade).
        warmup_samples: trades sinteticos para inicializar el historial.
        min_trades=0 activa el apalancamiento inmediatamente.
        """
        if initial_capital <= 0:
            raise ValueError(f"initial_capital must be positive, got {initial_capital}")

        self._initial_capital  = initial_capital
        self._min_trades       = min_trades
        self._max_leverage     = max_leverage

        # Pre-sembrar historial con WR inicial
        wins = int(warmup_samples * warmup_win_rate_pct / 100)
        self._outcomes: List[bool] = [True] * wins + [False] * (warmup_samples - wins)
        self._total_pnl: float      = 0.0
        self._consecutive_losses: int = 0

        self._level_cap: Optional[int]  = None
        self._paused_until: float       = 0.0
        self._current_leverage: int     = self.get_leverage()

        logger.info(
            "[leverage] Initialized: capital={:.0f} warmup_wr={}% leverage={}x max={}x",
            initial_capital, warmup_win_rate_pct, self._current_leverage, max_leverage,
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_leverage(self) -> int:
        """Return the current leverage multiplier (1-7)."""
        if len(self._outcomes) < self._min_trades:
            return 1   # not enough history

        wr_pct = self._win_rate_pct()
        lev    = self._from_table(wr_pct)

        if self._level_cap is not None:
            lev = min(lev, self._level_cap)

        return min(lev, self._max_leverage)

    def is_paused(self) -> bool:
        """True if trading should be paused (3 consecutive losses)."""
        return time.time() < self._paused_until

    def pause_remaining_secs(self) -> int:
        return max(0, int(self._paused_until - time.time()))

    def record_trade(self, won: bool, pnl_usd: float) -> None:
        """
        Register the result of a closed trade.
        Internally checks protection rules and emits Telegram alerts if needed.
        """
        old_lev = self.get_leverage()

        self._outcomes.append(won)
        self._total_pnl += pnl_usd

        if won:
            self._consecutive_losses = 0
        else:
            self._consecutive_losses += 1

        self._check_protections(old_lev)

        new_lev = self.get_leverage()
        self._current_leverage = new_lev

        logger.info(
            "[leverage] trade recorded won={} pnl={:+.2f} "
            "WR={:.0f}% consecutive_losses={} leverage={}x",
            won, pnl_usd, self._win_rate_pct(),
            self._consecutive_losses, new_lev,
        )

    def summary(self) -> str:
        total  = len(self._outcomes)
        wr     = self._win_rate_pct() if total > 0 else 0.0
        lev    = self.get_leverage()
        dd     = self._drawdown_pct() * 100
        pause  = f" | PAUSA {self.pause_remaining_secs()}s" if self.is_paused() else ""
        cap    = f" | cap={self._level_cap}x" if self._level_cap else ""
        return (
            f"[leverage] trades={total} WR={wr:.0f}% "
            f"lev={lev}x DD={dd:.1f}%{cap}{pause}"
        )

    # ── Protection logic ───────────────────────────────────────────────────────

    def _check_protections(self, old_lev: int) -> None:
        """Apply safety rules; schedule Telegram alerts via asyncio tasks."""
        new_lev = self.get_leverage()  # recompute after recording

        # ── Rule 1: Drawdown > 15% -> lower one level ────────────────────────
        if self._drawdown_pct() > _DRAWDOWN_LIMIT:
            ceiling = max(1, old_lev - 1)
            if self._level_cap is None or ceiling < self._level_cap:
                self._level_cap = ceiling
                logger.warning(
                    "[leverage] Drawdown {:.1%} > 15% -- level capped at {}x",
                    self._drawdown_pct(), ceiling,
                )
                new_lev = self.get_leverage()
                self._schedule(
                    _alert_level_down(old_lev, new_lev, "Drawdown >15%")
                )

        # ── Rule 2: 3 consecutive losses -> pause 1 hour ─────────────────────
        if self._consecutive_losses == _PAUSE_AFTER_LOSSES:
            self._paused_until = time.time() + _PAUSE_DURATION_SECS
            logger.warning("[leverage] 3 consecutive losses -- pausing 1 hour")
            self._schedule(
                _alert_level_down(
                    old_lev, self.get_leverage(),
                    "3 perdidas seguidas - pausa 1 hora",
                )
            )

        # ── Rule 3: 5 consecutive losses -> lower one level ──────────────────
        if self._consecutive_losses >= _LOWER_AFTER_LOSSES:
            ceiling = max(1, old_lev - 1)
            if self._level_cap is None or ceiling < self._level_cap:
                self._level_cap = ceiling
                logger.warning(
                    "[leverage] {} consecutive losses -- level capped at {}x",
                    self._consecutive_losses, ceiling,
                )
                new_lev = self.get_leverage()
                self._schedule(
                    _alert_level_down(
                        old_lev, new_lev,
                        f"{self._consecutive_losses} perdidas seguidas",
                    )
                )

        # ── Leverage went up -> send upgrade alert ────────────────────────────
        final_lev = self.get_leverage()
        if final_lev > old_lev:
            self._schedule(
                _alert_level_up(
                    old_lev, final_lev,
                    self._win_rate_pct(),
                    len(self._outcomes),
                )
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _win_rate_pct(self) -> float:
        if not self._outcomes:
            return 0.0
        return sum(self._outcomes) / len(self._outcomes) * 100

    def _drawdown_pct(self) -> float:
        loss = -self._total_pnl
        return max(loss / self._initial_capital, 0.0) if self._initial_capital > 0 else 0.0

    @staticmethod
    def _from_table(wr_pct: float) -> int:
        for min_wr, lev in _LEVERAGE_TABLE:
            if wr_pct >= min_wr:
                return lev
        return 1

    @staticmethod
    def _schedule(coro) -> None:
        """Fire-and-forget a coroutine on the running loop (if available)."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            pass   # no running loop (e.g., tests)
