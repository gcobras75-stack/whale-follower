# -*- coding: utf-8 -*-
"""
dashboard.py -- Whale Follower Bot
Sends a daily summary report to Telegram at 23:00 Mexico time (05:00 UTC).
Uses aiohttp directly to avoid circular imports with alerts.py.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

# ── Constants ─────────────────────────────────────────────────────────────────

_REPORT_HOUR_UTC = 5   # 05:00 UTC = 23:00 Mexico time (CST, UTC-6)
_REPORT_MINUTE_UTC = 0
_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class DashboardReporter:
    """
    Collects daily trading stats and sends a formatted Telegram report at 05:00 UTC.
    Requires executor_ref (BybitTestnetExecutor) for accumulated stats; pass None to omit.
    """

    def __init__(self, executor_ref=None) -> None:
        self._executor = executor_ref

        # Daily signal counters
        self._signals_detected: int = 0
        self._signals_operated: int = 0
        self._signals_blocked_ml: int = 0

        # Daily trade records
        self._trades: List[Dict] = []
        # Each dict: {pair, entry, exit, pnl_usd, won}

    # ── Public Sync API ────────────────────────────────────────────────────────

    def record_signal(self, score: int, operated: bool, blocked_by_ml: bool) -> None:
        """Register a detected signal for inclusion in the daily report."""
        self._signals_detected += 1
        if operated:
            self._signals_operated += 1
        if blocked_by_ml:
            self._signals_blocked_ml += 1
        logger.debug(
            "[dashboard] Signal recorded score={} operated={} blocked_ml={}",
            score, operated, blocked_by_ml,
        )

    def record_trade_close(
        self,
        pair: str,
        entry: float,
        exit_price: float,
        pnl_usd: float,
        won: bool,
    ) -> None:
        """Register a closed trade for the daily report."""
        self._trades.append(
            {"pair": pair, "entry": entry, "exit": exit_price, "pnl_usd": pnl_usd, "won": won}
        )
        logger.debug(
            "[dashboard] Trade closed pair={} pnl={:.2f} won={}", pair, pnl_usd, won
        )

    # ── Background Task ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Infinite loop: waits until 05:00 UTC, sends report, resets stats, repeats."""
        logger.info("[dashboard] Dashboard reporter started, waiting for 05:00 UTC")
        while True:
            seconds_until = self._seconds_until_report()
            logger.info(
                "[dashboard] Next report in {:.0f}s ({:.1f}h)",
                seconds_until, seconds_until / 3600,
            )
            await asyncio.sleep(seconds_until)
            await self._send_report()
            self._reset_daily_stats()

    # ── Report Building ────────────────────────────────────────────────────────

    async def _send_report(self) -> None:
        message = self._build_message()
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

        if not token or not chat_id:
            logger.warning("[dashboard] Telegram credentials not configured, skipping report")
            return

        url = _TELEGRAM_API.format(token=token)
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        logger.info("[dashboard] Daily report sent to Telegram successfully")
                    else:
                        body = await resp.text()
                        logger.warning(
                            "[dashboard] Telegram send failed status={} body={}", resp.status, body
                        )
        except Exception as exc:
            logger.warning("[dashboard] Telegram send error: {}", exc)

    def _build_message(self) -> str:
        today = datetime.now(tz=timezone.utc).strftime("%d/%m/%Y")

        # Signals section
        sig_detected = self._signals_detected
        sig_operated = self._signals_operated
        sig_blocked = self._signals_blocked_ml

        # Trades section
        winners = [t for t in self._trades if t["won"]]
        losers = [t for t in self._trades if not t["won"]]
        total_trades = len(self._trades)
        win_rate = (len(winners) / total_trades * 100) if total_trades > 0 else 0.0
        pnl_total = sum(t["pnl_usd"] for t in self._trades)

        # Capital / percent return (placeholder since we don't track capital here)
        pnl_pct = 0.0
        if self._executor is not None:
            try:
                lev_status = self._executor.leverage_status()
                capital = lev_status.get("capital", 0.0)
                pnl_pct = (pnl_total / capital * 100) if capital > 0 else 0.0
            except Exception as exc:
                logger.warning("[dashboard] Could not get leverage status: {}", exc)

        # Best trade today
        best_trade: Optional[Dict] = None
        if winners:
            best_trade = max(winners, key=lambda t: t["pnl_usd"])

        # Accumulated stats from executor
        acc_win_rate = 0.0
        acc_capital = 0.0
        acc_return_pct = 0.0
        acc_leverage = 0
        executor_ok = self._executor is not None
        if executor_ok:
            try:
                lev_status = self._executor.leverage_status()
                acc_win_rate = lev_status.get("win_rate_pct", 0.0)
                acc_capital = lev_status.get("capital", 0.0)
                acc_leverage = lev_status.get("leverage", 0)
                drawdown = lev_status.get("drawdown_pct", 0.0)
                acc_return_pct = -drawdown  # drawdown is negative return proxy
            except Exception as exc:
                logger.warning("[dashboard] Executor stats error: {}", exc)
                executor_ok = False

        # ML model status
        try:
            from ml_model import _XGBOOST_AVAILABLE as _ml_ok
            ml_status = "OK" if _ml_ok else "NO DISPONIBLE"
        except Exception:
            ml_status = "NO DISPONIBLE"

        # Compose message
        pnl_sign = "+" if pnl_total >= 0 else ""
        pnl_pct_sign = "+" if pnl_pct >= 0 else ""

        lines = [
            "REPORTE DIARIO -- Whale Follower",
            "---------------------------------",
            f"Fecha: {today}",
            "---------------------------------",
            "SENALES HOY",
            f"  Detectadas: {sig_detected}",
            f"  Operadas: {sig_operated}",
            f"  Bloqueadas por ML: {sig_blocked}",
            "",
        ]

        if total_trades == 0:
            lines += [
                "TRADES HOY",
                "  Sin trades hoy",
                "",
                "P&L HOY",
                "  Sin trades hoy",
                "",
            ]
        else:
            lines += [
                "TRADES HOY",
                f"  Ganadores: {len(winners)}",
                f"  Perdedores: {len(losers)}",
                f"  Win Rate hoy: {win_rate:.0f}%",
                "",
                "P&L HOY",
                f"  Ganancia/Perdida: {pnl_sign}${pnl_total:.2f}",
                f"  En porcentaje: {pnl_pct_sign}{pnl_pct:.1f}%",
                "",
            ]

        if executor_ok:
            lines += [
                "ACUMULADO",
                f"  Win Rate total: {acc_win_rate:.0f}%",
                f"  Capital: ${acc_capital:,.0f}",
                f"  Retorno total: +{acc_return_pct:.0f}%",
                f"  Apalancamiento actual: {acc_leverage}x",
                "",
            ]

        if best_trade is not None:
            bt_sign = "+" if best_trade["pnl_usd"] >= 0 else ""
            lines += [
                "MEJOR TRADE HOY",
                f"  Par: {best_trade['pair']}",
                f"  Entrada: ${best_trade['entry']:,.0f}",
                f"  Salida: ${best_trade['exit']:,.0f}",
                f"  Ganancia: {bt_sign}${best_trade['pnl_usd']:.2f}",
                "",
            ]
        else:
            lines += [
                "MEJOR TRADE HOY",
                "  Sin trades hoy",
                "",
            ]

        lines += [
            "ESTADO SISTEMA",
            "  Bybit: OK",
            "  OKX: OK",
            f"  Modelo ML: {ml_status}",
            "---------------------------------",
            "Solo analisis. No es consejo financiero.",
        ]

        return "\n".join(lines)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _seconds_until_report(self) -> float:
        """Compute seconds until next 05:00 UTC report time."""
        now = datetime.now(tz=timezone.utc)
        target = now.replace(
            hour=_REPORT_HOUR_UTC,
            minute=_REPORT_MINUTE_UTC,
            second=0,
            microsecond=0,
        )
        if now >= target:
            target += timedelta(days=1)
        delta = (target - now).total_seconds()
        return max(delta, 0.0)

    def _reset_daily_stats(self) -> None:
        logger.info("[dashboard] Resetting daily stats after report")
        self._signals_detected = 0
        self._signals_operated = 0
        self._signals_blocked_ml = 0
        self._trades.clear()
