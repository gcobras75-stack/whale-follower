# -*- coding: utf-8 -*-
"""
position_monitor.py — Monitor OKX SWAP positions for TP/SL hits.

Runs every 30s, checks open positions against stored targets,
closes via /api/v5/trade/close-position when hit.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Dict, Optional

from loguru import logger

import config

# ── Tracked positions ────────────────────────────────────────────────────────
# Key: instId (e.g. "ETH-USDT-SWAP")
# Value: {"pair", "entry", "tp", "sl", "side", "size_usd", "opened_at"}
_tracked: Dict[str, dict] = {}

_INST_TO_PAIR = {
    "BTC-USDT-SWAP":  "BTCUSDT",
    "ETH-USDT-SWAP":  "ETHUSDT",
    "SOL-USDT-SWAP":  "SOLUSDT",
    "BNB-USDT-SWAP":  "BNBUSDT",
    "DOGE-USDT-SWAP": "DOGEUSDT",
    "XRP-USDT-SWAP":  "XRPUSDT",
    "ADA-USDT-SWAP":  "ADAUSDT",
    "AVAX-USDT-SWAP": "AVAXUSDT",
    "LINK-USDT-SWAP": "LINKUSDT",
}


def register_position(
    inst_id: str,
    pair: str,
    entry: float,
    tp: float,
    sl: float,
    side: str = "buy",
    size_usd: float = 0.0,
) -> None:
    """Call after opening an OKX position to start monitoring it."""
    _tracked[inst_id] = {
        "pair": pair,
        "entry": entry,
        "tp": tp,
        "sl": sl,
        "side": side,
        "size_usd": size_usd,
        "opened_at": time.time(),
    }
    logger.info(
        "[pos_mon] Registered {} entry={:.2f} TP={:.2f} SL={:.2f}",
        inst_id, entry, tp, sl,
    )


def get_tracked() -> Dict[str, dict]:
    return dict(_tracked)


class PositionMonitor:
    """Async loop that checks OKX positions every 30s for TP/SL."""

    def __init__(
        self,
        okx_executor,
        price_getter: Callable[[], Dict[str, float]],
        telegram_fn=None,
    ):
        self._okx = okx_executor
        self._get_prices = price_getter
        self._send_telegram = telegram_fn
        self._interval = 30  # seconds

    async def run(self) -> None:
        """Main loop — call via asyncio.create_task(monitor.run())."""
        logger.info("[pos_mon] Started — checking every {}s", self._interval)
        while True:
            try:
                await self._check_cycle()
            except Exception as exc:
                logger.error("[pos_mon] cycle error: {!r}", exc)
            await asyncio.sleep(self._interval)

    async def _check_cycle(self) -> None:
        if not self._okx or not self._okx.enabled:
            return

        # 1. Get real positions from OKX API
        positions = await self._okx.get_positions()
        if not positions:
            # If we have tracked positions but OKX says none → they were
            # closed externally (manually, liquidated). Clean up.
            if _tracked:
                logger.info("[pos_mon] OKX has 0 positions, clearing {} tracked", len(_tracked))
                _tracked.clear()
            return

        prices = self._get_prices()

        # 2. Auto-register untracked positions (recovery after restart)
        for p in positions:
            inst_id = p["instId"]
            if inst_id not in _tracked:
                pair = _INST_TO_PAIR.get(inst_id)
                if not pair:
                    continue
                avg_px = p["avgPx"]
                if avg_px <= 0:
                    continue
                sl_pct = config.STOP_LOSS_PCT
                rr = config.RISK_REWARD
                side = "buy" if p["pos"] > 0 else "sell"
                if side == "buy":
                    sl = avg_px * (1.0 - sl_pct)
                    tp = avg_px * (1.0 + sl_pct * rr)
                else:
                    sl = avg_px * (1.0 + sl_pct)
                    tp = avg_px * (1.0 - sl_pct * rr)
                register_position(
                    inst_id, pair, avg_px, tp, sl,
                    side=side, size_usd=p["margin"],
                )
                logger.warning(
                    "[pos_mon] Auto-registered untracked position {} "
                    "entry={:.2f} TP={:.2f} SL={:.2f}",
                    inst_id, avg_px, tp, sl,
                )

        # 3. Check each tracked position for TP/SL
        for p in positions:
            inst_id = p["instId"]
            info = _tracked.get(inst_id)
            if not info:
                continue

            pair = info["pair"]
            price = prices.get(pair, 0.0)
            if price <= 0:
                continue

            side = info["side"]
            tp = info["tp"]
            sl = info["sl"]
            reason = None

            if side == "buy":
                if price >= tp:
                    reason = "TP"
                elif price <= sl:
                    reason = "SL"
            else:  # sell/short
                if price <= tp:
                    reason = "TP"
                elif price >= sl:
                    reason = "SL"

            if reason:
                await self._close_position(p, info, price, reason)

    async def _close_position(self, pos: dict, info: dict, price: float, reason: str) -> None:
        inst_id = pos["instId"]
        pair = info["pair"]
        entry = info["entry"]

        logger.info(
            "[pos_mon] {} HIT for {} — price={:.2f} entry={:.2f} closing...",
            reason, inst_id, price, entry,
        )

        # Use OKX close-position endpoint (works for SWAP)
        path = "/api/v5/trade/close-position"
        body_dict = {
            "instId": inst_id,
            "mgnMode": pos.get("mgnMode", "isolated"),
        }
        pos_side = pos.get("posSide", "net")
        if pos_side and pos_side != "net":
            body_dict["posSide"] = pos_side

        body = json.dumps(body_dict)
        success = False
        try:
            from okx_executor import _session, _BASE_URL, _auth_headers, _TIMEOUT_QUERY
            async with _session() as session:
                resp = await session.post(
                    _BASE_URL + path,
                    headers=_auth_headers("POST", path, body),
                    data=body,
                    timeout=_TIMEOUT_QUERY,
                )
                data = await resp.json()
            if data.get("code") == "0":
                success = True
            else:
                logger.error(
                    "[pos_mon] close FAILED {}: {} {}",
                    inst_id, data.get("code"), data.get("msg"),
                )
        except Exception as exc:
            logger.error("[pos_mon] close exception {}: {!r}", inst_id, exc)

        if success:
            # Calculate PnL
            if info["side"] == "buy":
                pnl_pct = (price - entry) / entry * 100
            else:
                pnl_pct = (entry - price) / entry * 100
            pnl_usd = info.get("size_usd", 0) * pnl_pct / 100

            emoji = "🟢" if reason == "TP" else "🔴"
            msg = (
                f"{emoji} **{reason} HIT** — {pair}\n"
                f"Entry: ${entry:.2f} → Exit: ${price:.2f}\n"
                f"PnL: {pnl_pct:+.2f}% (≈${pnl_usd:+.2f})\n"
                f"Duration: {self._format_duration(info.get('opened_at', 0))}"
            )
            logger.info("[pos_mon] CLOSED {} {} pnl={:+.2f}%", inst_id, reason, pnl_pct)

            # Remove from tracking
            _tracked.pop(inst_id, None)

            # Telegram notification
            if self._send_telegram:
                try:
                    await self._send_telegram(msg)
                except Exception as exc:
                    logger.warning("[pos_mon] Telegram notify failed: {}", exc)

    @staticmethod
    def _format_duration(opened_at: float) -> str:
        if opened_at <= 0:
            return "?"
        secs = int(time.time() - opened_at)
        if secs < 60:
            return f"{secs}s"
        if secs < 3600:
            return f"{secs // 60}m {secs % 60}s"
        hours = secs // 3600
        mins = (secs % 3600) // 60
        return f"{hours}h {mins}m"
