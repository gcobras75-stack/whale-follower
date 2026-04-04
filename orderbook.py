# -*- coding: utf-8 -*-
"""
orderbook.py -- Whale Follower Bot
Detects order book imbalance via Binance combined WebSocket stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Dict, Tuple

import websockets
from loguru import logger

# ── Constants ─────────────────────────────────────────────────────────────────

# Bybit linear — sin restriccion geografica en Railway
_BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

_BYBIT_TOPICS = [
    "orderbook.50.BTCUSDT",
    "orderbook.50.ETHUSDT",
    "orderbook.50.SOLUSDT",
    "orderbook.50.BNBUSDT",
]

# topic -> pair
_TOPIC_TO_PAIR: Dict[str, str] = {
    "orderbook.50.BTCUSDT": "BTCUSDT",
    "orderbook.50.ETHUSDT": "ETHUSDT",
    "orderbook.50.SOLUSDT": "SOLUSDT",
    "orderbook.50.BNBUSDT": "BNBUSDT",
}

_STALE_SECS = 5.0
_FAVORABLE_THRESHOLD = 0.6


class OrderBookEngine:
    """
    Connects to Binance combined depth stream and tracks order book imbalance.

    Imbalance ratio:
        0.0 = all asks (bearish pressure)
        1.0 = all bids (bullish pressure)
    """

    def __init__(self) -> None:
        # pair -> {"imbalance": float, "ts": float}
        self._data: Dict[str, Dict[str, float]] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def imbalance(self, pair: str = "BTCUSDT") -> Tuple[float, bool, int]:
        """
        Returns (imbalance_ratio, favorable, pts).
        Falls back to neutral (0.5, False, 0) if data is missing or stale.
        """
        entry = self._data.get(pair)
        if entry is None:
            return (0.5, False, 0)

        age = time.time() - entry["ts"]
        if age > _STALE_SECS:
            logger.debug(
                "[orderbook] Stale data for {} ({:.1f}s old), returning neutral", pair, age
            )
            return (0.5, False, 0)

        ratio = entry["imbalance"]
        favorable = ratio > _FAVORABLE_THRESHOLD
        pts = 8 if favorable else 0
        return (ratio, favorable, pts)

    # ── Background Task ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Infinite loop — connects to Binance and processes depth updates."""
        logger.info("[orderbook] Starting order book engine")
        backoff = 1.0
        while True:
            try:
                await self._connect_and_process()
                backoff = 1.0  # reset on clean disconnect
            except Exception as exc:
                logger.warning("[orderbook] WebSocket error: {}", exc)
                logger.info("[orderbook] Reconnecting in {:.0f}s...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _connect_and_process(self) -> None:
        logger.info("[orderbook] Connecting to Bybit order book stream")
        async with websockets.connect(
            _BYBIT_WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            # Suscribirse a los 4 pares
            sub_msg = json.dumps({"op": "subscribe", "args": _BYBIT_TOPICS})
            await ws.send(sub_msg)
            logger.info("[orderbook] Connected to Bybit order book (4 pairs)")
            async for raw in ws:
                try:
                    self._process_message(raw)
                except Exception as exc:
                    logger.warning("[orderbook] Message parse error: {}", exc)

    def _process_message(self, raw: str) -> None:
        msg = json.loads(raw)
        topic = msg.get("topic", "")
        pair = _TOPIC_TO_PAIR.get(topic)
        if pair is None:
            return

        # Bybit OB message: data.b = bids [[price, size], ...], data.a = asks
        data = msg.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])

        bid_volume = sum(float(entry[1]) for entry in bids)
        ask_volume = sum(float(entry[1]) for entry in asks)

        total = bid_volume + ask_volume
        if total == 0.0:
            return

        ratio = bid_volume / total
        self._data[pair] = {"imbalance": ratio, "ts": time.time()}
        logger.debug(
            "[orderbook] {} imbalance={:.3f} bids={:.2f} asks={:.2f}",
            pair, ratio, bid_volume, ask_volume,
        )
