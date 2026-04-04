"""
aggregator.py — Whale Follower Bot
Simultaneous WebSocket connections to Binance, Bybit, OKX.
Normalizes trades, computes CVD + CVD velocity, detects stop cascades.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import AsyncGenerator, Callable, Deque, Dict, List

import websockets
from loguru import logger

import config

# ── Normalized trade object ───────────────────────────────────────────────────

@dataclass
class Trade:
    exchange: str
    price: float
    quantity: float
    side: str          # "buy" | "sell"
    timestamp: float   # unix seconds (float for sub-second precision)
    pair: str = "BTCUSDT"  # normalised symbol, e.g. "ETHUSDT"


# ── CVD state shared across all consumers ─────────────────────────────────────

@dataclass
class CVDState:
    """Mutable CVD accumulator.  Thread-safe enough for single-event-loop asyncio."""
    cumulative: float = 0.0
    # Ring buffers for velocity: (timestamp, cvd_snapshot)
    _snapshots: Deque[tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=2000)
    )

    def update(self, trade: Trade) -> None:
        signed = trade.quantity if trade.side == "buy" else -trade.quantity
        self.cumulative += signed
        self._snapshots.append((trade.timestamp, self.cumulative))

    def velocity(self, window_secs: float) -> float:
        """CVD change over the last *window_secs* seconds."""
        now = time.time()
        cutoff = now - window_secs
        # Find oldest snapshot still inside the window
        baseline = None
        for ts, cvd in self._snapshots:
            if ts >= cutoff:
                baseline = cvd
                break
        if baseline is None:
            return 0.0
        return self.cumulative - baseline


# ── Cascade detector ──────────────────────────────────────────────────────────

class CascadeDetector:
    """Detects bursts of sell trades (stop cascade)."""

    def __init__(self) -> None:
        self._window: Deque[float] = deque()  # timestamps of sell trades

    def add(self, trade: Trade) -> bool:
        """Return True if a stop cascade is currently active."""
        if trade.side != "sell":
            return False
        now = trade.timestamp
        self._window.append(now)
        cutoff = now - config.CASCADE_SECS
        while self._window and self._window[0] < cutoff:
            self._window.popleft()
        return len(self._window) >= config.CASCADE_SELL_COUNT


# ── Main aggregator ───────────────────────────────────────────────────────────

class Aggregator:
    """
    Connects to all active exchanges and publishes a unified trade stream.

    Usage:
        agg = Aggregator()
        async for trade, state in agg.stream():
            ...
    """

    def __init__(self) -> None:
        self.cvd = CVDState()
        self.cascade = CascadeDetector()
        self._queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=10_000)
        self._tasks: List[asyncio.Task] = []

    # ── Public API ────────────────────────────────────────────────────────────

    async def stream(self) -> AsyncGenerator[tuple[Trade, Dict], None]:
        """Yield (trade, enriched_state_dict) for every incoming trade."""
        self._tasks = self._launch_exchange_tasks()
        while True:
            trade = await self._queue.get()
            self.cvd.update(trade)
            cascade_active = self.cascade.add(trade)
            state = {
                "cvd": self.cvd.cumulative,
                "cvd_velocity_3s":  self.cvd.velocity(3),
                "cvd_velocity_10s": self.cvd.velocity(10),
                "cvd_velocity_30s": self.cvd.velocity(30),
                "cascade": cascade_active,
            }
            yield trade, state

    def cancel(self) -> None:
        for t in self._tasks:
            t.cancel()

    # ── Exchange launchers ────────────────────────────────────────────────────

    def _launch_exchange_tasks(self) -> List[asyncio.Task]:
        tasks = []
        if config.ENABLE_BINANCE:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("binance", self._binance_handler),
                name="binance"
            ))
        if config.ENABLE_BYBIT:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("bybit", self._bybit_handler),
                name="bybit"
            ))
        if config.ENABLE_OKX:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("okx", self._okx_handler),
                name="okx"
            ))
        return tasks

    # ── Reconnection wrapper ──────────────────────────────────────────────────

    async def _connect_with_backoff(
        self, name: str, handler: Callable[[], None]
    ) -> None:
        delay = config.BACKOFF_BASE
        while True:
            try:
                logger.info(f"[{name}] Connecting…")
                await handler()
            except asyncio.CancelledError:
                logger.info(f"[{name}] Task cancelled.")
                return
            except Exception as exc:
                logger.warning(f"[{name}] Disconnected: {exc}. Retry in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, config.BACKOFF_MAX)
            else:
                delay = config.BACKOFF_BASE   # reset on clean exit

    # ── Helpers de par ────────────────────────────────────────────────────────

    @staticmethod
    def _binance_stream_name(pair: str) -> str:
        """BTCUSDT → btcusdt@aggTrade"""
        return f"{pair.lower()}@aggTrade"

    @staticmethod
    def _okx_inst_id(pair: str) -> str:
        """BTCUSDT → BTC-USDT"""
        base = pair.replace("USDT", "")
        return f"{base}-USDT"

    # ── Binance ───────────────────────────────────────────────────────────────
    # Combined stream: wss://stream.binance.com:9443/stream?streams=s1/s2/...
    # Each message: {"stream":"btcusdt@aggtrade","data":{p,q,m,T,...}}

    async def _binance_handler(self) -> None:
        streams = "/".join(self._binance_stream_name(p) for p in config.TRADING_PAIRS)
        url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            logger.success(f"[binance] Connected ({len(config.TRADING_PAIRS)} pairs).")
            async for raw in ws:
                msg = json.loads(raw)
                data = msg.get("data", {})
                if data.get("e") != "aggTrade":
                    continue
                # stream name "ethusdt@aggtrade" → "ETHUSDT"
                stream: str = msg.get("stream", "btcusdt@aggtrade")
                pair = stream.split("@")[0].upper()
                side = "sell" if data["m"] else "buy"
                trade = Trade(
                    exchange="binance",
                    price=float(data["p"]),
                    quantity=float(data["q"]),
                    side=side,
                    timestamp=data["T"] / 1000.0,
                    pair=pair,
                )
                await self._enqueue(trade)

    # ── Bybit ─────────────────────────────────────────────────────────────────
    # Subscribe all pairs in one message: args=["publicTrade.BTCUSDT",...]
    # Each message topic: "publicTrade.ETHUSDT"

    async def _bybit_handler(self) -> None:
        url = "wss://stream.bybit.com/v5/public/linear"
        args = [f"publicTrade.{p}" for p in config.TRADING_PAIRS]
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            logger.success(f"[bybit] Connected ({len(args)} pairs).")
            async for raw in ws:
                msg = json.loads(raw)
                topic: str = msg.get("topic", "")
                if not topic.startswith("publicTrade."):
                    continue
                pair = topic.split(".")[1]          # "publicTrade.ETHUSDT" → "ETHUSDT"
                for t in msg.get("data", []):
                    trade = Trade(
                        exchange="bybit",
                        price=float(t["p"]),
                        quantity=float(t["v"]),
                        side=t["S"].lower(),
                        timestamp=t["T"] / 1000.0,
                        pair=pair,
                    )
                    await self._enqueue(trade)

    # ── OKX ───────────────────────────────────────────────────────────────────
    # Subscribe: args=[{"channel":"trades","instId":"BTC-USDT"},...]
    # Each message arg.instId: "ETH-USDT" → "ETHUSDT"

    async def _okx_handler(self) -> None:
        url = "wss://ws.okx.com:8443/ws/v5/public"
        sub_args = [{"channel": "trades", "instId": self._okx_inst_id(p)}
                    for p in config.TRADING_PAIRS]
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": sub_args}))
            logger.success(f"[okx] Connected ({len(sub_args)} pairs).")
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("arg", {}).get("channel") != "trades":
                    continue
                inst_id: str = msg["arg"].get("instId", "BTC-USDT")
                pair = inst_id.replace("-", "")     # "ETH-USDT" → "ETHUSDT"
                for t in msg.get("data", []):
                    trade = Trade(
                        exchange="okx",
                        price=float(t["px"]),
                        quantity=float(t["sz"]),
                        side=t["side"],
                        timestamp=int(t["ts"]) / 1000.0,
                        pair=pair,
                    )
                    await self._enqueue(trade)

    # ── Internal helpers ──────────────────────────────────────────────────────

    async def _enqueue(self, trade: Trade) -> None:
        try:
            self._queue.put_nowait(trade)
        except asyncio.QueueFull:
            logger.warning("Trade queue full — dropping oldest trade.")
            self._queue.get_nowait()
            self._queue.put_nowait(trade)
