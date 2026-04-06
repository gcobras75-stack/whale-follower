"""
aggregator.py — Whale Follower Bot
Simultaneous WebSocket connections to Kraken, Bybit, OKX.
Kraken reemplaza Binance (geo-bloqueado en México/VPS, HTTP 451).
Normalizes trades, computes CVD + CVD velocity, detects stop cascades.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
        # Binance geo-bloqueado en México/VPS — reemplazado por Kraken
        if config.ENABLE_KRAKEN:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("kraken", self._kraken_handler),
                name="kraken"
            ))
        if config.ENABLE_BYBIT:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("bybit", self._bybit_handler),
                name="bybit"
            ))
            # ETHBTC en Bybit spot — necesario para arbitraje triangular real
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("bybit_ethbtc", self._bybit_ethbtc_handler),
                name="bybit_ethbtc"
            ))
        if config.ENABLE_OKX:
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("okx", self._okx_handler),
                name="okx"
            ))
            # ETH-BTC en OKX spot — segunda fuente para triangular arb
            tasks.append(asyncio.create_task(
                self._connect_with_backoff("okx_ethbtc", self._okx_ethbtc_handler),
                name="okx_ethbtc"
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

    # ── Kraken ────────────────────────────────────────────────────────────────
    # Kraken WS v2: wss://ws.kraken.com/v2
    # Subscribe: {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD","ETH/USD"]}}
    # Message: {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":65000,"qty":0.1,"timestamp":"..."}]}
    # Nota: Kraken usa USD (no USDT) — precio equivalente para CVD

    @staticmethod
    def _kraken_symbol(pair: str) -> str:
        """BTCUSDT → BTC/USD  |  ETHUSDT → ETH/USD  |  SOLUSDT → SOL/USD"""
        base = pair.replace("USDT", "").replace("BUSD", "")
        return f"{base}/USD"

    @staticmethod
    def _kraken_to_pair(symbol: str) -> str:
        """BTC/USD → BTCUSDT"""
        base = symbol.split("/")[0]
        return f"{base}USDT"

    async def _kraken_handler(self) -> None:
        url = "wss://ws.kraken.com/v2"
        symbols = [self._kraken_symbol(p) for p in config.TRADING_PAIRS]
        sub_msg = json.dumps({
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": symbols},
        })
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            await ws.send(sub_msg)
            logger.success(f"[kraken] Connected ({len(symbols)} pairs).")
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("channel") != "trade" or msg.get("type") not in ("update", "snapshot"):
                    continue
                for t in msg.get("data", []):
                    pair = self._kraken_to_pair(t.get("symbol", "BTC/USD"))
                    trade = Trade(
                        exchange="kraken",
                        price=float(t["price"]),
                        quantity=float(t["qty"]),
                        side=t["side"],          # "buy" | "sell"
                        # Kraken timestamp es ISO string "2024-01-01T00:00:00.000000Z"
                        timestamp=datetime.fromisoformat(
                            t["timestamp"].replace("Z", "+00:00")
                        ).timestamp() if isinstance(t.get("timestamp"), str)
                        else float(t.get("timestamp", time.time())),
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

    # ── ETHBTC Bybit spot ─────────────────────────────────────────────────────
    # ETH/BTC spot en Bybit — necesario para arbitraje triangular real.
    # Stream: wss://stream.bybit.com/v5/public/spot con topic publicTrade.ETHBTC
    # El precio ETHBTC spot permite comparar con el tipo implicito ETH_USDT/BTC_USDT.

    async def _bybit_ethbtc_handler(self) -> None:
        url = "wss://stream.bybit.com/v5/public/spot"
        sub = json.dumps({"op": "subscribe", "args": ["publicTrade.ETHBTC"]})
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            await ws.send(sub)
            logger.success("[bybit_ethbtc] Connected (ETHBTC spot).")
            async for raw in ws:
                msg = json.loads(raw)
                if not msg.get("topic", "").startswith("publicTrade.ETHBTC"):
                    continue
                for t in msg.get("data", []):
                    trade = Trade(
                        exchange  = "bybit",
                        price     = float(t["p"]),
                        quantity  = float(t["v"]),
                        side      = t["S"].lower(),
                        timestamp = t["T"] / 1000.0,
                        pair      = "ETHBTC",
                    )
                    await self._enqueue(trade)

    # ── ETHBTC OKX spot ───────────────────────────────────────────────────────
    # ETH-BTC spot en OKX — segunda fuente de precio ETHBTC para triangular arb.

    async def _okx_ethbtc_handler(self) -> None:
        url = "wss://ws.okx.com:8443/ws/v5/public"
        sub = json.dumps({"op": "subscribe", "args": [{"channel": "trades", "instId": "ETH-BTC"}]})
        async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
            await ws.send(sub)
            logger.success("[okx_ethbtc] Connected (ETH-BTC spot).")
            async for raw in ws:
                msg = json.loads(raw)
                if msg.get("arg", {}).get("instId") != "ETH-BTC":
                    continue
                for t in msg.get("data", []):
                    trade = Trade(
                        exchange  = "okx",
                        price     = float(t["px"]),
                        quantity  = float(t["sz"]),
                        side      = t["side"],
                        timestamp = int(t["ts"]) / 1000.0,
                        pair      = "ETHBTC",
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
