"""
main.py — Whale Follower Bot
Entry point.  Starts the HTTP healthcheck, aggregator, spring detector,
and alert dispatcher — all inside a single asyncio event loop.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import time

from aiohttp import web
from loguru import logger

import config
from aggregator import Aggregator
from alerts import dispatch
from spring_detector import SpringDetector

# ── Logging setup ──────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="INFO",
    colorize=True,
)
logger.add(
    "whale_follower.log",
    rotation="100 MB",
    retention="7 days",
    compression="gz",
    level="DEBUG",
)

# ── Healthcheck HTTP server ────────────────────────────────────────────────────

_start_time = time.time()
_signal_count = 0


async def health_handler(request: web.Request) -> web.Response:
    uptime = int(time.time() - _start_time)
    return web.json_response({
        "status": "ok",
        "uptime_secs": uptime,
        "signals_emitted": _signal_count,
        "pair": config.TRADING_PAIR,
        "exchanges": {
            "binance": config.ENABLE_BINANCE,
            "bybit":   config.ENABLE_BYBIT,
            "okx":     config.ENABLE_OKX,
        },
    })


async def start_healthcheck() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/",       health_handler)   # Railway root probe
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", config.PORT)
    await site.start()
    logger.info(f"[health] HTTP server running on port {config.PORT}")
    return runner


# ── Main trading loop ──────────────────────────────────────────────────────────

async def trading_loop() -> None:
    global _signal_count

    aggregator = Aggregator()
    detector   = SpringDetector()

    logger.info("=" * 60)
    logger.info(" Whale Follower Bot — Sprint 1")
    logger.info(f" Pair: {config.TRADING_PAIR}")
    logger.info(f" Exchanges: Binance={config.ENABLE_BINANCE} "
                f"Bybit={config.ENABLE_BYBIT} OKX={config.ENABLE_OKX}")
    logger.info(f" Score threshold: {config.SIGNAL_SCORE_THRESHOLD}")
    logger.info("=" * 60)

    async for trade, state in aggregator.stream():
        signal = detector.feed(trade, state)
        if signal is not None:
            _signal_count += 1
            logger.info(
                f"[main] *** SIGNAL #{_signal_count} *** "
                f"score={signal.score} entry={signal.price_entry}"
            )
            asyncio.create_task(dispatch(signal))


# ── Graceful shutdown ──────────────────────────────────────────────────────────

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    def _shutdown():
        logger.info("[main] Shutdown signal received.")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            # Windows does not support add_signal_handler for all signals
            pass


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    runner = await start_healthcheck()
    try:
        await trading_loop()
    except asyncio.CancelledError:
        logger.info("[main] Loop cancelled — shutting down cleanly.")
    finally:
        await runner.cleanup()
        logger.info("[main] Bye.")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("[main] KeyboardInterrupt — exiting.")
    finally:
        loop.close()
