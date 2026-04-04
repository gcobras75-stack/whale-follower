"""
main.py — Whale Follower Bot — Sprint 2
HTTP healthcheck arranca PRIMERO (lección del Sprint 1).
Luego: CVD engine, Cascade detector, Context engine, Spring detector,
Scoring engine, Bybit executor, Telegram command handler.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import time

from aiohttp import web
from loguru import logger

# ── Logging sin dependencia de config ─────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    level="INFO",
    colorize=False,
)

# ── Estado global ─────────────────────────────────────────────────────────────
_start_time   = time.time()
_signal_count = 0
_ready        = False
_error_msg    = ""
_executor_ref = None   # set once trading_loop starts, for healthcheck


# ── Healthcheck HTTP ─────────────────────────────────────────────────────────
async def health_handler(request: web.Request) -> web.Response:
    uptime = int(time.time() - _start_time)
    payload = {
        "status":          "ok",
        "uptime_secs":     uptime,
        "signals_emitted": _signal_count,
        "ready":           _ready,
        "pair":            os.getenv("TRADING_PAIR", "BTC/USDT"),
        "exchanges": {
            "binance": os.getenv("ENABLE_BINANCE", "true").lower() == "true",
            "bybit":   os.getenv("ENABLE_BYBIT",   "true").lower() == "true",
            "okx":     os.getenv("ENABLE_OKX",     "true").lower() == "true",
        },
        "error": _error_msg or None,
    }
    if _executor_ref is not None:
        payload["leverage"] = _executor_ref.leverage_status()
    return web.json_response(payload)


async def start_healthcheck() -> web.AppRunner:
    port = int(os.environ.get("PORT", 8080))
    app  = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/",       health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info(f"[health] HTTP server on 0.0.0.0:{port}")
    return runner


# ── Trading loop ─────────────────────────────────────────────────────────────
async def trading_loop() -> None:
    global _signal_count, _ready, _error_msg

    try:
        import config
        from aggregator        import Aggregator
        from context_engine    import ContextEngine
        from bybit_executor    import BybitTestnetExecutor
        from multi_pair        import MultiPairMonitor
        from pair_selector     import PairSelector
        from capital_allocator import CapitalAllocator
        from risk_manager      import RiskManager
        import alerts
    except Exception as exc:
        _error_msg = str(exc)
        logger.error(f"[main] Import error: {exc}")
        while True:
            await asyncio.sleep(60)

    # ── Inicializar motores ────────────────────────────────────────────────────
    global _executor_ref
    aggregator = Aggregator()
    context    = ContextEngine()
    monitor    = MultiPairMonitor()
    selector   = PairSelector()
    allocator  = CapitalAllocator()
    risk_mgr   = RiskManager(config.PAPER_CAPITAL)
    executor   = BybitTestnetExecutor()
    _executor_ref = executor

    asyncio.create_task(context.run(), name="context_engine")
    asyncio.create_task(
        alerts.handle_telegram_commands(executor),
        name="telegram_commands",
    )

    _ready = True

    logger.info("=" * 60)
    logger.info(" Whale Follower Bot -- Sprint 3 (Multi-Par)")
    logger.info(f" Pares:     {', '.join(config.TRADING_PAIRS)}")
    logger.info(f" Modo:      {config.ALLOCATION_MODE} | Corr: {config.CORRELATION_WINDOW_SECS}s")
    logger.info(f" Exchanges: B={config.ENABLE_BINANCE} By={config.ENABLE_BYBIT} O={config.ENABLE_OKX}")
    logger.info(f" Threshold: {config.SIGNAL_SCORE_THRESHOLD} | High: {config.HIGH_CONFIDENCE_SCORE}")
    logger.info(f" Capital:   ${config.PAPER_CAPITAL:,.0f}")
    logger.info(f" Bybit:     {'ENABLED' if executor._enabled else 'DISABLED'}")
    logger.info("=" * 60)

    recent_signals: list = []

    # ── Loop principal ─────────────────────────────────────────────────────────
    async for trade, _legacy_state in aggregator.stream():

        # 1. Gestionar trades activos del par recibido
        if executor._enabled:
            await executor.update_trades(trade.price, pair=trade.pair)

        # 2. Procesar en monitor multi-par
        ctx_snapshot = context.snapshot()
        signal = monitor.process(trade, ctx_snapshot)
        if signal is None:
            continue

        _signal_count += 1
        logger.info(
            f"[main] *** SIGNAL #{_signal_count} {signal.pair} "
            f"score={signal.score} entry={signal.entry_price:.2f} ***"
        )

        # Actualizar buffer de señales recientes (ventana 10s para correlación)
        now = time.time()
        recent_signals.append(signal)
        recent_signals = [s for s in recent_signals if now - s.timestamp < 10]

        # 3. Verificar risk manager
        open_count = executor.open_count()
        allowed, reason = risk_mgr.can_open_trade(signal.score, open_count)
        if not allowed:
            logger.info(f"[main] {signal.pair} bloqueado: {reason}")
            asyncio.create_task(alerts.dispatch_multi(
                signals=[signal], allocation=None,
                all_scores=monitor.get_all_scores(),
            ))
            continue

        # 4. Selección de par(es) y asignación de capital
        selection  = selector.select(recent_signals, monitor.btc_spring_ts, monitor.get_all_scores())
        allocation = allocator.allocate(
            signals=recent_signals,
            available_capital=executor.available_capital(),
            mode=config.ALLOCATION_MODE,
            btc_correlation_active=selection.btc_correlation_active,
        )

        # 5. Ejecutar trades
        import uuid
        for alloc in allocation.trades:
            paper = await executor.open_trade(
                signal_score = alloc.signal_score,
                entry_price  = alloc.entry_price,
                stop_loss    = alloc.stop_loss,
                take_profit  = alloc.take_profit,
                signal_id    = str(uuid.uuid4()),
                pair         = alloc.pair,
                size_usd     = alloc.size_usd,
            )
            if paper:
                logger.info(
                    f"[main] Trade {alloc.pair} ${paper.size_usd:.0f} "
                    f"entry={paper.entry_price:.2f} sl={paper.stop_loss:.2f}"
                )

        # 6. Alerta multi-par Telegram + Supabase
        asyncio.create_task(alerts.dispatch_multi(
            signals=recent_signals,
            allocation=allocation,
            all_scores=monitor.get_all_scores(),
        ))


# ── Graceful shutdown ─────────────────────────────────────────────────────────
def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    def _shutdown():
        logger.info("[main] Shutdown signal.")
        for task in asyncio.all_tasks(loop):
            task.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass


# ── Entry point ───────────────────────────────────────────────────────────────
async def main() -> None:
    runner = await start_healthcheck()
    trading_task = asyncio.create_task(trading_loop())
    try:
        await trading_task
    except asyncio.CancelledError:
        logger.info("[main] Cancelled.")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
