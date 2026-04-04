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


# ── Healthcheck HTTP ─────────────────────────────────────────────────────────
async def health_handler(request: web.Request) -> web.Response:
    uptime = int(time.time() - _start_time)
    return web.json_response({
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
    })


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
        from cascade_detector  import CascadeDetector
        from context_engine    import ContextEngine
        from cvd_real          import CVDEngine
        from scoring_engine    import ScoringEngine
        from spring_detector   import SpringDetector
        from bybit_executor    import BybitTestnetExecutor
        import alerts
    except Exception as exc:
        _error_msg = str(exc)
        logger.error(f"[main] Import error: {exc}")
        while True:
            await asyncio.sleep(60)

    # ── Inicializar motores ────────────────────────────────────────────────────
    aggregator = Aggregator()
    cvd_engine = CVDEngine()
    cascade    = CascadeDetector(
        window_secs=config.CASCADE_SECS,
        min_trades=config.CASCADE_SELL_COUNT,
    )
    context    = ContextEngine()
    detector   = SpringDetector()
    scorer     = ScoringEngine()
    executor   = BybitTestnetExecutor()

    # Tareas de fondo
    asyncio.create_task(context.run(),    name="context_engine")
    asyncio.create_task(
        alerts.handle_telegram_commands(executor),
        name="telegram_commands",
    )

    _ready = True

    logger.info("=" * 60)
    logger.info(" Whale Follower Bot -- Sprint 2")
    logger.info(f" Pair:      {config.TRADING_PAIR}")
    logger.info(f" Exchanges: B={config.ENABLE_BINANCE} By={config.ENABLE_BYBIT} O={config.ENABLE_OKX}")
    logger.info(f" Threshold: {config.SIGNAL_SCORE_THRESHOLD} | High: {config.HIGH_CONFIDENCE_SCORE}")
    logger.info(f" Paper capital: ${config.PAPER_CAPITAL:,.0f}")
    logger.info(f" Bybit Testnet: {'ENABLED' if executor._enabled else 'DISABLED (no keys)'}")
    logger.info("=" * 60)

    # ── Loop principal ─────────────────────────────────────────────────────────
    async for trade, _legacy_state in aggregator.stream():

        # 1. Actualizar CVD real
        cvd_engine.update(trade.side, trade.quantity, trade.timestamp)
        cvd_metrics = cvd_engine.metrics()

        # 2. Actualizar cascade detector
        cascade_event = cascade.add(trade.side, trade.quantity, trade.timestamp)

        # 3. Actualizar gestión activa de trades open
        if executor._enabled:
            await executor.update_trades(trade.price)

        # 4. Spring detector
        spring_data = detector.feed(trade, cvd_metrics.cumulative)
        if spring_data is None:
            continue

        # 5. Scoring engine (contexto + CVD + cascade + spring)
        ctx_snapshot = context.snapshot()
        score, breakdown = scorer.score(
            spring_data   = spring_data,
            cvd           = cvd_metrics,
            cascade       = cascade_event,
            context       = ctx_snapshot,
            current_price = trade.price,
        )

        if score < config.SIGNAL_SCORE_THRESHOLD:
            logger.debug(f"[main] Spring score={score} < threshold={config.SIGNAL_SCORE_THRESHOLD} — ignorado")
            continue

        _signal_count += 1
        entry     = spring_data["current_price"]
        sl        = spring_data["spring_low"] * (1 - config.STOP_LOSS_PCT)
        risk      = entry - sl
        tp        = entry + risk * config.RISK_REWARD
        exchange  = spring_data["strongest_exchange"]

        logger.info(
            f"[main] *** SIGNAL #{_signal_count} score={score} "
            f"entry={entry:.2f} sl={sl:.2f} tp={tp:.2f} ***"
        )

        # 6. Abrir paper trade si executor habilitado
        trade_info: str = ""
        if executor._enabled:
            import uuid
            sig_id = str(uuid.uuid4())
            paper = await executor.open_trade(
                signal_score = score,
                entry_price  = entry,
                stop_loss    = sl,
                take_profit  = tp,
                signal_id    = sig_id,
            )
            if paper:
                trade_info = (
                    f"Trade Bybit Testnet abierto\n"
                    f"   Tamaño: ${paper.size_usd:.2f} ({paper.size_contracts} BTC)"
                )

        # 7. Enviar alerta Telegram + Supabase
        asyncio.create_task(
            alerts.dispatch(
                score       = score,
                bd          = breakdown,
                spring_data = spring_data,
                exchange    = exchange,
                entry       = round(entry, 2),
                sl          = round(sl, 2),
                tp          = round(tp, 2),
                trade_info  = trade_info or None,
            )
        )


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
