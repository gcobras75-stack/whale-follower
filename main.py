# -*- coding: utf-8 -*-
"""
main.py -- Whale Follower Bot -- Sprint 4
HTTP healthcheck arranca PRIMERO.
Sprint 4: CVD combinado, Liquidation Map, Fear & Greed, News Filter,
On-Chain, Order Book, Correlacion BTC-ETH, Session Volume,
Machine Learning XGBoost, Dashboard diario.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
import uuid

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
_executor_ref = None   # para healthcheck
_arb_ref      = None   # para healthcheck arb


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
    if _arb_ref is not None:
        s = _arb_ref.summary()
        payload["arb"] = {
            "total_pnl_usd":    s.total_pnl_usd,
            "funding_pnl":      s.funding_pnl_usd,
            "cross_opps_1h":    s.cross_opps_1h,
            "lead_triggers_1h": s.lead_triggers_1h,
            "tri_spread_pct":   s.tri_spread_pct,
        }
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


# ── Import helper ─────────────────────────────────────────────────────────────

def _try_import(module_name: str):
    """Importar un modulo Sprint 4 de forma segura; retorna None si falla."""
    try:
        import importlib
        return importlib.import_module(module_name)
    except Exception as exc:
        logger.warning(f"[main] Modulo {module_name} no disponible: {exc}")
        return None


# ── Trading loop ─────────────────────────────────────────────────────────────
async def trading_loop() -> None:
    global _signal_count, _ready, _error_msg, _executor_ref, _arb_ref

    try:
        import config
        from aggregator        import Aggregator
        from context_engine    import ContextEngine
        from bybit_executor    import BybitTestnetExecutor
        from multi_pair        import MultiPairMonitor
        from pair_selector     import PairSelector
        from capital_allocator import CapitalAllocator
        from risk_manager      import RiskManager
        from scoring_engine    import ExtendedContext
        import alerts
    except Exception as exc:
        _error_msg = str(exc)
        logger.error(f"[main] Import error: {exc}")
        while True:
            await asyncio.sleep(60)

    # ── Arbitraje (tolerante a fallos) ───────────────────────────────────────
    arb_mod = _try_import("arb_engine")
    arb_engine = arb_mod.ArbEngine(production=False) if arb_mod else None
    _arb_ref = arb_engine

    # ── Estrategias avanzadas (tolerantes a fallos) ───────────────────────────
    mr_mod  = _try_import("mean_reversion")
    grid_mod = _try_import("grid_trading")
    ofi_mod  = _try_import("ofi_strategy")
    mom_mod  = _try_import("momentum_scaling")
    dn_mod   = _try_import("delta_neutral")

    mean_rev  = mr_mod.MeanReversionEngine(production=False)   if mr_mod  else None
    grid_eng  = grid_mod.GridTradingEngine(production=False)   if grid_mod else None
    ofi_eng   = ofi_mod.OFIEngine(production=False)            if ofi_mod  else None
    mom_eng   = mom_mod.MomentumScalingEngine(production=False) if mom_mod else None
    dn_eng    = dn_mod.DeltaNeutralEngine(production=False)    if dn_mod  else None

    # ── Modulos Sprint 4 (tolerantes a fallos) ────────────────────────────────
    cvd_comb_mod    = _try_import("cvd_combined")
    liq_map_mod     = _try_import("liquidation_map")
    fear_greed_mod  = _try_import("fear_greed")
    news_filter_mod = _try_import("news_filter")
    onchain_mod     = _try_import("onchain")
    orderbook_mod   = _try_import("orderbook")
    correlation_mod = _try_import("correlation")
    session_vol_mod = _try_import("session_volume")
    ml_mod          = _try_import("ml_model")
    dashboard_mod   = _try_import("dashboard")

    # Instanciar
    cvd_combined  = cvd_comb_mod.CVDCombinedEngine()   if cvd_comb_mod    else None
    liq_map       = liq_map_mod.LiquidationMap()        if liq_map_mod     else None
    fear_greed    = fear_greed_mod.FearGreedEngine()    if fear_greed_mod  else None
    news_filter   = news_filter_mod.NewsFilter()        if news_filter_mod else None
    onchain       = onchain_mod.OnChainEngine()         if onchain_mod     else None
    orderbook     = orderbook_mod.OrderBookEngine()     if orderbook_mod   else None
    correlation   = correlation_mod.CorrelationEngine() if correlation_mod else None
    session_vol   = session_vol_mod.SessionVolumeTracker() if session_vol_mod else None
    ml_model      = ml_mod.MLModel()                   if ml_mod          else None

    # ── Motores principales ────────────────────────────────────────────────────
    aggregator = Aggregator()
    context    = ContextEngine()
    monitor    = MultiPairMonitor()
    selector   = PairSelector()
    allocator  = CapitalAllocator()
    risk_mgr   = RiskManager(config.PAPER_CAPITAL)
    executor   = BybitTestnetExecutor()
    _executor_ref = executor

    dashboard  = (dashboard_mod.DashboardReporter(executor)
                  if dashboard_mod else None)

    # ── Tareas de fondo ────────────────────────────────────────────────────────
    asyncio.create_task(context.run(), name="context_engine")
    asyncio.create_task(alerts.handle_telegram_commands(executor), name="telegram_commands")

    if fear_greed:
        asyncio.create_task(fear_greed.run(), name="fear_greed")
    if news_filter:
        asyncio.create_task(news_filter.run(), name="news_filter")
    if onchain:
        asyncio.create_task(onchain.run(), name="onchain")
    if liq_map:
        asyncio.create_task(liq_map.run(), name="liquidation_map")
    if orderbook:
        asyncio.create_task(orderbook.run(), name="orderbook")
    if session_vol:
        asyncio.create_task(session_vol.run(), name="session_volume")
    if dashboard:
        asyncio.create_task(dashboard.run(), name="dashboard")
    if arb_engine:
        asyncio.create_task(arb_engine.run(), name="arb_engine")
    if dn_eng:
        asyncio.create_task(dn_eng.run(), name="delta_neutral")

    _ready = True

    # ── Banner ────────────────────────────────────────────────────────────────
    active_layers = sum([
        cvd_combined is not None, liq_map is not None,
        fear_greed is not None,   news_filter is not None,
        onchain is not None,      orderbook is not None,
        correlation is not None,  session_vol is not None,
        ml_model is not None,     dashboard is not None,
    ])
    logger.info(f" Arbitraje:  {'ACTIVO (4 estrategias)' if arb_engine else 'NO DISPONIBLE'}")
    strats_active = sum([mean_rev is not None, grid_eng is not None,
                         ofi_eng is not None, mom_eng is not None, dn_eng is not None])
    logger.info(f" Estrategias avanzadas: {strats_active}/5 activas")
    logger.info(f"  Mean Reversion:   {'ON' if mean_rev else 'OFF'}")
    logger.info(f"  Grid Trading:     {'ON' if grid_eng  else 'OFF'}")
    logger.info(f"  OFI Strategy:     {'ON' if ofi_eng   else 'OFF'}")
    logger.info(f"  Momentum Scaling: {'ON' if mom_eng   else 'OFF'}")
    logger.info(f"  Delta Neutral:    {'ON' if dn_eng    else 'OFF'}")

    logger.info("=" * 60)
    logger.info(" Whale Follower Bot -- Sprint 4")
    logger.info(f" Pares:     {', '.join(config.TRADING_PAIRS)}")
    logger.info(f" Threshold: {config.SIGNAL_SCORE_THRESHOLD} | High: {config.HIGH_CONFIDENCE_SCORE}")
    logger.info(f" Capital:   ${config.PAPER_CAPITAL:,.0f} | Modo: {config.ALLOCATION_MODE}")
    logger.info(f" Capas S4:  {active_layers}/10 activas")
    logger.info(f" ML:        {'DISPONIBLE' if ml_model else 'NO DISPONIBLE'}")
    logger.info(f" Dashboard: {'SI' if dashboard else 'NO'}")
    logger.info("=" * 60)

    recent_signals: list = []

    # ── Loop principal ─────────────────────────────────────────────────────────
    async for trade, _legacy_state in aggregator.stream():

        # 1. Gestionar trades activos
        if executor._enabled:
            await executor.update_trades(trade.price, pair=trade.pair)

        # 2. Actualizar motores de datos en tiempo real (sync)
        if cvd_combined:
            cvd_combined.update(trade)
        if correlation:
            correlation.update(trade)
        if session_vol:
            session_vol.record_volume(trade.quantity, trade.timestamp)
        if arb_engine:
            arb_engine.on_trade(trade.exchange, trade.pair, trade.price, trade.timestamp * 1000)
            # ETHBTC spot → triangular arb con precio real (no implicito)
            if trade.pair == "ETHBTC":
                arb_engine.on_eth_btc_price(trade.price)

        # Estrategias avanzadas — datos en tiempo real
        if grid_eng:
            grid_eng.on_price(trade.pair, trade.price)
        if ofi_eng:
            ofi_eng.on_trade_volume(trade.pair, trade.quantity)
            ofi_eng.on_price(trade.pair, trade.price)
            # OFI recibe order book completo multi-nivel para calculo preciso
            if orderbook:
                ob_bids, ob_asks = orderbook.raw_snapshot(trade.pair)
                if ob_bids and ob_asks:
                    ofi_eng.on_orderbook(trade.pair, ob_bids, ob_asks)
            # OFI necesita CVD velocity 10s para confirmacion cruzada
            if cvd_combined:
                snap_cvd = cvd_combined.snapshot()
                ofi_eng.on_cvd_snapshot(snap_cvd.weighted_velocity)
        if dn_eng:
            dn_eng.on_perp_price(trade.exchange, trade.price)

        # 3. Procesar spring detection
        ctx_snapshot = context.snapshot()
        signal = monitor.process(trade, ctx_snapshot)
        if signal is None:
            continue

        # 4. Construir ExtendedContext con todos los nuevos modulos
        ext = ExtendedContext()

        if cvd_combined:
            snap = cvd_combined.snapshot()
            ext.cvd_all_positive = snap.all_three_positive
            ext.cvd_two_positive = snap.two_positive
            ext.cvd_weighted_vel = snap.weighted_velocity

        if liq_map:
            zone_active, zone_pts = liq_map.is_zone_active(signal.entry_price)
            ext.liq_zone_active = zone_active
            ext.liq_extra_pts   = zone_pts

        if fear_greed:
            fg = fear_greed.snapshot()
            ext.fear_greed_value       = fg.value
            ext.fear_greed_multiplier  = fg.multiplier
            ext.fear_greed_block_long  = fg.block_long
            ext.fear_greed_block_short = fg.block_short

        if news_filter:
            blocked, reason = news_filter.is_blocked()
            ext.news_blocked = blocked
            ext.news_reason  = reason

        if onchain:
            oc = onchain.snapshot()
            ext.onchain_signal = oc.signal
            ext.onchain_pts    = oc.pts

        if orderbook:
            ob_ratio, ob_fav, ob_pts = orderbook.imbalance(signal.pair)
            ext.ob_imbalance = ob_ratio
            ext.ob_favorable = ob_fav
            ext.ob_pts       = ob_pts

        if correlation:
            corr_sig, corr_pts = correlation.btc_eth_signal()
            ext.btc_eth_signal = corr_sig
            ext.btc_eth_pts    = corr_pts

        if session_vol:
            vol_unusual, vol_ratio, vol_pts = session_vol.is_unusual()
            ext.vol_unusual   = vol_unusual
            ext.vol_hist_ratio = vol_ratio
            ext.vol_hist_pts  = vol_pts

        # 5. Re-score la senal con ExtendedContext
        from scoring_engine import ScoringEngine
        scorer = ScoringEngine()
        new_score, new_bd = scorer.score(
            spring_data   = signal.spring_data,
            cvd           = signal.cvd_metrics,
            cascade       = signal.cascade_event,
            context       = ctx_snapshot,
            current_price = signal.entry_price,
            ext           = ext,
        )

        # 5b. Aplicar ML (ultimo filtro)
        if ml_model:
            features = {
                "cvd_velocity_3s":     signal.cvd_metrics.velocity_3s,
                "cvd_velocity_10s":    signal.cvd_metrics.velocity_10s,
                "cvd_velocity_30s":    signal.cvd_metrics.velocity_30s,
                "cvd_acceleration":    signal.cvd_metrics.acceleration,
                "volume_ratio":        signal.spring_data.get("vol_ratio", 1.0),
                "cascade_intensity":   float(signal.cascade_event.sell_count),
                "funding_rate":        ctx_snapshot.funding_rate or 0.0,
                "oi_change_pct":       ctx_snapshot.oi_change_pct or 0.0,
                "fear_greed_index":    float(ext.fear_greed_value),
                "session_multiplier":  ctx_snapshot.session_multiplier,
                "orderbook_imbalance": ext.ob_imbalance,
                "spring_drop_pct":     signal.spring_data.get("drop_pct", 0.0),
                "spring_bounce_pct":   signal.spring_data.get("bounce_pct", 0.0),
                "price_vs_vwap":       0.0,
                "hour_of_day":         float(time.gmtime().tm_hour),
            }
            ml_block, ml_prob = ml_model.should_block(features)
            ext.ml_probability = ml_prob
            ext.ml_block       = ml_block
            if ml_block:
                logger.info(f"[main] {signal.pair} bloqueado por ML prob={ml_prob:.2f}")
                if dashboard:
                    dashboard.record_signal(new_score, operated=False, blocked_by_ml=True)
                continue

        # Si la senal fue bloqueada por filtro (score=0 con block_reason)
        if new_score == 0 and new_bd.block_reason:
            logger.info(f"[main] {signal.pair} bloqueado: {new_bd.block_reason}")
            if dashboard:
                dashboard.record_signal(new_score, operated=False, blocked_by_ml=False)
            continue

        if new_score < config.SIGNAL_SCORE_THRESHOLD:
            continue

        _signal_count += 1
        logger.info(
            f"[main] *** SIGNAL #{_signal_count} {signal.pair} "
            f"score={new_score} entry={signal.entry_price:.2f} "
            f"fg={ext.fear_greed_value} ml={ext.ml_probability:.2f} ***"
        )

        # Activar lead-lag arb en BTC spring
        if signal.pair == "BTCUSDT" and arb_engine:
            arb_engine.on_btc_spring(signal.entry_price)

        # Mean Reversion: pasar datos de cascade al motor
        if mean_rev:
            cascade = signal.cascade_event
            ob_ratio_mr, _, _ = orderbook.imbalance(signal.pair) if orderbook else (0.5, False, 0)
            cvd_delta = signal.cvd_metrics.velocity_3s if hasattr(signal, "cvd_metrics") else 0.0
            mean_rev.on_trade(
                pair              = signal.pair,
                price             = signal.entry_price,
                cascade_intensity = cascade.intensity,
                cascade_velocity  = cascade.trades_per_sec,
                ob_ratio          = ob_ratio_mr,
                cvd_delta_3s      = cvd_delta,
            )
            mean_rev.try_add_tramo(signal.pair, signal.entry_price, cvd_delta)

        # Momentum Scaling: pasar todas las metricas CVD + OB
        if mom_eng and hasattr(signal, "cvd_metrics"):
            cv = signal.cvd_metrics
            ob_ratio_mom, _, _ = orderbook.imbalance(signal.pair) if orderbook else (0.5, False, 0)
            cvd_snap = cvd_combined.snapshot() if cvd_combined else None
            mom_eng.on_tick(
                pair               = signal.pair,
                price              = signal.entry_price,
                cvd_vel_3s         = cv.velocity_3s,
                cvd_vel_10s        = cv.velocity_10s,
                cvd_vel_30s        = cv.velocity_30s,
                cvd_acceleration   = cv.acceleration,
                all_three_positive = cvd_snap.all_three_positive if cvd_snap else False,
                ob_ratio           = ob_ratio_mom,
                fg_blocked         = ext.fear_greed_block_long,
            )

        # 6. Buffer de señales recientes
        now = time.time()
        recent_signals.append(signal)
        recent_signals = [s for s in recent_signals if now - s.timestamp < 10]

        # 7. Risk manager
        open_count = executor.open_count()
        allowed, reason = risk_mgr.can_open_trade(new_score, open_count)
        if not allowed:
            logger.info(f"[main] {signal.pair} bloqueado por risk: {reason}")
            asyncio.create_task(alerts.dispatch_multi(
                signals=[signal], allocation=None,
                all_scores=monitor.get_all_scores(),
            ))
            if dashboard:
                dashboard.record_signal(new_score, operated=False, blocked_by_ml=False)
            continue

        # 8. Seleccion de par y asignacion de capital
        selection  = selector.select(recent_signals, monitor.btc_spring_ts, monitor.get_all_scores())
        allocation = allocator.allocate(
            signals               = recent_signals,
            available_capital     = executor.available_capital(),
            mode                  = config.ALLOCATION_MODE,
            btc_correlation_active = selection.btc_correlation_active,
        )

        # 9. Ejecutar trades (features guardadas para ML record_outcome al cierre)
        for alloc in allocation.trades:
            paper = await executor.open_trade(
                signal_score     = alloc.signal_score,
                entry_price      = alloc.entry_price,
                stop_loss        = alloc.stop_loss,
                take_profit      = alloc.take_profit,
                signal_id        = str(uuid.uuid4()),
                pair             = alloc.pair,
                size_usd         = alloc.size_usd,
                signal_features  = features if ml_model else None,
            )
            if paper:
                logger.info(
                    f"[main] Trade {alloc.pair} ${paper.size_usd:.0f} "
                    f"entry={paper.entry_price:.2f} sl={paper.stop_loss:.2f}"
                )

        if dashboard:
            dashboard.record_signal(new_score, operated=bool(allocation.trades), blocked_by_ml=False)

        # 10. Alerta multi-par Telegram + Supabase
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
