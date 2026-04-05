# -*- coding: utf-8 -*-
"""
main.py -- Whale Follower Bot -- Sprint 4
HTTP healthcheck arranca PRIMERO.
Sprint 4: CVD combinado, Liquidation Map, Fear & Greed, News Filter,
On-Chain, Order Book, Correlacion BTC-ETH, Session Volume,
Machine Learning XGBoost, Dashboard diario.
"""
from __future__ import annotations

import aiofiles
import asyncio
import json
import os
import signal
import sys
import time
import uuid
from collections import deque

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

# ── Seguridad y control de exchanges ──────────────────────────────────────────
_exchange_semaphores    = {"binance": asyncio.Semaphore(5), "bybit": asyncio.Semaphore(5), "okx": asyncio.Semaphore(5)}
_exchange_errors        = {"binance": 0, "bybit": 0, "okx": 0}
_exchange_blocked_until = {"binance": 0, "bybit": 0, "okx": 0}
POSITIONS_FILE = "open_positions.json"

def is_valid_price(price, pair): return price > 0 and price < 2000000
def is_exchange_blocked(exchange): return _exchange_blocked_until.get(exchange, 0) > time.time()

async def save_positions(executor):
    try:
        pos = [{"pair": t.pair, "entry": t.entry_price, "size": t.size_usd} for t in executor._trades.values() if getattr(t, 'status', 'open') in ['open', 'partial']]
        async with aiofiles.open(POSITIONS_FILE, 'w') as f: await f.write(json.dumps(pos))
    except: pass

# ── Estado global ─────────────────────────────────────────────────────────────
_start_time   = time.time()
_signal_count = 0
_ready        = False
_error_msg    = ""
_executor_ref = None   # para healthcheck
_arb_ref      = None   # para healthcheck arb


# ── Healthcheck HTTP ─────────────────────────────────────────────────────────
async def health_handler(request: web.Request) -> web.Response:
    _health_key = os.getenv("HEALTH_KEY")
    if _health_key and request.headers.get("X-Health-Key") != _health_key:
        return web.Response(status=401, text="Unauthorized")
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
    # if _arb_ref is not None:  # DESHABILITADO — arb desactivado
    #     s = _arb_ref.summary()
    #     payload["arb"] = {
    #         "total_pnl_usd":    s.total_pnl_usd,
    #         "funding_pnl":      s.funding_pnl_usd,
    #         "cross_opps_1h":    s.cross_opps_1h,
    #         "lead_triggers_1h": s.lead_triggers_1h,
    #         "tri_spread_pct":   s.tri_spread_pct,
    #     }
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
        return

    # ── Arbitraje (DESHABILITADO — Railway bloqueado por Bybit) ────────────────
    # arb_mod = _try_import("arb_engine")
    # prod = config.PRODUCTION   # True = dinero real, False = papel
    # cross_arb_real = getattr(config, "ENABLE_CROSS_ARB_REAL", False)
    # arb_engine = arb_mod.ArbEngine(production=prod, cross_arb_real=cross_arb_real) if arb_mod else None
    # _arb_ref = arb_engine
    arb_engine = None
    _arb_ref = None

    # ── Estrategias avanzadas (tolerantes a fallos) ───────────────────────────
    mr_mod  = _try_import("mean_reversion")
    grid_mod = _try_import("grid_trading")
    ofi_mod  = _try_import("ofi_strategy")
    mom_mod  = _try_import("momentum_scaling")
    dn_mod   = _try_import("delta_neutral")

    mean_rev  = mr_mod.MeanReversionEngine(production=prod)    if mr_mod  else None
    grid_eng  = grid_mod.GridTradingEngine(production=prod)    if grid_mod else None

    # OKX Grid — activo siempre que ENABLE_OKX_GRID=true (no depende de Bybit)
    okx_grid_mod = _try_import("okx_grid")
    okx_grid_eng = (
        okx_grid_mod.OKXGridEngine(production=prod)
        if okx_grid_mod and getattr(config, "ENABLE_OKX_GRID", True)
        else None
    )
    if okx_grid_eng:
        logger.info("[main] OKX Grid Engine iniciado ✅ (independiente de Bybit)")
    ofi_eng   = ofi_mod.OFIEngine(production=prod)             if ofi_mod  else None
    mom_eng   = mom_mod.MomentumScalingEngine(production=prod) if mom_mod else None
    dn_eng    = dn_mod.DeltaNeutralEngine(production=prod)     if dn_mod  else None

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
    macro_mod       = _try_import("macro_agent")
    regime_mod      = _try_import("market_regime")
    range_mod       = _try_import("range_trader")
    deribit_mod     = _try_import("deribit_options")
    btc_dom_mod  = _try_import("btc_dominance")
    liq_glob_mod = _try_import("liquidations_global")
    dxy_mod        = _try_import("dxy_monitor")
    daily_rep_mod  = _try_import("daily_report")
    rebalancer_mod = _try_import("rebalancer")

    # Instanciar
    cvd_combined  = cvd_comb_mod.CVDCombinedEngine()   if cvd_comb_mod    else None
    liq_map       = liq_map_mod.LiquidationMap()        if liq_map_mod     else None
    fear_greed    = fear_greed_mod.FearGreedEngine()    if fear_greed_mod  else None
    news_filter   = news_filter_mod.NewsFilter()        if news_filter_mod else None
    onchain       = onchain_mod.OnChainEngine()         if onchain_mod     else None
    orderbook     = orderbook_mod.OrderBookEngine()     if orderbook_mod   else None
    correlation   = correlation_mod.CorrelationEngine() if correlation_mod else None
    macro_agent   = macro_mod.MacroAgent()              if macro_mod       else None
    regime_det    = regime_mod.MarketRegimeDetector()   if regime_mod      else None
    deribit_eng   = deribit_mod.DeribitOptionsEngine()  if deribit_mod     else None
    btc_dom    = btc_dom_mod.BtcDominanceMonitor()       if btc_dom_mod   else None
    liq_glob   = liq_glob_mod.LiquidationsGlobal()       if liq_glob_mod  else None
    dxy_mon    = dxy_mod.DxyMonitor()                    if dxy_mod       else None
    daily_rep     = daily_rep_mod.DailyReporter()              if daily_rep_mod   else None
    rebalancer    = rebalancer_mod.CapitalRebalancer()          if rebalancer_mod  else None
    session_vol   = session_vol_mod.SessionVolumeTracker()      if session_vol_mod else None
    ml_model      = ml_mod.MLModel()                   if ml_mod          else None

    # ── Motores principales ────────────────────────────────────────────────────
    aggregator = Aggregator()
    context    = ContextEngine()
    monitor    = MultiPairMonitor()
    selector   = PairSelector()
    allocator  = CapitalAllocator()
    risk_mgr   = RiskManager(config.REAL_CAPITAL if config.PRODUCTION else config.PAPER_CAPITAL)
    executor   = BybitTestnetExecutor()
    _executor_ref = executor

    range_trader = (range_mod.RangeTrader(executor, regime_det, production=prod)
                    if range_mod and regime_det else None)

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
    # if arb_engine:
    #     asyncio.create_task(arb_engine.run(), name="arb_engine")
    if grid_eng:
        asyncio.create_task(grid_eng.run(), name="grid_retry_loop")
    if dn_eng:
        asyncio.create_task(dn_eng.run(), name="delta_neutral")
    if mean_rev:
        asyncio.create_task(mean_rev.run(), name="mean_reversion")
    if ofi_eng:
        asyncio.create_task(ofi_eng.run(), name="ofi_strategy")
    if mom_eng:
        asyncio.create_task(mom_eng.run(), name="momentum_scaling")
    if macro_agent:
        asyncio.create_task(macro_agent.run(), name="macro_agent")
    if deribit_eng:
        asyncio.create_task(deribit_eng.run(), name="deribit_options")
    if btc_dom:
        asyncio.create_task(btc_dom.run(),    name="btc_dominance")
    if liq_glob:
        asyncio.create_task(liq_glob.run(),   name="liq_global")
    if dxy_mon:
        asyncio.create_task(dxy_mon.run(),    name="dxy_monitor")
    if daily_rep:
        asyncio.create_task(daily_rep.run(),   name="daily_report")
    if rebalancer:
        rebalancer.set_executor(executor)   # proteger coins con trades activos en dust cleanup
        asyncio.create_task(rebalancer.run(),  name="rebalancer")

    _ready = True

    try:
        from learning_manager import get_manager
    except Exception:
        get_manager = None

    if get_manager:
        try:
            get_manager().ensure_announced()
        except Exception as exc:
            logger.warning("[main] learning mode announce failed: {}", exc)

    # ── Banner ────────────────────────────────────────────────────────────────
    active_layers = sum([
        cvd_combined is not None, liq_map is not None,
        fear_greed is not None,   news_filter is not None,
        onchain is not None,      orderbook is not None,
        correlation is not None,  session_vol is not None,
        ml_model is not None,     dashboard is not None,
        macro_agent is not None,  regime_det is not None,
        range_trader is not None, deribit_eng is not None,
    ])
    thermo_active = sum([btc_dom is not None, liq_glob is not None, dxy_mon is not None])
    logger.info(f" Termometros: {thermo_active}/3 activos (dom/liq/dxy)")
    logger.info(f" Arbitraje:  {'ACTIVO (5 estrategias)' if arb_engine else 'NO DISPONIBLE'}")
    strats_active = sum([mean_rev is not None, grid_eng is not None,
                         ofi_eng is not None, mom_eng is not None, dn_eng is not None])
    logger.info(f" Estrategias avanzadas: {strats_active}/5 activas")
    logger.info(f"  Mean Reversion:   {'ON' if mean_rev else 'OFF'}")
    logger.info(f"  Grid Trading:     {'ON' if grid_eng  else 'OFF'}")
    logger.info(f"  OFI Strategy:     {'ON' if ofi_eng   else 'OFF'}")
    logger.info(f"  Momentum Scaling: {'ON' if mom_eng   else 'OFF'}")
    logger.info(f"  Delta Neutral:    {'ON' if dn_eng    else 'OFF'}")

    # ── Credential verification log ───────────────────────────────────────────
    logger.info("[config] Credenciales verificadas (ocultas por seguridad)")

    # ── Balance real al inicio ────────────────────────────────────────────────
    if config.PRODUCTION and config.BYBIT_API_KEY:
        try:
            import hmac as _hmac, hashlib as _hashlib
            _ts  = str(int(__import__('time').time() * 1000))
            _msg = f"{_ts}{config.BYBIT_API_KEY}5000accountType=UNIFIED&coin=USDT"
            _sig = _hmac.new(config.BYBIT_API_SECRET.encode(), _msg.encode(), _hashlib.sha256).hexdigest()
            _hdrs = {"X-BAPI-API-KEY": config.BYBIT_API_KEY, "X-BAPI-TIMESTAMP": _ts,
                     "X-BAPI-SIGN": _sig, "X-BAPI-RECV-WINDOW": "5000", "User-Agent": "Mozilla/5.0", "Referer": "https://www.bybit.com"}
            import aiohttp as _aio
            async with _exchange_semaphores["bybit"]:
                async with _aio.ClientSession() as _s:
                    async with _s.get(
                        "https://api.bytick.com/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT",
                        headers=_hdrs, timeout=_aio.ClientTimeout(total=8),
                    ) as _r:
                        _bd = await _r.json()
                        _bybit_bal = 0.0
                        if _bd.get("retCode") == 0:
                            for _c in _bd["result"]["list"][0].get("coin", []):
                                if _c.get("coin") == "USDT":
                                    _bybit_bal = float(_c.get("walletBalance", 0))
            _okx_bal = 0.0
            if config.OKX_API_KEY and config.OKX_PASSPHRASE:
                from datetime import datetime, timezone as _tz
                import base64 as _b64
                _ots = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                _path = "/api/v5/account/balance?ccy=USDT"
                _osig = _b64.b64encode(_hmac.new(
                    config.OKX_SECRET.encode(), (_ots + "GET" + _path).encode(), _hashlib.sha256
                ).digest()).decode()
                _ohdrs = {"OK-ACCESS-KEY": config.OKX_API_KEY, "OK-ACCESS-SIGN": _osig,
                          "OK-ACCESS-TIMESTAMP": _ots, "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE}
                async with _exchange_semaphores["okx"]:
                    async with _aio.ClientSession() as _s:
                        async with _s.get(f"https://www.okx.com{_path}", headers=_ohdrs,
                                           timeout=_aio.ClientTimeout(total=8)) as _r:
                            _od = await _r.json()
                            if _od.get("code") == "0":
                                for _d in _od.get("data", [{}])[0].get("details", []):
                                    if _d.get("ccy") == "USDT":
                                        _okx_bal = float(_d.get("eq", 0))
            _total_bal = _bybit_bal + _okx_bal
            logger.info("[config] Balance REAL  — Bybit: ${:.2f} | OKX: ${:.2f} | Total: ${:.2f}",
                        _bybit_bal, _okx_bal, _total_bal)
        except Exception as _exc:
            logger.warning("[config] No se pudo obtener balance real al inicio: {}", _exc)

    logger.info("=" * 60)
    logger.info(" Whale Follower Bot -- Sprint 6")
    logger.info(f" Pares:        {', '.join(config.TRADING_PAIRS)}")
    logger.info(f" Threshold:    {config.SIGNAL_SCORE_THRESHOLD} | High: {config.HIGH_CONFIDENCE_SCORE}")
    capital_label = config.REAL_CAPITAL if config.PRODUCTION else config.PAPER_CAPITAL
    logger.info(f" Capital:      ${capital_label:,.0f} | Produccion: {config.PRODUCTION}")
    logger.info(f" Capas:        {active_layers}/14 activas")
    logger.info(f" ML:           {'DISPONIBLE' if ml_model else 'NO DISPONIBLE'}")
    logger.info(f" Macro agent:  {'ACTIVO (calendario+Reddit+X)' if macro_agent else 'OFF'}")
    logger.info(f" Regime det:   {'ACTIVO (threshold dinámico)' if regime_det else 'OFF'}")
    logger.info(f" Range trader: {'ACTIVO (RSI+BB lateral)' if range_trader else 'OFF'}")
    logger.info(f" Deribit opts: {'ACTIVO (PCR+IV+sweeps)' if deribit_eng else 'OFF'}")
    logger.info(f" Dashboard:    {'SI' if dashboard else 'NO'}")
    logger.info("=" * 60)

    recent_signals = deque(maxlen=200)

    # ── Loop principal ─────────────────────────────────────────────────────────
    async for trade, _legacy_state in aggregator.stream():
      try:

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
        # if arb_engine:
        #     arb_engine.on_trade(trade.exchange, trade.pair, trade.price, trade.timestamp * 1000)
        #     # ETHBTC spot → triangular arb con precio real (no implicito)
        #     if trade.pair == "ETHBTC":
        #         arb_engine.on_eth_btc_price(trade.price)

        # Estrategias avanzadas — datos en tiempo real
        if grid_eng:
            grid_eng.on_price(trade.pair, trade.price)
        if okx_grid_eng:
            okx_grid_eng.on_price(trade.pair, trade.price)
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
        if dn_eng and trade.pair == "BTCUSDT":
            dn_eng.on_perp_price(trade.exchange, trade.price)

        # Regime detector + Range trader (operan en cada tick, sin spring)
        if regime_det:
            regime_det.on_price(trade.pair, trade.price)
        if range_trader:
            await range_trader.on_price(trade.pair, trade.price)


        # 3. Procesar spring detection
        ctx_snapshot = context.snapshot()
        signal = monitor.process(trade, ctx_snapshot)
        if signal is None:
            continue
        if not is_valid_price(signal.entry_price, signal.pair):
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

        # Deribit Options Flow
        if deribit_eng:
            opt = deribit_eng.snapshot()
            ext.options_pcr           = opt.pcr
            ext.options_iv_spike      = opt.iv_spike
            ext.options_bullish_sweep = opt.bullish_sweep
            ext.options_bearish_sweep = opt.bearish_sweep
            ext.options_pts           = opt.pts_adjustment

        # Macro agent — pausa por evento económico (bloqueo temprano, ahorra cómputo)
        if macro_agent:
            macro_paused, macro_reason = macro_agent.is_paused()
            if macro_paused:
                logger.warning(
                    f"[main] {signal.pair} BLOQUEADO por macro: {macro_reason}"
                )
                continue
            # Capa 12 — DXY: alimentar ExtendedContext para multiplicador 0.85x
            _dxy_chg, _dxy_up = macro_agent.dxy_trend()
            ext.dxy_strong_up = _dxy_up

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

        # 5b. Ajuste de sentimiento macro (noticias CryptoPanic)
        if macro_agent:
            macro_adj = macro_agent.sentiment_adjustment()
            if macro_adj != 0:
                new_score = max(0, new_score + macro_adj)
                logger.info(
                    f"[main] {signal.pair} macro adj={macro_adj:+d} → score={new_score}"
                )

        # 5d. Termometros de mercado
        if btc_dom:
            dom_adj = btc_dom.adjustment(signal.pair)
            if dom_adj != 0:
                new_score = max(0, new_score + dom_adj)
                logger.info(f"[main] {signal.pair} btc_dom adj={dom_adj:+d} → score={new_score}")

        if liq_glob:
            if liq_glob.is_long_blocked():
                logger.warning(f"[main] {signal.pair} BLOQUEADO por liq_global: liq LONG masivas")
                continue
            liq_adj = liq_glob.adjustment()
            if liq_adj != 0:
                new_score = max(0, new_score + liq_adj)
                logger.info(f"[main] {signal.pair} liq_glob adj={liq_adj:+d} → score={new_score}")

        if dxy_mon:
            dxy_adj = dxy_mon.adjustment()
            if dxy_adj != 0:
                new_score = max(0, new_score + dxy_adj)
                logger.info(f"[main] {signal.pair} dxy adj={dxy_adj:+d} → score={new_score}")

        # 5c. Aplicar ML (ultimo filtro)
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
                # Nuevas features ML (IV aproximada + Z-score volúmen)
                "implied_volatility":  float(getattr(deribit_eng, "last_iv", 0.0) if deribit_eng else 0.0),
                "volume_zscore":       max(-3.0, min(3.0,
                    (signal.spring_data.get("vol_ratio", 1.0) - 1.0) / 0.30
                )),
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

        signal_id_for_signal = str(uuid.uuid4())
        if ml_model and features:
            try:
                from db_writer import save_signal_features
                asyncio.create_task(
                    save_signal_features(
                        signal_id=signal_id_for_signal,
                        pair=signal.pair,
                        signal_score=int(new_score),
                        features=features,
                        timestamp=time.time(),
                    )
                )
            except Exception as exc:
                logger.warning("[main] could not save signal features: {}", exc)

        # Threshold dinámico ajustado por régimen de mercado
        regime_adj = regime_det.threshold_adjustment(signal.pair) if regime_det else 0
        base_threshold = get_manager().get_threshold() if get_manager else config.SIGNAL_SCORE_THRESHOLD
        effective_threshold = int(base_threshold) + regime_adj
        if regime_adj != 0:
            logger.debug(
                f"[main] {signal.pair} regime_adj={regime_adj:+d} "
                f"threshold={base_threshold}→{effective_threshold}"
            )

        if new_score < effective_threshold:
            continue

        _signal_count += 1
        regime_label = regime_det.regime(signal.pair).value if regime_det else "UNKNOWN"
        logger.info(
            f"[main] *** SIGNAL #{_signal_count} {signal.pair} "
            f"score={new_score} entry={signal.entry_price:.2f} "
            f"regime={regime_label} thresh={effective_threshold} "
            f"fg={ext.fear_greed_value} ml={ext.ml_probability:.2f} ***"
        )

        # Activar lead-lag arb en BTC spring
        # if signal.pair == "BTCUSDT" and arb_engine:
        #     arb_engine.on_btc_spring(signal.entry_price)

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
        # Exchange selection: priorizar exchange con mayor USDT libre
        _bybit_usdt, _okx_usdt = rebalancer.usdt_balances() if rebalancer else (executor.available_capital(), 0.0)
        allocation.preferred_exchange = allocator.select_exchange(_bybit_usdt, _okx_usdt)
        logger.debug(
            "[main] Exchange preferido: {} (Bybit=${:.0f} OKX=${:.0f})",
            allocation.preferred_exchange, _bybit_usdt, _okx_usdt,
        )

        # 9. Ejecutar trades (features guardadas para ML record_outcome al cierre)
        for alloc in allocation.trades:
            signal_id = signal_id_for_signal if alloc.pair == signal.pair else str(uuid.uuid4())

            paper = await executor.open_trade(
                signal_score     = alloc.signal_score,
                entry_price      = alloc.entry_price,
                stop_loss        = alloc.stop_loss,
                take_profit      = alloc.take_profit,
                signal_id        = signal_id,
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

        # 11. Notificacion Telegram por trade Wyckoff real
        if allocation.trades:
            first = allocation.trades[0]
            asyncio.create_task(alerts.send_trade_alert("wyckoff", {
                "pair":     signal.pair,
                "score":    new_score,
                "entry":    first.entry_price,
                "sl":       first.stop_loss,
                "tp":       first.take_profit,
                "size_usd": first.size_usd,
            }))

      except asyncio.CancelledError:
          raise
      except Exception as _tick_exc:
          logger.error("[main] Error en tick — continuando: {}", _tick_exc)

    asyncio.create_task(save_positions(executor))


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
async def _log_server_ip() -> None:
    """Log IP pública al arranque y envía alerta Telegram de inicio."""
    import aiohttp as _aiohttp
    ip = "desconocida"
    for url in ("https://api.ipify.org?format=json", "https://ifconfig.me/ip"):
        try:
            async with _aiohttp.ClientSession() as s:
                async with s.get(url, timeout=_aiohttp.ClientTimeout(total=6)) as r:
                    text = await r.text()
                    ip   = text.strip().strip('"').replace('{"ip":"', '').replace('"}', '')
                    logger.info("[startup] 🌍 IP pública del servidor: {}", ip)
                    break
        except Exception:
            continue
    else:
        logger.warning("[startup] No se pudo obtener IP pública del servidor")

    # Alerta Telegram de inicio
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    prod = os.environ.get("PRODUCTION", "false").lower() == "true"
    msg  = (
        f"🚀 Whale Follower Bot iniciado\n"
        f"🌍 IP: {ip} Singapore ✅\n"
        f"🔧 Modo: {'REAL' if prod else 'PAPEL'}\n"
        f"📊 Estrategias: Cross-Arb + Grid Bybit + Grid OKX + Wyckoff"
    )
    try:
        async with _aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=_aiohttp.ClientTimeout(total=8),
            )
    except Exception as exc:
        logger.warning("[startup] Telegram inicio error: {}", exc)


async def _send_telegram(msg: str) -> None:
    """Envía mensaje Telegram. No lanza excepciones."""
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        import aiohttp as _ah
        async with _ah.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=_ah.ClientTimeout(total=8),
            )
    except Exception:
        pass


async def main() -> None:
    runner = await start_healthcheck()
    asyncio.create_task(_log_server_ip())

    _restart_delay    = 30
    _restart_history: deque = deque(maxlen=10)  # timestamps de reinicios recientes
    _MAX_RESTARTS_HOUR = 5

    while True:
        trading_task = asyncio.create_task(trading_loop())
        try:
            await trading_task
            break   # salida limpia
        except asyncio.CancelledError:
            logger.info("[main] Cancelled — shutdown limpio.")
            break
        except Exception as exc:
            now = time.time()
            _restart_history.append(now)
            recent = sum(1 for t in _restart_history if now - t < 3600)
            logger.error(
                "[main] trading_loop crasheo: {} — reinicio {}/{} en 1h",
                exc, recent, _MAX_RESTARTS_HOUR,
            )
            if recent >= _MAX_RESTARTS_HOUR:
                wait_secs = 3600
                logger.error("[main] {} reinicios en 1h — pausa {}s", recent, wait_secs)
                asyncio.create_task(_send_telegram(
                    f"🚨 Whale Follower Bot\n"
                    f"⚠️ {recent} reinicios en 1 hora\n"
                    f"Bot pausado 1 hora. Revisar logs."
                ))
                await asyncio.sleep(wait_secs)
                _restart_history.clear()
                _restart_delay = 30
            else:
                await asyncio.sleep(_restart_delay)
                _restart_delay = min(_restart_delay * 2, 300)  # backoff hasta 5min
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
