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

# ── KILL SWITCH DE EMERGENCIA ─────────────────────────────────────────────────
# Pon STOP_BOT=1 en .env del VPS para detener el bot sin matar el proceso
if os.getenv("STOP_BOT", "0") == "1":
    print("[STOP_BOT] Bot detenido por variable de entorno STOP_BOT=1", flush=True)
    sys.exit(0)

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


# ── Security: request rate limiter & intrusion detection ─────────────────────
_req_counts: dict = {}     # ip → [timestamps]
_blocked_ips: set = set()
_RATE_LIMIT = 30           # max requests per window
_RATE_WINDOW = 60          # seconds
_ALERT_COOLDOWN = 300      # don't alert same IP more than once per 5 min
_last_alert_ts: dict = {}

_SUSPICIOUS_PATHS = (
    "/cgi-bin", "/.env", "/wp-", "/admin", "/shell", "/setup",
    "/config", "/passwd", "/phpmyadmin", "/actuator", "/.git",
)


def _get_client_ip(request: web.Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    peername = request.transport.get_extra_info("peername")
    return peername[0] if peername else "unknown"


async def _alert_intrusion(ip: str, path: str, reason: str) -> None:
    now = time.time()
    if now - _last_alert_ts.get(ip, 0) < _ALERT_COOLDOWN:
        return
    _last_alert_ts[ip] = now
    msg = (
        f"🚨 INTRUSION DETECTED\n"
        f"IP: {ip}\n"
        f"Path: {path}\n"
        f"Reason: {reason}\n"
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
    )
    logger.warning("[security] {}", msg.replace("\n", " | "))
    try:
        import alerts
        asyncio.create_task(alerts._send_telegram(msg, priority="critical"))
    except Exception:
        pass
    try:
        import db_writer
        asyncio.create_task(db_writer._run(
            lambda: db_writer._client().table("security_incidents").insert({
                "ip": ip, "path": path, "reason": reason,
                "created_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            }).execute()
        ))
    except Exception:
        pass


# ── Healthcheck HTTP ─────────────────────────────────────────────────────────
async def health_handler(request: web.Request) -> web.Response:
    ip = _get_client_ip(request)
    path = request.path

    # Block known bad IPs
    if ip in _blocked_ips:
        return web.Response(status=403, text="")

    # Detect suspicious paths (scanners, bots)
    if any(s in path.lower() for s in _SUSPICIOUS_PATHS):
        _blocked_ips.add(ip)
        asyncio.create_task(_alert_intrusion(ip, path, "suspicious_path_scan"))
        return web.Response(status=403, text="")

    # Rate limiting per IP
    now = time.time()
    hits = _req_counts.setdefault(ip, [])
    hits[:] = [t for t in hits if now - t < _RATE_WINDOW]
    hits.append(now)
    if len(hits) > _RATE_LIMIT:
        _blocked_ips.add(ip)
        asyncio.create_task(_alert_intrusion(ip, path, f"rate_limit_{len(hits)}_in_{_RATE_WINDOW}s"))
        return web.Response(status=429, text="")

    # Health key auth
    _health_key = os.getenv("HEALTH_KEY")
    if _health_key and request.headers.get("X-Health-Key") != _health_key:
        return web.Response(status=401, text="")

    uptime = int(time.time() - _start_time)
    payload = {
        "status":          "ok",
        "uptime_secs":     uptime,
        "signals_emitted": _signal_count,
        "ready":           _ready,
    }
    if _executor_ref is not None:
        payload["leverage"] = _executor_ref.leverage_status()
    return web.json_response(payload)


async def _catch_all_handler(request: web.Request) -> web.Response:
    """Block any path that isn't /health or /."""
    ip = _get_client_ip(request)
    if ip not in _blocked_ips:
        _blocked_ips.add(ip)
        asyncio.create_task(_alert_intrusion(ip, request.path, "unknown_path_probe"))
    return web.Response(status=404, text="")


async def start_healthcheck() -> web.AppRunner:
    port = int(os.environ.get("PORT", 8080))
    app  = web.Application()
    app.router.add_get("/health", health_handler)
    app.router.add_get("/",       health_handler)
    app.router.add_route("*", "/{path:.*}", _catch_all_handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    try:
        await web.TCPSite(runner, "0.0.0.0", port).start()
        logger.info(f"[health] HTTP server on 0.0.0.0:{port} (rate_limit={_RATE_LIMIT}/{_RATE_WINDOW}s)")
    except OSError as e:
        logger.warning(f"[health] Puerto {port} ocupado, healthcheck deshabilitado ({e})")
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

    # ── Arbitraje DESHABILITADO — ENABLE_CROSS_ARB=false / ENABLE_TRI_ARB=false ──
    prod           = config.PRODUCTION       # requerido por estrategias OKX
    arb_engine     = None
    _arb_ref       = None

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

    # OKX Futures Grid — perpetuos USDT-M, fees 0.02% maker
    okx_fut_mod = _try_import("okx_futures_grid")
    okx_fut_eng = (
        okx_fut_mod.OKXFuturesGrid(production=prod)
        if okx_fut_mod
        else None
    )
    if okx_fut_eng:
        logger.info("[main] OKX Futures Grid iniciado ✅ (fees 0.02% maker)")
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
    mempool_mod    = _try_import("mempool_thermometer")
    daily_rep_mod  = _try_import("daily_report")
    rebalancer_mod  = _try_import("rebalancer")
    strat_mgr_mod   = _try_import("strategy_manager")
    meta_agent_mod  = _try_import("meta_agent")

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
    mempool_th = mempool_mod.MempoolThermometer()         if mempool_mod and config.MEMPOOL_ENABLED else None
    daily_rep     = daily_rep_mod.DailyReporter()              if daily_rep_mod   else None
    rebalancer    = rebalancer_mod.CapitalRebalancer()          if rebalancer_mod  else None
    session_vol   = session_vol_mod.SessionVolumeTracker()      if session_vol_mod else None
    ml_model      = ml_mod.MLModel()                   if ml_mod          else None

    # ── Motores principales ────────────────────────────────────────────────────
    aggregator = Aggregator()
    context    = ContextEngine()
    monitor    = MultiPairMonitor(regime_detector=regime_det)
    selector   = PairSelector()
    allocator  = CapitalAllocator()
    risk_mgr   = RiskManager(config.REAL_CAPITAL if config.PRODUCTION else config.PAPER_CAPITAL)
    executor   = BybitTestnetExecutor()
    _executor_ref = executor

    # OKX executor como fallback para Wyckoff cuando Bybit rechaza
    _okx_wyckoff = None
    if config.OKX_API_KEY and config.OKX_SECRET and config.OKX_PASSPHRASE:
        try:
            from okx_executor import OKXExecutor
            _okx_wyckoff = OKXExecutor()
            logger.info("[main] OKX executor disponible (enabled={}) como exchange principal",
                        _okx_wyckoff.enabled)
        except Exception as exc:
            logger.warning("[main] OKX executor no disponible: {}", exc)

    # MEXC executor como segundo fallback (deshabilitado — CloudFront 403 desde Contabo)
    _mexc_wyckoff = None
    if config.MEXC_ENABLED and config.MEXC_API_KEY and config.MEXC_API_SECRET:
        try:
            from mexc_executor import MEXCExecutor
            _mexc_wyckoff = MEXCExecutor()
            logger.info("[main] MEXC executor disponible como segundo fallback")
        except Exception as exc:
            logger.warning("[main] MEXC executor no disponible: {}", exc)
    elif not config.MEXC_ENABLED:
        logger.info("[main] MEXC deshabilitado (MEXC_ENABLED=false)")

    range_trader = (range_mod.RangeTrader(executor, regime_det, production=prod)
                    if range_mod and regime_det else None)

    strat_mgr = (
        strat_mgr_mod.StrategyManager(
            regime_detector      = regime_det,
            liq_monitor          = liq_glob,
            delta_neutral_engine = dn_eng,
            btc_dominance_monitor= btc_dom,
        ) if strat_mgr_mod else None
    )

    meta_agt = (
        meta_agent_mod.MetaAgent(
            fear_greed  = fear_greed,
            btc_dom     = btc_dom,
            dxy_mon     = dxy_mon,
            liq_monitor = liq_glob,
        ) if meta_agent_mod else None
    )

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
        alerts.set_rebalancer(rebalancer)
        asyncio.create_task(rebalancer.run(),  name="rebalancer")
    if strat_mgr:
        asyncio.create_task(strat_mgr.run(),   name="strategy_manager")
        logger.info("[main] Strategy Manager iniciado ✅ (eval cada 60s)")
    if meta_agt:
        asyncio.create_task(meta_agt.run(),    name="meta_agent")
        logger.info("[main] Meta-Agente iniciado ✅ (eval cada 5min | ATR+EMA20/50+F&G+DXY)")

    # ── Adaptive Position Sizer + Auto-Reporte Telegram cada 4h ──────────────
    try:
        from position_sizer import get_sizer
        import alerts as _alerts_main
        total_cap = (
            (_alerts_main._stats.get("capital_bybit") or 0)
            + (_alerts_main._stats.get("capital_okx") or 0)
        )
        if total_cap <= 0:
            total_cap = config.REAL_CAPITAL
        get_sizer().update_capital(total_cap)
        logger.info("[main] AdaptivePositionSizer iniciado ✅ capital=${:.0f}", total_cap)
        asyncio.create_task(_alerts_main.auto_report_loop(interval_hours=4.0), name="auto_report")
        logger.info("[main] Auto-reporte Telegram cada 4h iniciado ✅")
    except Exception as _ps_err:
        logger.warning("[main] position_sizer no disponible: {}", _ps_err)

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

    # ── Paper Trader paralelo (threshold bajo → ML aprende rápido) ──────────
    _paper_trader = None
    _latest_prices: Dict[str, float] = {}   # actualizado en cada tick
    try:
        from paper_trader import PaperTrader
        _paper_trader = PaperTrader(threshold=35, capital=10_000.0)
        asyncio.create_task(
            _paper_trader.run_check_loop(lambda: _latest_prices)
        )
        asyncio.create_task(
            _paper_trader.run_daily_report_loop()
        )
        logger.info("[main] PaperTrader paralelo activo (threshold=35)")
    except Exception as exc:
        logger.warning("[main] PaperTrader no disponible: {}", exc)

    # ── Threshold Optimizer (auto-ajuste cada 6h) ────────────────────────────
    try:
        from threshold_optimizer import get_optimizer
        thr_optimizer = get_optimizer()
        # No bloquear arranque — cargar thresholds en background
        async def _load_thresholds():
            try:
                await thr_optimizer.load_from_supabase()
                logger.info("[main] ThresholdOptimizer: thresholds cargados de Supabase")
            except Exception as exc:
                logger.warning("[main] ThresholdOptimizer: Supabase no disponible, usando defaults: {}", exc)
        asyncio.create_task(_load_thresholds())
        asyncio.create_task(thr_optimizer.run_loop())
        logger.info("[main] ThresholdOptimizer activo (carga Supabase en background)")
    except Exception as exc:
        thr_optimizer = None
        logger.warning("[main] ThresholdOptimizer no disponible: {}", exc)

    # ── Auto-Healer (monitoreo 24/7 + auto-corrección) ──────────────────────
    _healer = None
    try:
        from auto_healer import get_healer
        _healer = get_healer()
        _healer._refs["okx_wyckoff"] = _okx_wyckoff
        asyncio.create_task(_healer.run_loop())
        logger.info("[main] AutoHealer activo (check cada 5min)")
    except Exception as exc:
        logger.warning("[main] AutoHealer no disponible: {}", exc)

    # ── OKX Position Monitor (TP/SL cada 30s) ────────────────────────────────
    _pos_monitor = None
    if _okx_wyckoff and _okx_wyckoff.enabled:
        try:
            from position_monitor import PositionMonitor
            _pos_monitor = PositionMonitor(
                okx_executor=_okx_wyckoff,
                price_getter=lambda: _latest_prices,
                telegram_fn=alerts.send_system_message,
            )
            asyncio.create_task(_pos_monitor.run(), name="position_monitor")
            logger.info("[main] OKX PositionMonitor activo (TP/SL check cada 30s)")
        except Exception as exc:
            logger.warning("[main] PositionMonitor no disponible: {}", exc)

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
    thermo_active = sum([btc_dom is not None, liq_glob is not None, dxy_mon is not None, mempool_th is not None])
    logger.info(f" Termometros: {thermo_active}/4 activos (dom/liq/dxy/mempool)")
    logger.info(" Arbitraje:  DESHABILITADO (ENABLE_CROSS_ARB=false — solo OKX Grid activo)")
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

    # ── Balance real OKX al inicio ────────────────────────────────────────────
    if config.PRODUCTION and config.OKX_API_KEY and config.OKX_PASSPHRASE:
        try:
            import hmac as _hmac, hashlib as _hashlib
            from datetime import datetime, timezone as _tz
            import base64 as _b64
            import aiohttp as _aio
            _ots  = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
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
                        _okx_bal = 0.0
                        if _od.get("code") == "0":
                            for _d in _od.get("data", [{}])[0].get("details", []):
                                if _d.get("ccy") == "USDT":
                                    _okx_bal = float(_d.get("eq", 0))
            logger.info("[config] Balance REAL OKX: ${:.2f} USDT", _okx_bal)
        except Exception as _exc:
            logger.warning("[config] No se pudo obtener balance OKX al inicio: {}", _exc)

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
    _last_capital_warn_ts: float = 0.0
    _CAPITAL_WARN_INTERVAL = 600  # 10 minutos
    _BYBIT_MIN_FOR_STRATEGIES = 50.0

    # ── Ajuste mempool por estrategia ─────────────────────────────────────────
    def apply_mempool_adjustments(score: float, strategy_name: str):
        if not mempool_th:
            return score
        try:
            analysis = mempool_th.get_full_analysis()
            ms = analysis["mempool_score"]
            # Actualizar termometros en alerts
            alerts.update_thermometers(
                mempool_score=ms,
                mempool_signal=analysis["signal"],
            )

            if strategy_name in ("grid_btc", "grid_eth", "grid_sol", "grid"):
                if ms >= 80:
                    logger.warning(f"[mempool] GRID BLOQUEADO score={ms}")
                    return None
                elif ms >= 60: score -= 5
                elif ms <= 25: score += 5

            elif strategy_name in ("cross_exchange_arb", "cross_arb"):
                if ms >= 80:   score += 10
                elif ms >= 60: score += 7
                elif ms <= 25: score -= 3

            elif strategy_name in ("whale_spring", "wyckoff", "spring"):
                if ms >= 80:   score += 5
                elif ms >= 60: score += 8

            elif strategy_name in ("mean_reversion", "mean_rev"):
                if ms >= 80:
                    logger.warning(f"[mempool] MEAN_REV BLOQUEADO score={ms}")
                    return None
                elif ms >= 60: score -= 8
                elif ms <= 25: score += 5

            elif strategy_name in ("momentum_scaling", "momentum"):
                if ms >= 60:   score += 8
                elif ms <= 25: score -= 5

            elif strategy_name in ("lead_lag", "lead_lag_arb"):
                if ms >= 60:   score += 6
                elif ms <= 25: score -= 5

            elif strategy_name in ("funding_arb", "funding_rate_arb"):
                if ms >= 80:   score += 8
                elif ms >= 60: score += 5

            logger.debug(f"[mempool] {strategy_name} ms={ms} signal={analysis['signal']}")
            return score
        except Exception as e:
            logger.warning(f"[mempool] Error: {e}")
            return score

    # ── Loop principal ─────────────────────────────────────────────────────────
    async for trade, _legacy_state in aggregator.stream():
      try:

        # 0. Actualizar precios para paper_trader
        _latest_prices[trade.pair] = trade.price

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
        if okx_fut_eng:
            okx_fut_eng.on_price(trade.pair, trade.price)
        if ofi_eng and (not strat_mgr or strat_mgr_mod.is_active("ofi")):
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
        if range_trader and (not strat_mgr or strat_mgr_mod.is_active("range_trader")):
            await range_trader.on_price(trade.pair, trade.price)
        if meta_agt and trade.pair == "BTCUSDT":
            meta_agt.on_price(trade.pair, trade.price, trade.quantity)


        # 2b. Capital gate warning (cada 10 min si OKX bajo)
        _now_cg = time.time()
        if rebalancer and (_now_cg - _last_capital_warn_ts) >= _CAPITAL_WARN_INTERVAL:
            _snap_cg = rebalancer.snapshot()
            _okx_bal_cg = getattr(_snap_cg, "okx_usdt", 0.0)
            if 0 < _okx_bal_cg < _BYBIT_MIN_FOR_STRATEGIES:
                logger.warning(
                    "[capital] OKX=${:.2f} — capital bajo para Wyckoff/MeanRev.",
                    _okx_bal_cg,
                )
                _last_capital_warn_ts = _now_cg

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

        # ── Diagnóstico ExtendedContext ──────────────────────────────────────
        logger.info(
            "[main] ExtCtx: cvd_all={} cvd_2={} vel={:.2f} | "
            "ob={:.2f} ob_fav={} | corr={} | fg={} fg_block={} | "
            "liq={} liq_pts={} | onchain={} | vol_unusual={} | "
            "news_block={} | iv_spike={} | modules: cvd={} ob={} corr={} fg={} liq={} oc={}",
            ext.cvd_all_positive, ext.cvd_two_positive, ext.cvd_weighted_vel,
            ext.ob_imbalance, ext.ob_favorable,
            ext.btc_eth_signal,
            ext.fear_greed_value, ext.fear_greed_block_long,
            ext.liq_zone_active, ext.liq_extra_pts,
            ext.onchain_signal,
            ext.vol_unusual,
            ext.news_blocked,
            ext.options_iv_spike,
            bool(cvd_combined), bool(orderbook), bool(correlation),
            bool(fear_greed), bool(liq_map), bool(onchain),
        )

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

        # 5e. Termometro 4 — Mempool BTC
        if mempool_th:
            new_score = apply_mempool_adjustments(new_score, "wyckoff")
            if new_score is None:
                logger.info(f"[mempool] {signal.pair} operacion bloqueada por congestion mempool")
                continue

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

        # Threshold dinámico: optimizer (auto-ajustado) > SPRING_PARAMS (default)
        regime_label = regime_det.regime(signal.pair).value if regime_det else "LATERAL"
        if regime_label == "UNKNOWN":
            regime_label = "LATERAL"
        if thr_optimizer:
            effective_threshold = thr_optimizer.get_threshold(regime_label)
            thr_optimizer.record_signal_detected()
        else:
            regime_params = config.SPRING_PARAMS.get(regime_label, config.SPRING_PARAMS["LATERAL"])
            effective_threshold = regime_params["score_min"]
        logger.info(
            "[main] {} threshold={} (régimen={} src={})",
            signal.pair, effective_threshold, regime_label,
            "optimizer" if thr_optimizer else "SPRING_PARAMS",
        )

        # Paper trader: evaluar TODAS las señales (threshold=35, independiente del real)
        if _paper_trader and new_score > 0:
            regime_p = config.SPRING_PARAMS.get(regime_label, config.SPRING_PARAMS["LATERAL"])
            _paper_sl = signal.entry_price * (1 - regime_p.get("sl_pct", 0.005))
            _paper_tp = signal.entry_price * (1 + regime_p.get("tp_pct", 0.010))
            _paper_trader.evaluate_signal(
                pair=signal.pair,
                score=new_score,
                entry=signal.entry_price,
                sl=_paper_sl,
                tp=_paper_tp,
                regime=regime_label,
                features=features if ml_model else None,
            )

        # Healer hooks
        if _healer:
            _healer.on_spring_eval()
            _healer.on_ctx_score(new_bd.context_pts)

        if new_score < effective_threshold:
            logger.info(
                "[pipeline] {} RECHAZADO final: score={}<{} | "
                "primary={} vol={} ctx={} struct={} opts={} | "
                "spring_confirmed={} block={}",
                signal.pair, new_score, effective_threshold,
                new_bd.primary_pts, new_bd.volume_pts,
                new_bd.context_pts, new_bd.structure_pts,
                getattr(ext, "options_pts", 0),
                new_bd.spring_confirmed,
                new_bd.block_reason or "none",
            )
            continue

        _signal_count += 1
        if thr_optimizer:
            thr_optimizer.record_signal_executed()
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
        if mean_rev and (not strat_mgr or strat_mgr_mod.is_active("mean_reversion")):
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
        if mom_eng and hasattr(signal, "cvd_metrics") and (not strat_mgr or strat_mgr_mod.is_active("momentum")):
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

        # 9. Ejecutar trades — gate Wyckoff por régimen + size_multiplier meta_agent
        # meta_agent pausa wyckoff en LATERAL/BAJISTA/FEAR; reduce size en BAJISTA (0.5x)
        if strat_mgr_mod and not strat_mgr_mod.is_active("wyckoff_spring"):
            logger.info(
                "[main] {} Wyckoff spring PAUSADO por meta_agent (régimen={}) — "
                "trade y alerta Telegram cancelados",
                signal.pair,
                meta_agt.current_regime().value if meta_agt else "?",
            )
            continue

        _size_mult = meta_agent_mod.size_multiplier if meta_agent_mod else 1.0

        if not allocation.trades:
            logger.warning(
                "[main] {} SIGNAL score={} pasó threshold pero allocation vacío — "
                "capital_disponible=${:.2f} open_trades={}",
                signal.pair, new_score,
                executor.available_capital(), executor.open_count(),
            )

        for alloc in allocation.trades:
            signal_id = signal_id_for_signal if alloc.pair == signal.pair else str(uuid.uuid4())
            final_size = alloc.size_usd * _size_mult
            final_size = max(config.MIN_TRADE_SIZE_USD, final_size)
            final_size = min(config.MAX_TRADE_SIZE_USD, final_size)

            # OKX principal + MEXC fallback (Bybit deshabilitado)
            paper = None
            _traded = False

            # ── 1. Intentar OKX ──────────────────────────────────────────
            okx_result = None
            if _okx_wyckoff and _okx_wyckoff.enabled and final_size >= 1.0:
                try:
                    _okx_bal = await asyncio.wait_for(
                        _okx_wyckoff.get_balance(), timeout=5.0)
                except Exception:
                    _okx_bal = 0.0

                exec_pair  = alloc.pair
                exec_price = alloc.entry_price

                if _okx_bal >= 20:
                    logger.info(
                        "[main] OKX ejecutando {} ${:.2f} (bal=${:.0f})",
                        exec_pair, final_size, _okx_bal)
                    try:
                        okx_result = await asyncio.wait_for(
                            _okx_wyckoff.market_order(
                                pair=exec_pair, side="buy",
                                size_usd=final_size, price_hint=exec_price),
                            timeout=10.0)
                    except Exception as exc:
                        logger.warning("[main] OKX {} error: {!r} type={}", exec_pair, exc, type(exc).__name__)

                    # Si falló → buscar par alternativo más barato
                    if not okx_result:
                        effective = _okx_wyckoff.effective_size(final_size)
                        logger.info(
                            "[main] OKX {} falló — buscando alternativo "
                            "(size_orig=${:.0f} size_real=${:.0f} precios={})",
                            exec_pair, final_size, effective,
                            [p for p in _latest_prices if _latest_prices[p] > 0][:6],
                        )
                        alt = _okx_wyckoff.find_affordable_pair(
                            effective, _latest_prices, original_pair=exec_pair)
                        if alt and alt in _latest_prices:
                            exec_pair  = alt
                            exec_price = _latest_prices[alt]
                            logger.info(
                                "[main] Par alternativo OKX: {} (contrato cabe en ${:.0f})",
                                alt, final_size)
                            try:
                                okx_result = await asyncio.wait_for(
                                    _okx_wyckoff.market_order(
                                        pair=alt, side="buy",
                                        size_usd=final_size, price_hint=exec_price),
                                    timeout=10.0)
                            except Exception as exc:
                                logger.error("[main] OKX alt {} error: {!r} type={}", alt, exc, type(exc).__name__)
                else:
                    logger.warning(
                        "[main] OKX sin USDT suficiente (${:.0f}) — intentando MEXC",
                        _okx_bal)

                if okx_result:
                    _traded = True
                    logger.info(
                        "[main] Trade OKX {} ${:.2f} OK orderId={}",
                        exec_pair, final_size,
                        okx_result.get("orderId", "?"))
                    _sl = alloc.stop_loss
                    _tp = alloc.take_profit
                    if exec_pair != alloc.pair:
                        _sl_pct = config.STOP_LOSS_PCT
                        _rr     = config.RISK_REWARD
                        _sl = exec_price * (1.0 - _sl_pct)
                        _tp = exec_price * (1.0 + _sl_pct * _rr)
                    # Register for TP/SL monitoring
                    try:
                        from position_monitor import register_position
                        _inst = okx_result.get("inst_id", "")
                        if _inst:
                            register_position(
                                _inst, exec_pair, exec_price,
                                tp=_tp, sl=_sl, side="buy",
                                size_usd=final_size,
                            )
                    except Exception as _reg_exc:
                        logger.warning("[main] register_position failed: {}", _reg_exc)
                    asyncio.create_task(alerts.send_trade_alert("wyckoff", {
                        "pair":     exec_pair,
                        "score":    new_score,
                        "entry":    exec_price,
                        "sl":       _sl,
                        "tp":       _tp,
                        "size_usd": final_size,
                    }))

            # ── 2. Fallback MEXC si OKX no operó ─────────────────────────
            if not _traded and _mexc_wyckoff and _mexc_wyckoff.enabled and final_size >= 1.0:
                logger.info("[main] OKX no operó — intentando MEXC {} ${:.0f} price={:.2f}",
                            alloc.pair, final_size, alloc.entry_price)
                mexc_pair  = alloc.pair
                mexc_price = alloc.entry_price
                mexc_result = None
                try:
                    mexc_result = await asyncio.wait_for(
                        _mexc_wyckoff.market_order(
                            pair=mexc_pair, side="buy",
                            size_usd=final_size, price_hint=mexc_price),
                        timeout=10.0)
                except Exception as exc:
                    mexc_result = None
                    logger.warning("[main] MEXC {} exception: {!r} type={}",
                                   mexc_pair, exc, type(exc).__name__)

                if not mexc_result:
                    logger.warning("[main] MEXC {} retornó None — buscando par alternativo",
                                   mexc_pair)
                    alt = _mexc_wyckoff.find_affordable_pair(
                        10.0, _latest_prices, original_pair=mexc_pair)
                    if alt and alt in _latest_prices:
                        mexc_pair  = alt
                        mexc_price = _latest_prices[alt]
                        logger.info("[main] MEXC par alternativo: {} price={:.2f}",
                                    alt, mexc_price)
                        try:
                            mexc_result = await asyncio.wait_for(
                                _mexc_wyckoff.market_order(
                                    pair=alt, side="buy",
                                    size_usd=final_size, price_hint=mexc_price),
                                timeout=10.0)
                        except Exception as exc:
                            logger.error("[main] MEXC alt {} exception: {!r} type={}",
                                         alt, exc, type(exc).__name__)
                    else:
                        logger.warning("[main] MEXC no encontró par alternativo asequible")

                if mexc_result:
                    _traded = True
                    logger.info("[main] Trade MEXC {} ${:.2f} OK orderId={}",
                                mexc_pair, final_size,
                                mexc_result.get("orderId", "?"))
                    _sl_m = alloc.stop_loss
                    _tp_m = alloc.take_profit
                    if mexc_pair != alloc.pair:
                        _sl_pct = config.STOP_LOSS_PCT
                        _rr     = config.RISK_REWARD
                        _sl_m = mexc_price * (1.0 - _sl_pct)
                        _tp_m = mexc_price * (1.0 + _sl_pct * _rr)
                    asyncio.create_task(alerts.send_trade_alert("wyckoff", {
                        "pair":     mexc_pair,
                        "score":    new_score,
                        "entry":    mexc_price,
                        "sl":       _sl_m,
                        "tp":       _tp_m,
                        "size_usd": final_size,
                    }))

            if not _traded:
                _exchanges = "OKX" if not config.MEXC_ENABLED else "OKX+MEXC"
                logger.error(
                    "[main] {} agotados para ${:.0f}", _exchanges, final_size)

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
                    logger.info("[startup] IP pública del servidor: {}", ip)
                    logger.warning(
                        "[startup] Railway cambia IP en cada deploy. "
                        "Bybit API debe estar en modo 'No IP restriction' "
                        "(solo firma HMAC)."
                    )
                    break
        except Exception:
            continue
    else:
        logger.warning("[startup] No se pudo obtener IP pública del servidor")

    # NO enviar alerta Telegram al arrancar — conservar rate limit.
    # Solo se envía Telegram cuando hay una señal real ejecutada.
    logger.info("[startup] Telegram: sin mensaje de inicio (rate limit protection)")


async def _send_telegram(msg: str) -> None:
    """Envía mensaje Telegram via tg_sender centralizado."""
    try:
        import tg_sender
        await tg_sender.send(msg)
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
