"""
alerts.py — Whale Follower Bot — Sprint 2
Alertas Telegram enriquecidas + comandos /status /stats /last /trades
+ persistencia en Supabase.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp
from loguru import logger
from supabase import create_client, Client

import config
from scoring_engine import ScoreBreakdown

# ── Estado global de stats ───────────────────────────────────────────────
_stats: Dict[str, Any] = {
    "signals_total":   0,
    "signals_high":    0,      # score >= 80
    "last_signal":     None,   # dict con datos de la última señal
    "start_time":      time.time(),
    # Grid
    "grid_cycles":     0,      # ciclos buy+sell completados
    "grid_pnl":        0.0,    # P&L acumulado de grid
    # Arbitraje triangular
    "arb_opportunities": 0,    # oportunidades detectadas
    "arb_executed":      0,    # arb reales ejecutados (total)
    "arb_btc_exec":      0,    # ejecutados BTCUSDT
    "arb_eth_exec":      0,    # ejecutados ETHUSDT
    "arb_sol_exec":      0,    # ejecutados SOLUSDT
    "arb_pnl":           0.0,  # P&L acumulado arb
    # Capital (se actualiza por llamada externa)
    "capital_bybit":       0.0,
    "capital_okx":         0.0,
    "capital_okx_breakdown": {},   # {"USDT": xx, "ETH": xx, "SOL": xx}
    "capital_usdt_only":     0.0,  # solo USDT para drawdown (sin cripto holdings)
    # Bitso monitor
    "bitso_opportunities": 0,
    "bitso_spread_sum":    0.0,
    "bitso_spread_count":  0,
    # P&L por estrategia (Adaptive Position Sizing)
    "pnl_by_strategy": {
        "grid":           0.0,
        "okx_grid":       0.0,
        "mean_reversion": 0.0,
        "ofi":            0.0,
        "delta_neutral":  0.0,
        "wyckoff":        0.0,
        "arb":            0.0,
    },
    # Contadores de trades por estrategia: {strat: {"total": 0, "wins": 0, "losses": 0}}
    "trades_by_strategy": {
        "grid":           {"total": 0, "wins": 0, "losses": 0},
        "okx_grid":       {"total": 0, "wins": 0, "losses": 0},
        "mean_reversion": {"total": 0, "wins": 0, "losses": 0},
        "ofi":            {"total": 0, "wins": 0, "losses": 0},
        "delta_neutral":  {"total": 0, "wins": 0, "losses": 0},
        "wyckoff":        {"total": 0, "wins": 0, "losses": 0},
        "arb":            {"total": 0, "wins": 0, "losses": 0},
    },
    # Historial de PnL para gráficos (últimas 24h, una entrada por hora)
    "pnl_hourly": [],          # lista de floats, se agrega cada hora
    "pnl_hourly_ts": [],       # timestamps correspondientes
    "pnl_session_curve": [],   # todos los P&L acumulados en orden para curva de equity
    # Top trades para el reporte
    "top_trades": [],          # lista de dicts {strategy, pnl, pair, ts}
    # Drawdown tracking
    "capital_peak":      0.0,
    "drawdown_alerted":  False,   # para no spamear alertas
    "max_drawdown_pct":  0.0,     # máximo drawdown histórico de la sesión
    # Auto-report
    "last_auto_report":  time.time(),
}

# ── Termometros de mercado (actualizados por los monitores) ───────────────
_thermometers: Dict[str, Any] = {
    "btc_dom_pct":    0.0,
    "btc_dom_signal": "?",
    "liq_long_m":     0.0,
    "liq_short_m":    0.0,
    "liq_signal":     "?",
    "dxy_value":      0.0,
    "dxy_change_pct": 0.0,
    "dxy_signal":     "?",
}

# ── Stats public API ──────────────────────────────────────────────────

def record_strategy_pnl(strategy: str, pnl: float, pair: str = "") -> None:
    """Registrar P&L por estrategia. Actualiza contadores de wins/losses e historial."""
    _stats["pnl_by_strategy"][strategy] = (
        _stats["pnl_by_strategy"].get(strategy, 0.0) + pnl
    )
    # Win/Loss counter
    t = _stats["trades_by_strategy"].setdefault(
        strategy, {"total": 0, "wins": 0, "losses": 0}
    )
    t["total"] += 1
    if pnl > 0:
        t["wins"] += 1
    elif pnl < 0:
        t["losses"] += 1
    # Equity curve (P&L acumulado)
    prev = _stats["pnl_session_curve"][-1] if _stats["pnl_session_curve"] else 0.0
    _stats["pnl_session_curve"].append(round(prev + pnl, 4))
    if len(_stats["pnl_session_curve"]) > 500:
        _stats["pnl_session_curve"] = _stats["pnl_session_curve"][-500:]
    # Top trades (guardar los 20 mejores y 20 peores)
    _stats["top_trades"].append({
        "strategy": strategy, "pnl": round(pnl, 4),
        "pair": pair or strategy.upper(), "ts": time.time(),
    })
    if len(_stats["top_trades"]) > 200:
        _stats["top_trades"] = _stats["top_trades"][-200:]
    # Hourly bucket
    now = time.time()
    ts_list  = _stats["pnl_hourly_ts"]
    pnl_list = _stats["pnl_hourly"]
    if ts_list and now - ts_list[-1] < 3600:
        pnl_list[-1] = round(pnl_list[-1] + pnl, 4)
    else:
        ts_list.append(now)
        pnl_list.append(round(pnl, 4))
        if len(ts_list) > 24:
            ts_list.pop(0)
            pnl_list.pop(0)

def check_drawdown() -> float:
    """Calcula drawdown sobre capital USDT (no incluye cripto en holdings)."""
    # Usar solo USDT para drawdown — ETH/SOL/BTC varían con el mercado
    cap = _stats.get("capital_usdt_only", 0.0)
    if cap <= 0:
        # Fallback: usar capital reportado pero con nota
        cap = _stats["capital_bybit"] + _stats["capital_okx"]
    if cap <= 0:
        return 0.0
    if cap > _stats["capital_peak"]:
        _stats["capital_peak"] = cap
        _stats["drawdown_alerted"] = False
        return 0.0
    peak = _stats["capital_peak"]
    if peak <= 0:
        return 0.0
    dd_pct = (peak - cap) / peak * 100
    if dd_pct > _stats["max_drawdown_pct"]:
        _stats["max_drawdown_pct"] = round(dd_pct, 2)
    if dd_pct >= 5.0 and not _stats["drawdown_alerted"]:
        _stats["drawdown_alerted"] = True
        asyncio.ensure_future(_send_drawdown_alert(dd_pct, cap, peak))
    if dd_pct < 3.0:
        _stats["drawdown_alerted"] = False
    return dd_pct

async def _send_drawdown_alert(dd_pct: float, capital: float, peak: float) -> None:
    token   = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return
    emoji = "🚨" if dd_pct >= 10 else "⚠️"
    msg = (
        f"{emoji} ALERTA DRAWDOWN\n"
        f"──────────────────────\n"
        f"Drawdown:   -{dd_pct:.1f}%\n"
        f"Capital:    ${capital:.0f}\n"
        f"Peak:       ${peak:.0f}\n"
        f"Pérdida:    -${peak - capital:.0f}\n"
        f"──────────────────────\n"
        f"Hora: {time.strftime('%H:%M', time.localtime())} CST"
    )
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=aiohttp.ClientTimeout(total=10),
            )
    except Exception as exc:
        logger.warning("[alerts] drawdown alert error: {}", exc)

async def auto_report_loop(interval_hours: float = 4.0) -> None:
    """Tarea de fondo: envía reporte automático a Telegram cada N horas."""
    interval_secs = int(interval_hours * 3600)
    await asyncio.sleep(300)  # esperar 5 min antes del primer reporte
    while True:
        await asyncio.sleep(interval_secs)
        try:
            await _send_auto_report()
        except Exception as exc:
            logger.warning("[alerts] auto_report error: {}", exc)

async def _send_auto_report() -> None:
    token   = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return

    uptime    = int(time.time() - _stats["start_time"])
    h, m      = divmod(uptime // 60, 60)
    total_pnl = sum(_stats["pnl_by_strategy"].values())
    cap_bybit = _stats["capital_bybit"]
    cap_okx   = _stats["capital_okx"]
    total_cap = cap_bybit + cap_okx
    dd        = check_drawdown()

    # P&L por estrategia
    pnl_lines = ""
    for strat, pnl in _stats["pnl_by_strategy"].items():
        if pnl != 0.0:
            pnl_lines += f"  {strat:<16} ${pnl:+.4f}\n"
    if not pnl_lines:
        pnl_lines = "  (sin trades completados)\n"

    # Asignación de capital desde position_sizer
    alloc_lines = ""
    try:
        from position_sizer import get_sizer
        sz = get_sizer()
        sz.update_capital(total_cap)
        alloc = sz.allocation_summary()
        for strat, max_usd in alloc.items():
            alloc_lines += f"  {strat:<16} máx ${max_usd:.0f}\n"
        regime = sz._regime()
        atr    = sz._atr_pct()
        sm     = sz._size_mult()
    except Exception:
        alloc_lines = "  (no disponible)\n"
        regime = "?"
        atr    = 0.0
        sm     = 1.0

    dd_str = f"-{dd:.1f}% ⚠️" if dd >= 5 else f"-{dd:.1f}% ✅"

    msg = (
        f"📊 REPORTE AUTOMÁTICO\n"
        f"──────────────────────\n"
        f"Uptime:  {h}h {m}m\n"
        f"Capital: Bybit=${cap_bybit:.0f} | OKX=${cap_okx:.0f} | Total=${total_cap:.0f}\n"
        f"Drawdown: {dd_str}\n"
        f"\nRégimen: {regime} | ATR={atr:.2f}% | size_mult={sm:.1f}x\n"
        f"\nP&L por estrategia:\n{pnl_lines}"
        f"PnL Total sesión: ${total_pnl:+.4f}\n"
        f"\nAsignación máx por estrategia:\n{alloc_lines}"
        f"──────────────────────\n"
        f"Hora: {time.strftime('%H:%M', time.localtime())} CST"
    )
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=aiohttp.ClientTimeout(total=10),
            )
        logger.info("[alerts] Auto-reporte enviado a Telegram ✅")
    except Exception as exc:
        logger.warning("[alerts] auto_report send error: {}", exc)

def record_grid_cycle(pnl: float = 0.0) -> None:
    """Llamar desde grid_trading._fill_sell() en cada ciclo completado."""
    _stats["grid_cycles"] += 1
    _stats["grid_pnl"]    += pnl
    record_strategy_pnl("grid", pnl)

def record_arb_opportunity() -> None:
    """Llamar desde triangular_arb._check_arb() cuando detecta oportunidad."""
    _stats["arb_opportunities"] += 1

def record_arb_executed(pnl: float = 0.0) -> None:
    """Llamar desde triangular_arb._execute_real() cuando ejecuta en real."""
    _stats["arb_executed"] += 1
    _stats["arb_pnl"]      += pnl
    record_strategy_pnl("arb", pnl)

def record_arb_executed_pair(pair: str, pnl: float = 0.0) -> None:
    """Registrar ejecucion real por par especifico."""
    _stats["arb_executed"] += 1
    _stats["arb_pnl"]      += pnl
    record_strategy_pnl("arb", pnl)
    key_map = {"BTCUSDT": "arb_btc_exec", "ETHUSDT": "arb_eth_exec", "SOLUSDT": "arb_sol_exec"}
    k = key_map.get(pair)
    if k:
        _stats[k] = _stats.get(k, 0) + 1

async def send_trade_alert(trade_type: str, data: dict) -> None:
    """
    Envia notificacion Telegram cuando se ejecuta un trade real.
    trade_type: "cross_arb" | "grid" | "wyckoff"
    """
    token   = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return

    now_str = time.strftime("%H:%M", time.localtime())
    pnl_today = _stats["arb_pnl"] + _stats["grid_pnl"]

    try:
        if trade_type == "cross_arb":
            pair      = data.get("pair", "BTCUSDT")
            pair_disp = pair.replace("USDT", "/USDT")
            buy_ex    = data.get("buy_ex", "Bybit")
            sell_ex   = data.get("sell_ex", "OKX")
            msg = (
                f"\U0001f7e2 TRADE REAL EJECUTADO\n"
                f"──────────────────────\n"
                f"Par:        {pair_disp}\n"
                f"Estrategia: Cross-Arb {buy_ex}↔{sell_ex}\n"
                f"Lado:       COMPRA {buy_ex} + VENTA {sell_ex}\n"
                f"Precio:     ${data.get('buy_price', 0):,.2f} / ${data.get('sell_price', 0):,.2f}\n"
                f"Net:        +${data.get('net_pnl', 0):.4f}\n"
                f"──────────────────────\n"
                f"PnL hoy:    +${pnl_today:.4f}\n"
                f"Hora:       {now_str} CST"
            )
        elif trade_type == "grid":
            pair      = data.get("pair", "BTCUSDT")
            pair_disp = pair.replace("USDT", "/USDT")
            msg = (
                f"\U0001f7e2 GRID CICLO COMPLETADO\n"
                f"──────────────────────\n"
                f"Par:        {pair_disp}\n"
                f"Compra:     ${data.get('buy_price', 0):,.2f}\n"
                f"Venta:      ${data.get('sell_price', 0):,.2f}\n"
                f"PnL ciclo:  +${data.get('pnl', 0):.4f}\n"
                f"──────────────────────\n"
                f"PnL hoy:    +${pnl_today:.4f}\n"
                f"Hora:       {now_str} CST"
            )
        elif trade_type == "wyckoff":
            pair      = data.get("pair", "BTCUSDT")
            pair_disp = pair.replace("USDT", "/USDT")
            msg = (
                f"\U0001f433 WHALE SPRING DETECTADO\n"
                f"──────────────────────\n"
                f"Par:        {pair_disp}\n"
                f"Score:      {data.get('score', 0)}/130\n"
                f"Entrada:    ${data.get('entry', 0):,.2f}\n"
                f"Stop Loss:  ${data.get('sl', 0):,.2f}\n"
                f"Target:     ${data.get('tp', 0):,.2f}\n"
                f"Tamaño:     ${data.get('size_usd', 0):.2f}\n"
                f"──────────────────────\n"
                f"Hora:       {now_str} CST"
            )
        else:
            return

        await _send_telegram(msg, priority="critical")
    except Exception as exc:
        logger.warning("[alerts] send_trade_alert error: {}", exc)

def update_thermometers(**kwargs: Any) -> None:
    """Llamar desde btc_dominance, liquidations_global y dxy_monitor al actualizar."""
    _thermometers.update(kwargs)

def record_bitso_opportunity(spread_pct: float = 0.0) -> None:
    """Llamar desde bitso_arb._check_vs() cuando detecta spread >= umbral."""
    _stats["bitso_opportunities"]  += 1
    _stats["bitso_spread_sum"]     += spread_pct
    _stats["bitso_spread_count"]   += 1

def set_capital(bybit: float = 0.0, okx: float = 0.0, okx_breakdown: dict = None) -> None:
    """Actualizar capital conocido. También actualiza position_sizer y verifica drawdown."""
    if bybit > 0:
        _stats["capital_bybit"] = bybit
    if okx > 0:
        _stats["capital_okx"] = okx
    if okx_breakdown:
        _stats["capital_okx_breakdown"] = okx_breakdown
    # Capital USDT-only para drawdown (no incluye cripto holdings)
    _stats["capital_usdt_only"] = (bybit or 0) + (okx_breakdown or {}).get("USDT", 0)

    # Actualizar position_sizer con capital total real
    total = (_stats["capital_bybit"] or 0) + (_stats["capital_okx"] or 0)
    if total > 0:
        try:
            from position_sizer import get_sizer
            get_sizer().update_capital(total)
        except Exception:
            pass
    # Verificar drawdown
    check_drawdown()

# ── Supabase cliente (lazy) ────────────────────────────────────────────────────
_supabase: Optional[Client] = None

def _get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    return _supabase


# ── Formateo del mensaje ───────────────────────────────────────────────────────

def _conditions_text(bd: ScoreBreakdown) -> str:
    lines = []
    if bd.spring_confirmed:
        lines.append(f"  Spring confirmado (caida {bd.drop_pct:.2f}% / rebote {bd.bounce_pct:.2f}%)")
    elif bd.drop_pct > 0:
        lines.append(f"  Caida parcial {bd.drop_pct:.2f}% (sin rebote aun)")
    if bd.cvd_divergence:
        lines.append(f"  CVD divergencia: +{bd.cvd_vel_10s:.4f} BTC/10s")
    if bd.cascade_active:
        lines.append(f"  Stop cascade: {bd.cascade_trades} trades de venta")
    if bd.vol_spike:
        lines.append(f"  Volumen {bd.vol_ratio:.1f}x promedio")
    if bd.funding_favorable and bd.funding_rate is not None:
        lines.append(f"  Funding Rate: {bd.funding_rate:.4f}% (shorts liquidandose)")
    if bd.oi_confirming and bd.oi_change_pct is not None:
        lines.append(f"  Open Interest: +{bd.oi_change_pct:.3f}% (movimiento real)")
    if bd.institutional_session:
        lines.append(f"  Sesion {bd.session_name.upper()} activa (x{bd.session_mult:.1f})")
    if bd.near_vwap:
        lines.append("  Precio cerca del VWAP (estructura)")
    if bd.ema_favorable:
        lines.append("  Precio sobre EMA 200 (tendencia alcista)")
    if not lines:
        lines.append("  (condiciones basicas cumplidas)")
    return "\n".join(lines)


def _build_message(
    score: int,
    bd: ScoreBreakdown,
    spring_low: float,
    entry: float,
    sl: float,
    tp: float,
    exchange: str,
    trade_info: Optional[str] = None,
) -> str:
    high_confidence = score >= config.HIGH_CONFIDENCE_SCORE
    header = (
        "WHALE SPRING - ALTA CONFIANZA - BTC/USDT" if high_confidence
        else "WHALE SPRING DETECTADO - BTC/USDT"
    )
    stars = ""
    if score >= 90: stars = "[***]"
    elif score >= 80: stars = "[**]"
    elif score >= 65: stars = "[*]"

    risk      = entry - sl
    reward    = tp - entry
    rr_ratio  = round(reward / risk, 1) if risk > 0 else 0

    cond_text = _conditions_text(bd)

    score_bar = "#" * (score // 10) + "-" * (10 - score // 10)

    msg = (
        f"{header}\n"
        f"[{score_bar}] {score}/100 {stars}\n"
        f"------------------------------\n"
        f"Entrada sugerida: ${entry:,.2f}\n"
        f"Stop Loss:        ${sl:,.2f} (-{config.STOP_LOSS_PCT*100:.1f}%)\n"
        f"Take Profit:      ${tp:,.2f} (ratio 1:{rr_ratio})\n"
        f"Spring Low:       ${spring_low:,.2f}\n"
        f"------------------------------\n"
        f"CVD Velocity 10s: {bd.cvd_vel_10s:+.4f} BTC\n"
        f"Exchange: {exchange.upper()}\n"
        f"------------------------------\n"
        f"Condiciones activas:\n{cond_text}\n"
        f"------------------------------\n"
    )

    # Desglose de puntos
    msg += (
        f"Score: primarias={bd.primary_pts} volumen={bd.volume_pts} "
        f"contexto={bd.context_pts} estructura={bd.structure_pts}\n"
    )

    if trade_info:
        msg += f"------------------------------\n{trade_info}\n"

    msg += "Solo analisis. No es consejo financiero."
    return msg


# ── Envío de alerta completa ───────────────────────────────────────────────────

async def dispatch(
    score: int,
    bd: ScoreBreakdown,
    spring_data: Dict,
    exchange: str,
    entry: float,
    sl: float,
    tp: float,
    trade_info: Optional[str] = None,
    signal_id:  Optional[str] = None,
) -> None:
    """Punto de entrada principal — envía Telegram y guarda en Supabase."""
    global _stats

    _stats["signals_total"] += 1
    if score >= config.HIGH_CONFIDENCE_SCORE:
        _stats["signals_high"] += 1
    _stats["last_signal"] = {
        "score": score, "entry": entry, "sl": sl, "tp": tp,
        "exchange": exchange, "ts": time.time(),
    }

    msg = _build_message(score, bd, spring_data.get("spring_low", entry), entry, sl, tp, exchange, trade_info)

    await asyncio.gather(
        _send_telegram(msg),
        _save_supabase(score, bd, spring_data, exchange, entry, sl, tp, signal_id),
        return_exceptions=True,
    )


# ── Telegram — delega a tg_sender centralizado ──────────────────────────────
# Backwards-compatible: _send_telegram sigue existiendo para los módulos
# que ya lo importan, pero delega a tg_sender.send().
_tg_retry_after: float = 0.0   # legacy — ahora lo maneja tg_sender


async def _send_telegram(text: str, priority: str = "normal") -> None:
    """Envía mensaje a Telegram via tg_sender centralizado."""
    import tg_sender
    await tg_sender.send(text, priority=priority)


async def send_system_message(text: str) -> None:
    await _send_telegram(text)


async def _save_supabase(
    score: int, bd: ScoreBreakdown, spring_data: Dict,
    exchange: str, entry: float, sl: float, tp: float,
    signal_id: Optional[str],
) -> None:
    row = {
        "pair":               config.TRADING_PAIR,
        "score":              score,
        "price_entry":        entry,
        "stop_loss":          sl,
        "take_profit":        tp,
        "strongest_exchange": exchange,
        "cvd_velocity":       bd.cvd_vel_10s,
        "conditions_met": {
            "spring_confirmed":    bd.spring_confirmed,
            "cvd_divergence":      bd.cvd_divergence,
            "cascade_active":      bd.cascade_active,
            "vol_spike":           bd.vol_spike,
            "funding_favorable":   bd.funding_favorable,
            "oi_confirming":       bd.oi_confirming,
            "institutional_session": bd.institutional_session,
            "drop_pct":            bd.drop_pct,
            "bounce_pct":          bd.bounce_pct,
            "vol_ratio":           bd.vol_ratio,
            "funding_rate":        bd.funding_rate,
            "oi_change_pct":       bd.oi_change_pct,
            "session_name":        bd.session_name,
            "session_multiplier":  bd.session_mult,
        },
    }
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _get_supabase().table("whale_signals").insert(row).execute()
        )
        logger.success(f"[supabase] Signal guardada. Score={score}")
    except Exception as exc:
        logger.error(f"[supabase] Error guardando: {exc}")


# ── Alerta multi-par ─────────────────────────────────────────────────────────

async def dispatch_multi(
    signals: list,
    allocation: Any,
    all_scores: dict,
) -> None:
    """Alerta multi-par: scores de todos los pares y capital distribuido."""
    global _stats
    if not signals:
        return

    best = max(signals, key=lambda s: s.score)
    _stats["signals_total"] += 1
    if best.score >= config.HIGH_CONFIDENCE_SCORE:
        _stats["signals_high"] += 1
    _stats["last_signal"] = {
        "score": best.score, "entry": best.entry_price,
        "sl": best.stop_loss, "tp": best.take_profit,
        "exchange": best.spring_data.get("strongest_exchange", "multi"),
        "ts": time.time(),
    }

    score_min = config.SIGNAL_SCORE_THRESHOLD
    try:
        from learning_manager import get_threshold
        score_min = int(get_threshold())
    except Exception:
        pass

    score_lines = []
    for pair, score in sorted(all_scores.items(), key=lambda x: -x[1]):
        is_trading = allocation and any(t.pair == pair for t in allocation.trades)
        if is_trading:
            tag = "OPERANDO"
        elif score >= score_min:
            tag = "En espera"
        else:
            tag = "Sin senal"
        score_lines.append(f"  {pair}: {score:3d}/100 [{tag}]")

    capital_lines = []
    if allocation:
        for alloc in allocation.trades:
            capital_lines.append(
                f"  {alloc.pair}: ${alloc.size_usd:,.0f} ({alloc.capital_fraction*100:.0f}%)"
            )

    mode_labels = {"A": "Par unico", "B": "Diversificado", "C": "Correlacion BTC activa"}
    mode_label = mode_labels.get(getattr(allocation, "mode", "A"), "Auto")

    msg = (
        "MULTI-PAR DETECTADO\n"
        "------------------------------\n"
        "Scores actuales:\n"
        + "\n".join(score_lines) + "\n"
        "------------------------------\n"
        "Capital distribuido:\n"
        + ("\n".join(capital_lines) if capital_lines else "  (ninguno)") + "\n"
        "------------------------------\n"
        f"Modo: {mode_label}\n"
        f"Score min: {score_min} | RR 1:{config.RISK_REWARD:.0f}\n"
        "Solo analisis. No es consejo financiero."
    )

    await asyncio.gather(
        _send_telegram(msg),
        *[
            _save_supabase(
                s.score, s.breakdown, s.spring_data,
                s.spring_data.get("strongest_exchange", "multi"),
                round(s.entry_price, 2), round(s.stop_loss, 2), round(s.take_profit, 2),
                None,
            )
            for s in signals
        ],
        return_exceptions=True,
    )


# ── Comandos de Telegram (/status /stats /last /trades) ───────────────────────

async def handle_telegram_commands(executor: Any) -> None:
    """
    Polling de comandos Telegram. Corre como tarea de fondo.
    executor: instancia de BybitTestnetExecutor para /trades
    """
    offset = 0
    token  = config.TELEGRAM_BOT_TOKEN
    url    = f"https://api.telegram.org/bot{token}/getUpdates"

    # Eliminar webhook activo antes de iniciar polling (si hay webhook, getUpdates falla)
    try:
        async with aiohttp.ClientSession() as _s:
            r = await _s.get(
                f"https://api.telegram.org/bot{token}/deleteWebhook",
                params={"drop_pending_updates": "false"},
                timeout=aiohttp.ClientTimeout(total=10),
            )
            data = await r.json()
            if data.get("result"):
                logger.info("[telegram_cmd] Webhook eliminado ✅ — polling activo")
            else:
                logger.debug("[telegram_cmd] deleteWebhook: {}", data)
    except Exception as exc:
        logger.warning("[telegram_cmd] No se pudo eliminar webhook: {}", exc)

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url,
                    params={"offset": offset, "timeout": 30, "allowed_updates": ["message"]},
                    timeout=aiohttp.ClientTimeout(total=35),
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(5)
                        continue
                    data = await resp.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg    = update.get("message", {})
                text   = msg.get("text", "").strip()
                chat   = str(msg.get("chat", {}).get("id", ""))

                if chat != config.TELEGRAM_CHAT_ID:
                    continue

                try:
                    if text == "/status":
                        await asyncio.wait_for(_cmd_status(), timeout=10.0)
                    elif text == "/stats":
                        await asyncio.wait_for(_cmd_stats(), timeout=10.0)
                    elif text in ("/report", "/analytics"):
                        await asyncio.wait_for(_cmd_report(), timeout=15.0)
                    elif text == "/last":
                        await asyncio.wait_for(_cmd_last(), timeout=10.0)
                    elif text == "/trades":
                        await asyncio.wait_for(_cmd_trades(executor), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.error("[telegram_cmd] Timeout procesando {}", text)
                    await _reply(f"Timeout procesando {text}. Bot activo.")
                except Exception as exc:
                    logger.error("[telegram_cmd] Error procesando {}: {}", text, exc)
                    await _reply(f"Error en {text}. Bot activo.")

        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(f"[telegram_cmd] Error en polling: {exc}")
            await asyncio.sleep(10)


async def _reply(text: str) -> None:
    await _send_telegram(text)


async def _cmd_status() -> None:
    uptime = int(time.time() - _stats["start_time"])
    h, m   = divmod(uptime // 60, 60)

    # Threshold real: optimizer > SPRING_PARAMS > fallback
    score_min = config.SIGNAL_SCORE_THRESHOLD
    regime_label = "LATERAL"
    try:
        from threshold_optimizer import get_optimizer
        opt = get_optimizer()
        score_min = opt.get_threshold(regime_label)
    except Exception:
        score_min = config.SPRING_PARAMS.get(regime_label, {}).get("score_min", score_min)

    # Señales de Supabase (sobrevive reinicios)
    signals_db = _stats["signals_total"]   # in-memory como fallback
    trades_db  = 0
    try:
        from db_writer import _client, _run
        from datetime import datetime, timezone as _tz
        today_start = datetime.now(_tz.utc).replace(hour=0, minute=0, second=0).isoformat()
        result = await _run(
            lambda: _client().table("paper_trades")
            .select("id", count="exact")
            .gte("created_at", today_start)
            .execute()
        )
        if result:
            trades_db = result.count if hasattr(result, "count") and result.count else len(result.data or [])
    except Exception:
        pass
    signals_display = max(signals_db, trades_db)

    await _reply(
        f"Estado del bot\n"
        f"--------------\n"
        f"Pares: {', '.join(config.TRADING_PAIRS)}\n"
        f"Uptime: {h}h {m}m\n"
        f"Señales detectadas: {signals_display}\n"
        f"Trades hoy (Supabase): {trades_db}\n"
        f"Score minimo: {score_min} ({regime_label})\n"
        f"Exchanges: Kraken/Bybit/OKX activos"
    )


async def _cmd_stats() -> None:
    uptime = int(time.time() - _stats["start_time"])
    h, m   = divmod(uptime // 60, 60)

    total_pnl = sum(_stats["pnl_by_strategy"].values())

    # Capital: usar valor guardado o fallback a config
    cap_bybit = _stats["capital_bybit"] or config.REAL_CAPITAL
    cap_okx   = _stats["capital_okx"]   or 0.0
    total_cap = cap_bybit + cap_okx

    # Desglose OKX por coin
    okx_bd    = _stats.get("capital_okx_breakdown", {})
    if okx_bd:
        okx_detail = " + ".join(f"{c}=${v:.0f}" for c, v in okx_bd.items())
        cap_okx_str = f"~${cap_okx:.0f} ({okx_detail})"
    elif cap_okx > 0:
        cap_okx_str = f"~${cap_okx:.0f} (solo USDT visible)"
    else:
        cap_okx_str = "sin datos"

    # Drawdown
    try:
        dd     = check_drawdown()
        dd_str = f"-{dd:.1f}% ⚠️" if dd >= 5 else f"-{dd:.1f}% ✅"
    except Exception:
        dd_str = "N/A"

    # P&L por estrategia (solo las que tienen actividad)
    pnl_strat = _stats["pnl_by_strategy"]
    pnl_lines = ""
    for strat, pnl in pnl_strat.items():
        if pnl != 0.0:
            pnl_lines += f"  {strat:<16} ${pnl:+.4f}\n"
    if not pnl_lines:
        pnl_lines = "  (sin trades completados aun)\n"

    # Adaptive sizing info
    regime_line = "?"
    atr_line    = "?"
    sm_line     = "1.0x"
    alloc_lines = ""
    try:
        from position_sizer import get_sizer
        sz = get_sizer()
        sz.update_capital(total_cap)
        regime_line = sz._regime()
        atr_line    = f"{sz._atr_pct():.2f}%"
        sm_line     = f"{sz._size_mult():.1f}x"
        alloc = sz.allocation_summary()
        alloc_lines = " | ".join(f"{s}=${v:.0f}" for s, v in alloc.items())
    except Exception:
        pass

    # Bitso stats
    b_opps  = _stats["bitso_opportunities"]
    b_count = _stats["bitso_spread_count"]
    b_avg   = (_stats["bitso_spread_sum"] / b_count) if b_count > 0 else 0.0
    bitso_prima = f"{b_avg:+.2f}%" if b_opps > 0 else "sin datos"

    # Termometros
    t = _thermometers
    dom_line = f"{t['btc_dom_pct']:.1f}% ({t['btc_dom_signal']})"
    liq_line = (f"LONG=${t['liq_long_m']:.1f}M  SHORT=${t['liq_short_m']:.1f}M"
                f" ({t['liq_signal']})")
    dxy_sign = "+" if t['dxy_change_pct'] >= 0 else ""
    dxy_line = (f"{t['dxy_value']:.2f} ({dxy_sign}{t['dxy_change_pct']:.2f}%"
                f" {t['dxy_signal']})")

    await _reply(
        f"Estadisticas\n"
        f"------------\n"
        f"Señales Wyckoff:      {_stats['signals_total']}\n"
        f"Señales Grid:         {_stats['grid_cycles']} ciclos\n"
        f"Oportunidades Arb:    {_stats['arb_opportunities']}\n"
        f"Arb BTC ejecutados:   {_stats['arb_btc_exec']}\n"
        f"Arb ETH ejecutados:   {_stats['arb_eth_exec']}\n"
        f"Arb SOL ejecutados:   {_stats['arb_sol_exec']}\n"
        f"Oportunidades Bitso:  {b_opps}\n"
        f"Prima Bitso:          {bitso_prima}\n"
        f"\nP&L por estrategia\n"
        f"------------------\n"
        f"{pnl_lines}"
        f"PnL Total sesion:     ${total_pnl:+.4f}\n"
        f"\nCapital & Riesgo\n"
        f"----------------\n"
        f"Capital Bybit:        ~${cap_bybit:.0f}\n"
        f"Capital OKX:          {cap_okx_str}\n"
        f"Capital Total:        ~${total_cap:.0f}\n"
        f"Drawdown:             {dd_str}\n"
        f"\nAdaptive Sizing\n"
        f"---------------\n"
        f"Régimen:              {regime_line}\n"
        f"ATR volatilidad:      {atr_line}\n"
        f"Size multiplier:      {sm_line}\n"
        f"\nTermometros de mercado\n"
        f"----------------------\n"
        f"BTC Dominancia:       {dom_line}\n"
        f"Liquidaciones 1h:     {liq_line}\n"
        f"DXY:                  {dxy_line}\n"
        f"\nUptime:               {h}h {m}m\n"
    )


async def _cmd_report() -> None:
    """Reporte avanzado /report con métricas, gráficos de texto y top trades."""
    import math

    def _color_pnl(pnl: float) -> str:
        if pnl > 0:   return f"🟢 +${pnl:.4f}"
        if pnl < 0:   return f"🔴 ${pnl:.4f}"
        return f"⚪ $0.0000"

    def _bar(value: float, max_val: float, width: int = 12) -> str:
        if max_val == 0:
            return "⬜" * width
        ratio = min(abs(value) / abs(max_val), 1.0)
        filled = max(1, int(ratio * width)) if value != 0 else 0
        empty  = width - filled
        tile   = "🟩" if value >= 0 else "🟥"
        return tile * filled + "⬜" * empty

    uptime    = int(time.time() - _stats["start_time"])
    h, m      = divmod(uptime // 60, 60)
    total_cap = (_stats["capital_bybit"] or 0) + (_stats["capital_okx"] or 0)
    pnl_strat = _stats["pnl_by_strategy"]
    trades_st = _stats["trades_by_strategy"]
    total_pnl = sum(pnl_strat.values())
    roi_pct   = (total_pnl / total_cap * 100) if total_cap > 0 else 0.0

    # ── Métricas globales ────────────────────────────────────────────────────
    total_trades  = sum(v["total"]  for v in trades_st.values())
    total_wins    = sum(v["wins"]   for v in trades_st.values())
    total_losses  = sum(v["losses"] for v in trades_st.values())
    win_rate      = (total_wins / total_trades * 100) if total_trades > 0 else 0.0

    # Profit Factor = suma ganancias / suma pérdidas
    gross_profit = sum(t["pnl"] for t in _stats["top_trades"] if t["pnl"] > 0)
    gross_loss   = abs(sum(t["pnl"] for t in _stats["top_trades"] if t["pnl"] < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    # Sharpe simplificado sobre equity curve
    curve = _stats["pnl_session_curve"]
    if len(curve) >= 2:
        returns = [curve[i] - curve[i-1] for i in range(1, len(curve))]
        avg_r   = sum(returns) / len(returns)
        std_r   = math.sqrt(sum((r - avg_r)**2 for r in returns) / len(returns)) if len(returns) > 1 else 0
        sharpe  = (avg_r / std_r * math.sqrt(len(returns))) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    max_dd  = _stats["max_drawdown_pct"]
    curr_dd = check_drawdown()

    # ── Tabla por estrategia ─────────────────────────────────────────────────
    max_abs_pnl = max((abs(v) for v in pnl_strat.values()), default=1.0) or 1.0
    strat_lines = ""
    for strat, pnl in pnl_strat.items():
        td   = trades_st.get(strat, {"total": 0, "wins": 0, "losses": 0})
        tot  = td["total"]
        if tot == 0 and pnl == 0.0:
            continue
        wr   = (td["wins"] / tot * 100) if tot > 0 else 0.0
        bar  = _bar(pnl, max_abs_pnl, width=10)
        wr_e = "🟢" if wr >= 55 else ("🟡" if wr >= 45 else "🔴")
        strat_lines += (
            f"\n{strat.upper()[:13]:<13}\n"
            f"  Trades: {tot} ({td['wins']}✓/{td['losses']}✗)  WR:{wr_e}{wr:.0f}%\n"
            f"  PnL: {_color_pnl(pnl)}\n"
            f"  {bar}\n"
        )
    if not strat_lines:
        strat_lines = "\n  (sin operaciones completadas aun)\n"

    # ── Gráfico hourly ───────────────────────────────────────────────────────
    hourly  = _stats["pnl_hourly"]
    h_ts    = _stats["pnl_hourly_ts"]
    max_h   = max((abs(v) for v in hourly), default=1.0) or 1.0
    hourly_chart = ""
    for i, (ts, pnl_h) in enumerate(zip(h_ts, hourly)):
        label = time.strftime("%H:%M", time.localtime(ts))
        bar   = _bar(pnl_h, max_h, width=8)
        hourly_chart += f"\n{label} {bar} {_color_pnl(pnl_h)}"
    if not hourly_chart:
        hourly_chart = "\n  (sin datos horarios aun)"

    # ── Top 3 wins y losses ──────────────────────────────────────────────────
    all_trades  = _stats["top_trades"]
    top_wins    = sorted(all_trades, key=lambda x: x["pnl"], reverse=True)[:3]
    top_losses  = sorted(all_trades, key=lambda x: x["pnl"])[:3]

    win_lines  = "\n".join(
        f"  {i+1}. {t['strategy']}: 🟢 +${t['pnl']:.4f} ({t['pair']})"
        for i, t in enumerate(top_wins)
    ) or "  (ninguna)"

    loss_lines = "\n".join(
        f"  {i+1}. {t['strategy']}: 🔴 ${t['pnl']:.4f} ({t['pair']})"
        for i, t in enumerate(top_losses)
    ) or "  (ninguna)"

    # ── Recomendación ────────────────────────────────────────────────────────
    if win_rate > 60 and profit_factor > 1.5:
        rec = "✅ Excelente. Considera aumentar capital gradualmente."
    elif win_rate > 50 and profit_factor > 1.0:
        rec = "📈 Rendimiento positivo. Mantén la estrategia actual."
    elif max_dd > 15:
        rec = "⚠️ Drawdown elevado. Reduce tamaño de posiciones."
    elif total_trades == 0:
        rec = "📊 Sin trades aun. El bot está escaneando el mercado."
    else:
        rec = "📊 Mercado lateral. Espera señales más claras."

    pf_str = f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞"

    report = (
        f"📊 REPORTE ANALYTICS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Uptime: {h}h {m}m\n"
        f"📅 {time.strftime('%Y-%m-%d %H:%M', time.localtime())} CST\n"
        f"\n📈 RESUMEN GENERAL\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Capital Total:  ${total_cap:.0f}\n"
        f"PnL Total:      {_color_pnl(total_pnl)}\n"
        f"ROI sesion:     {roi_pct:+.2f}%\n"
        f"\n📊 RENDIMIENTO POR ESTRATEGIA\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
        f"{strat_lines}"
        f"\n📐 METRICAS AVANZADAS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Total Trades:   {total_trades} ({total_wins}✓ / {total_losses}✗)\n"
        f"Win Rate:       {'🟢' if win_rate>=55 else '🔴'} {win_rate:.1f}%\n"
        f"Profit Factor:  {pf_str}\n"
        f"Sharpe Ratio:   {sharpe:.2f}\n"
        f"Drawdown Max:   🔴 {max_dd:.1f}%\n"
        f"Drawdown Act:   {curr_dd:.1f}%\n"
        f"\n📉 EVOLUCION HORARIA\n"
        f"━━━━━━━━━━━━━━━━━━━━━━"
        f"{hourly_chart}\n"
        f"\n🏆 MEJORES TRADES\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{win_lines}\n"
        f"\n💀 PEORES TRADES\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{loss_lines}\n"
        f"\n💡 RECOMENDACION\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{rec}"
    )
    await _reply(report)


async def _cmd_last() -> None:
    last = _stats.get("last_signal")
    if not last:
        await _reply("No se han detectado señales aun.")
        return
    age = int(time.time() - last["ts"])
    await _reply(
        f"Ultima señal\n"
        f"------------\n"
        f"Score:    {last['score']}/100\n"
        f"Entrada:  ${last['entry']:,.2f}\n"
        f"Stop:     ${last['sl']:,.2f}\n"
        f"Target:   ${last['tp']:,.2f}\n"
        f"Exchange: {last['exchange'].upper()}\n"
        f"Hace:     {age}s"
    )


async def _cmd_trades(executor: Any) -> None:
    if executor is None:
        await _reply("Paper trading no configurado (faltan BYBIT_TESTNET_API_KEY/SECRET).")
        return
    trades = executor.active_trades_summary()
    if not trades:
        await _reply("No hay trades registrados aun.")
        return
    lines = ["Ultimos trades de paper trading\n"]
    for t in trades:
        sign  = "+" if t["pnl_usd"] >= 0 else ""
        lines.append(
            f"ID: {t['id']} | Score: {t['score']}\n"
            f"  Entrada: ${t['entry']:,.2f} | SL: ${t['sl']:,.2f}\n"
            f"  Estado: {t['status']} | P&L: {sign}${t['pnl_usd']:.2f}\n"
            f"  Razon cierre: {t['close_reason'] or 'activo'}\n"
        )
    await _reply("\n".join(lines))
