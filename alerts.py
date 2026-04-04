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

# ── Estado global de stats ─────────────────────────────────────────────────────
_stats: Dict[str, Any] = {
    "signals_total":   0,
    "signals_high":    0,    # score >= 80
    "last_signal":     None, # dict con datos de la última señal
    "start_time":      time.time(),
}

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


async def _send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":    config.TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"[telegram] HTTP {resp.status}: {body}")
                else:
                    logger.success("[telegram] Alerta enviada.")
    except Exception as exc:
        logger.error(f"[telegram] Error: {exc}")


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


# ── Comandos de Telegram (/status /stats /last /trades) ───────────────────────

async def handle_telegram_commands(executor: Any) -> None:
    """
    Polling de comandos Telegram. Corre como tarea de fondo.
    executor: instancia de BybitTestnetExecutor para /trades
    """
    offset = 0
    url    = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/getUpdates"

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

                if text == "/status":
                    await _cmd_status()
                elif text == "/stats":
                    await _cmd_stats()
                elif text == "/last":
                    await _cmd_last()
                elif text == "/trades":
                    await _cmd_trades(executor)

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
    await _reply(
        f"Estado del bot\n"
        f"--------------\n"
        f"Par: {config.TRADING_PAIR}\n"
        f"Uptime: {h}h {m}m\n"
        f"Señales detectadas: {_stats['signals_total']}\n"
        f"Alta confianza (80+): {_stats['signals_high']}\n"
        f"Score minimo: {config.SIGNAL_SCORE_THRESHOLD}\n"
        f"Exchanges: Binance/Bybit/OKX activos"
    )


async def _cmd_stats() -> None:
    total  = _stats["signals_total"]
    high   = _stats["signals_high"]
    uptime = int(time.time() - _stats["start_time"])
    h, m   = divmod(uptime // 60, 60)
    await _reply(
        f"Estadisticas\n"
        f"------------\n"
        f"Señales totales:      {total}\n"
        f"Alta confianza (80+): {high}\n"
        f"Uptime:               {h}h {m}m\n"
        f"Capital simulado:     ${config.PAPER_CAPITAL:,.0f}\n"
    )


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
