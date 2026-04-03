"""
alerts.py — Whale Follower Bot
Sends Telegram alerts and logs signals to Supabase.
"""
from __future__ import annotations

import asyncio
import time
from typing import Optional

import aiohttp
from loguru import logger
from supabase import create_client, Client

import config
from spring_detector import SpringSignal

# ── Supabase client (lazy init) ───────────────────────────────────────────────
_supabase: Optional[Client] = None


def _get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        _supabase = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    return _supabase


# ── Telegram ───────────────────────────────────────────────────────────────────

def _format_conditions(conds: dict) -> str:
    lines = []
    if conds.get("cond_a"):
        lines.append(f"  ✅ Caída {conds['drop_pct']:.2f}% en <10s (caza de stops)")
    if conds.get("cond_b"):
        lines.append(f"  ✅ Rebote {conds['bounce_pct']:.2f}% en <5s (absorción)")
    if conds.get("cond_c"):
        lines.append("  ✅ CVD subiendo mientras precio bajó (divergencia)")
    if conds.get("cond_d"):
        lines.append(f"  ✅ Volumen {conds['vol_ratio']:.1f}x el promedio (spike)")
    if conds.get("cascade_detected"):
        lines.append("  ⚡ Stop cascade detectado")
    if not lines:
        lines.append("  (sin condiciones individuales registradas)")
    return "\n".join(lines)


def _build_message(signal: SpringSignal) -> str:
    bar = "🟢" * min(signal.score // 10, 10)
    conditions_text = _format_conditions(signal.conditions)
    return (
        f"🐋 *WHALE SPRING DETECTADO* — BTC/USDT\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Score: *{signal.score}/100* {bar}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entrada sugerida: `${signal.price_entry:,.2f}`\n"
        f"🛑 Stop Loss:        `${signal.stop_loss:,.2f}` (−{config.STOP_LOSS_PCT*100:.1f}%)\n"
        f"🎯 Take Profit:      `${signal.take_profit:,.2f}` (ratio 1:{config.RISK_REWARD:.0f})\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 CVD Velocity (10s): `{signal.cvd_velocity:+.4f} BTC`\n"
        f"📍 Spring Low: `${signal.spring_low:,.2f}`\n"
        f"🏦 Exchange más fuerte: *{signal.strongest_exchange.upper()}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Condiciones cumplidas:\n{conditions_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ _Solo análisis. No es consejo financiero._"
    )


async def send_telegram(signal: SpringSignal) -> None:
    """Send Markdown alert via Telegram Bot API."""
    text = _build_message(signal)
    url = (
        f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    )
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"[telegram] HTTP {resp.status}: {body}")
                else:
                    logger.success(f"[telegram] Alert sent. Score={signal.score}")
    except Exception as exc:
        logger.error(f"[telegram] Failed to send alert: {exc}")


# ── Supabase persistence ───────────────────────────────────────────────────────

async def save_to_supabase(signal: SpringSignal) -> None:
    """Persist signal to whale_signals table (non-blocking via thread executor)."""
    row = {
        "pair": config.TRADING_PAIR,
        "score": signal.score,
        "price_entry": signal.price_entry,
        "stop_loss": signal.stop_loss,
        "take_profit": signal.take_profit,
        "conditions_met": signal.conditions,
        "strongest_exchange": signal.strongest_exchange,
        "cvd_velocity": signal.cvd_velocity,
    }
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _get_supabase().table("whale_signals").insert(row).execute()
        )
        logger.success(f"[supabase] Signal saved. Score={signal.score}")
    except Exception as exc:
        logger.error(f"[supabase] Failed to save signal: {exc}")


# ── Unified dispatch ───────────────────────────────────────────────────────────

async def dispatch(signal: SpringSignal) -> None:
    """Send Telegram alert and save to Supabase concurrently."""
    await asyncio.gather(
        send_telegram(signal),
        save_to_supabase(signal),
        return_exceptions=True,
    )
