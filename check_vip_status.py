# -*- coding: utf-8 -*-
"""
check_vip_status.py -- Whale Follower Bot
Consulta el nivel VIP actual en Bybit, calcula cuánto volumen falta
para VIP 1 y estima en cuántos días se llega al ritmo actual.
Envía reporte a Telegram.

Uso:
    python check_vip_status.py
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import time
from datetime import datetime, timezone

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
_BASE_URL   = "https://api.bybit.com"
_RECV_WIN   = "5000"

_TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── Bybit fee tiers (Unified Trading Account, perp linear) ───────────────────
# Source: https://www.bybit.com/en/help-center/article/Trading-Fee-Structure
_FEE_TIERS = [
    {"level": "No VIP",  "vol30d_m": 0,    "assets_k": 0,    "maker": 0.0200, "taker": 0.0550},
    {"level": "VIP 1",   "vol30d_m": 1,    "assets_k": 250,  "maker": 0.0160, "taker": 0.0460},
    {"level": "VIP 2",   "vol30d_m": 5,    "assets_k": 1000, "maker": 0.0120, "taker": 0.0420},
    {"level": "VIP 3",   "vol30d_m": 25,   "assets_k": 5000, "maker": 0.0100, "taker": 0.0400},
    {"level": "PRO 1",   "vol30d_m": 50,   "assets_k": 10000,"maker": 0.0060, "taker": 0.0350},
]

# ── Signing ───────────────────────────────────────────────────────────────────

def _sign_get(query_str: str) -> tuple[str, str]:
    """Return (timestamp_ms, signature) for a Bybit v5 GET request."""
    ts = str(int(time.time() * 1000))
    payload = f"{ts}{_API_KEY}{_RECV_WIN}{query_str}"
    sig = hmac.new(
        _API_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return ts, sig


def _auth_headers(ts: str, sig: str) -> dict:
    return {
        "X-BAPI-API-KEY":      _API_KEY,
        "X-BAPI-TIMESTAMP":    ts,
        "X-BAPI-SIGN":         sig,
        "X-BAPI-RECV-WINDOW":  _RECV_WIN,
        "Content-Type":        "application/json",
    }


# ── Bybit API calls ───────────────────────────────────────────────────────────

async def _get(session: aiohttp.ClientSession, path: str, params: dict = {}) -> dict:
    """Authenticated GET request to Bybit v5."""
    query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    ts, sig = _sign_get(query)
    url = f"{_BASE_URL}{path}" + (f"?{query}" if query else "")
    async with session.get(url, headers=_auth_headers(ts, sig)) as resp:
        return await resp.json(content_type=None)


async def fetch_account_info(session: aiohttp.ClientSession) -> dict:
    """GET /v5/account/info → vipLevel, marginMode, etc."""
    data = await _get(session, "/v5/account/info")
    return data.get("result", {})


async def fetch_fee_rate(session: aiohttp.ClientSession) -> dict:
    """GET /v5/account/fee-rate → current maker/taker rates for linear perps."""
    data = await _get(session, "/v5/account/fee-rate", {
        "category": "linear",
        "symbol":   "BTCUSDT",
    })
    items = data.get("result", {}).get("list", [])
    if items:
        return items[0]   # {"symbol": "BTCUSDT", "makerFeeRate": "0.0002", ...}
    return {}


async def fetch_wallet_balance(session: aiohttp.ClientSession) -> float:
    """GET /v5/account/wallet-balance → total equity in USDT."""
    data = await _get(session, "/v5/account/wallet-balance", {"accountType": "UNIFIED"})
    accounts = data.get("result", {}).get("list", [])
    if accounts:
        return float(accounts[0].get("totalEquity", 0))
    return 0.0


# ── Volume estimation from Supabase ───────────────────────────────────────────

def _estimate_daily_real_volume() -> float:
    """
    Approximate daily real-money volume from the Wyckoff Spring strategy.
    Since production=true trades use RISK_PER_TRADE=1% of ~$90 capital,
    each real trade is ~$0.90 in notional. With 0 real trades executed yet,
    return a projection based on expected signal frequency (~2 signals/day).
    """
    capital     = float(os.environ.get("REAL_CAPITAL", "75"))
    risk_pct    = float(os.environ.get("RISK_PER_TRADE", "0.01"))
    signals_day = 2    # conservative estimate for Wyckoff Spring
    size_usd    = capital * risk_pct
    return round(size_usd * signals_day, 2)


# ── Analysis ──────────────────────────────────────────────────────────────────

def _next_tier(current_level: str) -> dict | None:
    for i, tier in enumerate(_FEE_TIERS):
        if tier["level"] == current_level and i + 1 < len(_FEE_TIERS):
            return _FEE_TIERS[i + 1]
    return None


def _ofi_ev(taker_pct: float, gross_tp_pct: float = 0.25, win_rate: float = 0.65) -> float:
    """Expected value per OFI trade in % given a taker fee rate."""
    fee_rt = taker_pct * 2       # round-trip
    ev = win_rate * gross_tp_pct - (1 - win_rate) * gross_tp_pct - fee_rt
    return round(ev, 4)


# ── Telegram ──────────────────────────────────────────────────────────────────

async def _send_telegram(session: aiohttp.ClientSession, text: str) -> None:
    if not _TELEGRAM_TOKEN or not _TELEGRAM_CHAT_ID:
        print("[telegram] no credentials — skipping send")
        return
    url = f"https://api.telegram.org/bot{_TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    _TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "Markdown",
    }
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print(f"[telegram] error {resp.status}: {await resp.text()}")
    except Exception as exc:
        print(f"[telegram] exception: {exc}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        # Fetch data in parallel
        account, fee_data, equity = await asyncio.gather(
            fetch_account_info(session),
            fetch_fee_rate(session),
            fetch_wallet_balance(session),
        )

        # Parse results
        vip_level     = account.get("vipLevel", "No VIP")
        maker_rate    = float(fee_data.get("makerFeeRate", "0.0002")) * 100   # % form
        taker_rate    = float(fee_data.get("takerFeeRate", "0.00055")) * 100

        # Next tier gap
        next_tier     = _next_tier(vip_level)
        vol30d_needed = (next_tier["vol30d_m"] * 1_000_000) if next_tier else 0
        assets_needed = (next_tier["assets_k"] * 1_000)    if next_tier else 0

        daily_vol     = _estimate_daily_real_volume()
        days_to_vip1  = (
            round(vol30d_needed / (daily_vol * 30 / 30), 0)   # days at current daily vol
            if daily_vol > 0 and next_tier else None
        )

        # OFI EV at current vs VIP 1
        ev_now   = _ofi_ev(taker_rate)
        ev_vip1  = _ofi_ev(_FEE_TIERS[1]["taker"])
        ev_vip2  = _ofi_ev(_FEE_TIERS[2]["taker"])

        # ── Console output ────────────────────────────────────────────────────
        now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"\n{'='*55}")
        print(f"  BYBIT VIP STATUS — {now_str}")
        print(f"{'='*55}")
        print(f"  Nivel actual:      {vip_level}")
        print(f"  Balance (equity):  ${equity:,.2f} USDT")
        print(f"  Maker fee:         {maker_rate:.4f}%")
        print(f"  Taker fee:         {taker_rate:.4f}%")

        if next_tier:
            print(f"\n  --- Siguiente nivel: {next_tier['level']} ---")
            print(f"  Vol 30d requerido:  ${vol30d_needed:>12,.0f}   ({next_tier['vol30d_m']}M)")
            print(f"  Assets requeridos:  ${assets_needed:>12,.0f}   ({next_tier['assets_k']}K)")
            print(f"  Vol real diario:    ${daily_vol:>12,.2f}   (estimado)")
            if days_to_vip1 and daily_vol > 0:
                years = days_to_vip1 / 365
                print(f"  Días hasta VIP 1:   {days_to_vip1:>12,.0f}   ({years:.1f} años al ritmo actual)")

        print(f"\n  --- OFI EV en real por tier ---")
        print(f"  No VIP  (taker {_FEE_TIERS[0]['taker']:.4f}%):  EV = {ev_now:+.4f}% per trade")
        print(f"  VIP 1   (taker {_FEE_TIERS[1]['taker']:.4f}%):  EV = {ev_vip1:+.4f}% per trade")
        print(f"  VIP 2   (taker {_FEE_TIERS[2]['taker']:.4f}%):  EV = {ev_vip2:+.4f}% per trade")
        print()
        ofi_note = (
            "OFI en real NO es viable aún (EV negativo con cualquier tier VIP)."
            if ev_vip1 < 0 else
            "OFI en real viable con VIP 1."
        )
        print(f"  CONCLUSIÓN: {ofi_note}")
        print(f"{'='*55}\n")

        # ── Telegram message ──────────────────────────────────────────────────
        vip_bar = "🔵" if vip_level == "No VIP" else "🟢"
        days_str = (
            f"~{days_to_vip1:,.0f} días ({days_to_vip1/365:.1f} años)"
            if days_to_vip1 else "N/A"
        )

        msg = (
            f"📊 *BYBIT VIP STATUS — {now_str}*\n\n"
            f"{vip_bar} Nivel: *{vip_level}*\n"
            f"💰 Balance: *${equity:,.2f} USDT*\n"
            f"📉 Maker fee: `{maker_rate:.4f}%`\n"
            f"📈 Taker fee: `{taker_rate:.4f}%`\n"
        )

        if next_tier:
            msg += (
                f"\n🎯 *Siguiente nivel: {next_tier['level']}*\n"
                f"• Vol 30d necesario: `${vol30d_needed:,.0f}` ({next_tier['vol30d_m']}M)\n"
                f"• Assets necesarios: `${assets_needed:,.0f}` ({next_tier['assets_k']}K)\n"
                f"• Vol real diario actual: `${daily_vol:,.2f}`\n"
                f"• Tiempo estimado: `{days_str}`\n"
            )

        msg += (
            f"\n📐 *OFI EV en real (TP=0.25%, WR=65%)*\n"
            f"• No VIP: `{ev_now:+.4f}%` /trade\n"
            f"• VIP 1:  `{ev_vip1:+.4f}%` /trade\n"
            f"• VIP 2:  `{ev_vip2:+.4f}%` /trade\n"
            f"\n{'✅' if ev_vip1 >= 0 else '❌'} *{ofi_note}*"
        )

        await _send_telegram(session, msg)
        print("[telegram] reporte enviado")


if __name__ == "__main__":
    asyncio.run(main())
