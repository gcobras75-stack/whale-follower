# -*- coding: utf-8 -*-
"""
rebalancer.py -- Whale Follower Bot
Monitor de balance entre Bybit y OKX cada 6 horas.

Verifica que el capital no este demasiado concentrado en un solo exchange,
y envia alertas Telegram con recomendaciones de rebalanceo.

Solo lectura — no ejecuta transferencias automaticas.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from loguru import logger

import config

# ── Config ────────────────────────────────────────────────────────────────────
_CHECK_INTERVAL_SECS = 6 * 3600     # cada 6 horas
_TARGET_BYBIT_PCT    = 0.60         # objetivo: 60% en Bybit (opera grid + Wyckoff)
_SUGGESTED_DEV       = 0.20         # desviacion > 20% → sugerir rebalanceo
_URGENT_DEV          = 0.35         # desviacion > 35% → alerta urgente


@dataclass
class RebalanceSnapshot:
    bybit_usdt:    float = 0.0
    okx_usdt:      float = 0.0
    total_usdt:    float = 0.0
    bybit_pct:     float = 0.0
    deviation_pct: float = 0.0
    status:        str   = "ok"      # "ok" | "suggested" | "urgent"
    last_check_ts: float = 0.0


class CapitalRebalancer:
    """
    Monitorea la distribucion de capital entre Bybit y OKX.
    Emite alertas Telegram cuando el desbalance supera los umbrales.
    Solo lectura — el usuario decide cuando transferir manualmente.
    """

    def __init__(self) -> None:
        self._snap: RebalanceSnapshot = RebalanceSnapshot()
        logger.info(
            "[rebalancer] Iniciado | check={}h | target_bybit={:.0f}% | "
            "suggested_dev={:.0f}% | urgent_dev={:.0f}%",
            _CHECK_INTERVAL_SECS // 3600,
            _TARGET_BYBIT_PCT * 100,
            _SUGGESTED_DEV * 100,
            _URGENT_DEV * 100,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Loop principal — verifica balance cada 6 horas."""
        while True:
            await self._check()
            await asyncio.sleep(_CHECK_INTERVAL_SECS)

    # ── Public ────────────────────────────────────────────────────────────────

    def snapshot(self) -> RebalanceSnapshot:
        return self._snap

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _check(self) -> None:
        if not config.PRODUCTION:
            return

        bybit_bal = await self._fetch_bybit()
        okx_bal   = await self._fetch_okx()
        total     = bybit_bal + okx_bal

        if total < 5.0:
            logger.warning("[rebalancer] Balance total muy bajo (${:.2f}) — omitiendo verificacion",
                           total)
            return

        bybit_pct = bybit_bal / total
        deviation = abs(bybit_pct - _TARGET_BYBIT_PCT)

        if deviation >= _URGENT_DEV:
            status = "urgent"
        elif deviation >= _SUGGESTED_DEV:
            status = "suggested"
        else:
            status = "ok"

        self._snap = RebalanceSnapshot(
            bybit_usdt    = round(bybit_bal, 2),
            okx_usdt      = round(okx_bal, 2),
            total_usdt    = round(total, 2),
            bybit_pct     = round(bybit_pct * 100, 1),
            deviation_pct = round(deviation * 100, 1),
            status        = status,
            last_check_ts = time.time(),
        )

        logger.info(
            "[rebalancer] Bybit=${:.2f} ({:.1f}%) | OKX=${:.2f} ({:.1f}%) | "
            "Total=${:.2f} | Estado={}",
            bybit_bal, bybit_pct * 100,
            okx_bal, (1 - bybit_pct) * 100,
            total, status,
        )

        if status in ("suggested", "urgent"):
            await self._alert(self._snap)

    async def _fetch_bybit(self) -> float:
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return 0.0
        try:
            ts  = str(int(time.time() * 1000))
            msg = f"{ts}{config.BYBIT_API_KEY}5000accountType=UNIFIED&coin=USDT"
            sig = hmac.new(config.BYBIT_API_SECRET.encode(),
                           msg.encode(), hashlib.sha256).hexdigest()
            headers = {
                "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
                "X-BAPI-TIMESTAMP":   ts,
                "X-BAPI-SIGN":        sig,
                "X-BAPI-RECV-WINDOW": "5000",
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://api.bybit.com/v5/account/wallet-balance"
                    "?accountType=UNIFIED&coin=USDT",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
                    if data.get("retCode") == 0:
                        for c in data["result"]["list"][0].get("coin", []):
                            if c.get("coin") == "USDT":
                                return float(c.get("walletBalance", 0))
        except Exception as exc:
            logger.warning("[rebalancer] Bybit balance error: {}", exc)
        return 0.0

    async def _fetch_okx(self) -> float:
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return 0.0
        try:
            from datetime import datetime, timezone as _tz
            ts   = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path = "/api/v5/account/balance?ccy=USDT"
            sig  = base64.b64encode(
                hmac.new(config.OKX_SECRET.encode(),
                         (ts + "GET" + path).encode(), hashlib.sha256).digest()
            ).decode()
            headers = {
                "OK-ACCESS-KEY":        config.OKX_API_KEY,
                "OK-ACCESS-SIGN":       sig,
                "OK-ACCESS-TIMESTAMP":  ts,
                "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://www.okx.com{path}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
                    if data.get("code") == "0":
                        for d in data.get("data", [{}])[0].get("details", []):
                            if d.get("ccy") == "USDT":
                                return float(d.get("eq", 0))
        except Exception as exc:
            logger.warning("[rebalancer] OKX balance error: {}", exc)
        return 0.0

    async def _alert(self, snap: RebalanceSnapshot) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        if snap.status == "urgent":
            icon   = "🚨"
            titulo = "REBALANCEO URGENTE"
            accion = (f"Bybit tiene {snap.bybit_pct:.1f}% del capital "
                      f"(objetivo {_TARGET_BYBIT_PCT*100:.0f}%).\n"
                      f"Desviacion: {snap.deviation_pct:.1f}% > limite {_URGENT_DEV*100:.0f}%\n"
                      f"Transfiere USDT entre exchanges para equilibrar.")
        else:
            icon   = "⚠️"
            titulo = "Rebalanceo sugerido"
            accion = (f"Bybit tiene {snap.bybit_pct:.1f}% del capital "
                      f"(objetivo {_TARGET_BYBIT_PCT*100:.0f}%).\n"
                      f"Desviacion: {snap.deviation_pct:.1f}% — considera rebalancear.")

        msg = (
            f"{icon} [{titulo}]\n"
            f"Bybit:  ${snap.bybit_usdt:.2f} USDT ({snap.bybit_pct:.1f}%)\n"
            f"OKX:    ${snap.okx_usdt:.2f} USDT ({100-snap.bybit_pct:.1f}%)\n"
            f"Total:  ${snap.total_usdt:.2f} USDT\n\n"
            f"{accion}"
        )
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception:
            pass
