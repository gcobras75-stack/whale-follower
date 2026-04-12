# -*- coding: utf-8 -*-
"""
mexc_executor.py — Whale Follower Bot
MEXC Futures executor para trades Wyckoff (fallback después de OKX).

API: https://contract.mexc.com/api/v1
Auth: ApiKey + Request-Time + HMAC-SHA256(api_key + timestamp)
Modo: Isolated margin, leverage 1x
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

import config

_BASE_URL = "https://contract.mexc.com"

# Par bot → símbolo MEXC futuros
_PAIR_TO_SYMBOL: Dict[str, str] = {
    "BTCUSDT":  "BTC_USDT",
    "ETHUSDT":  "ETH_USDT",
    "SOLUSDT":  "SOL_USDT",
    "BNBUSDT":  "BNB_USDT",
    "DOGEUSDT": "DOGE_USDT",
    "XRPUSDT":  "XRP_USDT",
    "ADAUSDT":  "ADA_USDT",
    "AVAXUSDT": "AVAX_USDT",
    "LINKUSDT": "LINK_USDT",
}

# Tamaño mínimo de contrato por par (en moneda base)
_CT_VAL: Dict[str, float] = {
    "BTC_USDT":  0.001,
    "ETH_USDT":  0.01,
    "SOL_USDT":  0.1,
    "BNB_USDT":  0.01,
    "DOGE_USDT": 100.0,
    "XRP_USDT":  10.0,
    "ADA_USDT":  10.0,
    "AVAX_USDT": 0.1,
    "LINK_USDT": 1.0,
}

_WYCKOFF_RESERVE    = 30.0
_MAX_ORDER_FRACTION = 0.30
_MIN_MARGIN_FREE    = 10.0


def _make_headers(api_key: str, secret: str) -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    sign = hmac.new(
        secret.encode(), (api_key + ts).encode(), hashlib.sha256
    ).hexdigest()
    return {
        "ApiKey":       api_key,
        "Request-Time": ts,
        "Signature":    sign,
        "Content-Type": "application/json",
    }


class MEXCExecutor:
    """MEXC Futures executor — isolated margin, leverage 1x."""

    def __init__(self) -> None:
        self._api_key = config.GATE_API_KEY if hasattr(config, "MEXC_API_KEY") and config.MEXC_API_KEY else getattr(config, "MEXC_API_KEY", "")
        self._secret  = config.GATE_API_SECRET if hasattr(config, "MEXC_API_SECRET") and config.MEXC_API_SECRET else getattr(config, "MEXC_API_SECRET", "")
        # Use dedicated MEXC vars
        self._api_key = getattr(config, "MEXC_API_KEY", "")
        self._secret  = getattr(config, "MEXC_API_SECRET", "")
        self._enabled = bool(self._api_key and self._secret)
        self._last_balance: float = 0.0
        self._leverage_set: set = set()

        if self._enabled:
            logger.info("[mexc] Executor iniciado (${:.0f} API key configurada)", len(self._api_key))
        else:
            logger.warning("[mexc] Credenciales MEXC no configuradas — deshabilitado")

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ── Balance ──────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        if not self._enabled:
            return 0.0
        try:
            headers = _make_headers(self._api_key, self._secret)
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{_BASE_URL}/api/v1/private/account/assets",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
            if data.get("success"):
                for d in data.get("data", []):
                    if d.get("currency") == "USDT":
                        bal = float(d.get("availableBalance", 0))
                        self._last_balance = bal
                        logger.info("[mexc] Balance USDT=${:.2f}", bal)
                        return bal
            logger.warning("[mexc] Balance response: {}", data.get("code"))
        except Exception as exc:
            logger.warning("[mexc] get_balance error: {}", exc)
        return self._last_balance

    # ── Leverage ─────────────────────────────────────────────────────────────

    async def _ensure_leverage(self, symbol: str) -> None:
        if symbol in self._leverage_set:
            return
        try:
            headers = _make_headers(self._api_key, self._secret)
            body = json.dumps({
                "symbol":      symbol,
                "leverage":    1,
                "openType":    2,    # 1=isolated, 2=cross — MEXC requiere probar
                "positionType": 1,   # 1=one-way
            })
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{_BASE_URL}/api/v1/private/position/change_leverage",
                    headers=headers, data=body,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
            if data.get("success"):
                self._leverage_set.add(symbol)
                logger.info("[mexc] Leverage 1x configurado para {}", symbol)
            else:
                logger.warning("[mexc] set_leverage {} error: {}", symbol, data)
        except Exception as exc:
            logger.warning("[mexc] _ensure_leverage error: {}", exc)

    # ── Market order ─────────────────────────────────────────────────────────

    async def market_order(
        self, pair: str, side: str, size_usd: float, price_hint: float
    ) -> Optional[Dict[str, Any]]:
        """Place market order on MEXC futures. Returns {"orderId": ...} or None."""
        if not self._enabled:
            return None

        symbol = _PAIR_TO_SYMBOL.get(pair)
        if not symbol:
            logger.warning("[mexc] Par desconocido: {}", pair)
            return None

        # Sizing dinámico
        try:
            bal = await asyncio.wait_for(self.get_balance(), timeout=5.0)
        except Exception:
            bal = self._last_balance

        libre = bal - _WYCKOFF_RESERVE
        if libre < _MIN_MARGIN_FREE:
            logger.warning("[mexc] Margen insuficiente: ${:.0f} libre < ${:.0f} min",
                           libre, _MIN_MARGIN_FREE)
            return None

        max_order = max(50.0, bal * 0.15)
        size_usd = min(size_usd, libre * _MAX_ORDER_FRACTION, max_order)
        size_usd = max(10.0, size_usd)

        # Calcular contratos
        ct_val = _CT_VAL.get(symbol, 1.0)
        if price_hint <= 0:
            return None
        usd_per_ct = ct_val * price_hint
        if usd_per_ct > size_usd:
            logger.warning("[mexc] {} contrato=${:.0f} > size=${:.0f} — saltando",
                           symbol, usd_per_ct, size_usd)
            return None
        n_contracts = max(1, int(size_usd / usd_per_ct))

        logger.info("[mexc] Orden: {} {} {} contratos ~${:.0f}",
                    side.upper(), symbol, n_contracts, n_contracts * usd_per_ct)

        await self._ensure_leverage(symbol)

        # MEXC futures order: side 1=open_long, 2=close_short, 3=open_short, 4=close_long
        side_code = 1 if side.lower() == "buy" else 3
        body = json.dumps({
            "symbol":    symbol,
            "price":     str(round(price_hint, 2)),
            "vol":       str(n_contracts),
            "side":      side_code,
            "type":      5,          # 5 = market order
            "openType":  1,          # 1 = isolated
            "leverage":  1,
        })

        try:
            headers = _make_headers(self._api_key, self._secret)
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{_BASE_URL}/api/v1/private/order/submit",
                    headers=headers, data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json()

            if data.get("success"):
                order_id = data.get("data", "")
                logger.info("[mexc] ORDER OK {} {} contratos orderId={}",
                            symbol, n_contracts, order_id)
                return {"orderId": order_id, "pair": pair, "symbol": symbol}
            else:
                logger.error("[mexc] ORDER FAILED {}: code={} msg={}",
                             symbol, data.get("code"), data.get("message", data.get("msg")))
                return None
        except Exception as exc:
            logger.error("[mexc] market_order error: {}", exc)
            return None

    # ── Par alternativo ──────────────────────────────────────────────────────

    def find_affordable_pair(self, size_usd: float, prices: Dict[str, float],
                             original_pair: str = "") -> Optional[str]:
        candidates = []
        for pair, symbol in _PAIR_TO_SYMBOL.items():
            if pair == original_pair:
                continue
            price = prices.get(pair, 0)
            if price <= 0:
                continue
            ct_val = _CT_VAL.get(symbol, 1.0)
            usd_per_ct = ct_val * price
            if usd_per_ct <= size_usd:
                candidates.append((pair, usd_per_ct))
        if not candidates:
            return None
        candidates.sort(key=lambda x: -x[1])
        return candidates[0][0]
