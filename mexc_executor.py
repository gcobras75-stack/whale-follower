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


def _make_headers(api_key: str, secret: str, body: str = "") -> Dict[str, str]:
    """MEXC Futures auth. Signature = HMAC-SHA256(api_key + timestamp + body)."""
    ts = str(int(time.time() * 1000))
    sign_payload = api_key + ts + body
    sign = hmac.new(
        secret.encode(), sign_payload.encode(), hashlib.sha256
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
            connector = aiohttp.TCPConnector(family=2)  # AF_INET = IPv4
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.get(
                    f"{_BASE_URL}/api/v1/private/account/assets",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8, connect=5),
                ) as r:
                    data = await r.json()
            if data.get("success"):
                for d in data.get("data", []):
                    if d.get("currency") == "USDT":
                        bal = float(d.get("availableBalance", 0))
                        self._last_balance = bal
                        logger.info("[mexc] Balance USDT=${:.2f}", bal)
                        return bal
            logger.warning("[mexc] Balance response: code={} data={}", data.get("code"), str(data)[:200])
        except Exception as exc:
            logger.warning("[mexc] get_balance error: {!r} type={}", exc, type(exc).__name__)
        return self._last_balance

    # ── Leverage ─────────────────────────────────────────────────────────────

    async def _ensure_leverage(self, symbol: str) -> None:
        if symbol in self._leverage_set:
            return
        try:
            body = json.dumps({
                "symbol":      symbol,
                "leverage":    1,
                "openType":    1,    # 1=isolated
                "positionType": 1,   # 1=one-way
            })
            headers = _make_headers(self._api_key, self._secret, body)
            connector = aiohttp.TCPConnector(family=2)  # AF_INET = IPv4
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.post(
                    f"{_BASE_URL}/api/v1/private/position/change_leverage",
                    headers=headers, data=body,
                    timeout=aiohttp.ClientTimeout(total=8, connect=5),
                ) as r:
                    data = await r.json()
            if data.get("success"):
                self._leverage_set.add(symbol)
                logger.info("[mexc] Leverage 1x configurado para {}", symbol)
            else:
                logger.warning("[mexc] set_leverage {} error: {}", symbol, str(data)[:200])
        except Exception as exc:
            logger.warning("[mexc] _ensure_leverage error: {!r}", exc)

    # ── Market order ─────────────────────────────────────────────────────────

    async def market_order(
        self, pair: str, side: str, size_usd: float, price_hint: float
    ) -> Optional[Dict[str, Any]]:
        """Place market order on MEXC futures. Returns {"orderId": ...} or None."""
        logger.info(
            "[mexc] market_order called: pair={} side={} size=${:.0f} price={:.2f} enabled={}",
            pair, side, size_usd, price_hint, self._enabled,
        )
        if not self._enabled:
            logger.warning("[mexc] ABORT: executor deshabilitado (sin API keys)")
            return None

        symbol = _PAIR_TO_SYMBOL.get(pair)
        if not symbol:
            logger.warning("[mexc] ABORT: par desconocido '{}' — pares válidos: {}",
                           pair, list(_PAIR_TO_SYMBOL.keys()))
            return None

        # Sizing dinámico
        try:
            bal = await asyncio.wait_for(self.get_balance(), timeout=5.0)
        except Exception as exc:
            logger.warning("[mexc] get_balance timeout/error: {!r} — usando cache ${:.0f}",
                           exc, self._last_balance)
            bal = self._last_balance

        libre = bal - _WYCKOFF_RESERVE
        logger.info("[mexc] Capital: bal=${:.0f} reserva=${:.0f} libre=${:.0f}",
                    bal, _WYCKOFF_RESERVE, libre)
        if libre < _MIN_MARGIN_FREE:
            logger.warning("[mexc] ABORT: margen libre ${:.0f} < mínimo ${:.0f}",
                           libre, _MIN_MARGIN_FREE)
            return None

        max_order = max(50.0, bal * 0.15)
        size_usd = min(size_usd, libre * _MAX_ORDER_FRACTION, max_order)
        size_usd = max(10.0, size_usd)

        # Calcular contratos
        ct_val = _CT_VAL.get(symbol, 1.0)
        if price_hint <= 0:
            logger.warning("[mexc] ABORT: price_hint={} inválido", price_hint)
            return None
        usd_per_ct = ct_val * price_hint
        if usd_per_ct > size_usd:
            logger.warning("[mexc] ABORT: {} contrato=${:.2f} (ct_val={} × price={:.2f}) > size=${:.0f}",
                           symbol, usd_per_ct, ct_val, price_hint, size_usd)
            return None
        n_contracts = max(1, int(size_usd / usd_per_ct))

        logger.info("[mexc] Sizing OK: {} {} {} contratos × ${:.2f} = ~${:.0f}",
                    side.upper(), symbol, n_contracts, usd_per_ct, n_contracts * usd_per_ct)

        await self._ensure_leverage(symbol)

        # MEXC futures order: side 1=open_long, 2=close_short, 3=open_short, 4=close_long
        side_code = 1 if side.lower() == "buy" else 3
        order_params = {
            "symbol":    symbol,
            "vol":       n_contracts,
            "side":      side_code,
            "type":      5,          # 5 = market order
            "openType":  1,          # 1 = isolated
            "leverage":  1,
        }
        body = json.dumps(order_params)
        logger.info("[mexc] Enviando orden: {}", order_params)

        try:
            headers = _make_headers(self._api_key, self._secret, body)
            url = f"{_BASE_URL}/api/v1/private/order/submit"
            logger.info("[mexc] POST {} timeout=10s", url)

            data = None
            connector = aiohttp.TCPConnector(family=2)  # AF_INET = IPv4 only
            async with aiohttp.ClientSession(connector=connector) as s:
                async with s.post(
                    url, headers=headers, data=body,
                    timeout=aiohttp.ClientTimeout(total=10, connect=5),
                ) as r:
                    raw_text = await r.text()
                    http_status = r.status
                    logger.info("[mexc] HTTP {} response: {}", http_status, raw_text[:500])
                    try:
                        data = json.loads(raw_text)
                    except Exception:
                        logger.error("[mexc] ABORT: response no es JSON (HTTP {}): {}",
                                     http_status, raw_text[:300])
                        return None

            if data is None:
                logger.error("[mexc] ABORT: data es None después de request")
                return None

            if data.get("success"):
                order_id = data.get("data", "")
                logger.info("[mexc] ORDER OK {} {} contratos orderId={}",
                            symbol, n_contracts, order_id)

                try:
                    import db_writer
                    asyncio.create_task(db_writer.save_real_trade({
                        "trade_id":    order_id,
                        "pair":        pair,
                        "side":        "Buy",
                        "entry_price": price_hint,
                        "size_usd":    round(size_usd, 2),
                        "source":      "real_mexc",
                    }))
                except Exception:
                    pass

                return {"orderId": order_id, "pair": pair, "symbol": symbol}
            else:
                logger.error(
                    "[mexc] ORDER REJECTED: symbol={} success={} code={} "
                    "message='{}' full={}",
                    symbol, data.get("success"), data.get("code"),
                    data.get("message", data.get("msg", "")),
                    str(data)[:400],
                )
                return None
        except asyncio.TimeoutError:
            logger.error("[mexc] ORDER TIMEOUT: no response from MEXC in 10s (symbol={})", symbol)
            return None
        except aiohttp.ClientError as exc:
            logger.error("[mexc] ORDER NETWORK ERROR: {!r} type={}", exc, type(exc).__name__)
            return None
        except Exception as exc:
            logger.error("[mexc] ORDER EXCEPTION: {!r} type={}", exc, type(exc).__name__)
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
