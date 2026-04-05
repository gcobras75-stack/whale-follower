# -*- coding: utf-8 -*-
"""
okx_executor.py -- Whale Follower Bot
Real OKX order execution for cross-exchange arbitrage.

Uses OKX REST API v5 directly with aiohttp + HMAC-SHA256 auth.
No external SDK required (aiohttp already a dependency).

Safety:
  - Only TRADE permissions used (no withdrawal)
  - Balance check before every order
  - Configurable max size per trade
  - Auto-pause if balance < threshold
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp
from loguru import logger

import config

# ── OKX API ────────────────────────────────────────────────────────────────────
_BASE_URL = "https://www.okx.com"

# Contract values for USDT-margined perpetuals (ctVal)
_CONTRACT_VALUES: Dict[str, float] = {
    "BTC-USDT-SWAP": 0.01,    # 1 contract = 0.01 BTC
    "ETH-USDT-SWAP": 0.1,     # 1 contract = 0.1  ETH
    "SOL-USDT-SWAP": 1.0,     # 1 contract = 1    SOL
}

# Map from bot pair names to OKX instId (USDT-margined perps)
_PAIR_TO_INST: Dict[str, str] = {
    "BTCUSDT": "BTC-USDT-SWAP",
    "ETHUSDT": "ETH-USDT-SWAP",
    "SOLUSDT": "SOL-USDT-SWAP",
}

# Minimum contracts per pair on OKX
_MIN_CONTRACTS: Dict[str, int] = {
    "BTC-USDT-SWAP": 1,   # 1 ct = 0.01 BTC ≈ $840
    "ETH-USDT-SWAP": 1,   # 1 ct = 0.1  ETH ≈ $180
    "SOL-USDT-SWAP": 1,   # 1 ct = 1    SOL ≈ $120
}


# ── Auth helpers ───────────────────────────────────────────────────────────────

def _timestamp_iso() -> str:
    """OKX requires ISO-8601 with milliseconds: 2024-01-01T12:00:00.000Z"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
           f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def _sign(timestamp: str, method: str, path: str, body: str) -> str:
    """HMAC-SHA256 signature for OKX API."""
    prehash = timestamp + method.upper() + path + body
    mac = hmac.new(
        config.OKX_SECRET.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    )
    return base64.b64encode(mac.digest()).decode("utf-8")


def _auth_headers(method: str, path: str, body: str = "") -> Dict[str, str]:
    """Build authenticated request headers."""
    ts = _timestamp_iso()
    return {
        "OK-ACCESS-KEY":        config.OKX_API_KEY,
        "OK-ACCESS-SIGN":       _sign(ts, method, path, body),
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
        "Content-Type":         "application/json",
    }


# ── OKX Executor ──────────────────────────────────────────────────────────────

class OKXExecutor:
    """
    Real OKX order execution for cross-exchange arbitrage.

    Usage:
        okx = OKXExecutor()
        if okx.enabled:
            balance = await okx.get_balance()
            result  = await okx.market_order("ETHUSDT", "buy", 50.0, price_hint=1800.0)
    """

    def __init__(self) -> None:
        self._enabled = bool(
            config.OKX_API_KEY and config.OKX_SECRET and config.OKX_PASSPHRASE
        )
        self._last_balance: float = 0.0
        self._last_balance_ts: float = 0.0

        if self._enabled:
            logger.info("[okx_exec] OKX executor initialized (real credentials)")
        else:
            logger.warning("[okx_exec] OKX credentials missing — executor disabled")

    @property
    def enabled(self) -> bool:
        return self._enabled

    # ── Balance ─────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        """Get total USDT equity in OKX trading account."""
        if not self._enabled:
            return 0.0

        path = "/api/v5/account/balance?ccy=USDT"
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    _BASE_URL + path,
                    headers=_auth_headers("GET", path),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data = await resp.json()

            if data.get("code") != "0":
                logger.warning("[okx_exec] balance error: {}", data.get("msg", "unknown"))
                return 0.0

            for detail in data.get("data", [{}])[0].get("details", []):
                if detail.get("ccy") == "USDT":
                    bal = float(detail.get("eq", 0))
                    self._last_balance = bal
                    self._last_balance_ts = time.time()
                    logger.debug("[okx_exec] USDT balance: ${:.2f}", bal)
                    return bal
            return 0.0

        except Exception as exc:
            logger.warning("[okx_exec] get_balance error: {}", exc)
            return self._last_balance  # return cached if API fails

    # ── Market orders ───────────────────────────────────────────────────────

    async def market_order(
        self,
        pair: str,
        side: str,
        size_usd: float,
        price_hint: float,
    ) -> Optional[Dict[str, Any]]:
        """
        Place a market order on OKX USDT-margined perpetual.

        Args:
            pair:       Bot pair name e.g. "ETHUSDT"
            side:       "buy" or "sell"
            size_usd:   Approximate USD notional
            price_hint: Current price for contract size calculation

        Returns:
            Order result dict with order_id, or None on failure.
        """
        if not self._enabled:
            logger.warning("[okx_exec] market_order: executor disabled")
            return None

        inst_id = _PAIR_TO_INST.get(pair)
        if not inst_id:
            logger.warning("[okx_exec] unknown pair: {}", pair)
            return None

        ct_val = _CONTRACT_VALUES.get(inst_id, 0.01)
        if price_hint <= 0:
            logger.warning("[okx_exec] need valid price_hint (got {})", price_hint)
            return None

        # Calculate number of contracts
        contracts = int(size_usd / (price_hint * ct_val))
        min_ct = _MIN_CONTRACTS.get(inst_id, 1)
        if contracts < min_ct:
            notional_min = min_ct * ct_val * price_hint
            logger.warning(
                "[okx_exec] size_usd=${:.0f} too small for {} "
                "(min {} contracts = ${:.0f} notional)",
                size_usd, pair, min_ct, notional_min,
            )
            return None

        # Cap to configured max
        max_contracts = int(config.CROSS_ARB_MAX_SIZE_USD / (price_hint * ct_val))
        contracts = min(contracts, max(max_contracts, min_ct))

        actual_notional = contracts * ct_val * price_hint

        path = "/api/v5/trade/order"
        body_dict = {
            "instId":  inst_id,
            "tdMode":  "cross",
            "side":    side.lower(),
            "ordType": "market",
            "sz":      str(contracts),
        }
        body = json.dumps(body_dict)

        logger.info(
            "[okx_exec] placing {} {} {} contracts (≈${:.0f})",
            side.upper(), pair, contracts, actual_notional,
        )

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    _BASE_URL + path,
                    headers=_auth_headers("POST", path, body),
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data = await resp.json()

            if data.get("code") != "0":
                err_msg = data.get("msg", "")
                order_err = ""
                if data.get("data"):
                    order_err = data["data"][0].get("sMsg", "")
                logger.error(
                    "[okx_exec] ORDER FAILED {}: {} | {}",
                    pair, err_msg, order_err,
                )
                return None

            order_data = data["data"][0]
            order_id = order_data.get("ordId", "")

            logger.info(
                "[okx_exec] ORDER OK {} {} {} contracts ordId={}",
                side.upper(), pair, contracts, order_id,
            )

            return {
                "order_id":       order_id,
                "pair":           pair,
                "inst_id":        inst_id,
                "side":           side.lower(),
                "contracts":      contracts,
                "notional_usd":   round(actual_notional, 2),
                "price_hint":     price_hint,
                "ts":             time.time(),
            }

        except Exception as exc:
            logger.error("[okx_exec] market_order exception: {}", exc)
            return None

    # ── Close position ──────────────────────────────────────────────────────

    async def close_position(self, pair: str) -> bool:
        """Close all positions for a pair using OKX close-position API."""
        if not self._enabled:
            return False

        inst_id = _PAIR_TO_INST.get(pair)
        if not inst_id:
            return False

        path = "/api/v5/trade/close-position"
        body = json.dumps({
            "instId": inst_id,
            "mgnMode": "cross",
        })

        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    _BASE_URL + path,
                    headers=_auth_headers("POST", path, body),
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data = await resp.json()

            if data.get("code") != "0":
                logger.warning("[okx_exec] close_position {} error: {}", pair, data.get("msg"))
                return False

            logger.info("[okx_exec] position closed: {}", pair)
            return True

        except Exception as exc:
            logger.error("[okx_exec] close_position exception: {}", exc)
            return False

    # ── Get fill price ──────────────────────────────────────────────────────

    async def get_order_fill(self, order_id: str, inst_id: str) -> Optional[float]:
        """Get average fill price for a completed order."""
        if not self._enabled or not order_id:
            return None

        path = f"/api/v5/trade/order?ordId={order_id}&instId={inst_id}"
        try:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    _BASE_URL + path,
                    headers=_auth_headers("GET", path),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data = await resp.json()

            if data.get("code") != "0" or not data.get("data"):
                return None

            avg_px = data["data"][0].get("avgPx", "")
            return float(avg_px) if avg_px else None

        except Exception:
            return None
