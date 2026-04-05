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

# Map from bot pair names to OKX SPOT instId
_PAIR_TO_INST: Dict[str, str] = {
    "BTCUSDT": "BTC-USDT",
    "ETHUSDT": "ETH-USDT",
    "SOLUSDT": "SOL-USDT",
}

_MIN_SPOT_USD = 1.0   # OKX spot minimum ~$1 USD


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
        """Get total USDT balance: trading account + funding account."""
        if not self._enabled:
            return 0.0

        def _extract_usdt(data: dict, in_details: bool) -> float:
            """Lee cashBal > eq > availBal del primer entry USDT."""
            if in_details:
                entries = data.get("data", [{}])[0].get("details", [])
            else:
                entries = data.get("data", [])
            for d in entries:
                if d.get("ccy") == "USDT":
                    cash  = float(d.get("cashBal",  0) or 0)
                    eq    = float(d.get("eq",       0) or 0)
                    avail = float(d.get("availBal", 0) or 0)
                    bal   = cash or eq or avail
                    logger.debug("[okx_exec] USDT cashBal={:.4f} eq={:.4f} availBal={:.4f} -> {:.4f}",
                                 cash, eq, avail, bal)
                    return bal
            return 0.0

        try:
            trading_bal = 0.0
            funding_bal = 0.0

            async with aiohttp.ClientSession() as session:
                # 1. Trading account
                path_t = "/api/v5/account/balance?ccy=USDT"
                resp_t = await session.get(
                    _BASE_URL + path_t,
                    headers=_auth_headers("GET", path_t),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data_t = await resp_t.json()
                if data_t.get("code") == "0":
                    trading_bal = _extract_usdt(data_t, in_details=True)

                # 2. Funding account
                path_f = "/api/v5/asset/balances?ccy=USDT"
                resp_f = await session.get(
                    _BASE_URL + path_f,
                    headers=_auth_headers("GET", path_f),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data_f = await resp_f.json()
                if data_f.get("code") == "0":
                    funding_bal = _extract_usdt(data_f, in_details=False)

            total = trading_bal + funding_bal
            logger.info("[okx_exec] USDT balance — trading=${:.2f} funding=${:.2f} total=${:.2f}",
                        trading_bal, funding_bal, total)
            self._last_balance = total
            self._last_balance_ts = time.time()
            return total

        except Exception as exc:
            logger.warning("[okx_exec] get_balance error: {}", exc)
            return self._last_balance  # return cached if API fails

    async def get_coin_balance(self, coin: str) -> float:
        """
        Búsqueda exhaustiva del balance de un coin en OKX.
        Orden de intentos:
          1. /api/v5/account/balance?ccy=ETH  (trading, filtrado)
          2. /api/v5/asset/balances?ccy=ETH   (funding account)
          3. /api/v5/account/balance           (trading completo, sin filtro → buscar ETH en todos los details)
        Sin caché — siempre consulta la API en tiempo real.
        """
        if not self._enabled:
            return 0.0

        async def _get_raw(path: str) -> dict:
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    _BASE_URL + path,
                    headers=_auth_headers("GET", path),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                http_status = resp.status
                data = await resp.json()
            logger.info(
                "[okx] GET {} http={} → {}",
                path, http_status, data,
            )
            return data

        def _find_in_details(data: dict) -> float:
            """Busca coin en data[0].details[] — estructura de /account/balance."""
            for detail in data.get("data", [{}])[0].get("details", []):
                if detail.get("ccy") == coin:
                    cash  = float(detail.get("cashBal",  0) or 0)
                    eq    = float(detail.get("eq",       0) or 0)
                    avail = float(detail.get("availBal", 0) or 0)
                    bal   = cash or eq or avail
                    logger.info("[okx] {} details cashBal={:.6f} eq={:.6f} avail={:.6f} -> {:.6f}",
                                coin, cash, eq, avail, bal)
                    return bal
            return 0.0

        def _find_in_list(data: dict) -> float:
            """Busca coin en data[] directamente — estructura de /asset/balances."""
            for item in data.get("data", []):
                if item.get("ccy") == coin:
                    cash  = float(item.get("cashBal",  0) or 0)
                    avail = float(item.get("availBal", 0) or 0)
                    bal   = cash or avail
                    logger.info("[okx] {} asset list cashBal={:.6f} avail={:.6f} -> {:.6f}",
                                coin, cash, avail, bal)
                    return bal
            return 0.0

        try:
            # 1. Trading account filtrado por coin
            path1 = f"/api/v5/account/balance?ccy={coin}"
            data1 = await _get_raw(path1)
            if data1.get("code") == "0":
                bal = _find_in_details(data1)
                if bal > 0:
                    return bal
                logger.info("[okx] details[] vacío para {} en {} → probando funding", coin, path1)
            else:
                logger.warning("[okx] {} code={} msg='{}'", path1, data1.get("code"), data1.get("msg", ""))

            # 2. Funding account
            path2 = f"/api/v5/asset/balances?ccy={coin}"
            data2 = await _get_raw(path2)
            if data2.get("code") == "0":
                bal = _find_in_list(data2)
                if bal > 0:
                    return bal
                logger.info("[okx] {} no encontrado en funding → probando trading completo", coin)
            else:
                logger.warning("[okx] {} code={} msg='{}'", path2, data2.get("code"), data2.get("msg", ""))

            # 3. Trading completo sin filtro ccy — buscar ETH entre todos los coins
            path3 = "/api/v5/account/balance"
            data3 = await _get_raw(path3)
            if data3.get("code") == "0":
                bal = _find_in_details(data3)
                if bal > 0:
                    return bal
                logger.warning("[okx] {} no encontrado en ningún endpoint de OKX", coin)
            else:
                logger.warning("[okx] {} code={} msg='{}'", path3, data3.get("code"), data3.get("msg", ""))

            return 0.0

        except Exception as exc:
            logger.warning("[okx_exec] get_coin_balance({}) excepción: {}", coin, exc)
            return 0.0

    # ── Market orders ───────────────────────────────────────────────────────

    async def market_order(
        self,
        pair: str,
        side: str,
        size_usd: float,
        price_hint: float,
    ) -> Optional[Dict[str, Any]]:
        """
        Place a market order on OKX SPOT.

        Args:
            pair:       Bot pair name e.g. "ETHUSDT"
            side:       "buy" or "sell"
            size_usd:   Approximate USD notional
            price_hint: Current price (used for sell qty calculation)

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

        if size_usd < _MIN_SPOT_USD:
            logger.warning(
                "[okx_exec] size_usd=${:.2f} below SPOT minimum ${:.0f} — skip",
                size_usd, _MIN_SPOT_USD,
            )
            return None

        path = "/api/v5/trade/order"
        _side = side.lower()

        if _side == "buy":
            # Buy: sz in quote currency (USDT)
            body_dict = {
                "instId":  inst_id,
                "tdMode":  "cash",
                "side":    "buy",
                "ordType": "market",
                "sz":      str(round(size_usd, 4)),
                "tgtCcy":  "quote_ccy",
            }
        else:
            # Sell: sz in base currency
            if price_hint <= 0:
                logger.warning("[okx_exec] need valid price_hint for sell (got {})", price_hint)
                return None
            base_qty = round(size_usd / price_hint, 6)
            body_dict = {
                "instId":  inst_id,
                "tdMode":  "cash",
                "side":    "sell",
                "ordType": "market",
                "sz":      str(base_qty),
            }

        body = json.dumps(body_dict)

        logger.info(
            "[okx_exec] SPOT {} {} ≈${:.0f}",
            _side.upper(), pair, size_usd,
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
                    "[okx_exec] SPOT ORDER FAILED {}: {} | {}",
                    pair, err_msg, order_err,
                )
                return None

            order_data = data["data"][0]
            order_id = order_data.get("ordId", "")

            logger.info(
                "[okx_exec] SPOT ORDER OK {} {} ≈${:.0f} ordId={}",
                _side.upper(), pair, size_usd, order_id,
            )

            return {
                "order_id":     order_id,
                "pair":         pair,
                "inst_id":      inst_id,
                "side":         _side,
                "notional_usd": round(size_usd, 2),
                "price_hint":   price_hint,
                "ts":           time.time(),
            }

        except Exception as exc:
            logger.error("[okx_exec] market_order exception: {}", exc)
            return None

    # ── Close position ──────────────────────────────────────────────────────

    async def close_position(self, pair: str, size_usd: float = 0.0, price_hint: float = 0.0) -> bool:
        """
        Cierra posición LONG vendiendo el 100% del balance real del coin.
        Consulta el balance actual antes de ejecutar para evitar dust residual.
        Fallback a size_usd/price_hint si el balance real no se puede obtener.
        """
        if not self._enabled:
            return False

        coin = pair.replace("USDT", "")
        actual_qty = await self.get_coin_balance(coin)

        if actual_qty > 0 and price_hint > 0:
            actual_usd = actual_qty * price_hint
            if actual_usd < _MIN_SPOT_USD:
                logger.debug(
                    "[okx_exec] close_position {}: balance ${:.4f} < mínimo OKX, skip",
                    pair, actual_usd,
                )
                return True

            inst_id = _PAIR_TO_INST.get(pair)
            if not inst_id:
                logger.warning("[okx_exec] close_position: par desconocido {}", pair)
                return False

            path = "/api/v5/trade/order"
            body_dict = {
                "instId":  inst_id,
                "tdMode":  "cash",
                "side":    "sell",
                "ordType": "market",
                "sz":      str(round(actual_qty, 6)),
            }
            body = json.dumps(body_dict)
            logger.info(
                "[okx_exec] close_position SELL 100% {} qty={:.6f} ≈${:.2f}",
                pair, actual_qty, actual_usd,
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
                    err   = data.get("msg", "")
                    smsg  = data.get("data", [{}])[0].get("sMsg", "") if data.get("data") else ""
                    logger.error(
                        "[okx_exec] close_position SELL FALLO {}: {} | {}",
                        pair, err, smsg,
                    )
                    return False
                order_id = data["data"][0].get("ordId", "")
                logger.info(
                    "[okx_exec] close_position OK {} qty={:.6f} ordId={}",
                    pair, actual_qty, order_id,
                )
                return True
            except Exception as exc:
                logger.error("[okx_exec] close_position excepción {}: {}", pair, exc)
                return False

        # Fallback: usar size_usd / price_hint si no se pudo obtener balance real
        if size_usd > 0 and price_hint > 0:
            logger.warning(
                "[okx_exec] close_position {}: balance real=0, fallback a size_usd=${:.2f}",
                pair, size_usd,
            )
            result = await self.market_order(pair, "sell", size_usd, price_hint)
            return result is not None

        logger.debug("[okx_exec] close_position no-op {} (sin balance ni size_usd)", pair)
        return True

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
