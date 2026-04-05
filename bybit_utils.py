"""
bybit_utils.py — Utilidades compartidas para llamadas a Bybit API (aiohttp directo a api.bytick.com).
Sin dependencia de pybit SDK para evitar problemas de compatibilidad de versiones.

Centraliza la lógica de fetch de balance para evitar duplicación
y garantizar UNIFIED→SPOT fallback + logging correcto en todos los módulos.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional

import aiohttp
from loguru import logger

import config

_BYBIT_BASE   = "https://api.bytick.com"
_RECV_WINDOW  = "5000"
_BYBIT_HEADERS_EXTRA = {
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://www.bybit.com",
}


def _make_bybit_get_headers(query: str) -> dict:
    """Genera headers de autenticación para GET requests a Bybit v5."""
    ts  = str(int(time.time() * 1000))
    msg = f"{ts}{config.BYBIT_API_KEY}{_RECV_WINDOW}{query}"
    sig = hmac.new(
        config.BYBIT_API_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
        **_BYBIT_HEADERS_EXTRA,
    }


async def fetch_usdt_balance(caller: str = "bybit") -> float:
    """
    Lee balance USDT en Bybit con fallback UNIFIED → SPOT → CONTRACT.

    - Si retCode != 0 → loggea el error real en lugar de fallar silenciosamente.
    - Si UNIFIED=0 → intenta SPOT.
    - Si SPOT=0   → intenta CONTRACT.
    - Retorna el primer valor > 0 encontrado, o 0.0 si ninguno tiene fondos.
    - Retorna None si hubo error de conexión (distingue de balance legítimamente $0).
    """
    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        return 0.0

    try:
        async with aiohttp.ClientSession() as s:
            for acct_type in ("UNIFIED", "SPOT", "CONTRACT"):
                query = f"accountType={acct_type}&coin=USDT"
                headers = _make_bybit_get_headers(query)
                url = f"{_BYBIT_BASE}/v5/account/wallet-balance?{query}"

                async with s.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        logger.warning(
                            "[{}] Bybit balance HTTP {} para {} — continuando",
                            caller, r.status, acct_type,
                        )
                        continue

                    data = await r.json()
                    ret_code = data.get("retCode", -1)

                    if ret_code != 0:
                        logger.warning(
                            "[{}] Bybit balance retCode={} msg='{}' para {} — continuando",
                            caller, ret_code, data.get("retMsg", ""), acct_type,
                        )
                        continue

                    accounts = data.get("result", {}).get("list", [])
                    if not accounts:
                        logger.debug("[{}] Bybit {} sin cuentas en result.list", caller, acct_type)
                        continue

                    # UNIFIED: buscar en coin list
                    for c in accounts[0].get("coin", []):
                        if c.get("coin") == "USDT":
                            bal = float(c.get("walletBalance", 0) or 0)
                            if bal > 0:
                                logger.info(
                                    "[{}] Balance Bybit {}=${:.2f} ✅",
                                    caller, acct_type, bal,
                                )
                                return bal

                    # Intentar totalEquity como fallback si coin list vacío
                    eq = float(accounts[0].get("totalEquity", 0) or 0)
                    if eq > 0:
                        logger.info(
                            "[{}] Balance Bybit {} (equity)=${:.2f} ✅",
                            caller, acct_type, eq,
                        )
                        return eq

                    logger.info(
                        "[{}] Bybit {}=$0 → intentando siguiente tipo de cuenta",
                        caller, acct_type,
                    )

        logger.warning(
            "[{}] Bybit balance=$0 en UNIFIED/SPOT/CONTRACT — "
            "¿cuenta vacía o tipo de cuenta no soportado?",
            caller,
        )
        return 0.0

    except Exception as exc:
        logger.warning("[{}] fetch_usdt_balance error de conexión: {}", caller, exc)
        return 0.0


async def place_spot_order(
    symbol: str,
    side: str,
    qty: float,
    caller: str = "bybit",
) -> dict:
    """
    Coloca una orden de mercado spot en Bybit via aiohttp directo a api.bytick.com.
    side: "Buy" o "Sell"
    Retorna el response dict completo, o {} en caso de error.
    """
    import json

    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        logger.error("[{}] place_spot_order: keys vacías", caller)
        return {}

    url = f"{_BYBIT_BASE}/v5/order/create"
    body = {
        "category":  "spot",
        "symbol":    symbol,
        "side":      side,
        "orderType": "Market",
        "qty":       str(qty),
    }
    body_str = json.dumps(body, separators=(",", ":"))
    ts  = str(int(time.time() * 1000))
    # HMAC-SHA256: timestamp + api_key + recv_window + body_json
    msg = f"{ts}{config.BYBIT_API_KEY}{_RECV_WINDOW}{body_str}"
    sig = hmac.new(
        config.BYBIT_API_SECRET.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
        "Content-Type":       "application/json",
        **_BYBIT_HEADERS_EXTRA,
    }

    logger.info(
        "[{}] → Bybit ORDER REQUEST {} {} qty={} | url={} | body={}",
        caller, side, symbol, qty, url, body_str,
    )

    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                url, headers=headers, data=body_str,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                http_status = r.status
                data = await r.json()
                ret_code = data.get("retCode", -1)
                ret_msg  = data.get("retMsg", "")

                logger.info(
                    "[{}] ← Bybit ORDER RESPONSE http={} retCode={} retMsg='{}' | full={}",
                    caller, http_status, ret_code, ret_msg, data,
                )

                if ret_code == 0:
                    order_id = data.get("result", {}).get("orderId", "")
                    logger.info(
                        "[{}] Bybit spot {} {} qty={} orderId={} ✅",
                        caller, side, symbol, qty, order_id,
                    )
                else:
                    logger.error(
                        "[{}] ORDER FAILED {} {} qty={} — retCode={} msg='{}'",
                        caller, side, symbol, qty, ret_code, ret_msg,
                    )
                return data
    except Exception as exc:
        logger.error(
            "[{}] place_spot_order EXCEPCIÓN {}/{} qty={}: {}",
            caller, symbol, side, qty, exc,
        )
        return {}


async def get_bybit_coin_balance(coin: str, caller: str = "bybit") -> float:
    """
    Obtiene balance disponible de una criptomoneda específica (ETH, BTC, SOL...)
    en Bybit via aiohttp. Prueba UNIFIED→SPOT.
    """
    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        return 0.0
    try:
        async with aiohttp.ClientSession() as s:
            for acct_type in ("UNIFIED", "SPOT"):
                query   = f"accountType={acct_type}&coin={coin}"
                headers = _make_bybit_get_headers(query)
                url     = f"{_BYBIT_BASE}/v5/account/wallet-balance?{query}"
                async with s.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                    if data.get("retCode") != 0:
                        continue
                    coins = data.get("result", {}).get("list", [{}])[0].get("coin", [])
                    for c in coins:
                        if c.get("coin") == coin:
                            bal = float(
                                c.get("availableToWithdraw", c.get("walletBalance", 0)) or 0
                            )
                            if bal > 0:
                                logger.debug(
                                    "[{}] {} balance Bybit {}={:.6f} ✅",
                                    caller, acct_type, coin, bal,
                                )
                                return bal
    except Exception as exc:
        logger.warning("[{}] get_bybit_coin_balance({}) error: {}", caller, coin, exc)
    return 0.0
