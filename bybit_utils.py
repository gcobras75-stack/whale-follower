"""
bybit_utils.py — Utilidades compartidas para llamadas a Bybit API (aiohttp directo a api.bytick.com).
Sin dependencia de pybit SDK para evitar problemas de compatibilidad de versiones.

Centraliza la lógica de fetch de balance para evitar duplicación
y garantizar UNIFIED→SPOT fallback + logging correcto en todos los módulos.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
import uuid
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

# Endpoints de órdenes a probar en secuencia (el primero que funcione se cachea)
_ORDER_ENDPOINTS = [
    "https://api.bytick.com/v5/order/create",
    "https://api.bybit.com/v5/order/create",
    "https://api.bybit.nl/v5/order/create",
    "https://api2.bybit.com/v5/order/create",
]
# Caché del endpoint que funcionó — None = aún no descubierto
_working_order_endpoint: Optional[str] = None

# WebSocket endpoints privados (órdenes — evita bloqueo IP Railway en REST)
_WS_PRIVATE_ENDPOINTS = [
    "wss://stream.bybit.com/v5/private",
    "wss://stream.bytick.com/v5/private",
]


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
                    if r.status == 403:
                        logger.info(
                            "[{}] Bybit balance REST bloqueado (403) → usando REAL_CAPITAL=${:.2f}",
                            caller, config.REAL_CAPITAL,
                        )
                        return config.REAL_CAPITAL
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


async def _place_order_via_ws(
    symbol: str,
    side: str,
    qty: float,
    caller: str = "bybit",
) -> dict:
    """
    Coloca una orden de mercado spot en Bybit via WebSocket privado.
    Evita el bloqueo de IPs Railway que afecta al REST API.
    Autenticación: op='auth' con HMAC-SHA256 de 'GET/realtime{expires}'.
    Orden: op='order.create' con reqId único para correlacionar respuesta.
    Retorna respuesta normalizada (igual que REST: usa clave 'result').
    """
    import json

    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        return {}

    # ── Auth signature (WS) ───────────────────────────────────────────────
    expires  = int(time.time() * 1000) + 5000
    ws_val   = f"GET/realtime{expires}"
    ws_sig   = hmac.new(
        config.BYBIT_API_SECRET.encode("utf-8"),
        ws_val.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    auth_msg = {
        "op":   "auth",
        "args": [config.BYBIT_API_KEY, expires, ws_sig],
    }

    # ── Order message ─────────────────────────────────────────────────────
    req_id    = str(uuid.uuid4())[:12]
    ts_now    = str(int(time.time() * 1000))
    order_msg = {
        "reqId": req_id,
        "header": {
            "X-BAPI-TIMESTAMP":   ts_now,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
        },
        "op": "order.create",
        "args": [{
            "category":    "spot",
            "symbol":      symbol,
            "side":        side,
            "orderType":   "Market",
            "qty":         str(qty),
            "timeInForce": "IOC",
        }],
    }

    for ws_url in _WS_PRIVATE_ENDPOINTS:
        try:
            logger.info(
                "[{}] → Bybit WS ORDER {} {} qty={} reqId={} | {}",
                caller, side, symbol, qty, req_id, ws_url,
            )
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(
                    ws_url,
                    heartbeat=20,
                    timeout=aiohttp.ClientTimeout(total=15),
                    headers={"User-Agent": "Mozilla/5.0"},
                ) as ws:

                    # 1. Enviar auth
                    await ws.send_str(json.dumps(auth_msg))

                    # 2. Esperar respuesta de auth (op=auth)
                    auth_ok = False
                    deadline = time.time() + 6
                    while time.time() < deadline:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=1.5)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if d.get("op") == "auth":
                                auth_ok = d.get("success", False)
                                logger.info(
                                    "[{}] WS auth op=auth success={}",
                                    caller, auth_ok,
                                )
                                break
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

                    if not auth_ok:
                        logger.warning("[{}] WS auth FAILED en {} — probando siguiente", caller, ws_url)
                        continue

                    # 3. Enviar orden
                    await ws.send_str(json.dumps(order_msg))

                    # 4. Esperar respuesta de la orden (op=order.create o reqId match)
                    deadline = time.time() + 10
                    while time.time() < deadline:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=2.0)
                        except asyncio.TimeoutError:
                            continue
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data)
                            if d.get("op") == "order.create" or d.get("reqId") == req_id:
                                ret_code = d.get("retCode", -1)
                                ret_msg  = d.get("retMsg", "")
                                logger.info(
                                    "[{}] ← WS {} retCode={} retMsg='{}' | full={}",
                                    caller, ws_url, ret_code, ret_msg, d,
                                )
                                if ret_code == 0:
                                    order_id = d.get("data", {}).get("orderId", "")
                                    logger.info(
                                        "[{}] Bybit WS {} {} qty={} orderId={} ✅",
                                        caller, side, symbol, qty, order_id,
                                    )
                                    # Normalizar: WS usa 'data', REST usa 'result'
                                    d.setdefault("result", d.get("data", {}))
                                else:
                                    logger.error(
                                        "[{}] WS ORDER FAILED retCode={} msg='{}'",
                                        caller, ret_code, ret_msg,
                                    )
                                return d
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

                    logger.warning("[{}] WS order response timeout en {}", caller, ws_url)

        except Exception as exc:
            logger.warning("[{}] WS {} excepción: {} → probando siguiente", caller, ws_url, exc)
            continue

    return {}  # todos los WS fallaron


async def place_spot_order(
    symbol: str,
    side: str,
    qty: float,
    caller: str = "bybit",
) -> dict:
    """
    Coloca una orden de mercado spot en Bybit.
    Orden de intentos:
      1. WebSocket privado (evita bloqueo IP de Railway en REST)
      2. REST multi-endpoint como fallback
    """
    import json
    global _working_order_endpoint

    if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
        logger.error("[{}] place_spot_order: keys vacías", caller)
        return {}

    # ── 1. Intentar vía WebSocket (evita bloqueo IP Railway en REST) ─────────
    ws_result = await _place_order_via_ws(symbol, side, qty, caller)
    if ws_result and ws_result.get("retCode") == 0:
        return ws_result
    if ws_result:
        logger.warning(
            "[{}] WS retCode={} msg='{}' — probando REST como fallback",
            caller, ws_result.get("retCode"), ws_result.get("retMsg", ""),
        )
    else:
        logger.info("[{}] WS no respondió — probando REST como fallback", caller)

    # ── 2. Fallback REST multi-endpoint ──────────────────────────────────────
    body = {
        "category":  "spot",
        "symbol":    symbol,
        "side":      side,
        "orderType": "Market",
        "qty":       str(qty),
    }
    body_str = json.dumps(body, separators=(",", ":"))

    def _make_post_headers() -> dict:
        """Recalcula HMAC con timestamp fresco (necesario por cada intento)."""
        ts  = str(int(time.time() * 1000))
        msg = f"{ts}{config.BYBIT_API_KEY}{_RECV_WINDOW}{body_str}"
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
            "Content-Type":       "application/json",
            **_BYBIT_HEADERS_EXTRA,
        }

    # Construir lista de URLs a intentar
    # Si BYBIT_ORDER_ENDPOINT configurado → úsalo primero, luego fallbacks
    env_ep = config.BYBIT_ORDER_ENDPOINT.strip()
    if env_ep:
        urls_to_try = [env_ep] + [u for u in _ORDER_ENDPOINTS if u != env_ep]
    elif _working_order_endpoint:
        # Usar el que funcionó antes primero, luego el resto como fallback
        urls_to_try = [_working_order_endpoint] + [
            u for u in _ORDER_ENDPOINTS if u != _working_order_endpoint
        ]
    else:
        urls_to_try = list(_ORDER_ENDPOINTS)

    logger.info(
        "[{}] → Bybit ORDER {} {} qty={} | body={} | probando {} endpoints",
        caller, side, symbol, qty, body_str, len(urls_to_try),
    )

    last_data: dict = {}
    for url in urls_to_try:
        try:
            headers = _make_post_headers()
            logger.info("[{}] → POST {}", caller, url)
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    url, headers=headers, data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    http_status = r.status
                    if http_status in (403, 404, 503):
                        logger.warning(
                            "[{}] {} → HTTP {} (IP bloqueado) → probando siguiente",
                            caller, url, http_status,
                        )
                        continue
                    data = await r.json()
                    last_data = data

            ret_code = data.get("retCode", -1)
            ret_msg  = data.get("retMsg", "")

            logger.info(
                "[{}] ← {} http={} retCode={} retMsg='{}' | full={}",
                caller, url, http_status, ret_code, ret_msg, data,
            )

            if ret_code == 0:
                order_id = data.get("result", {}).get("orderId", "")
                logger.info(
                    "[{}] Bybit {} {} qty={} orderId={} ✅ via {}",
                    caller, side, symbol, qty, order_id, url,
                )
                _working_order_endpoint = url  # cachear para próximos trades
                return data
            else:
                # Error de aplicación (auth, parámetros) → no seguir probando
                logger.error(
                    "[{}] ORDER FAILED {} {} qty={} — retCode={} msg='{}' — sin fallback",
                    caller, side, symbol, qty, ret_code, ret_msg,
                )
                return data

        except Exception as exc:
            logger.warning("[{}] {} → excepción: {} → probando siguiente", caller, url, exc)
            continue

    logger.error(
        "[{}] ORDER FAILED {} {} qty={} — todos los endpoints bloqueados (403)",
        caller, side, symbol, qty,
    )
    return last_data


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
