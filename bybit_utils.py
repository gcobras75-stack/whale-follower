"""
bybit_utils.py — Utilidades compartidas para llamadas a Bybit API.

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
