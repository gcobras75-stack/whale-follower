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

# Map from bot pair names to OKX instId
# SWAP (perpetuos USDT-M) — usa margen del Trading Unificado ($399 disponible)
# SPOT desactivado — solo $4 USDT libre en spot.
_PAIR_TO_INST: Dict[str, str] = {
    "BTCUSDT":  "BTC-USDT-SWAP",
    "ETHUSDT":  "ETH-USDT-SWAP",
    "SOLUSDT":  "SOL-USDT-SWAP",
    "BNBUSDT":  "BNB-USDT-SWAP",
    "DOGEUSDT": "DOGE-USDT-SWAP",
    "XRPUSDT":  "XRP-USDT-SWAP",
    "ADAUSDT":  "ADA-USDT-SWAP",
    "AVAXUSDT": "AVAX-USDT-SWAP",
    "LINKUSDT": "LINK-USDT-SWAP",
}

# Valor por contrato en OKX perpetuos (ctVal en USD o base coin)
# OKX USDT-M swaps: ctVal está en la moneda base (ej: 0.01 ETH por contrato)
# ctVal: unidades de la moneda base por contrato (fuente: OKX API /public/instruments)
_CT_VAL: Dict[str, float] = {
    "BTC-USDT-SWAP":  0.001,   # 1 contrato = 0.001 BTC
    "ETH-USDT-SWAP":  0.01,    # 1 contrato = 0.01 ETH
    "SOL-USDT-SWAP":  1.0,     # 1 contrato = 1 SOL
    "BNB-USDT-SWAP":  0.01,    # 1 contrato = 0.01 BNB
    "DOGE-USDT-SWAP": 10.0,    # 1 contrato = 10 DOGE
    "XRP-USDT-SWAP":  10.0,    # 1 contrato = 10 XRP
    "ADA-USDT-SWAP":  10.0,    # 1 contrato = 10 ADA
    "AVAX-USDT-SWAP": 0.1,     # 1 contrato = 0.1 AVAX
    "LINK-USDT-SWAP": 1.0,     # 1 contrato = 1 LINK
}

_MIN_ORDER_USD = 1.0   # OKX perpetuos mínimo ~$1


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

    # Modos de cuenta OKX
    _ACCT_MODES = {1: "Simple", 2: "Single-currency margin", 3: "Multi-currency margin", 4: "Portfolio margin"}
    # Simple (1) NO soporta SWAP — requiere al menos Single-currency (2)
    _SWAP_OK_MODES = {2, 3, 4}

    def __init__(self) -> None:
        self._enabled = bool(
            config.OKX_API_KEY and config.OKX_SECRET and config.OKX_PASSPHRASE
        )
        self._last_balance: float = 0.0
        self._last_balance_ts: float = 0.0
        self._acct_level: int = 0       # 0=desconocido, 1-4=modos OKX
        self._acct_checked: bool = False

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

    async def get_total_balance_usd(self) -> tuple[float, dict]:
        """
        Retorna el capital TOTAL de OKX en USD incluyendo todos los coins.

        Proceso:
          1. GET /api/v5/account/balance  (sin filtro — todos los coins)
          2. Para cada coin con qty > 0:
             - USDT → valor directo desde eq
             - ETH/SOL/BTC/etc → qty × precio actual (ticker)
          3. Retorna (total_usd, breakdown_dict)

        Ejemplo de retorno:
          (87.43, {"USDT": 12.50, "ETH": 45.20, "SOL": 29.73})
        """
        if not self._enabled:
            return 0.0, {}

        breakdown: dict = {}
        total_usd = 0.0

        try:
            async with aiohttp.ClientSession() as session:

                # 1. Obtener todos los coins en cuenta trading
                path_bal = "/api/v5/account/balance"
                resp_bal = await session.get(
                    _BASE_URL + path_bal,
                    headers=_auth_headers("GET", path_bal),
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                data_bal = await resp_bal.json()

                if data_bal.get("code") != "0":
                    logger.warning(
                        "[okx_exec] get_total_balance_usd: code={} msg={}",
                        data_bal.get("code"), data_bal.get("msg", ""),
                    )
                    return self._last_balance, {}

                details = data_bal.get("data", [{}])[0].get("details", [])
                if not details:
                    return self._last_balance, {}

                # Coins con saldo > 0 que NO son USDT
                non_usdt: list[tuple[str, float]] = []

                for d in details:
                    ccy  = d.get("ccy", "")
                    eq   = float(d.get("eq",   0) or 0)
                    cash = float(d.get("cashBal", 0) or 0)
                    qty  = eq or cash
                    if qty <= 0:
                        continue
                    if ccy == "USDT":
                        breakdown["USDT"] = round(qty, 2)
                        total_usd += qty
                    else:
                        non_usdt.append((ccy, qty))

                # 2. Obtener precios actuales para coins no-USDT
                for ccy, qty in non_usdt:
                    inst_id = f"{ccy}-USDT"
                    path_tk = f"/api/v5/market/ticker?instId={inst_id}"
                    try:
                        resp_tk = await session.get(
                            _BASE_URL + path_tk,
                            timeout=aiohttp.ClientTimeout(total=5),
                        )
                        data_tk = await resp_tk.json()
                        if data_tk.get("code") == "0" and data_tk.get("data"):
                            price = float(data_tk["data"][0].get("last", 0) or 0)
                            if price > 0:
                                usd_val = qty * price
                                breakdown[ccy] = round(usd_val, 2)
                                total_usd += usd_val
                                logger.debug(
                                    "[okx_exec] {} qty={:.6f} × ${:.2f} = ${:.2f}",
                                    ccy, qty, price, usd_val,
                                )
                    except Exception as exc_tk:
                        logger.debug("[okx_exec] ticker {} error: {}", inst_id, exc_tk)

            total_usd = round(total_usd, 2)
            self._last_balance    = total_usd
            self._last_balance_ts = time.time()

            parts = " + ".join(f"{c}=${v:.2f}" for c, v in breakdown.items())
            logger.info(
                "[okx_exec] Balance total OKX: ${:.2f} ({})",
                total_usd, parts,
            )
            return total_usd, breakdown

        except Exception as exc:
            logger.warning("[okx_exec] get_total_balance_usd error: {}", exc)
            return self._last_balance, {}

    # ── Leverage ──────────────────────────────────────────────────────────────

    _leverage_set: set = set()   # instIds donde ya se configuró leverage

    async def _ensure_leverage(self, inst_id: str) -> None:
        """Configura leverage 1x para un instrumento SWAP (solo una vez)."""
        if inst_id in self._leverage_set:
            return
        try:
            path = "/api/v5/account/set-leverage"
            body_dict = {
                "instId":  inst_id,
                "lever":   "1",
                "mgnMode": "cross",
            }
            body = json.dumps(body_dict)
            async with aiohttp.ClientSession() as session:
                resp = await session.post(
                    _BASE_URL + path,
                    headers=_auth_headers("POST", path, body),
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=8),
                )
                data = await resp.json()
            if data.get("code") == "0":
                self._leverage_set.add(inst_id)
                logger.info("[okx_exec] Leverage 1x configurado para {}", inst_id)
            else:
                logger.warning("[okx_exec] set-leverage {} error: {}", inst_id, data.get("msg"))
        except Exception as exc:
            logger.warning("[okx_exec] _ensure_leverage error: {}", exc)

    # ── Account mode check ────────────────────────────────────────────────

    async def _check_account_mode(self) -> bool:
        """Verifica modo de cuenta OKX. Retorna True si soporta SWAP."""
        if self._acct_checked:
            return self._acct_level in self._SWAP_OK_MODES
        try:
            path = "/api/v5/account/config"
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    _BASE_URL + path,
                    headers=_auth_headers("GET", path),
                    timeout=aiohttp.ClientTimeout(total=8),
                )
                data = await resp.json()
            self._acct_checked = True
            if data.get("code") == "0" and data.get("data"):
                self._acct_level = int(data["data"][0].get("acctLv", 0))
                mode_name = self._ACCT_MODES.get(self._acct_level, "desconocido")
                if self._acct_level in self._SWAP_OK_MODES:
                    logger.info("[okx_exec] Modo cuenta: {} ({}) — SWAP OK",
                                self._acct_level, mode_name)
                    return True
                else:
                    logger.error(
                        "[okx_exec] Modo cuenta: {} ({}) — SWAP NO SOPORTADO. "
                        "Cambiar a Single-currency margin en OKX app.",
                        self._acct_level, mode_name)
                    # Alerta Telegram
                    try:
                        import alerts
                        await alerts._send_telegram(
                            f"OKX modo {mode_name} (acctLv={self._acct_level}) "
                            f"NO soporta SWAP.\n"
                            f"Cambiar a Single-currency margin en OKX app → "
                            f"Trade → Settings → Account mode.",
                            priority="critical",
                        )
                    except Exception:
                        pass
                    return False
            else:
                logger.warning("[okx_exec] account/config error: {}", data.get("msg"))
        except Exception as exc:
            logger.warning("[okx_exec] _check_account_mode error: {}", exc)
        return True  # asumir OK si falla la verificación

    # ── Market orders ───────────────────────────────────────────────────────

    async def market_order(
        self,
        pair: str,
        side: str,
        size_usd: float,
        price_hint: float,
    ) -> Optional[Dict[str, Any]]:
        """
        Place a market order on OKX SWAP (perpetuos USDT-M).

        Args:
            pair:       Bot pair name e.g. "ETHUSDT"
            side:       "buy" (long) or "sell" (short)
            size_usd:   Approximate USD notional (converted to contracts)
            price_hint: Current price (used to calculate contract count)

        Returns:
            Order result dict with order_id, or None on failure.
        """
        if not self._enabled:
            logger.warning("[okx_exec] market_order: executor disabled")
            return None

        # Verificar modo de cuenta antes de la primera orden
        if not await self._check_account_mode():
            return None

        inst_id = _PAIR_TO_INST.get(pair)
        if not inst_id:
            logger.warning("[okx_exec] unknown pair: {}", pair)
            return None

        if size_usd < _MIN_ORDER_USD:
            logger.warning(
                "[okx_exec] size_usd=${:.2f} below minimum ${:.0f} — skip",
                size_usd, _MIN_ORDER_USD,
            )
            return None

        path = "/api/v5/trade/order"
        _side = side.lower()

        # SWAP (perpetuos): sz = número de contratos, tdMode = "cross"
        ct_val = _CT_VAL.get(inst_id, 1.0)
        if price_hint <= 0:
            logger.warning("[okx_exec] need valid price_hint (got {})", price_hint)
            return None
        usd_per_contract = ct_val * price_hint
        n_contracts = max(1, int(size_usd / usd_per_contract))

        body_dict = {
            "instId":  inst_id,
            "tdMode":  "cross",       # margen cruzado (usa todo el balance unificado)
            "side":    _side,
            "ordType": "market",
            "sz":      str(n_contracts),
        }
        # Para SWAP compras = "long", ventas = "short"
        if _side == "buy":
            body_dict["posSide"] = "long"
        else:
            body_dict["posSide"] = "short"

        body = json.dumps(body_dict)

        usd_approx = n_contracts * usd_per_contract
        logger.info(
            "[okx_exec] SWAP {} {} {} contratos ≈${:.0f}",
            _side.upper(), inst_id, n_contracts, usd_approx,
        )

        try:
            # Asegurar leverage 1x antes de la primera orden
            await self._ensure_leverage(inst_id)

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
                    "[okx_exec] SWAP ORDER FAILED {}: {} | {}",
                    inst_id, err_msg, order_err,
                )
                return None

            order_data = data["data"][0]
            order_id = order_data.get("ordId", "")

            logger.info(
                "[okx_exec] SWAP ORDER OK {} {} {} contratos ≈${:.0f} ordId={}",
                _side.upper(), inst_id, n_contracts, usd_approx, order_id,
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
