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
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from loguru import logger

import config
import alerts as _alerts

# ── Config ────────────────────────────────────────────────────────────────────
_CHECK_INTERVAL_SECS  = 6 * 3600     # cada 6 horas
_URGENT_INTERVAL_SECS = 30 * 60     # alerta urgent cada 30 min
_SUGGESTED_INTERVAL   = 6 * 3600    # alerta suggested cada 6 horas
_TARGET_BYBIT_PCT     = 0.60         # objetivo: 60% en Bybit (opera grid + Wyckoff)
_SUGGESTED_DEV        = 0.20         # desviacion > 20% → sugerir rebalanceo
_URGENT_DEV           = 0.35         # desviacion > 35% → alerta urgente
_BYBIT_MIN_OK         = 100.0       # Bybit > $100 = balance normalizado

# ── Dust Cleanup ──────────────────────────────────────────────────────────────
_DUST_CLEANUP_SECS    = 4 * 3600     # limpieza cada 4 horas
_DUST_MAX_USD         = 10.0         # vender si valor < $10 USD
_DUST_MIN_USD         = 1.0          # no vender si valor < $1 (monto muy pequeño)
_DUST_SKIP_COINS      = {"USDT", "USDC", "BUSD", "DAI", "TUSD"}  # stablecoins


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
        self._executor = None   # referencia al BybitTestnetExecutor (opcional)
        self._okx_exec = None   # OKXExecutor lazy — para get_total_balance_usd()
        self._last_urgent_alert_ts: float = 0.0
        self._last_suggested_alert_ts: float = 0.0
        logger.info(
            "[rebalancer] Iniciado | check={}h | target_bybit={:.0f}% | "
            "suggested_dev={:.0f}% | urgent_dev={:.0f}%",
            _CHECK_INTERVAL_SECS // 3600,
            _TARGET_BYBIT_PCT * 100,
            _SUGGESTED_DEV * 100,
            _URGENT_DEV * 100,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def set_executor(self, executor) -> None:
        """Registra el executor de Bybit para proteger coins con trades activos."""
        self._executor = executor
        logger.info("[rebalancer] Executor registrado para protección de trades activos")

    async def run(self) -> None:
        """Loop principal: verificación de balance (6h) + limpieza de dust (4h)."""
        await asyncio.gather(
            self._balance_loop(),
            self._dust_cleanup_loop(),
        )

    async def _balance_loop(self) -> None:
        while True:
            await self._check()
            # Si urgent, re-check cada 30 min; si no, cada 6h
            interval = _URGENT_INTERVAL_SECS if self._snap.status == "urgent" else _CHECK_INTERVAL_SECS
            await asyncio.sleep(interval)

    # ── Public ────────────────────────────────────────────────────────────────

    def snapshot(self) -> RebalanceSnapshot:
        return self._snap

    def usdt_balances(self) -> tuple[float, float]:
        """Retorna (bybit_usdt, okx_usdt) del último snapshot para exchange selection."""
        return self._snap.bybit_usdt, self._snap.okx_usdt

    async def force_check(self) -> RebalanceSnapshot:
        """Forzar verificación inmediata (llamado por /rebalance)."""
        await self._check()
        return self._snap

    def get_okx_breakdown(self) -> dict:
        """Retorna breakdown de OKX por coin desde alerts._stats."""
        return _alerts._stats.get("capital_okx_breakdown", {})

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _check(self) -> None:
        if not config.PRODUCTION:
            return

        bybit_raw = await self._fetch_bybit()   # None = error conexion
        okx_bal   = await self._fetch_okx()

        # Si _fetch_bybit devolvio None → error de conexion, no alertar
        if bybit_raw is None:
            logger.warning("[rebalancer] Bybit sin conexion — omitiendo verificacion de balance")
            return

        bybit_bal = bybit_raw

        # Si bybit=0 pero okx>0 → posible lectura incorrecta de subcuenta
        # Aun así actualizar capital en alerts para que OKX se refleje
        if bybit_bal == 0.0 and okx_bal > 5.0:
            logger.warning(
                "[rebalancer] Bybit=$0 con OKX=${:.2f} — posible subcuenta incorrecta, "
                "omitiendo alerta rebalanceo pero actualizando capital",
                okx_bal,
            )
            try:
                _alerts.set_capital(bybit=0.0, okx=okx_bal)
            except Exception:
                pass
            return

        total = bybit_bal + okx_bal

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

        # Actualizar capital en alerts para que el reporte muestre ambos balances
        try:
            _alerts.set_capital(bybit=bybit_bal, okx=okx_bal)
        except Exception:
            pass

        now = time.time()
        if status == "urgent":
            if now - self._last_urgent_alert_ts >= _URGENT_INTERVAL_SECS:
                await self._alert(self._snap)
                self._last_urgent_alert_ts = now
        elif status == "suggested":
            if now - self._last_suggested_alert_ts >= _SUGGESTED_INTERVAL:
                await self._alert(self._snap)
                self._last_suggested_alert_ts = now

        # Reset alert timers when balance normalizes
        if status == "ok" or bybit_bal >= _BYBIT_MIN_OK:
            self._last_urgent_alert_ts = 0.0
            self._last_suggested_alert_ts = 0.0

    async def _fetch_bybit(self) -> Optional[float]:
        """
        Lee balance USDT en Bybit delegando a bybit_utils.fetch_usdt_balance().
        Retorna None si hubo error de conexión (no confundir con balance=0 válido).
        """
        from bybit_utils import fetch_usdt_balance
        try:
            bal = await fetch_usdt_balance(caller="rebalancer")
            return bal
        except Exception as exc:
            logger.warning("[rebalancer] Bybit balance error de conexión: {}", exc)
            return None

    async def _fetch_okx(self) -> float:
        """Retorna capital TOTAL de OKX: USDT + ETH×precio + SOL×precio + ..."""
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return 0.0
        try:
            if self._okx_exec is None:
                from okx_executor import OKXExecutor
                self._okx_exec = OKXExecutor()
            total, breakdown = await self._okx_exec.get_total_balance_usd()
            if total > 0:
                return total
        except Exception as exc:
            logger.warning("[rebalancer] OKX total balance error: {}", exc)
        return 0.0

    # ── Dust Cleanup ──────────────────────────────────────────────────────────────

    def _active_coins(self) -> set:
        """
        Retorna el conjunto de coins con trades activos (OPEN o PARTIAL).
        Estos NO se tocarán durante la limpieza de dust.
        """
        if self._executor is None:
            return set()
        active: set = set()
        for trade in getattr(self._executor, "_trades", []):
            if trade.status in ("open", "partial"):
                active.add(trade.pair.replace("USDT", ""))
        return active

    async def _dust_cleanup_loop(self) -> None:
        await asyncio.sleep(3600)   # espera 1h después del arranque
        while True:
            try:
                await self._dust_cleanup()
            except Exception as exc:
                logger.warning("[rebalancer] dust_cleanup error (no bloquea): {}", exc)
            await asyncio.sleep(_DUST_CLEANUP_SECS)

    async def _dust_cleanup(self) -> None:
        """Ciclo de limpieza: escanea ambos exchanges y vende dust < $10."""
        if not config.PRODUCTION:
            return
        active_coins = self._active_coins()
        logger.info(
            "[rebalancer] Iniciando limpieza de dust | coins_activos={}",
            active_coins or "ninguno",
        )

        bybit_dust = await self._scan_bybit_dust(active_coins)
        okx_dust   = await self._scan_okx_dust(active_coins)

        cleaned_usd = 0.0
        sold_items: list = []

        for coin, qty, usd_val in bybit_dust:
            logger.info(
                "[rebalancer] DUST Bybit: {} qty={:.6f} ≈${:.2f} → MARKET SELL",
                coin, qty, usd_val,
            )
            ok = await self._bybit_sell_dust(coin + "USDT", qty)
            if ok:
                cleaned_usd += usd_val
                sold_items.append(f"Bybit {coin} ${usd_val:.2f}")

        for coin, qty, usd_val in okx_dust:
            logger.info(
                "[rebalancer] DUST OKX: {} qty={:.6f} ≈${:.2f} → MARKET SELL",
                coin, qty, usd_val,
            )
            ok = await self._okx_sell_dust(coin + "-USDT", qty)
            if ok:
                cleaned_usd += usd_val
                sold_items.append(f"OKX {coin} ${usd_val:.2f}")

        if sold_items:
            logger.info(
                "[rebalancer] Dust cleanup completo: ${:.2f} convertidos a USDT | {}",
                cleaned_usd, ", ".join(sold_items),
            )
            await self._alert_dust(sold_items, cleaned_usd)
        else:
            logger.debug("[rebalancer] Dust cleanup: sin activos para limpiar")

    async def _scan_bybit_dust(self, active_coins: set) -> list:
        """
        Retorna [(coin, qty, usd_value)] de dust en Bybit
        (coins con valor $1-$10 que no estén en trades activos).
        """
        coins = await self._fetch_all_bybit_coins()
        result = []
        for coin, qty in coins.items():
            if coin in _DUST_SKIP_COINS or coin in active_coins:
                if coin in active_coins:
                    logger.debug("[rebalancer] Dust Bybit: {} protegido por trade activo", coin)
                continue
            price = await self._ticker_price_bybit(coin)
            if price <= 0:
                continue
            usd_val = qty * price
            if _DUST_MIN_USD <= usd_val <= _DUST_MAX_USD:
                result.append((coin, qty, usd_val))
        return result

    async def _scan_okx_dust(self, active_coins: set) -> list:
        """
        Retorna [(coin, qty, usd_value)] de dust en OKX
        (coins con valor $1-$10 que no estén en trades activos).
        """
        coins = await self._fetch_all_okx_coins()
        result = []
        for coin, qty in coins.items():
            if coin in _DUST_SKIP_COINS or coin in active_coins:
                if coin in active_coins:
                    logger.debug("[rebalancer] Dust OKX: {} protegido por trade activo", coin)
                continue
            price = await self._ticker_price_okx(coin)
            if price <= 0:
                continue
            usd_val = qty * price
            if _DUST_MIN_USD <= usd_val <= _DUST_MAX_USD:
                result.append((coin, qty, usd_val))
        return result

    async def _fetch_all_bybit_coins(self) -> dict:
        """Obtiene todos los coins con balance > 0 en Bybit (UNIFIED)."""
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return {}
        try:
            import hashlib as _hl, hmac as _hm
            query   = "accountType=UNIFIED"
            ts      = str(int(time.time() * 1000))
            msg     = f"{ts}{config.BYBIT_API_KEY}5000{query}"
            sig     = _hm.new(config.BYBIT_API_SECRET.encode(), msg.encode(), _hl.sha256).hexdigest()
            headers = {
                "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
                "X-BAPI-TIMESTAMP":   ts,
                "X-BAPI-SIGN":        sig,
                "X-BAPI-RECV-WINDOW": "5000",
                "User-Agent":         "Mozilla/5.0",
                "Referer":            "https://www.bybit.com",
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"https://api.bybit.com/v5/account/wallet-balance?{query}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    data = await r.json()
            if data.get("retCode") == 0:
                result = {}
                for c in data["result"]["list"][0].get("coin", []):
                    coin = c.get("coin", "")
                    qty  = float(c.get("walletBalance", 0))
                    if qty > 0 and coin not in _DUST_SKIP_COINS:
                        result[coin] = qty
                return result
        except Exception as exc:
            logger.debug("[rebalancer] _fetch_all_bybit_coins error: {}", exc)
        return {}

    async def _fetch_all_okx_coins(self) -> dict:
        """Obtiene todos los coins con balance > 0 en OKX (trading account)."""
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return {}
        try:
            from datetime import datetime, timezone as _tz
            ts   = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path = "/api/v5/account/balance"
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
                result = {}
                for d in data.get("data", [{}])[0].get("details", []):
                    coin = d.get("ccy", "")
                    qty  = float(d.get("availBal", 0) or 0)
                    if qty > 0 and coin not in _DUST_SKIP_COINS:
                        result[coin] = qty
                return result
        except Exception as exc:
            logger.debug("[rebalancer] _fetch_all_okx_coins error: {}", exc)
        return {}

    async def _ticker_price_bybit(self, coin: str) -> float:
        """Precio actual de un coin en Bybit (endpoint público, sin auth)."""
        try:
            url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={coin}USDT"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    data = await r.json()
            items = data.get("result", {}).get("list", [])
            if items:
                return float(items[0].get("lastPrice", 0))
        except Exception:
            pass
        return 0.0

    async def _ticker_price_okx(self, coin: str) -> float:
        """Precio actual de un coin en OKX (endpoint público, sin auth)."""
        try:
            url = f"https://www.okx.com/api/v5/market/ticker?instId={coin}-USDT"
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                    data = await r.json()
            items = data.get("data", [])
            if items:
                return float(items[0].get("last", 0))
        except Exception:
            pass
        return 0.0

    async def _bybit_sell_dust(self, pair: str, coin_qty: float) -> bool:
        """Market SELL en Bybit Spot para dust. pair=SOLUSDT."""
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            return False
        import hashlib as _hl, hmac as _hm
        ts       = int(time.time() * 1000)
        body_d   = {
            "category":    "spot",
            "symbol":      pair,
            "side":        "Sell",
            "orderType":   "Market",
            "qty":         str(round(coin_qty, 8)),
            "timeInForce": "GTC",
        }
        body_str = json.dumps(body_d, separators=(",", ":"))
        msg      = f"{ts}{config.BYBIT_API_KEY}5000{body_str}"
        sig      = _hm.new(config.BYBIT_API_SECRET.encode(), msg.encode(), _hl.sha256).hexdigest()
        headers  = {
            "X-BAPI-API-KEY":     config.BYBIT_API_KEY,
            "X-BAPI-TIMESTAMP":   str(ts),
            "X-BAPI-SIGN":        sig,
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type":       "application/json",
            "User-Agent":         "Mozilla/5.0",
            "Referer":            "https://www.bybit.com",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://api.bybit.com/v5/order/create",
                    headers=headers,
                    data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json()
            if data.get("retCode") == 0:
                logger.info(
                    "[rebalancer] Dust SELL Bybit {} qty={:.6f} OK orderId={}",
                    pair, coin_qty, data.get("result", {}).get("orderId", ""),
                )
                return True
            logger.error(
                "[rebalancer] Dust SELL Bybit {} FALLO retCode={} msg={}",
                pair, data.get("retCode"), data.get("retMsg"),
            )
            return False
        except Exception as exc:
            logger.error("[rebalancer] _bybit_sell_dust({}) excepción: {}", pair, exc)
            return False

    async def _okx_sell_dust(self, inst_id: str, coin_qty: float) -> bool:
        """Market SELL en OKX Spot para dust. inst_id=SOL-USDT."""
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            return False
        from datetime import datetime, timezone as _tz
        ts       = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        path     = "/api/v5/trade/order"
        body_d   = {
            "instId":  inst_id,
            "tdMode":  "cash",
            "side":    "sell",
            "ordType": "market",
            "sz":      str(round(coin_qty, 6)),
        }
        body    = json.dumps(body_d)
        sig     = base64.b64encode(
            hmac.new(config.OKX_SECRET.encode(),
                     (ts + "POST" + path + body).encode(), hashlib.sha256).digest()
        ).decode()
        headers = {
            "OK-ACCESS-KEY":        config.OKX_API_KEY,
            "OK-ACCESS-SIGN":       sig,
            "OK-ACCESS-TIMESTAMP":  ts,
            "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
            "Content-Type":         "application/json",
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://www.okx.com{path}",
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    data = await r.json()
            if data.get("code") == "0":
                order_id = data.get("data", [{}])[0].get("ordId", "")
                logger.info(
                    "[rebalancer] Dust SELL OKX {} qty={:.6f} OK orderId={}",
                    inst_id, coin_qty, order_id,
                )
                return True
            logger.error(
                "[rebalancer] Dust SELL OKX {} FALLO code={} msg={}",
                inst_id, data.get("code"), data.get("msg"),
            )
            return False
        except Exception as exc:
            logger.error("[rebalancer] _okx_sell_dust({}) excepción: {}", inst_id, exc)
            return False

    async def _alert_dust(self, sold_items: list, total_usd: float) -> None:
        """Notificación Telegram cuando se limpia dust."""
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        lines = "\n".join(f"  • {item}" for item in sold_items)
        msg   = (
            f"🧹 [DUST CLEANUP]\n"
            f"Altcoins convertidas a USDT:\n{lines}\n"
            f"Total recuperado: ${total_usd:.2f} USDT"
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

    async def _alert(self, snap: RebalanceSnapshot) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return

        target_bybit = snap.total_usdt * _TARGET_BYBIT_PCT
        deficit      = max(0, target_bybit - snap.bybit_usdt)

        bybit_label = f"${snap.bybit_usdt:.2f}   ({snap.bybit_pct:.0f}%)"
        okx_label   = f"${snap.okx_usdt:.2f} ({100-snap.bybit_pct:.0f}%)"

        if snap.bybit_pct < 20:
            bybit_label += "  <- muy bajo"
            okx_label   += "  <- concentrado"

        if snap.status == "urgent":
            msg = (
                f"\U0001f6a8 REBALANCEO URGENTE\n"
                f"------------------------------\n"
                f"Bybit:  {bybit_label}\n"
                f"OKX:    {okx_label}\n"
                f"Total:  ${snap.total_usdt:.2f}\n"
                f"------------------------------\n"
                f"Accion manual necesaria:\n"
                f"Transfiere ${deficit:.0f} USDT de OKX -> Bybit\n\n"
                f"Pasos:\n"
                f"1. Abre OKX app -> Assets -> Withdraw\n"
                f"2. Selecciona USDT (red TRC20 o ERC20)\n"
                f"3. Monto: ${deficit:.0f} USDT\n"
                f"4. Direccion destino Bybit:\n"
                f"   Ve a Bybit -> Assets -> Deposit -> USDT\n"
                f"   Copia tu direccion de deposito\n"
                f"5. Confirma la transferencia\n"
                f"------------------------------\n"
                f"  Wyckoff y Mean Reversion bloqueados\n"
                f"   hasta que Bybit tenga capital suficiente"
            )
        else:
            msg = (
                f"  Rebalanceo sugerido\n"
                f"------------------------------\n"
                f"Bybit:  {bybit_label}\n"
                f"OKX:    {okx_label}\n"
                f"Total:  ${snap.total_usdt:.2f}\n"
                f"------------------------------\n"
                f"Considera transferir ~${deficit:.0f} USDT de OKX -> Bybit\n"
                f"Target: {_TARGET_BYBIT_PCT*100:.0f}% Bybit / "
                f"{(1-_TARGET_BYBIT_PCT)*100:.0f}% OKX"
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
