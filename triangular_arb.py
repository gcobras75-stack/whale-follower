# -*- coding: utf-8 -*-
"""
triangular_arb.py -- Whale Follower Bot
Arbitraje Cross-Exchange: BTC/USDT en Bybit Spot vs OKX Spot

Estrategia:
  Compara el precio de BTC/USDT en Bybit y OKX en tiempo real.
  Si la diferencia supera el coste de fees (0.10% x 2 patas = 0.20%):

    Ruta A (Bybit mas barato):
      1. Comprar BTC/USDT en Bybit Spot
      2. Vender BTC/USDT en OKX Spot

    Ruta B (OKX mas barato):
      1. Comprar BTC/USDT en OKX Spot
      2. Vender BTC/USDT en Bybit Spot

  Umbral minimo: net P&L > $0.01 por operacion.
  Tamano real: hasta $30 por ciclo.

En papel: simula las dos patas y registra spread capturado.
En real:  ejecuta ordenes simultaneas en Bybit (pybit) y OKX (REST API v5).
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import base64
import hashlib
import hmac
import json

import aiohttp
from loguru import logger

import config

# ── Config ────────────────────────────────────────────────────────────────────
_MAX_SPREAD_PCT    = 1.5     # spread > 1.5% = dato anomalo (compartido)
_TAKER_FEE_PCT     = 0.10    # fee por pata en spot (Bybit/OKX taker 0.10%)
_COOLDOWN_SECS     = 10      # cooldown minimo entre ejecuciones
_PRICE_STALE_MS    = 2000    # datos stale si > 2s
_LOG_INTERVAL_SECS = 30      # frecuencia del log periodico de spread

# Configuracion por par — threshold, tamano y fees especificos
_PAIR_CONFIG: Dict[str, dict] = {
    "BTCUSDT": {
        "min_spread":  0.03,    # 0.03%
        "paper_size":  300.0,   # USD en modo papel
        "max_size":     30.0,   # USD maximo en real
        "min_net":       0.01,  # P&L neto minimo para ejecutar
        "okx_symbol": "BTC-USDT",
    },
    "ETHUSDT": {
        "min_spread":  0.04,
        "paper_size":  250.0,
        "max_size":     25.0,
        "min_net":       0.01,
        "okx_symbol": "ETH-USDT",
    },
    "SOLUSDT": {
        "min_spread":  0.05,
        "paper_size":  200.0,
        "max_size":     20.0,
        "min_net":       0.01,
        "okx_symbol": "SOL-USDT",
    },
}


@dataclass
class TriPrice:
    btc_usdt:     float = 0.0   # BTC/USDT Bybit
    eth_usdt:     float = 0.0   # ETH/USDT Bybit
    sol_usdt:     float = 0.0   # SOL/USDT Bybit
    eth_btc:      float = 0.0   # reservado
    btc_usdt_okx: float = 0.0   # BTC/USDT OKX
    eth_usdt_okx: float = 0.0   # ETH/USDT OKX
    sol_usdt_okx: float = 0.0   # SOL/USDT OKX
    ts_ms:        float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class TriArbTrade:
    trade_id:       str
    pair:           str       # "BTCUSDT" | "ETHUSDT" | "SOLUSDT"
    route:          str       # "A" (buy Bybit/sell OKX) | "B" (buy OKX/sell Bybit)
    btc_usdt:       float     # precio Bybit
    eth_usdt:       float     # precio OKX (campo reutilizado)
    eth_btc_real:   float     # precio en exchange de compra
    eth_btc_impl:   float     # precio en exchange de venta
    spread_pct:     float
    size_usd:       float
    gross_pnl:      float
    net_pnl:        float
    opened_at:      float = field(default_factory=time.time)
    production:     bool  = False


@dataclass
class TriArbSnapshot:
    eth_btc_implied:   float = 0.0   # precio BTC en Bybit
    eth_btc_real:      float = 0.0   # precio BTC en OKX
    spread_pct:        float = 0.0
    opportunities_1h:  int   = 0
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    stale:             bool  = True


class TriangularArb:
    """
    Detecta y ejecuta arbitraje cross-exchange BTC/USDT (Bybit vs OKX).

    Uso:
        arb = TriangularArb(production=True)
        arb.update_btc_usdt(83500.0)       # precio BTC en Bybit
        arb.update_btc_usdt_okx(83600.0)   # precio BTC en OKX
    """

    def __init__(self, production: bool = False) -> None:
        self._production  = production
        self._prices:    TriPrice           = TriPrice()
        self._trades:    List[TriArbTrade]  = []
        self._opportunities: Deque[float]   = deque(maxlen=500)
        self._last_check: Dict[str, float]  = {p: 0.0 for p in _PAIR_CONFIG}
        self._last_log:   Dict[str, float]  = {p: 0.0 for p in _PAIR_CONFIG}
        self._exec_by_pair: Dict[str, int]  = {p: 0 for p in _PAIR_CONFIG}

        mode = "REAL" if production else "PAPEL"
        logger.info(
            "[cross_arb] Iniciado en modo {} | pares={} | min_net=${}",
            mode, list(_PAIR_CONFIG.keys()),
            _PAIR_CONFIG["BTCUSDT"]["min_net"],
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def update_btc_usdt(self, price: float) -> None:
        self._prices.btc_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_pair("BTCUSDT")

    def update_btc_usdt_okx(self, price: float) -> None:
        self._prices.btc_usdt_okx = price
        self._prices.ts_ms        = time.time() * 1000
        self._check_pair("BTCUSDT")

    def update_eth_usdt(self, price: float) -> None:
        self._prices.eth_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_pair("ETHUSDT")

    def update_eth_usdt_okx(self, price: float) -> None:
        self._prices.eth_usdt_okx = price
        self._prices.ts_ms        = time.time() * 1000
        self._check_pair("ETHUSDT")

    def update_sol_usdt(self, price: float) -> None:
        self._prices.sol_usdt = price
        self._prices.ts_ms    = time.time() * 1000
        self._check_pair("SOLUSDT")

    def update_sol_usdt_okx(self, price: float) -> None:
        self._prices.sol_usdt_okx = price
        self._prices.ts_ms        = time.time() * 1000
        self._check_pair("SOLUSDT")

    def update_eth_btc(self, price: float) -> None:
        """Reservado por compatibilidad — no usado en estrategia cross-exchange."""
        self._prices.eth_btc = price
        self._prices.ts_ms   = time.time() * 1000

    def snapshot(self) -> TriArbSnapshot:
        now    = time.time()
        p      = self._prices
        spread = self._cross_spread(p.btc_usdt, p.btc_usdt_okx)
        stale  = (time.time() * 1000 - p.ts_ms) > _PRICE_STALE_MS
        ops_1h = sum(1 for t in self._opportunities if now - t < 3600)
        return TriArbSnapshot(
            eth_btc_implied  = round(p.btc_usdt, 2),
            eth_btc_real     = round(p.btc_usdt_okx, 2),
            spread_pct       = round(spread, 4),
            opportunities_1h = ops_1h,
            trades_total     = len(self._trades),
            pnl_total_usd    = round(sum(t.net_pnl for t in self._trades), 4),
            stale            = stale,
        )

    def exec_by_pair(self) -> Dict[str, int]:
        """Retorna conteo de ejecuciones reales por par."""
        return dict(self._exec_by_pair)

    async def startup_check(self) -> None:
        """Verifica conectividad Bybit SPOT y OKX SPOT al inicio. Envia resultado a Telegram."""
        if not self._production:
            logger.info("[cross_arb] startup_check omitido (modo PAPEL)")
            return

        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

        # ── Bybit SPOT balance ────────────────────────────────────────────────
        bybit_ok  = False
        bybit_bal = 0.0
        bybit_err = ""
        try:
            from pybit.unified_trading import HTTP as BybitHTTP
            session = BybitHTTP(
                testnet=False,
                api_key=config.BYBIT_API_KEY,
                api_secret=config.BYBIT_API_SECRET,
                base_url="https://api.bytick.com",
            )
            loop   = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: session.get_wallet_balance(accountType="UNIFIED", coin="USDT"),
            )
            if result.get("retCode") == 0:
                coins = result.get("result", {}).get("list", [{}])[0].get("coin", [])
                for c in coins:
                    if c.get("coin") == "USDT":
                        bybit_bal = float(c.get("walletBalance", 0))
                bybit_ok = True
            else:
                bybit_err = f"retCode={result.get('retCode')} {result.get('retMsg','')}"
        except Exception as exc:
            bybit_err = str(exc)[:120]

        if bybit_ok:
            logger.info("[cross_arb] Bybit SPOT \u2705 balance=${:.2f}", bybit_bal)
        else:
            logger.error("[cross_arb] Bybit SPOT \u274c {}", bybit_err)

        # ── OKX SPOT balance ──────────────────────────────────────────────────
        okx_ok  = False
        okx_bal = 0.0
        okx_err = ""
        try:
            from datetime import datetime, timezone as _tz
            ts_okx  = datetime.now(_tz.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            path    = "/api/v5/asset/balances?ccy=USDT"
            prehash = ts_okx + "GET" + path
            sig_okx = base64.b64encode(
                hmac.new(config.OKX_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
            ).decode()
            headers_okx = {
                "OK-ACCESS-KEY":        config.OKX_API_KEY,
                "OK-ACCESS-SIGN":       sig_okx,
                "OK-ACCESS-TIMESTAMP":  ts_okx,
                "OK-ACCESS-PASSPHRASE": config.OKX_PASSPHRASE,
            }
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    "https://www.okx.com" + path,
                    headers=headers_okx,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if data.get("code") == "0":
                        for item in data.get("data", []):
                            if item.get("ccy") == "USDT":
                                okx_bal = float(item.get("availBal", 0))
                        okx_ok = True
                    else:
                        okx_err = f"code={data.get('code')} {data.get('msg','')}"
        except Exception as exc:
            okx_err = str(exc)[:120]

        if okx_ok:
            logger.info("[cross_arb] OKX SPOT \u2705 balance=${:.2f}", okx_bal)
        else:
            logger.error("[cross_arb] OKX SPOT \u274c {}", okx_err)

        # ── Telegram summary ──────────────────────────────────────────────────
        if not token or not chat_id:
            return
        b_line = f"\u2705 Bybit SPOT: ${bybit_bal:.2f} USDT" if bybit_ok else f"\u274c Bybit: {bybit_err}"
        o_line = f"\u2705 OKX SPOT:   ${okx_bal:.2f} USDT" if okx_ok  else f"\u274c OKX:   {okx_err}"
        msg = f"\U0001f50c Cross-Arb Startup Check\n{b_line}\n{o_line}"
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
        except Exception:
            pass

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":          t.trade_id[:8],
                "pair":        getattr(t, "pair", "BTCUSDT"),
                "route":       t.route,
                "spread_pct":  t.spread_pct,
                "net_pnl":     round(t.net_pnl, 4),
            }
            for t in self._trades[-10:]
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _cross_spread(bybit: float, okx: float) -> float:
        """Spread porcentual entre los dos exchanges."""
        if bybit <= 0 or okx <= 0:
            return 0.0
        ref = min(bybit, okx)
        return abs(bybit - okx) / ref * 100

    def _get_pair_prices(self, pair: str) -> tuple:
        """Devuelve (bybit_price, okx_price) para el par solicitado."""
        p = self._prices
        if pair == "BTCUSDT":
            return p.btc_usdt, p.btc_usdt_okx
        if pair == "ETHUSDT":
            return p.eth_usdt, p.eth_usdt_okx
        if pair == "SOLUSDT":
            return p.sol_usdt, p.sol_usdt_okx
        return 0.0, 0.0

    def _check_pair(self, pair: str) -> None:
        """Detecta y ejecuta arbitraje cross-exchange para cualquier par."""
        now = time.time()
        cfg = _PAIR_CONFIG.get(pair)
        if cfg is None:
            return

        bybit_price, okx_price = self._get_pair_prices(pair)
        if not bybit_price or not okx_price:
            return
        if (now * 1000 - self._prices.ts_ms) > _PRICE_STALE_MS:
            return

        spread     = self._cross_spread(bybit_price, okx_price)
        short_name = pair.replace("USDT", "")

        # ─ Log periodico de estado (cada 30s) ────────────────────────────
        last_log = self._last_log.get(pair, 0.0)
        if now - last_log >= _LOG_INTERVAL_SECS:
            self._last_log[pair] = now
            if spread >= cfg["min_spread"]:
                sign = "+" if okx_price >= bybit_price else "-"
                logger.info(
                    "[cross_arb] 📊 {} Bybit={:.2f} OKX={:.2f} spread={}{:.4f}%",
                    short_name, bybit_price, okx_price, sign, spread,
                )
            else:
                logger.info(
                    "[cross_arb] ❌ {} spread insuficiente {:.4f}% < {:.2f}%",
                    short_name, spread, cfg["min_spread"],
                )

        # ─ Deteccion ─────────────────────────────────────────────────────
        if spread < cfg["min_spread"] or spread > _MAX_SPREAD_PCT:
            return

        # ─ Cooldown ──────────────────────────────────────────────────────
        if now - self._last_check.get(pair, 0.0) < _COOLDOWN_SECS:
            return

        # ─ Calcular P&L neto ─────────────────────────────────────────────
        if bybit_price <= okx_price:
            route      = "A"   # Compra Bybit, Venta OKX
            buy_price  = bybit_price
            sell_price = okx_price
        else:
            route      = "B"   # Compra OKX, Venta Bybit
            buy_price  = okx_price
            sell_price = bybit_price

        size_real = cfg["max_size"] if self._production else cfg["paper_size"]
        gross_pnl = size_real * spread / 100
        fees      = size_real * _TAKER_FEE_PCT / 100 * 2
        net_pnl   = gross_pnl - fees

        if net_pnl < cfg["min_net"]:
            logger.debug(
                "[cross_arb] {} spread={:.4f}% net=${:.4f} < ${} — no ejecutar",
                short_name, spread, net_pnl, cfg["min_net"],
            )
            return

        self._last_check[pair] = now
        self._opportunities.append(now)

        buy_ex  = "Bybit" if route == "A" else "OKX"
        sell_ex = "OKX"   if route == "A" else "Bybit"
        logger.info(
            "[cross_arb] 📊 {} Bybit={:.2f} OKX={:.2f} spread=+{:.4f}% → Compra {} + Venta {}",
            short_name, bybit_price, okx_price, spread, buy_ex, sell_ex,
        )

        _almod = sys.modules.get("alerts")
        if _almod is not None:
            try:
                _almod.record_arb_opportunity()
            except Exception as exc:
                logger.warning("[cross_arb] record_arb_opportunity error: {}", exc)

        trade = TriArbTrade(
            trade_id      = str(uuid.uuid4()),
            pair          = pair,
            route         = route,
            btc_usdt      = bybit_price,
            eth_usdt      = okx_price,
            eth_btc_real  = buy_price,
            eth_btc_impl  = sell_price,
            spread_pct    = round(spread, 4),
            size_usd      = size_real,
            gross_pnl     = round(gross_pnl, 4),
            net_pnl       = round(net_pnl, 4),
            production    = self._production,
        )

        if self._production:
            asyncio.create_task(self._execute_real(trade))
        else:
            self._trades.append(trade)

        asyncio.create_task(self._alert(trade))

    # ── Real execution ────────────────────────────────────────────────────────

    async def _execute_real(self, trade: TriArbTrade) -> None:
        """
        Ejecuta arbitraje cross-exchange real:
          Ruta A: Buy BTC en Bybit Spot + Sell BTC en OKX Spot
          Ruta B: Buy BTC en OKX Spot  + Sell BTC en Bybit Spot
        """
        if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
            logger.warning("[cross_arb] Faltan BYBIT_API_KEY/BYBIT_API_SECRET")
            self._trades.append(trade)
            return
        if not config.OKX_API_KEY or not config.OKX_SECRET or not config.OKX_PASSPHRASE:
            logger.warning("[cross_arb] Faltan credenciales OKX")
            self._trades.append(trade)
            return

        try:
            from pybit.unified_trading import HTTP as BybitHTTP
        except ImportError:
            logger.error("[cross_arb] pybit no disponible")
            self._trades.append(trade)
            return

        pair      = getattr(trade, "pair", "BTCUSDT")
        cfg       = _PAIR_CONFIG.get(pair, _PAIR_CONFIG["BTCUSDT"])
        size_usd  = min(trade.size_usd, cfg["max_size"])
        buy_price = trade.eth_btc_real
        asset_qty = round(size_usd / buy_price, 6) if buy_price > 0 else 0

        if asset_qty <= 0:
            logger.warning("[cross_arb] {} asset_qty <= 0, saltando", pair)
            self._trades.append(trade)
            return

        buy_ex     = "Bybit" if trade.route == "A" else "OKX"
        sell_ex    = "OKX"   if trade.route == "A" else "Bybit"
        okx_symbol = cfg["okx_symbol"]
        logger.info(
            "[cross_arb] Ejecutando: {} Compra {} ${:.0f} @ {:.2f} + Venta {} @ {:.2f}",
            pair, buy_ex, size_usd, trade.eth_btc_real, sell_ex, trade.eth_btc_impl,
        )

        session = BybitHTTP(testnet=False, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET, base_url="https://api.bytick.com")
        loop    = asyncio.get_event_loop()
        _qty    = asset_qty

        if trade.route == "A":
            bybit_fn = lambda: session.place_order(category="spot", symbol=pair, side="Buy",  orderType="Market", qty=str(_qty))
            okx_side = "sell"
        else:
            bybit_fn = lambda: session.place_order(category="spot", symbol=pair, side="Sell", orderType="Market", qty=str(_qty))
            okx_side = "buy"

        try:
            bybit_result, okx_result = await asyncio.gather(
                loop.run_in_executor(None, bybit_fn),
                self._okx_spot_order(okx_symbol, okx_side, _qty, size_usd=size_usd),
                return_exceptions=True,
            )

            bybit_ok = isinstance(bybit_result, dict) and bybit_result.get("retCode") == 0

            if bybit_ok and okx_result is True:
                short_name = pair.replace("USDT", "")
                logger.info(
                    "[cross_arb] ✅ {} EJECUTADO Compra {} + Venta {} net=+${:.4f} spread={:.4f}%",
                    short_name, buy_ex, sell_ex, trade.net_pnl, trade.spread_pct,
                )
                trade.production = True
                self._exec_by_pair[pair] = self._exec_by_pair.get(pair, 0) + 1
                _almod2 = sys.modules.get("alerts")
                if _almod2 is not None:
                  try:
                    _almod2.record_arb_executed(trade.net_pnl)
                    asyncio.create_task(_almod2.send_trade_alert("cross_arb", {
                        "pair":       pair,
                        "route":      trade.route,
                        "buy_ex":     buy_ex,
                        "sell_ex":    sell_ex,
                        "buy_price":  trade.eth_btc_real,
                        "sell_price": trade.eth_btc_impl,
                        "net_pnl":    trade.net_pnl,
                        "size_usd":   size_usd,
                    }))
                  except Exception as exc:
                    logger.warning("[cross_arb] record_arb_executed error: {}", exc)
            else:
                bybit_err = bybit_result if not bybit_ok else "OK"
                okx_err   = okx_result   if okx_result is not True else "OK"
                if not bybit_ok:
                    logger.error("[cross_arb] Bybit leg fallido: {}", bybit_result)
                if okx_result is not True:
                    logger.error("[cross_arb] OKX leg fallido: {}", okx_result)
                asyncio.create_task(self._alert_exec_error(pair, buy_ex, sell_ex, bybit_err, okx_err))

        except Exception as exc:
            logger.error("[cross_arb] _execute_real exception: {}", exc)

        self._trades.append(trade)

    async def _alert_exec_error(self, pair: str, buy_ex: str, sell_ex: str,
                                  bybit_err: object, okx_err: object) -> None:
        """Envia Telegram cuando un leg de ejecucion falla."""
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        msg = (
            f"⚠️ CROSS ARB ERROR — {pair}\n"
            f"Compra {buy_ex}: {bybit_err}\n"
            f"Venta  {sell_ex}: {okx_err}"
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

    async def _okx_spot_order(self, inst_id: str, side: str, sz: float,
                               size_usd: float = 0.0) -> bool:
        """Coloca una orden de mercado en OKX Spot. Retorna True si exitosa."""
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        if side == "buy":
            payload = {
                "instId":  inst_id,
                "tdMode":  "cash",
                "side":    "buy",
                "ordType": "market",
                "sz":      str(round(size_usd, 4)),
                "tgtCcy":  "quote_ccy",
            }
        else:
            payload = {
                "instId":  inst_id,
                "tdMode":  "cash",
                "side":    "sell",
                "ordType": "market",
                "sz":      str(round(sz, 6)),
            }
        body    = json.dumps(payload)
        prehash = ts + "POST" + "/api/v5/trade/order" + body
        sig     = base64.b64encode(
            hmac.new(config.OKX_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
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
                    "https://www.okx.com/api/v5/trade/order",
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    if data.get("code") == "0":
                        return True
                    logger.error("[cross_arb] OKX orden error: {}", data)
                    return False
        except Exception as exc:
            logger.error("[cross_arb] OKX request exception: {}", exc)
            return False

    # ── Telegram alert ────────────────────────────────────────────────────────

    async def _alert(self, trade: TriArbTrade) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        pair      = getattr(trade, "pair", "BTCUSDT")
        mode      = "REAL" if trade.production else "PAPEL"
        buy_name  = pair.replace("USDT", "")
        if trade.route == "A":
            route_desc = f"BUY {buy_name} Bybit @ {trade.btc_usdt:.2f} → SELL OKX @ {trade.eth_usdt:.2f}"
        else:
            route_desc = f"BUY {buy_name} OKX @ {trade.eth_usdt:.2f} → SELL Bybit @ {trade.btc_usdt:.2f}"
        msg = (
            f"[CROSS ARB] {pair} Oportunidad ({mode})\n"
            f"Ruta: {trade.route} — {route_desc}\n"
            f"Spread: {trade.spread_pct:.4f}%\n"
            f"P&L neto: +${trade.net_pnl:.4f}"
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
