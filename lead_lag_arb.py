# -*- coding: utf-8 -*-
"""
lead_lag_arb.py -- Whale Follower Bot
Lead-Lag Arbitrage: BTC spring predice movimiento en altcoins.

Estrategia:
  Cuando BTC detecta un spring (senial de inversion alcista),
  abrir LONG en ETH/SOL/BNB con probabilidad historica:
    ETH: 73% de seguir a BTC en < 60s
    SOL: 61% de seguir a BTC en < 90s
    BNB: 58% de seguir a BTC en < 90s

  Las posiciones se cierran automaticamente:
    - Take profit: entrada + lead_ratio * BTC_move
    - Stop loss:   entrada - 0.3% (riesgo minimo)

En papel: simula posiciones y calcula P&L al cierre.
En real:  ejecuta ordenes Bybit al detectar la senal.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
_LEAD_LAG_PAIRS: Dict[str, dict] = {
    "ETHUSDT": {"prob": 0.73, "window_secs": 60,  "lead_ratio": 0.85, "size_usd": 300.0},
    "SOLUSDT": {"prob": 0.61, "window_secs": 90,  "lead_ratio": 0.70, "size_usd": 150.0},
    "BNBUSDT": {"prob": 0.58, "window_secs": 90,  "lead_ratio": 0.60, "size_usd": 150.0},
}
_STOP_LOSS_PCT   = 0.003    # 0.3% stop loss
_MIN_BTC_MOVE    = 0.002    # BTC debe moverse >= 0.2% para que haya trailing TP
_COOLDOWN_SECS   = 120      # no abrir otro lote por 2 min


@dataclass
class LeadLagTrade:
    trade_id:     str
    pair:         str
    entry_price:  float
    stop_loss:    float
    take_profit:  float
    size_usd:     float
    btc_entry:    float
    opened_at:    float = field(default_factory=time.time)
    closed_at:    Optional[float] = None
    exit_price:   float = 0.0
    pnl_usd:      float = 0.0
    status:       str   = "open"   # "open" | "closed"
    production:   bool  = False


@dataclass
class LeadLagSnapshot:
    active_trades:     int   = 0
    trades_total:      int   = 0
    pnl_total_usd:     float = 0.0
    last_trigger_ts:   float = 0.0
    triggers_1h:       int   = 0


class LeadLagArb:
    """
    Escucha seniales BTC spring del main loop y abre posiciones en altcoins.

    Uso:
        arb = LeadLagArb(production=False)
        # Cada vez que BTC detecta spring:
        arb.on_btc_spring(btc_price=65000.0)
        # Cada tick de precio:
        arb.update_price("ETHUSDT", 3100.0)
    """

    def __init__(self, production: bool = False) -> None:
        self._production = production
        self._trades: List[LeadLagTrade] = []
        self._last_trigger: float = 0.0
        self._current_prices: Dict[str, float] = {}
        self._triggers: List[float] = []

        mode = "REAL" if production else "PAPEL"
        logger.info("[lead_lag] Iniciado en modo {} | pares={}", mode,
                    list(_LEAD_LAG_PAIRS.keys()))

    # ── Public API ─────────────────────────────────────────────────────────────

    def on_btc_spring(self, btc_price: float) -> None:
        """Llamar cuando el spring detector de BTC emite senal."""
        now = time.time()
        if now - self._last_trigger < _COOLDOWN_SECS:
            return

        self._last_trigger = now
        self._triggers.append(now)
        logger.info("[lead_lag] BTC spring @ {:.2f} — abriendo altcoins", btc_price)

        for pair, cfg in _LEAD_LAG_PAIRS.items():
            entry = self._current_prices.get(pair)
            if not entry:
                logger.debug("[lead_lag] Sin precio para {}, skip", pair)
                continue

            sl  = entry * (1 - _STOP_LOSS_PCT)
            tp  = entry * (1 + cfg["lead_ratio"] * _MIN_BTC_MOVE * 3)

            trade = LeadLagTrade(
                trade_id    = str(uuid.uuid4()),
                pair        = pair,
                entry_price = entry,
                stop_loss   = sl,
                take_profit = tp,
                size_usd    = cfg["size_usd"],
                btc_entry   = btc_price,
                production  = self._production,
            )

            if self._production:
                asyncio.create_task(self._execute_real(trade))
            else:
                logger.info(
                    "[lead_lag] PAPEL: {} LONG ${:.0f} entry={:.4f} sl={:.4f} tp={:.4f}",
                    pair, cfg["size_usd"], entry, sl, tp,
                )
                self._trades.append(trade)

            # Programar cierre automatico por ventana de tiempo
            asyncio.create_task(self._auto_close(trade, cfg["window_secs"]))

    def update_price(self, pair: str, price: float) -> None:
        """Recibir tick de precio para gestionar SL/TP."""
        self._current_prices[pair] = price
        self._check_sl_tp(pair, price)

    def snapshot(self) -> LeadLagSnapshot:
        now = time.time()
        open_trades  = [t for t in self._trades if t.status == "open"]
        total_pnl    = sum(t.pnl_usd for t in self._trades if t.status == "closed")
        triggers_1h  = sum(1 for ts in self._triggers if now - ts < 3600)
        return LeadLagSnapshot(
            active_trades   = len(open_trades),
            trades_total    = len(self._trades),
            pnl_total_usd   = round(total_pnl, 4),
            last_trigger_ts = self._last_trigger,
            triggers_1h     = triggers_1h,
        )

    def active_summary(self) -> List[dict]:
        return [
            {
                "id":          t.trade_id[:8],
                "pair":        t.pair,
                "entry":       t.entry_price,
                "sl":          round(t.stop_loss, 4),
                "tp":          round(t.take_profit, 4),
                "size_usd":    t.size_usd,
                "status":      t.status,
                "pnl":         round(t.pnl_usd, 4),
            }
            for t in self._trades[-10:]
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _check_sl_tp(self, pair: str, price: float) -> None:
        for trade in self._trades:
            if trade.pair != pair or trade.status != "open":
                continue
            if price <= trade.stop_loss:
                self._close_trade(trade, price, "stop_loss")
            elif price >= trade.take_profit:
                self._close_trade(trade, price, "take_profit")

    def _close_trade(self, trade: LeadLagTrade, exit_price: float, reason: str) -> None:
        if trade.status != "open":
            return
        trade.status    = "closed"
        trade.closed_at = time.time()
        trade.exit_price = exit_price
        pnl_pct  = (exit_price - trade.entry_price) / trade.entry_price
        trade.pnl_usd = trade.size_usd * pnl_pct

        logger.info(
            "[lead_lag] CIERRE {} {} exit={:.4f} pnl={:+.4f} USD ({})",
            trade.pair, trade.trade_id[:8], exit_price, trade.pnl_usd, reason,
        )
        asyncio.create_task(self._alert_close(trade, reason))

    async def _auto_close(self, trade: LeadLagTrade, window_secs: float) -> None:
        await asyncio.sleep(window_secs)
        if trade.status != "open":
            return
        price = self._current_prices.get(trade.pair, trade.entry_price)
        self._close_trade(trade, price, "timeout")

    # ── Real execution ────────────────────────────────────────────────────────

    async def _execute_real(self, trade: LeadLagTrade) -> None:
        """
        Para activar con dinero real:
          1. Configurar BYBIT_API_KEY real en .env
          2. Ejecutar market order en Bybit para el altcoin
          3. Gestionar SL/TP via ordenes condicionadas en Bybit

        Por ahora registra la operacion en papel aunque production=True
        hasta que las claves reales esten configuradas.
        """
        logger.warning(
            "[lead_lag] REAL execution pendiente — "
            "necesita BYBIT_API_KEY real y bybit_executor.market_order()."
        )
        # Cuando este configurado:
        # await bybit_executor.market_order(trade.pair, "Buy", trade.size_usd)
        self._trades.append(trade)

    # ── Telegram alert ────────────────────────────────────────────────────────

    async def _alert_close(self, trade: LeadLagTrade, reason: str) -> None:
        token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            return
        mode  = "REAL" if trade.production else "PAPEL"
        emoji = "✅" if trade.pnl_usd >= 0 else "❌"
        msg = (
            f"{emoji} [LEAD-LAG ARB] Cierre ({mode})\n"
            f"Par: {trade.pair}\n"
            f"Entrada: ${trade.entry_price:,.4f}\n"
            f"Salida:  ${trade.exit_price:,.4f}\n"
            f"Motivo: {reason}\n"
            f"P&L: {'+' if trade.pnl_usd >= 0 else ''}{trade.pnl_usd:.4f} USD"
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
