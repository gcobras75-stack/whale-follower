# -*- coding: utf-8 -*-
"""
range_trader.py — Whale Follower Bot — Sprint 6
Estrategia de mercado lateral: RSI + Bollinger Band Bounce.

Lógica:
  LONG  cuando RSI < 32 Y precio <= BB_lower  (oversold en soporte)
  SHORT cuando RSI > 68 Y precio >= BB_upper  (overbought en resistencia)
  TP    = BB_middle (reversión a la media)
  SL    = entry ± BB_half_width * 0.4  (stop ajustado al rango)

Solo opera cuando market_regime == LATERAL (o cuando se fuerza).
No requiere spring detection — opera en cada precio.
Persiste trades en Supabase via db_writer.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Optional

from loguru import logger

import config
from market_regime import MarketRegimeDetector, Regime, _bollinger_bands, _rsi

try:
    import db_writer
    _HAS_DB = True
except ImportError:
    _HAS_DB = False


# ── Parámetros ────────────────────────────────────────────────────────────────
_RSI_OVERSOLD     = 32     # umbral entrada long
_RSI_OVERBOUGHT   = 68     # umbral entrada short
_BB_PERIOD        = 20
_RSI_PERIOD       = 14
_SAMPLE_INTERVAL  = 15.0   # segundos entre muestras (mismo que regime detector)
_MAX_OPEN_TRADES  = 2      # máximo trades range abiertos simultáneos
_MIN_BB_WIDTH_PCT = 0.005  # no operar si BB demasiado estrecho (sin rango)
_TIMEOUT_SECS     = 900    # cierre forzado a los 15 min si no llega a TP/SL


# ── Dataclasses ───────────────────────────────────────────────────────────────
@dataclass
class RangeTrade:
    trade_id:    str
    pair:        str
    side:        str    # "long" | "short"
    entry_price: float
    tp:          float
    sl:          float
    size_usd:    float
    open_ts:     float = field(default_factory=time.time)
    production:  bool = False

    @property
    def pnl_usd(self) -> float:
        return 0.0   # calculado al cierre


# ── Motor ─────────────────────────────────────────────────────────────────────
class RangeTrader:
    """
    Llamar on_price() en cada tick del loop principal.
    No necesita señal de spring — opera de forma autónoma.
    """

    def __init__(
        self,
        executor,
        regime_detector: MarketRegimeDetector,
        production: bool = False,
    ) -> None:
        self._executor       = executor
        self._regime         = regime_detector
        self._production     = production
        self._prices: Dict[str, deque] = {}
        self._last_sample: Dict[str, float] = {}
        self._open_trades: Dict[str, RangeTrade] = {}   # trade_id → trade
        self._cooldown: Dict[str, float] = {}            # pair → ts último trade

    # ── API pública ───────────────────────────────────────────────────────────

    async def on_price(self, pair: str, price: float) -> None:
        """Llamar en cada tick. Internamente muestrea y evalúa."""
        # Muestrear precio
        now = time.time()
        last = self._last_sample.get(pair, 0.0)
        if now - last < _SAMPLE_INTERVAL:
            # Aún evaluar cierres aunque no haya nueva muestra
            await self._check_closes(pair, price)
            return

        if pair not in self._prices:
            self._prices[pair] = deque(maxlen=_BB_PERIOD * 3)
        self._prices[pair].append(price)
        self._last_sample[pair] = now

        await self._check_closes(pair, price)
        await self._try_entry(pair, price)

    def open_count(self) -> int:
        return len(self._open_trades)

    # ── Entradas ──────────────────────────────────────────────────────────────

    async def _try_entry(self, pair: str, price: float) -> None:
        closes = list(self._prices.get(pair, []))
        if len(closes) < _BB_PERIOD + _RSI_PERIOD:
            return

        # Solo operar en régimen lateral
        regime = self._regime.regime(pair)
        if regime not in (Regime.LATERAL, Regime.UNKNOWN):
            return

        # Respetar cooldown (evitar entradas repetidas en el mismo nivel)
        cooldown_end = self._cooldown.get(pair, 0.0)
        if time.time() < cooldown_end:
            return

        # Máximo trades abiertos
        pair_open = sum(1 for t in self._open_trades.values() if t.pair == pair)
        if pair_open >= 1 or len(self._open_trades) >= _MAX_OPEN_TRADES:
            return

        # Indicadores
        mid, upper, lower = _bollinger_bands(closes, _BB_PERIOD)
        bb_half = (upper - lower) / 2.0
        bb_width_pct = (upper - lower) / mid if mid > 0 else 0.0
        rsi_val = _rsi(closes, _RSI_PERIOD)

        # No operar si BB demasiado estrecho (spreads vs ganancia)
        if bb_width_pct < _MIN_BB_WIDTH_PCT:
            return

        side: Optional[str] = None

        if rsi_val <= _RSI_OVERSOLD and price <= lower * 1.001:
            side = "long"
        elif rsi_val >= _RSI_OVERBOUGHT and price >= upper * 0.999:
            side = "short"

        if side is None:
            return

        # Calcular TP / SL
        if side == "long":
            tp = mid                           # TP = media BB
            sl = price - bb_half * 0.40        # SL = 40% del semi-rango abajo
        else:
            tp = mid
            sl = price + bb_half * 0.40

        # Verificar que RR sea mínimo 1.5:1
        reward = abs(tp - price)
        risk   = abs(price - sl)
        if risk == 0 or reward / risk < 1.5:
            return

        # Tamaño de posición
        capital = config.REAL_CAPITAL if self._production else config.PAPER_CAPITAL
        size_mult = self._regime.size_multiplier(pair)
        size_usd = round(capital * config.RISK_PER_TRADE * size_mult, 2)

        trade = RangeTrade(
            trade_id    = str(uuid.uuid4())[:8],
            pair        = pair,
            side        = side,
            entry_price = price,
            tp          = round(tp, 4),
            sl          = round(sl, 4),
            size_usd    = size_usd,
            production  = self._production,
        )
        self._open_trades[trade.trade_id] = trade
        self._cooldown[pair] = time.time() + 180   # 3 min cooldown por par

        logger.info(
            "[range] {} {} {} entry={:.4f} tp={:.4f} sl={:.4f} "
            "RSI={:.0f} BB_w={:.2f}% size=${:.1f}",
            side.upper(), pair, "REAL" if self._production else "PAPER",
            price, tp, sl, rsi_val, bb_width_pct * 100, size_usd,
        )

        # Persistir en Supabase
        if _HAS_DB:
            asyncio.create_task(
                db_writer.save_range_open(trade)
            )

    # ── Cierres ───────────────────────────────────────────────────────────────

    async def _check_closes(self, pair: str, price: float) -> None:
        to_close: list[tuple[str, str, float]] = []

        for tid, trade in list(self._open_trades.items()):
            if trade.pair != pair:
                continue

            reason: Optional[str] = None
            exit_price = price

            if trade.side == "long":
                if price >= trade.tp:
                    reason = "tp"
                elif price <= trade.sl:
                    reason = "sl"
            else:
                if price <= trade.tp:
                    reason = "tp"
                elif price >= trade.sl:
                    reason = "sl"

            # Timeout
            if reason is None and time.time() - trade.open_ts > _TIMEOUT_SECS:
                reason = "timeout"

            if reason:
                to_close.append((tid, reason, exit_price))

        for tid, reason, exit_price in to_close:
            trade = self._open_trades.pop(tid)
            await self._close(trade, reason, exit_price)

    async def _close(self, trade: RangeTrade, reason: str, exit_price: float) -> None:
        if trade.side == "long":
            pnl = (exit_price - trade.entry_price) / trade.entry_price * trade.size_usd
        else:
            pnl = (trade.entry_price - exit_price) / trade.entry_price * trade.size_usd

        # Descontar fees 0.11% round-trip
        fee = trade.size_usd * 0.0011
        pnl_net = pnl - fee

        logger.info(
            "[range] CIERRE {} {} @ {:.4f} pnl={:+.4f} net={:+.4f} ({})",
            trade.side.upper(), trade.pair, exit_price, pnl, pnl_net, reason,
        )

        if _HAS_DB:
            asyncio.create_task(
                db_writer.save_range_close(trade, reason, exit_price, pnl_net)
            )
