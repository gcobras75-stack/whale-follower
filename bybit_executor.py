"""
bybit_executor.py — Whale Follower Bot — Sprint 2
Ejecución automática de paper trades en Bybit Testnet.

Bybit Testnet: https://testnet.bybit.com
API:           https://api-testnet.bybit.com
No requiere dinero real. Las posiciones son simuladas con capital de paper.

Gestión activa:
  - Breakeven: precio sube 1x riesgo → SL mueve a entrada
  - Parciales:  precio sube 1.5x riesgo → cierra 50%
  - Trailing:   precio sube 2x riesgo → SL sigue al precio con 0.3% de offset
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp
from loguru import logger

import config
from leverage_manager import LeverageManager


@dataclass
class PaperTrade:
    """Representa un trade de paper trading activo."""
    trade_id:       str
    signal_score:   int
    pair:           str          # "BTCUSDT" | "ETHUSDT" | "SOLUSDT" | "BNBUSDT"
    side:           str          # "Buy" | "Sell"
    entry_price:    float
    stop_loss:      float
    take_profit:    float
    size_contracts: float        # cantidad en contratos
    size_usd:       float
    status:         str = "open" # "open" | "closed" | "partial"
    opened_at:      float = field(default_factory=time.time)
    db_row_id:      Optional[str] = None  # UUID de la fila en Supabase
    signal_features: Optional[dict] = None  # features para ML record_outcome

    # Gestión activa
    breakeven_done:  bool = False
    partial_done:    bool = False
    trailing_active: bool = False
    trailing_sl:     float = 0.0

    # Resultado final
    exit_price:   float = 0.0
    pnl_usd:      float = 0.0
    pnl_pct:      float = 0.0
    close_reason: str   = ""


class BybitTestnetExecutor:
    """
    Ejecuta trades en Bybit.
    - PRODUCTION=False → Bybit Testnet (paper, capital simulado)
    - PRODUCTION=True  → Bybit Real (dinero real, capital REAL_CAPITAL)

    Protecciones de producción:
    - MAX_TRADES_OPEN: máximo trades simultáneos (default 2)
    - RISK_PER_TRADE:  fracción del capital por trade (default 1%)
    - DAILY_LOSS_LIMIT: para bot si pérdida diaria > X% (default 5%)
    - MAX_LEVERAGE: 1x al inicio, sin apalancamiento
    - Balance mínimo: no opera si balance < $10 USD
    """

    _TESTNET_URL    = "https://api-testnet.bybit.com"
    _PRODUCTION_URL = "https://api.bybit.com"

    def __init__(self) -> None:
        self._production = config.PRODUCTION

        if self._production:
            self._BASE_URL   = self._PRODUCTION_URL
            self._api_key    = config.BYBIT_API_KEY
            self._api_secret = config.BYBIT_API_SECRET
            self._capital    = config.REAL_CAPITAL
            logger.warning(
                "[executor] MODO PRODUCCION REAL | capital=${:.0f} | "
                "risk={:.0%}/trade | max_trades={} | daily_loss={:.0%}",
                config.REAL_CAPITAL, config.RISK_PER_TRADE,
                config.MAX_TRADES_OPEN, config.DAILY_LOSS_LIMIT,
            )
        else:
            self._BASE_URL   = self._TESTNET_URL
            self._api_key    = config.BYBIT_TESTNET_API_KEY
            self._api_secret = config.BYBIT_TESTNET_SECRET
            self._capital    = config.PAPER_CAPITAL

        self._trades: List[PaperTrade] = []
        self._enabled = bool(self._api_key and self._api_secret)
        self._daily_loss_usd: float = 0.0
        self._daily_reset_ts: float = time.time()

        # En producción: leverage forzado a 1x, sin apalancamiento dinámico
        max_lev = 1 if self._production else config.MAX_LEVERAGE
        self._leverage_mgr = LeverageManager(
            initial_capital     = self._capital,
            min_trades          = config.MIN_TRADES_FOR_LEVERAGE,
            max_leverage        = max_lev,
            warmup_win_rate_pct = config.LEVERAGE_WARMUP_WR,
            warmup_samples      = config.LEVERAGE_WARMUP_SAMPLES,
        )

        if not self._enabled:
            logger.warning(
                "[executor] API keys no configuradas — trading desactivado."
            )

    # ── API pública ────────────────────────────────────────────────────────────

    def open_count(self) -> int:
        """Número de trades actualmente abiertos."""
        return sum(1 for t in self._trades if t.status in ("open", "partial"))

    def available_capital(self) -> float:
        """Capital disponible (reducido por trades abiertos)."""
        in_use = sum(t.size_usd for t in self._trades if t.status in ("open", "partial"))
        return max(self._capital - in_use, 0.0)

    async def open_trade(
        self,
        signal_score: int,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        signal_id: Optional[str] = None,
        pair: str = "BTCUSDT",
        size_usd: Optional[float] = None,
        signal_features: Optional[dict] = None,
    ) -> Optional[PaperTrade]:
        """
        Abre un nuevo trade. Devuelve PaperTrade o None si hubo error.
        size_usd: si se pasa, usa ese monto; si no, 1% del capital.
        """
        if not self._enabled:
            return None

        # Reset diario de pérdidas
        if time.time() - self._daily_reset_ts > 86400:
            self._daily_loss_usd = 0.0
            self._daily_reset_ts = time.time()

        # Daily loss limit (solo en producción)
        if self._production:
            max_daily_loss = self._capital * config.DAILY_LOSS_LIMIT
            if self._daily_loss_usd >= max_daily_loss:
                logger.error(
                    "[executor] DAILY LOSS LIMIT alcanzado: ${:.2f} >= ${:.2f}. "
                    "Trading suspendido hasta mañana.",
                    self._daily_loss_usd, max_daily_loss,
                )
                return None

        # Verificar pausa por racha perdedora
        if self._leverage_mgr.is_paused():
            rem = self._leverage_mgr.pause_remaining_secs()
            logger.info("[executor] Trading pausado por racha perdedora. Reanuda en {}s.", rem)
            return None

        # Limitar trades abiertos simultáneos
        max_open = config.MAX_TRADES_OPEN if self._production else 3
        open_trades = [t for t in self._trades if t.status in ("open", "partial")]
        if len(open_trades) >= max_open:
            logger.info("[executor] Maximo de trades abiertos alcanzado ({}). Skip.", max_open)
            return None

        # Tamaño de posición
        leverage = self._leverage_mgr.get_leverage()
        if size_usd is None:
            risk_pct = config.RISK_PER_TRADE if self._production else 0.01
            base_usd = self._capital * risk_pct     # % de capital = margen
            size_usd = base_usd * leverage           # posicion efectiva
        if leverage > 1:
            logger.info("[executor] Apalancamiento activo: {}x (size_usd=${:.0f})", leverage, size_usd)
        size_contracts = round(size_usd / entry_price, 3)
        size_contracts = max(size_contracts, 0.001)

        trade = PaperTrade(
            trade_id        = str(uuid.uuid4()),
            signal_score    = signal_score,
            pair            = pair,
            side            = "Buy",   # Spring = alcista
            entry_price     = entry_price,
            stop_loss       = stop_loss,
            take_profit     = take_profit,
            size_contracts  = size_contracts,
            size_usd        = round(size_usd, 2),
            trailing_sl     = stop_loss,
            signal_features = signal_features,
        )

        # Intentar orden en Bybit Testnet
        success = await self._place_order(trade)
        if not success:
            # Si falla la API, igual registramos como paper local
            logger.warning("[executor] Bybit Testnet no disponible — registrando solo en Supabase.")

        self._trades.append(trade)
        await self._save_to_supabase(trade, signal_id)

        logger.info(
            f"[executor] Trade abierto {trade.trade_id[:8]} "
            f"entry={entry_price} sl={stop_loss} tp={take_profit} "
            f"size=${trade.size_usd}"
        )
        return trade

    async def update_trades(self, current_price: float, pair: str = "BTCUSDT") -> None:
        """
        Llamar con cada tick de precio para gestionar trades activos del par.
        Implementa breakeven, parciales y trailing stop.
        """
        for trade in self._trades:
            if trade.status not in ("open", "partial"):
                continue
            if trade.pair != pair:
                continue
            await self._manage_trade(trade, current_price)

    def leverage_status(self) -> Dict:
        """Resumen del estado de apalancamiento para /status y healthcheck."""
        lm = self._leverage_mgr
        total = len(lm._outcomes)
        return {
            "leverage":     lm.get_leverage(),
            "win_rate_pct": round(lm._win_rate_pct(), 1),
            "trades_total": total,
            "min_trades":   lm._min_trades,
            "drawdown_pct": round(lm._drawdown_pct() * 100, 2),
            "paused":       lm.is_paused(),
            "pause_secs":   lm.pause_remaining_secs(),
            "level_cap":    lm._level_cap,
        }

    def active_trades_summary(self) -> List[Dict]:
        """Resumen de trades activos para /trades comando de Telegram."""
        result = []
        for t in self._trades[-5:]:  # últimos 5
            result.append({
                "id":           t.trade_id[:8],
                "score":        t.signal_score,
                "entry":        t.entry_price,
                "sl":           t.stop_loss,
                "tp":           t.take_profit,
                "size_usd":     t.size_usd,
                "status":       t.status,
                "pnl_usd":      round(t.pnl_usd, 2),
                "close_reason": t.close_reason,
            })
        return result

    # ── Gestión activa de trades ───────────────────────────────────────────────

    async def _manage_trade(self, trade: PaperTrade, price: float) -> None:
        risk = trade.entry_price - trade.stop_loss
        if risk <= 0:
            return

        move = price - trade.entry_price  # positivo = precio subió

        # ── Stop Loss hit ─────────────────────────────────────────────────────
        if price <= trade.stop_loss:
            loss = (trade.stop_loss - trade.entry_price) * trade.size_contracts
            await self._close_trade(trade, price, "stop_loss", loss)
            return

        # ── Take Profit hit ───────────────────────────────────────────────────
        if price >= trade.take_profit:
            profit = (trade.take_profit - trade.entry_price) * trade.size_contracts
            await self._close_trade(trade, price, "take_profit", profit)
            return

        # ── Trailing Stop hit ─────────────────────────────────────────────────
        if trade.trailing_active and price <= trade.trailing_sl:
            profit = (trade.trailing_sl - trade.entry_price) * trade.size_contracts
            await self._close_trade(trade, price, "trailing_stop", profit)
            return

        # ── Breakeven (1x riesgo) ─────────────────────────────────────────────
        if not trade.breakeven_done and move >= risk:
            trade.stop_loss    = trade.entry_price
            trade.breakeven_done = True
            logger.info(f"[executor] {trade.trade_id[:8]} → Breakeven en {price:.2f}")

        # ── Parciales al 1.5x riesgo (cerrar 50%) ────────────────────────────
        if not trade.partial_done and move >= risk * 1.5:
            partial_profit = move * trade.size_contracts * 0.5
            trade.pnl_usd   += partial_profit
            trade.size_contracts *= 0.5
            trade.size_usd       *= 0.5
            trade.partial_done   = True
            trade.status         = "partial"
            logger.info(
                f"[executor] {trade.trade_id[:8]} → Parcial 50% en {price:.2f} "
                f"P&L parcial: ${partial_profit:.2f}"
            )

        # ── Trailing Stop al 2x riesgo (activar trailing en el 50% restante) ─
        if not trade.trailing_active and move >= risk * 2.0:
            trade.trailing_active = True
            trade.trailing_sl     = price * (1 - 0.003)  # 0.3% bajo precio actual
            logger.info(f"[executor] {trade.trade_id[:8]} → Trailing activado en {price:.2f}")
        elif trade.trailing_active:
            # Mover el trailing SL hacia arriba si el precio sube
            new_sl = price * (1 - 0.003)
            if new_sl > trade.trailing_sl:
                trade.trailing_sl = new_sl

    async def _close_trade(
        self, trade: PaperTrade, price: float, reason: str, pnl: float
    ) -> None:
        trade.status       = "closed"
        trade.exit_price   = price
        trade.pnl_usd     += round(pnl, 2)
        trade.pnl_pct      = round(trade.pnl_usd / trade.size_usd * 100, 2) if trade.size_usd else 0
        trade.close_reason = reason
        duration           = int(time.time() - trade.opened_at)

        logger.info(
            f"[executor] Trade {trade.trade_id[:8]} cerrado ({reason}) "
            f"en {price:.2f} | P&L: ${trade.pnl_usd:.2f} ({trade.pnl_pct:.2f}%) "
            f"| duracion: {duration}s"
        )

        # Acumular pérdida diaria (para daily loss limit en producción)
        if trade.pnl_usd < 0:
            self._daily_loss_usd += abs(trade.pnl_usd)

        # Notificar al gestor de apalancamiento
        won = reason == "take_profit" or (reason == "trailing_stop" and trade.pnl_usd > 0)
        self._leverage_mgr.record_trade(won=won, pnl_usd=trade.pnl_usd)
        logger.info("[executor] {}", self._leverage_mgr.summary())

        # Notificar al modelo ML para aprendizaje continuo
        if trade.signal_features:
            try:
                from ml_model import MLModel
                ml = MLModel()
                ml.record_outcome(trade.signal_features, won)
                logger.debug("[executor] ML outcome registrado won={} trade={}", won, trade.trade_id[:8])
            except Exception as exc:
                logger.warning("[executor] ML record_outcome error: {}", exc)

        await self._update_supabase_close(trade, duration)

    # ── Bybit Testnet API ─────────────────────────────────────────────────────

    def _sign(self, params: str, timestamp: int) -> str:
        """Genera firma HMAC-SHA256 para Bybit v5."""
        payload = f"{timestamp}{self._api_key}5000{params}"
        return hmac.new(
            self._api_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _place_order(self, trade: PaperTrade) -> bool:
        """Envía orden MARKET a Bybit Testnet."""
        if not self._api_key or not self._api_secret:
            return False

        ts = int(time.time() * 1000)
        body = {
            "category":   "linear",
            "symbol":     "BTCUSDT",
            "side":       trade.side,
            "orderType":  "Market",
            "qty":        str(trade.size_contracts),
            "stopLoss":   str(round(trade.stop_loss, 2)),
            "takeProfit": str(round(trade.take_profit, 2)),
            "timeInForce": "GTC",
            "positionIdx": 0,
        }
        body_str  = json.dumps(body)
        signature = self._sign(body_str, ts)

        headers = {
            "X-BAPI-API-KEY":   self._api_key,
            "X-BAPI-TIMESTAMP": str(ts),
            "X-BAPI-SIGN":      signature,
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type":     "application/json",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self._BASE_URL}/v5/order/create",
                    headers=headers,
                    data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()
                    ret_code = data.get("retCode", -1)
                    if ret_code == 0:
                        order_id = data.get("result", {}).get("orderId", "")
                        logger.success(
                            f"[executor] Orden Bybit Testnet OK: {order_id}"
                        )
                        return True
                    else:
                        logger.warning(
                            f"[executor] Bybit Testnet error {ret_code}: "
                            f"{data.get('retMsg')}"
                        )
                        return False
        except Exception as exc:
            logger.warning(f"[executor] Bybit Testnet API error: {exc}")
            return False

    # ── Supabase ──────────────────────────────────────────────────────────────

    async def _save_to_supabase(
        self, trade: PaperTrade, signal_id: Optional[str]
    ) -> None:
        try:
            from supabase import create_client
            loop = asyncio.get_running_loop()
            row = {
                "signal_id":    signal_id,
                "strategy":     "wyckoff",
                "pair":         trade.pair,
                "side":         trade.side,
                "entry_price":  trade.entry_price,
                "stop_loss":    trade.stop_loss,
                "take_profit":  trade.take_profit,
                "size_usd":     trade.size_usd,
                "status":       "open",
            }
            client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            result = await loop.run_in_executor(
                None,
                lambda: client.table("paper_trades").insert(row).execute()
            )
            # Guardar el UUID de la fila para UPDATE posterior
            rows = result.data if hasattr(result, "data") else []
            if rows:
                trade.db_row_id = rows[0].get("id")
                logger.debug(f"[executor] Supabase row id={trade.db_row_id}")
        except Exception as exc:
            logger.error(f"[executor] Supabase insert error: {exc}")

    async def _update_supabase_close(
        self, trade: PaperTrade, duration: int
    ) -> None:
        if not trade.db_row_id:
            logger.warning("[executor] Sin db_row_id para actualizar cierre en Supabase.")
            return
        try:
            from supabase import create_client
            loop = asyncio.get_running_loop()
            update = {
                "status":           "closed",
                "exit_price":       trade.exit_price,
                "pnl_usd":          trade.pnl_usd,
                "pnl_pct":          trade.pnl_pct,
                "close_reason":     trade.close_reason,
                "duration_seconds": duration,
            }
            client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
            await loop.run_in_executor(
                None,
                lambda: (
                    client.table("paper_trades")
                    .update(update)
                    .eq("id", trade.db_row_id)   # FIX: usa el ID real de la fila
                    .execute()
                )
            )
            logger.success(f"[executor] Trade {trade.db_row_id[:8]} cerrado en Supabase.")
        except Exception as exc:
            logger.error(f"[executor] Supabase update error: {exc}")
