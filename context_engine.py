"""
context_engine.py — Whale Follower Bot — Sprint 2
Motor de contexto institucional: Funding Rate, Open Interest, Session Intelligence.

Corre como tarea de fondo (asyncio) y expone sus datos via snapshot().
Todos los errores de API son silenciosos — el bot sigue funcionando sin contexto.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from loguru import logger


# ── Session windows (todas en UTC) ────────────────────────────────────────────
# EST = UTC - 5h (sin DST, ajustar en verano: UTC - 4h)
# Londres  03:00–12:00 UTC
# NY       14:30–21:00 UTC
# Overlap  13:00–16:00 UTC  ← Londres + NY simultáneos
# Asia     21:00–03:00 UTC  (siguiente día)

_SESSION_WINDOWS = [
    # (nombre, utc_hour_start, utc_hour_end, multiplier)
    # Nota: usa UTC — funciona en cualquier servidor (Railway, Contabo, etc.)
    ("overlap",  13, 16, 2.0),   # máxima liquidez — evaluar primero
    ("new_york", 14, 21, 1.5),
    ("london",    3, 12, 1.3),
    ("asia",     21, 27, 0.9),   # crypto opera 24/7, Asia no penalizar tanto
]


@dataclass
class MarketContext:
    """Snapshot del contexto institucional en un momento dado."""
    # Funding Rate (%)
    funding_rate: Optional[float] = None      # None si no disponible
    funding_signal: str = "neutral"           # "bullish" | "bearish" | "neutral"
    funding_pts: int = 0                      # puntos de score (0–8)

    # Open Interest
    oi_change_pct: Optional[float] = None     # cambio porcentual vs muestra anterior
    oi_signal: str = "neutral"                # "confirming" | "trap" | "neutral"
    oi_pts: int = 0                           # puntos de score (0–7)

    # Session
    session_name: str = "unknown"
    session_multiplier: float = 1.0
    session_pts: int = 0                      # puntos de score (0–5)

    # Meta
    last_update: float = field(default_factory=time.time)
    stale: bool = True                        # True hasta el primer update exitoso


class ContextEngine:
    """
    Corre como tarea de fondo y actualiza MarketContext periódicamente.

    Uso en main.py:
        ctx = ContextEngine()
        asyncio.create_task(ctx.run())
        ...
        context = ctx.snapshot()
    """

    _BYBIT_FUNDING_URL = (
        "https://api.bybit.com/v5/market/funding/history"
        "?category=linear&symbol=BTCUSDT&limit=1"
    )
    _BYBIT_OI_URL = (
        "https://api.bybit.com/v5/market/open-interest"
        "?category=linear&symbol=BTCUSDT&intervalTime=5min&limit=2"
    )

    def __init__(
        self,
        funding_interval: int = 300,   # cada 5 minutos
        oi_interval: int = 60,         # cada minuto
    ) -> None:
        self._funding_interval = funding_interval
        self._oi_interval      = oi_interval
        self._ctx = MarketContext()

    # ── API pública ────────────────────────────────────────────────────────────

    def snapshot(self) -> MarketContext:
        """Devuelve copia del contexto actual (thread-safe en asyncio)."""
        # Siempre actualizar la sesión (es cálculo local, sin latencia)
        self._update_session(self._ctx)
        return self._ctx

    async def run(self) -> None:
        """Tarea de fondo — corre para siempre actualizando contexto."""
        logger.info("[context] Motor de contexto iniciado.")
        while True:
            try:
                await asyncio.gather(
                    self._fetch_funding(),
                    self._fetch_oi(),
                    return_exceptions=True,
                )
                self._update_session(self._ctx)
                self._ctx.stale = False
                self._ctx.last_update = time.time()
            except Exception as exc:
                logger.warning(f"[context] Error en update: {exc}")
            await asyncio.sleep(min(self._funding_interval, self._oi_interval))

    # ── Funding Rate ──────────────────────────────────────────────────────────

    async def _fetch_funding(self) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._BYBIT_FUNDING_URL,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            entries = data.get("result", {}).get("list", [])
            if not entries:
                return

            rate = float(entries[0].get("fundingRate", 0))
            self._ctx.funding_rate = round(rate * 100, 4)  # convertir a %

            # Interpretar señal
            # Funding positivo alto → muchos longs pagando → mercado sobrecomprado
            # → liquidaciones inminentes → señal bajista para posiciones largas
            # Funding negativo → shorts pagando → señal alcista (cortos atrapados)
            if rate < -0.0001:   # < -0.01%
                self._ctx.funding_signal = "bullish"
                self._ctx.funding_pts    = 8
            elif rate > 0.0001:  # > 0.01%
                self._ctx.funding_signal = "bearish"
                self._ctx.funding_pts    = 0
            else:
                self._ctx.funding_signal = "neutral"
                self._ctx.funding_pts    = 4  # neutral = punto medio

            logger.debug(
                f"[context] Funding={self._ctx.funding_rate:.4f}% "
                f"→ {self._ctx.funding_signal} (+{self._ctx.funding_pts}pts)"
            )
        except Exception as exc:
            logger.debug(f"[context] Funding fetch error: {exc}")

    # ── Open Interest ─────────────────────────────────────────────────────────

    async def _fetch_oi(self) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._BYBIT_OI_URL,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as resp:
                    if resp.status != 200:
                        return
                    data = await resp.json()

            entries = data.get("result", {}).get("list", [])
            if len(entries) < 2:
                return

            # entries[0] = más reciente, entries[1] = anterior
            oi_now  = float(entries[0].get("openInterest", 0))
            oi_prev = float(entries[1].get("openInterest", 0))
            if oi_prev == 0:
                return

            change_pct = (oi_now - oi_prev) / oi_prev * 100
            self._ctx.oi_change_pct = round(change_pct, 3)

            # OI sube = nuevas posiciones abriéndose = movimiento real
            # OI baja = posiciones cerrándose = posible trampa o liquidación
            if change_pct > 0.1:    # sube > 0.1%
                self._ctx.oi_signal = "confirming"
                self._ctx.oi_pts    = 7
            elif change_pct < -0.1: # baja > 0.1%
                self._ctx.oi_signal = "trap"
                self._ctx.oi_pts    = 0
            else:
                self._ctx.oi_signal = "neutral"
                self._ctx.oi_pts    = 3

            logger.debug(
                f"[context] OI change={self._ctx.oi_change_pct:+.3f}% "
                f"→ {self._ctx.oi_signal} (+{self._ctx.oi_pts}pts)"
            )
        except Exception as exc:
            logger.debug(f"[context] OI fetch error: {exc}")

    # ── Session ───────────────────────────────────────────────────────────────

    @staticmethod
    def _update_session(ctx: MarketContext) -> None:
        utc_hour = datetime.now(timezone.utc).hour

        for name, start, end, mult in _SESSION_WINDOWS:
            # Manejar ventanas que cruzan medianoche (ej: Asia 21–27)
            if end > 24:
                active = utc_hour >= start or utc_hour < (end - 24)
            else:
                active = start <= utc_hour < end

            if active:
                ctx.session_name       = name
                ctx.session_multiplier = mult
                # Puntos según sesión
                if mult >= 2.0:
                    ctx.session_pts = 5
                elif mult >= 1.5:
                    ctx.session_pts = 4
                elif mult >= 1.3:
                    ctx.session_pts = 3
                else:
                    ctx.session_pts = 0   # Asia = sesión débil
                return

        # Fuera de todos los rangos (poco probable)
        ctx.session_name       = "off_hours"
        ctx.session_multiplier = 0.9
        ctx.session_pts        = 0
