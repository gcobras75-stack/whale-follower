# -*- coding: utf-8 -*-
"""
dxy_monitor.py -- Whale Follower Bot
Termometro 3: Fuerza del dolar (DXY) via Yahoo Finance (sin API key).

Poll: cada 10 minutos.
Logica:
  DXY sube > 0.3% en 1h → dolar fuerte → BTC tiende a bajar  → -5pts
  DXY baja > 0.3% en 1h → dolar debil  → BTC tiende a subir  → +5pts
  DXY estable (±0.1%)   → neutral                             → 0pts
"""
from __future__ import annotations

import asyncio
import time
from collections import deque

import aiohttp
from loguru import logger

_URL       = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=5m&range=1d"
_URL_ALT   = "https://query2.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=5m&range=1d"
_POLL_SECS = 600     # 10 minutos
_MOVE_UP   = 0.30    # DXY +0.3% en 1h = dolar fuerte
_MOVE_DOWN = -0.30   # DXY -0.3% en 1h = dolar debil
_HEADERS   = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}


class DxyMonitor:
    """
    Monitorea el indice del dolar (DXY) para ajustar el bias de senales BTC.
    Solo lectura — sin ejecucion de ordenes.
    """

    def __init__(self) -> None:
        self._value:      float = 0.0
        self._change_1h:  float = 0.0
        self._signal:     str   = "neutral"
        self._history:    deque = deque(maxlen=24)   # 24 x 5m = 2h buffer
        self._updated:    float = 0.0

        logger.info(
            "[dxy] Monitor iniciado | poll={}s | umbral_fuerte=+{:.1f}% umbral_debil={:.1f}%",
            _POLL_SECS, _MOVE_UP, _MOVE_DOWN,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        while True:
            await self._fetch()
            await asyncio.sleep(_POLL_SECS)

    # ── Public API ─────────────────────────────────────────────────────────────

    def adjustment(self) -> int:
        """Ajuste de score segun movimiento del DXY en la ultima hora."""
        if self._value <= 0:
            return 0
        if self._change_1h > _MOVE_UP:
            return -5
        if self._change_1h < _MOVE_DOWN:
            return +5
        return 0

    def snapshot(self) -> dict:
        return {
            "value":         round(self._value, 2),
            "change_1h_pct": round(self._change_1h, 3),
            "signal":        self._signal,
            "adj_pts":       self.adjustment(),
            "updated_secs":  int(time.time() - self._updated) if self._updated else -1,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch(self) -> None:
        for url in (_URL, _URL_ALT):
            try:
                async with aiohttp.ClientSession(headers=_HEADERS) as s:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self._parse(data)
                            return
                        logger.warning("[dxy] HTTP {} en {}", resp.status, url)
            except Exception as exc:
                logger.warning("[dxy] Fetch error ({}): {} — probando alternativa", url, exc)

        logger.warning("[dxy] Ambas URLs fallaron — reintento en {}s", _POLL_SECS)

    def _parse(self, data: dict) -> None:
        try:
            result = data["chart"]["result"]
            if not result:
                return
            closes = result[0]["indicators"]["quote"][0].get("close", [])
            closes = [c for c in closes if c is not None]
            if len(closes) < 2:
                return

            self._value   = closes[-1]
            self._updated = time.time()

            # 1h change: ultimos 12 candles de 5m = 60 min
            ref_idx = max(0, len(closes) - 12)
            ref     = closes[ref_idx]
            if ref > 0:
                self._change_1h = (self._value - ref) / ref * 100

            self._history.append((self._updated, self._value))
            self._classify()

        except (KeyError, IndexError, TypeError) as exc:
            logger.warning("[dxy] Parse error: {}", exc)

    def _classify(self) -> None:
        ch = self._change_1h
        v  = self._value

        if ch > _MOVE_UP:
            self._signal = "fuerte"
            logger.info(
                "[dxy] 📊 DXY={:.2f} cambio={:+.2f}% → dólar fuerte -5pts BTC",
                v, ch,
            )
        elif ch < _MOVE_DOWN:
            self._signal = "debil"
            logger.info(
                "[dxy] 📊 DXY={:.2f} cambio={:+.2f}% → dólar débil +5pts BTC",
                v, ch,
            )
        else:
            self._signal = "neutral"
            logger.info(
                "[dxy] 📊 DXY={:.2f} cambio={:+.2f}% → neutral",
                v, ch,
            )

        try:
            import alerts as _alerts
            _alerts.update_thermometers(
                dxy_value      = round(v, 2),
                dxy_change_pct = round(ch, 3),
                dxy_signal     = self._signal,
            )
        except Exception:
            pass
