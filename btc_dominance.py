# -*- coding: utf-8 -*-
"""
btc_dominance.py -- Whale Follower Bot
Termometro 1: Dominancia BTC via CoinGecko API publica (sin API key).

Poll: cada 5 minutos.
Ajuste de score:
  dominance > 60%  → alts debiles, -5pts en senales ETH/SOL/BNB
  dominance 45-60% → neutral, 0pts
  dominance < 45%  → altseason, +5pts en senales ETH/SOL/BNB
"""
from __future__ import annotations

import asyncio
import sys
import time

import aiohttp
from loguru import logger

_URL           = "https://api.coingecko.com/api/v3/global"
_POLL_SECS     = 300     # 5 minutos (CoinGecko free: max 30 req/min)
_STRONG_ABOVE  = 60.0    # dominancia > 60% → BTC fuerte, alts debiles
_ALT_BELOW     = 45.0    # dominancia < 45% → altseason
_ALT_PAIRS     = ("ETH", "SOL", "BNB", "ADA", "XRP", "AVAX", "MATIC")


class BtcDominanceMonitor:
    """
    Monitorea la dominancia de mercado de BTC.
    Solo lectura — ajusta el score de senales de altcoins.
    """

    def __init__(self) -> None:
        self._dominance: float = 0.0
        self._signal:    str   = "neutral"
        self._adj_pts:   int   = 0
        self._updated:   float = 0.0

        logger.info(
            "[btc_dom] Monitor iniciado | poll={}s | strong>{:.0f}% alt<{:.0f}%",
            _POLL_SECS, _STRONG_ABOVE, _ALT_BELOW,
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Loop de polling — arranca en asyncio.create_task()."""
        while True:
            await self._fetch()
            await asyncio.sleep(_POLL_SECS)

    # ── Public API ─────────────────────────────────────────────────────────────

    def adjustment(self, pair: str) -> int:
        """
        Devuelve ajuste de score segun dominancia y par de trading.
        Solo afecta altcoins (ETH/SOL/BNB/etc).
        """
        if self._dominance <= 0:
            return 0
        is_alt = any(k in pair.upper() for k in _ALT_PAIRS)
        if not is_alt:
            return 0
        if self._dominance > _STRONG_ABOVE:
            return -5
        if self._dominance < _ALT_BELOW:
            return +5
        return 0

    def snapshot(self) -> dict:
        return {
            "dominance_pct": round(self._dominance, 2),
            "signal":        self._signal,
            "adj_pts":       self._adj_pts,
            "updated_secs":  int(time.time() - self._updated) if self._updated else -1,
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch(self) -> None:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    _URL,
                    timeout=aiohttp.ClientTimeout(total=15),
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status != 200:
                        logger.warning("[btc_dom] HTTP {}", resp.status)
                        return
                    data = await resp.json()
                    btc_pct = data["data"]["market_cap_percentage"]["btc"]
                    self._dominance = float(btc_pct)
                    self._updated   = time.time()
                    self._classify()
        except Exception as exc:
            logger.warning("[btc_dom] Fetch error: {} — reintento en {}s", exc, _POLL_SECS)

    def _classify(self) -> None:
        d = self._dominance
        if d > _STRONG_ABOVE:
            self._signal  = "btc_fuerte"
            self._adj_pts = -5
            logger.info(
                "[btc_dom] 📊 Dominancia BTC={:.1f}% → BTC fuerte, cautela en alts -5pts",
                d,
            )
        elif d < _ALT_BELOW:
            self._signal  = "altseason"
            self._adj_pts = +5
            logger.info(
                "[btc_dom] 📊 Dominancia BTC={:.1f}% → altseason activo +5pts ETH/SOL",
                d,
            )
        else:
            self._signal  = "neutral"
            self._adj_pts = 0
            logger.info(
                "[btc_dom] 📊 Dominancia BTC={:.1f}% → neutral",
                d,
            )

        # Notificar a alerts para /stats
        _almod = sys.modules.get("alerts")
        if _almod is not None:
            try:
                _almod.update_thermometers(
                    btc_dom_pct=round(d, 2),
                    btc_dom_signal=self._signal,
                )
            except Exception as exc:
                logger.warning("[btc_dom] update_thermometers error: {}", exc)
