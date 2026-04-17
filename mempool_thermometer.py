# -*- coding: utf-8 -*-
"""
mempool_thermometer.py -- Modulo #49 Whale Follower Bot
Termometro del mempool de Bitcoin via mempool.space API (gratuita)
Score 0-100 de presion de red
"""
import requests
import logging
import time
from datetime import datetime
from typing import Optional

MEMPOOL_API_BASE = "https://mempool.space/api"
REQUEST_TIMEOUT = 30
CACHE_TTL_SECONDS = 60

logger = logging.getLogger("mempool_thermometer")


class MempoolThermometer:
    def __init__(self):
        self._cache: Optional[dict] = None
        self._cache_timestamp: float = 0.0

    def get_full_analysis(self) -> dict:
        raw = self._fetch_mempool_data()
        if raw is None:
            return {"mempool_score": 50.0, "signal": "SIN_DATOS",
                    "raw": {}, "trading_context": "Sin datos mempool, operar con precaucion."}

        tx_score   = self._score_tx_count(raw["tx_count"])
        fee_score  = self._score_fee(raw["fee_avg"])
        size_score = self._score_size(raw["mempool_size_mb"])
        final = round(tx_score * 0.35 + fee_score * 0.40 + size_score * 0.25, 2)

        return {
            "timestamp":       datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "mempool_score":   final,
            "signal":          self._get_signal(final),
            "raw":             raw,
            "trading_context": self._get_trading_context(final, raw),
        }

    def get_score(self) -> float:
        return self.get_full_analysis()["mempool_score"]

    def _fetch_mempool_data(self) -> Optional[dict]:
        now = time.time()
        if self._cache and (now - self._cache_timestamp) < CACHE_TTL_SECONDS:
            return self._cache
        try:
            r1 = requests.get(f"{MEMPOOL_API_BASE}/mempool", timeout=REQUEST_TIMEOUT)
            r1.raise_for_status()
            m = r1.json()
            r2 = requests.get(f"{MEMPOOL_API_BASE}/v1/fees/recommended", timeout=REQUEST_TIMEOUT)
            r2.raise_for_status()
            f = r2.json()
            fee_avg = (f.get("hourFee", 5) + f.get("halfHourFee", 10) + f.get("fastestFee", 20)) / 3
            result = {
                "tx_count":        m.get("count", 0),
                "fee_low":         f.get("hourFee", 5),
                "fee_medium":      f.get("halfHourFee", 10),
                "fee_fast":        f.get("fastestFee", 20),
                "fee_avg":         round(fee_avg, 2),
                "mempool_size_mb": round(m.get("vsize", 0) / 1_000_000, 3),
            }
            self._cache = result
            self._cache_timestamp = now
            return result
        except Exception as e:
            logger.warning(f"[mempool] Error consultando API: {e}")
            return None

    @staticmethod
    def _interpolate(v, mn, mx, s1, s2):
        return s1 + (v - mn) / (mx - mn) * (s2 - s1)

    def _score_tx_count(self, c):
        if c <= 5000:   return 10.0
        if c <= 20000:  return self._interpolate(c, 5000,  20000,  10, 40)
        if c <= 50000:  return self._interpolate(c, 20000, 50000,  40, 75)
        if c <= 100000: return self._interpolate(c, 50000, 100000, 75, 95)
        return 100.0

    def _score_fee(self, f):
        if f <= 5:   return 5.0
        if f <= 20:  return self._interpolate(f, 5,  20,  5,  40)
        if f <= 50:  return self._interpolate(f, 20, 50,  40, 80)
        if f <= 150: return self._interpolate(f, 50, 150, 80, 97)
        return 100.0

    def _score_size(self, s):
        if s <= 1:  return 10.0
        if s <= 5:  return self._interpolate(s, 1,  5,  10, 45)
        if s <= 20: return self._interpolate(s, 5,  20, 45, 80)
        if s <= 50: return self._interpolate(s, 20, 50, 80, 95)
        return 100.0

    def _get_signal(self, score):
        if score >= 80: return "CONGESTION_EXTREMA"
        if score >= 60: return "ALTA_ACTIVIDAD"
        if score >= 40: return "ACTIVIDAD_NORMAL"
        if score >= 20: return "RED_TRANQUILA"
        return "RED_INACTIVA"

    def _get_trading_context(self, score, raw):
        if score >= 80:
            return f"CONGESTION EXTREMA: pausar grids, max arb. Fee rapido: {raw.get('fee_fast', '?')} sat/vB"
        if score >= 60:
            return f"ALTA ACTIVIDAD: arb+momentum boost, grid cautela. Fee: {raw.get('fee_fast', '?')} sat/vB"
        if score >= 40:
            return f"NORMAL: bot en modo estandar. Fee normal: {raw.get('fee_medium', '?')} sat/vB"
        return f"RED TRANQUILA: ideal para grid+mean_rev. Fee bajo: {raw.get('fee_low', '?')} sat/vB"


def format_mempool_telegram(analysis: dict) -> str:
    score  = analysis.get("mempool_score", 50)
    signal = analysis.get("signal", "N/A")
    raw    = analysis.get("raw", {})
    emoji  = "\U0001f6a8" if score >= 80 else "\U0001f525" if score >= 60 else "\U0001f4ca" if score >= 40 else "\U0001f634"
    return (
        f"{emoji} *MEMPOOL BTC* -- Score: *{score}/100* ({signal})\n"
        f"Tx pendientes: {raw.get('tx_count', 0):,} | "
        f"Fee: {raw.get('fee_low', '?')}/{raw.get('fee_medium', '?')}/{raw.get('fee_fast', '?')} sat/vB\n"
        f"  {analysis.get('trading_context', '')}"
    )
