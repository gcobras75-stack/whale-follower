# -*- coding: utf-8 -*-
"""
session_volume.py -- Whale Follower Bot
Compares current trading volume against historical hourly averages.
Persists averages to Supabase for cross-session continuity.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Dict, Tuple

import aiohttp
from loguru import logger

import config

# ── Constants ─────────────────────────────────────────────────────────────────

_VOLUME_WINDOW_SECS = 60.0
_UNUSUAL_RATIO = 2.0
_PERSIST_INTERVAL_SECS = 3600  # update historical average every hour
_SUPABASE_TABLE = "volume_history"


class SessionVolumeTracker:
    """
    Tracks per-minute volume and compares it to historical hourly averages.

    - record_volume(): synchronous, called from trade loop
    - is_unusual(): synchronous query for current signal
    - run(): async background task that persists averages to Supabase
    """

    def __init__(self) -> None:
        # Ring buffer of {"ts": float, "qty": float} for the last 60 seconds
        self._current_volumes: Deque[Dict[str, float]] = deque(maxlen=3600)
        # hour_of_day (0-23) -> average volume
        self._historical: Dict[int, float] = {}

    # ── Public Sync API ────────────────────────────────────────────────────────

    def record_volume(self, quantity: float, timestamp: float) -> None:
        """Record a trade volume entry (synchronous)."""
        self._current_volumes.append({"ts": timestamp, "qty": quantity})

    def is_unusual(self) -> Tuple[bool, float, int]:
        """
        Returns (unusual, ratio_vs_historical, pts).
        unusual=True if ratio >= 2.0 -> pts=8, else pts=0.
        """
        try:
            return self._compute_is_unusual()
        except Exception as exc:
            logger.warning("[session_volume] Error in is_unusual: {}", exc)
            return (False, 1.0, 0)

    # ── Background Task ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Background task:
        1. Load historical data from Supabase on startup.
        2. Every hour, persist the previous hour's average.
        """
        logger.info("[session_volume] Starting session volume tracker")
        await self._load_historical()

        while True:
            await asyncio.sleep(_PERSIST_INTERVAL_SECS)
            await self._persist_current_hour()

    # ── Internal Logic ─────────────────────────────────────────────────────────

    def _compute_is_unusual(self) -> Tuple[bool, float, int]:
        now = time.time()
        cutoff = now - _VOLUME_WINDOW_SECS

        # Sum volume over the last 60 seconds
        volume_now = sum(
            entry["qty"] for entry in self._current_volumes if entry["ts"] >= cutoff
        )

        # Determine current hour of day (UTC)
        hour_of_day = datetime.now(tz=timezone.utc).hour
        historical_avg = self._historical.get(hour_of_day, 0.0)

        ratio = volume_now / historical_avg if historical_avg > 0.0 else 1.0
        unusual = ratio >= _UNUSUAL_RATIO
        pts = 8 if unusual else 0

        logger.debug(
            "[session_volume] hour={} vol_now={:.2f} hist_avg={:.2f} ratio={:.2f} unusual={}",
            hour_of_day, volume_now, historical_avg, ratio, unusual,
        )
        return (unusual, ratio, pts)

    async def _persist_current_hour(self) -> None:
        """Calculate the previous hour's average and save it to Supabase."""
        now = time.time()
        prev_hour_cutoff = now - _PERSIST_INTERVAL_SECS
        prev_hour = (datetime.now(tz=timezone.utc).hour - 1) % 24

        # Collect volume from the previous hour window
        entries = [
            entry["qty"]
            for entry in self._current_volumes
            if prev_hour_cutoff <= entry["ts"] <= now
        ]
        if not entries:
            logger.debug("[session_volume] No data to persist for hour {}", prev_hour)
            return

        avg = sum(entries) / len(entries)
        self._historical[prev_hour] = avg

        logger.info(
            "[session_volume] Persisting hour={} avg_volume={:.4f} to Supabase",
            prev_hour, avg,
        )

        timeout = aiohttp.ClientTimeout(total=10)
        url = f"{config.SUPABASE_URL}/rest/v1/{_SUPABASE_TABLE}"
        headers = {
            "apikey": config.SUPABASE_KEY,
            "Authorization": f"Bearer {config.SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        payload = {
            "hour_of_day": prev_hour,
            "avg_volume": avg,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, headers=headers) as resp:
                    if resp.status in (200, 201):
                        logger.info("[session_volume] Supabase persist OK for hour {}", prev_hour)
                    else:
                        body = await resp.text()
                        logger.warning(
                            "[session_volume] Supabase persist failed status={} body={}",
                            resp.status, body,
                        )
        except Exception as exc:
            logger.warning("[session_volume] Supabase persist error: {}", exc)

    async def _load_historical(self) -> None:
        """Load historical averages from Supabase on startup."""
        logger.info("[session_volume] Loading historical volume data from Supabase")
        timeout = aiohttp.ClientTimeout(total=10)
        url = f"{config.SUPABASE_URL}/rest/v1/{_SUPABASE_TABLE}?select=hour_of_day,avg_volume"
        headers = {
            "apikey": config.SUPABASE_KEY,
            "Authorization": f"Bearer {config.SUPABASE_KEY}",
        }
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        rows = await resp.json()
                        for row in rows:
                            self._historical[row["hour_of_day"]] = row["avg_volume"]
                        logger.info(
                            "[session_volume] Loaded {} historical hour records from Supabase",
                            len(rows),
                        )
                    else:
                        body = await resp.text()
                        logger.warning(
                            "[session_volume] Failed to load historical data status={} body={}",
                            resp.status, body,
                        )
        except Exception as exc:
            logger.warning("[session_volume] Could not load historical data: {}", exc)
