# -*- coding: utf-8 -*-
"""
deribit_options.py -- Whale Follower Bot
Monitors Deribit public options market data (no API key required) and
produces trading signals based on:
  - Put/Call ratio (PCR)
  - ATM implied volatility and IV spikes
  - Large option sweep detection (>500 contracts)

Covers both BTC and ETH options; combines their signals for a stronger
composite view.  Designed to run as a background asyncio task via
``await engine.run()``.  The ``snapshot()`` method never blocks.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional

import aiohttp
from loguru import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
_REFRESH_INTERVAL = 120          # seconds
_IV_HISTORY_SIZE  = 6            # readings kept for spike detection
_IV_SPIKE_RATIO   = 1.30         # current IV > avg * 1.30 → spike
_SWEEP_THRESHOLD  = 500          # contracts in one instrument = large sweep
_PCR_BULLISH      = 0.5          # PCR below this → +10 pts
_PCR_BEARISH      = 1.5          # PCR above this → -10 pts
_SWEEP_CALL_PTS   = 8
_SWEEP_PUT_PTS    = 8


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------

@dataclass
class OptionsSnapshot:
    """Immutable snapshot of the latest options signals.

    pts_adjustment is positive for bullish signals and negative for bearish.
    pause_trading should suppress new entries when IV is wildly elevated.
    stale is True until the first successful fetch completes.
    """
    pcr:             float = 1.0    # put/call volume ratio
    iv_current:      float = 0.0    # ATM implied volatility (%)
    iv_spike:        bool  = False  # IV jumped >30 % vs 6-sample average
    bullish_sweep:   bool  = False  # large call sweep detected this cycle
    bearish_sweep:   bool  = False  # large put sweep detected this cycle
    pts_adjustment:  int   = 0      # composite score delta
    pause_trading:   bool  = False  # True when IV spike makes market unpredictable
    stale:           bool  = True
    last_update:     float = 0.0


# ---------------------------------------------------------------------------
# Internal per-currency state
# ---------------------------------------------------------------------------

@dataclass
class _CurrencyState:
    """Tracks rolling IV history for one currency (BTC or ETH)."""
    currency:   str
    iv_history: Deque[float] = field(default_factory=lambda: deque(maxlen=_IV_HISTORY_SIZE))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_option_type(instrument_name: str) -> str:
    """Return 'C' or 'P' from a Deribit instrument name like BTC-28MAR25-80000-C."""
    parts = instrument_name.rsplit("-", 1)
    if len(parts) == 2:
        return parts[-1].upper()
    return ""


def _compute_pcr(instruments: List[dict]) -> float:
    """Sum 24h volumes for puts vs calls and return the PCR."""
    put_vol  = 0.0
    call_vol = 0.0
    for inst in instruments:
        vol  = inst.get("volume_usd") or inst.get("volume") or 0.0
        kind = _parse_option_type(inst.get("instrument_name", ""))
        if kind == "P":
            put_vol  += vol
        elif kind == "C":
            call_vol += vol
    if call_vol <= 0:
        return 1.0   # neutral fallback; avoid division by zero
    return put_vol / call_vol


def _find_atm_iv(instruments: List[dict]) -> float:
    """Return average IV of the two instruments (one call, one put) closest
    to the current spot price, approximated by mark_price weighting."""
    # Deribit mark_price for options is in BTC (or ETH) terms.
    # The underlying spot is embedded in the instrument name but is tedious
    # to parse.  Instead we pick the two instruments with the highest
    # mark_price (proxy for ATM — deep OTM options have near-zero mark_price)
    # among calls and puts separately, then average their IV.
    calls = [i for i in instruments if _parse_option_type(i.get("instrument_name", "")) == "C"]
    puts  = [i for i in instruments if _parse_option_type(i.get("instrument_name", "")) == "P"]

    def best_iv(group: List[dict]) -> Optional[float]:
        # Sort descending by mark_price to find ATM candidates
        ranked = sorted(
            (i for i in group if (i.get("mark_iv") or 0) > 0),
            key=lambda x: x.get("mark_price") or 0.0,
            reverse=True,
        )
        if not ranked:
            return None
        # Take up to 3 nearest-ATM and average their IV
        sample = ranked[:3]
        ivs = [float(i["mark_iv"]) for i in sample if i.get("mark_iv")]
        return sum(ivs) / len(ivs) if ivs else None

    call_iv = best_iv(calls)
    put_iv  = best_iv(puts)

    values = [v for v in (call_iv, put_iv) if v is not None]
    if not values:
        return 0.0
    return sum(values) / len(values)


def _detect_sweeps(instruments: List[dict]) -> tuple[bool, bool]:
    """Return (bullish_sweep, bearish_sweep).

    A sweep is any single instrument whose 24h volume exceeds
    _SWEEP_THRESHOLD contracts.
    """
    bullish = False
    bearish = False
    for inst in instruments:
        vol  = inst.get("volume") or 0.0   # in contracts
        kind = _parse_option_type(inst.get("instrument_name", ""))
        if vol > _SWEEP_THRESHOLD:
            if kind == "C":
                bullish = True
            elif kind == "P":
                bearish = True
    return bullish, bearish


def _compute_pts(
    pcr: float,
    iv_spike: bool,
    bullish_sweep: bool,
    bearish_sweep: bool,
) -> int:
    """Derive a score delta from the current options signals."""
    pts = 0
    if pcr < _PCR_BULLISH:
        pts += 10
    elif pcr > _PCR_BEARISH:
        pts -= 10
    if bullish_sweep:
        pts += _SWEEP_CALL_PTS
    if bearish_sweep:
        pts -= _SWEEP_PUT_PTS
    # IV spike is handled via pause_trading; no direct pts adjustment
    return pts


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class DeribitOptionsEngine:
    """Fetches Deribit options data for BTC and ETH every 120 seconds.

    Designed to run as a background asyncio task via ``await engine.run()``.
    The ``snapshot()`` method is always safe to call — it never blocks.
    """

    def __init__(self) -> None:
        self._snapshot = OptionsSnapshot()
        self._btc_state = _CurrencyState(currency="BTC")
        self._eth_state = _CurrencyState(currency="ETH")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Infinite background loop — fetch, sleep, repeat."""
        logger.info(
            "[deribit] background task started, interval={}s",
            _REFRESH_INTERVAL,
        )
        while True:
            await self._fetch_and_update()
            await asyncio.sleep(_REFRESH_INTERVAL)

    def snapshot(self) -> OptionsSnapshot:
        """Return the latest cached snapshot (never blocks)."""
        return self._snapshot

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_currency(
        self,
        session: aiohttp.ClientSession,
        currency: str,
    ) -> Optional[List[dict]]:
        """Fetch book summary for one currency; return list of instruments or None."""
        url    = _BASE_URL
        params = {"currency": currency, "kind": "option"}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status}")
                data = await resp.json(content_type=None)
            result = data.get("result")
            if not isinstance(result, list):
                raise ValueError("unexpected response shape")
            return result
        except Exception as exc:
            logger.warning("[deribit] {} fetch failed: {}", currency, exc)
            return None

    async def _fetch_and_update(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                btc_data, eth_data = await asyncio.gather(
                    self._fetch_currency(session, "BTC"),
                    self._fetch_currency(session, "ETH"),
                    return_exceptions=False,
                )
        except Exception as exc:
            logger.warning("[deribit] session error: {}", exc)
            self._mark_stale()
            return

        # Process BTC
        btc_ok = btc_data is not None
        if btc_ok:
            btc_pcr           = _compute_pcr(btc_data)
            btc_iv            = _find_atm_iv(btc_data)
            btc_bull, btc_bear = _detect_sweeps(btc_data)
            btc_spike, btc_iv_avg = self._update_iv_history(self._btc_state, btc_iv)
            self._log_currency("BTC", btc_pcr, btc_iv, btc_spike, btc_bull, btc_bear,
                               _compute_pts(btc_pcr, btc_spike, btc_bull, btc_bear))
        else:
            btc_pcr   = 1.0
            btc_iv    = 0.0
            btc_spike = False
            btc_bull  = False
            btc_bear  = False

        # Process ETH
        eth_ok = eth_data is not None
        if eth_ok:
            eth_pcr           = _compute_pcr(eth_data)
            eth_iv            = _find_atm_iv(eth_data)
            eth_bull, eth_bear = _detect_sweeps(eth_data)
            eth_spike, _       = self._update_iv_history(self._eth_state, eth_iv)
            self._log_currency("ETH", eth_pcr, eth_iv, eth_spike, eth_bull, eth_bear,
                               _compute_pts(eth_pcr, eth_spike, eth_bull, eth_bear))
        else:
            eth_pcr   = 1.0
            eth_iv    = 0.0
            eth_spike = False
            eth_bull  = False
            eth_bear  = False

        if not btc_ok and not eth_ok:
            self._mark_stale()
            return

        # Combine signals — use BTC as primary, ETH as confirmation
        # "Both bearish" amplifies the bearish signal (already encoded via pts)
        combined_pcr   = btc_pcr if btc_ok else eth_pcr
        combined_iv    = btc_iv  if btc_ok else eth_iv
        combined_spike = btc_spike or eth_spike

        # Sweep: call/put from either currency
        combined_bull = btc_bull or eth_bull
        combined_bear = btc_bear or eth_bear

        # If both currencies signal bearish sweeps, count it as double-bearish
        # by applying an extra pts penalty below
        both_bearish = btc_bear and eth_bear
        both_bullish = btc_bull and eth_bull

        pts = _compute_pts(combined_pcr, combined_spike, combined_bull, combined_bear)
        if both_bearish:
            pts -= 5   # additional penalty for cross-currency confirmation
        if both_bullish:
            pts += 5   # additional bonus for cross-currency confirmation

        self._snapshot = OptionsSnapshot(
            pcr=round(combined_pcr, 4),
            iv_current=round(combined_iv, 2),
            iv_spike=combined_spike,
            bullish_sweep=combined_bull,
            bearish_sweep=combined_bear,
            pts_adjustment=pts,
            pause_trading=combined_spike,
            stale=False,
            last_update=time.time(),
        )

        logger.info(
            "[deribit] combined PCR={:.3f} IV={:.1f}% iv_spike={} "
            "bullish_sweep={} bearish_sweep={} adj={:+d} pause={}",
            combined_pcr, combined_iv, combined_spike,
            combined_bull, combined_bear, pts, combined_spike,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _update_iv_history(state: _CurrencyState, iv: float) -> tuple[bool, float]:
        """Append iv to rolling history and return (spike_detected, history_avg)."""
        if iv <= 0:
            # Don't poison history with zeros
            avg = (sum(state.iv_history) / len(state.iv_history)
                   if state.iv_history else 0.0)
            return False, avg

        state.iv_history.append(iv)

        if len(state.iv_history) < 2:
            # Need at least two samples before we can detect a spike
            return False, iv

        # Exclude the just-added value when computing the baseline average
        history_without_current = list(state.iv_history)[:-1]
        avg = sum(history_without_current) / len(history_without_current)

        spike = avg > 0 and iv > avg * _IV_SPIKE_RATIO
        return spike, avg

    @staticmethod
    def _log_currency(
        currency: str,
        pcr:   float,
        iv:    float,
        spike: bool,
        bull:  bool,
        bear:  bool,
        pts:   int,
    ) -> None:
        logger.info(
            "[deribit] {} PCR={:.3f} IV={:.1f}% iv_spike={} bullish_sweep={} adj={:+d}",
            currency, pcr, iv, spike, bull, pts,
        )

    def _mark_stale(self) -> None:
        """Preserve last known values but flag snapshot as stale."""
        prev = self._snapshot
        self._snapshot = OptionsSnapshot(
            pcr=prev.pcr,
            iv_current=prev.iv_current,
            iv_spike=False,          # reset transient signals
            bullish_sweep=False,
            bearish_sweep=False,
            pts_adjustment=0,        # no adjustment while data is stale
            pause_trading=False,
            stale=True,
            last_update=prev.last_update,
        )
        logger.warning("[deribit] all fetches failed — snapshot marked stale")
