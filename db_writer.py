# -*- coding: utf-8 -*-
"""
db_writer.py -- Whale Follower Bot
Centralised async Supabase writer for all Sprint-6 strategies.

Each strategy calls the relevant async function and wraps it in
asyncio.create_task() so it never blocks the trading loop.

Pattern:
    asyncio.create_task(db_writer.save_ofi_open(trade))
    asyncio.create_task(db_writer.save_ofi_close(trade, reason))
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional

from loguru import logger

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ts(unix: float) -> str:
    """Unix float → ISO-8601 string with timezone for Supabase."""
    return datetime.fromtimestamp(unix, tz=timezone.utc).isoformat()

def _now_ts() -> str:
    return datetime.now(tz=timezone.utc).isoformat()

def _client():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL / SUPABASE_KEY not set")
    return create_client(url, key)

async def _run(fn) -> Optional[Any]:
    """Run a blocking supabase call in a thread pool."""
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, fn)
    except Exception as exc:
        logger.warning("[db_writer] Supabase error: {}", exc)
        return None


# ── Learning mode tables ─────────────────────────────────────────────────────

async def save_learning_history(
    trade_number: int,
    score_threshold_at_time: int,
    win_rate_at_time: float,
    ml_accuracy: Optional[float],
    timestamp: float,
) -> None:
    row = {
        "trade_number":            int(trade_number),
        "score_threshold_at_time": int(score_threshold_at_time),
        "win_rate_at_time":        float(win_rate_at_time),
        "ml_accuracy":             float(ml_accuracy) if ml_accuracy is not None else None,
        "timestamp":               _ts(timestamp),
    }
    await _run(lambda: _client().table("learning_history").insert(row).execute())


async def save_signal_features(
    signal_id: str,
    pair: str,
    signal_score: int,
    features: Dict[str, Any],
    timestamp: float,
) -> None:
    row = {
        "signal_id":    signal_id,
        "pair":         pair,
        "signal_score": int(signal_score),
        "features":     features,
        "timestamp":    _ts(timestamp),
    }
    await _run(lambda: _client().table("signal_features").insert(row).execute())


# ── Unified paper_trades row ──────────────────────────────────────────────────

async def _save_paper_open(
    strategy:    str,
    pair:        str,
    side:        str,        # "Buy" | "Sell"
    entry_price: float,
    stop_loss:   float,
    take_profit: float,
    size_usd:    float,
    trade_id:    str,
) -> None:
    row = {
        "id":          trade_id,
        "strategy":    strategy,
        "pair":        pair,
        "side":        side,
        "entry_price": entry_price,
        "stop_loss":   stop_loss,
        "take_profit": take_profit,
        "size_usd":    size_usd,
        "status":      "open",
    }
    await _run(lambda: _client().table("paper_trades").insert(row).execute())


async def _save_paper_close(
    trade_id:    str,
    exit_price:  float,
    pnl_usd:     float,
    pnl_pct:     float,
    close_reason: str,
    duration_secs: int,
) -> None:
    row = {
        "status":           "closed",
        "exit_price":       exit_price,
        "pnl_usd":          round(pnl_usd, 6),
        "pnl_pct":          round(pnl_pct, 6),
        "close_reason":     close_reason,
        "duration_seconds": duration_secs,
    }
    await _run(
        lambda: _client()
        .table("paper_trades")
        .update(row)
        .eq("id", trade_id)
        .execute()
    )


# ── OFI ──────────────────────────────────────────────────────────────────────

async def save_ofi_open(trade) -> None:
    """Insert open OFI trade into ofi_trades + paper_trades."""
    row = {
        "trade_id":    trade.trade_id,
        "pair":        trade.pair,
        "direction":   trade.direction,
        "ofi_score":   round(trade.ofi_score, 6),
        "entry_price": trade.entry_price,
        "size_usd":    trade.size_usd,
        "trade_status": "open",
        "production":  trade.production,
        "opened_at":   _ts(trade.opened_at),
    }
    await _run(lambda: _client().table("ofi_trades").insert(row).execute())

    side = "Buy" if trade.direction == "long" else "Sell"
    await _save_paper_open(
        strategy="ofi",
        pair=trade.pair,
        side=side,
        entry_price=trade.entry_price,
        stop_loss=trade.stop_loss,
        take_profit=trade.take_profit,
        size_usd=trade.size_usd,
        trade_id=trade.trade_id,
    )


async def save_ofi_close(trade, reason: str) -> None:
    """Update ofi_trades + paper_trades on close."""
    hold = int(time.time() - trade.opened_at)
    pnl_pct = trade.pnl_usd / trade.size_usd if trade.size_usd else 0.0

    upd = {
        "exit_price":   trade.exit_price,
        "pnl_usd":      round(trade.pnl_usd, 6),
        "hold_secs":    hold,
        "close_reason": reason,
        "trade_status": "closed",
        "closed_at":    _now_ts(),
    }
    await _run(
        lambda: _client()
        .table("ofi_trades")
        .update(upd)
        .eq("trade_id", trade.trade_id)
        .execute()
    )
    await _save_paper_close(
        trade_id=trade.trade_id,
        exit_price=trade.exit_price,
        pnl_usd=trade.pnl_usd,
        pnl_pct=pnl_pct,
        close_reason=reason,
        duration_secs=hold,
    )


# ── Grid ─────────────────────────────────────────────────────────────────────

async def save_grid_cycle(
    pair:       str,
    center:     float,
    spacing_pct: float,
    buy_price:  float,
    sell_price: float,
    size_usd:   float,
    pnl_usd:    float,
    production: bool,
) -> None:
    """Insert a completed grid buy→sell cycle."""
    # grid_cycles has no open/close lifecycle — one row per completed cycle
    row = {
        "pair":        pair,
        "center":      round(center, 4),
        "spacing_pct": round(spacing_pct * 100, 4),
        "buy_price":   round(buy_price, 4),
        "sell_price":  round(sell_price, 4),
        "size_usd":    round(size_usd, 2),
        "pnl_usd":     round(pnl_usd, 6),
        "production":  production,
        "cycle_at":    _now_ts(),
    }
    await _run(lambda: _client().table("grid_cycles").insert(row).execute())

    # Also log to paper_trades for unified history
    import uuid
    cycle_id = str(uuid.uuid4())
    pt_open = {
        "id":          cycle_id,
        "strategy":    "grid",
        "pair":        pair,
        "side":        "Buy",
        "entry_price": buy_price,
        "stop_loss":   buy_price * 0.99,   # synthetic — grid has no SL
        "take_profit": sell_price,
        "size_usd":    size_usd,
        "status":      "closed",
        "exit_price":  sell_price,
        "pnl_usd":     round(pnl_usd, 6),
        "pnl_pct":     round(pnl_usd / size_usd, 6) if size_usd else 0.0,
        "close_reason": "grid_cycle",
        "duration_seconds": 0,
    }
    await _run(lambda: _client().table("paper_trades").insert(pt_open).execute())


# ── Mean Reversion ────────────────────────────────────────────────────────────

async def save_mr_open(trade) -> None:
    """Insert open MR trade into mr_trades + paper_trades."""
    row = {
        "trade_id":         trade.trade_id,
        "pair":             trade.pair,
        "cascade_intensity": trade.cascade_intensity,
        "entry_price":      trade.entry_price,
        "avg_entry":        trade.avg_entry,
        "stop_loss":        trade.stop_loss,
        "take_profit":      trade.take_profit,
        "size_usd":         trade.size_usd,
        "tramos_done":      trade.tramos_done,
        "status":           "open",
        "production":       trade.production,
        "opened_at":        _ts(trade.opened_at),
    }
    await _run(lambda: _client().table("mr_trades").insert(row).execute())

    await _save_paper_open(
        strategy="mean_reversion",
        pair=trade.pair,
        side="Buy",
        entry_price=trade.entry_price,
        stop_loss=trade.stop_loss,
        take_profit=trade.take_profit,
        size_usd=trade.size_usd,
        trade_id=trade.trade_id,
    )


async def save_mr_close(trade, reason: str) -> None:
    """Update mr_trades + paper_trades on close."""
    hold = int(time.time() - trade.opened_at)
    pnl_pct = trade.pnl_usd / trade.size_usd if trade.size_usd else 0.0

    upd = {
        "avg_entry":    trade.avg_entry,
        "exit_price":   trade.exit_price,
        "tramos_done":  trade.tramos_done,
        "pnl_usd":      round(trade.pnl_usd, 6),
        "status":       "closed",
        "close_reason": reason,
        "closed_at":    _now_ts(),
    }
    await _run(
        lambda: _client()
        .table("mr_trades")
        .update(upd)
        .eq("trade_id", trade.trade_id)
        .execute()
    )
    await _save_paper_close(
        trade_id=trade.trade_id,
        exit_price=trade.exit_price,
        pnl_usd=trade.pnl_usd,
        pnl_pct=pnl_pct,
        close_reason=reason,
        duration_secs=hold,
    )


# ── Momentum Scaling ──────────────────────────────────────────────────────────

async def save_momentum_open(trade) -> None:
    """Insert open Momentum trade into momentum_trades + paper_trades."""
    row = {
        "trade_id":    trade.trade_id,
        "pair":        trade.pair,
        "stage":       trade.stage,
        "avg_entry":   trade.avg_entry,
        "size_usd":    trade.size_usd,
        "trade_status": "open",
        "production":  trade.production,
        "opened_at":   _ts(trade.opened_at),
    }
    await _run(lambda: _client().table("momentum_trades").insert(row).execute())

    await _save_paper_open(
        strategy="momentum",
        pair=trade.pair,
        side="Buy",
        entry_price=trade.entry_price,
        stop_loss=trade.stop_loss,
        take_profit=trade.entry_price * 1.005,   # ~0.5% target
        size_usd=trade.size_usd,
        trade_id=trade.trade_id,
    )


async def save_momentum_close(trade, reason: str) -> None:
    """Update momentum_trades + paper_trades on close."""
    hold = int(time.time() - trade.opened_at)
    pnl_pct = trade.pnl_usd / trade.size_usd if trade.size_usd else 0.0

    upd = {
        "stage":        trade.stage,
        "avg_entry":    trade.avg_entry,
        "exit_price":   trade.exit_price,
        "size_usd":     trade.size_usd,
        "pnl_usd":      round(trade.pnl_usd, 6),
        "hold_secs":    hold,
        "close_reason": reason,
        "trade_status": "closed",
        "closed_at":    _now_ts(),
    }
    await _run(
        lambda: _client()
        .table("momentum_trades")
        .update(upd)
        .eq("trade_id", trade.trade_id)
        .execute()
    )
    await _save_paper_close(
        trade_id=trade.trade_id,
        exit_price=trade.exit_price,
        pnl_usd=trade.pnl_usd,
        pnl_pct=pnl_pct,
        close_reason=reason,
        duration_secs=hold,
    )


# ── Delta Neutral ─────────────────────────────────────────────────────────────

async def save_dn_open(trade) -> None:
    """Insert open Delta Neutral trade into delta_neutral_trades + paper_trades."""
    row = {
        "trade_id":      trade.trade_id,
        "strategy":      trade.strategy,
        "long_exchange": trade.long_exchange,
        "short_exchange": trade.short_exchange,
        "size_usd":      trade.size_usd,
        "funding_diff":  round(trade.funding_diff, 6),
        "basis_pct":     round(trade.basis_pct, 6),
        "payments_count": 0,
        "rebalances":    0,
        "total_pnl":     0.0,
        "trade_status":  "open",
        "production":    trade.production,
        "opened_at":     _ts(trade.opened_at),
    }
    await _run(lambda: _client().table("delta_neutral_trades").insert(row).execute())

    # paper_trades: long leg
    await _save_paper_open(
        strategy="delta_neutral",
        pair="BTCUSDT",
        side="Buy",
        entry_price=trade.long_price,
        stop_loss=trade.long_price * 0.99,
        take_profit=trade.long_price * 1.01,
        size_usd=trade.size_usd,
        trade_id=trade.trade_id,
    )


async def save_dn_close(trade) -> None:
    """Update delta_neutral_trades + paper_trades on close."""
    hold = int(time.time() - trade.opened_at)
    pnl_pct = trade.total_pnl / trade.size_usd if trade.size_usd else 0.0

    upd = {
        "payments_count": len(trade.payments),
        "rebalances":     trade.rebalances,
        "total_pnl":      round(trade.total_pnl, 6),
        "trade_status":   "closed",
        "closed_at":      _now_ts(),
    }
    await _run(
        lambda: _client()
        .table("delta_neutral_trades")
        .update(upd)
        .eq("trade_id", trade.trade_id)
        .execute()
    )
    await _save_paper_close(
        trade_id=trade.trade_id,
        exit_price=trade.long_price,   # delta-neutral: no directional exit
        pnl_usd=trade.total_pnl,
        pnl_pct=pnl_pct,
        close_reason="funding_settled",
        duration_secs=hold,
    )


async def save_dn_payment(trade_id: str, new_total_pnl: float, payments_count: int) -> None:
    """Update delta_neutral_trades after each funding payment."""
    upd = {
        "total_pnl":      round(new_total_pnl, 6),
        "payments_count": payments_count,
    }
    await _run(
        lambda: _client()
        .table("delta_neutral_trades")
        .update(upd)
        .eq("trade_id", trade_id)
        .execute()
    )


# ── Range Trader ──────────────────────────────────────────────────────────────

async def save_range_open(trade) -> None:
    """Insert range_trades on open."""
    row = {
        "trade_id":    trade.trade_id,
        "pair":        trade.pair,
        "side":        trade.side,
        "entry_price": trade.entry_price,
        "tp":          trade.tp,
        "sl":          trade.sl,
        "size_usd":    trade.size_usd,
        "status":      "open",
        "production":  trade.production,
        "open_at":     _ts(trade.open_ts),
    }
    await _run(
        lambda: _client().table("range_trades").insert(row).execute()
    )
    await _save_paper_open(
        strategy    = "range",
        pair        = trade.pair,
        side        = "Buy" if trade.side == "long" else "Sell",
        entry_price = trade.entry_price,
        stop_loss   = trade.sl,
        take_profit = trade.tp,
        size_usd    = trade.size_usd,
        trade_id    = trade.trade_id,
    )


async def save_range_close(trade, reason: str, exit_price: float, pnl_net: float) -> None:
    """Update range_trades on close."""
    upd = {
        "exit_price": exit_price,
        "pnl_usd":    round(pnl_net, 6),
        "close_reason": reason,
        "status":     "closed",
        "closed_at":  _now_ts(),
    }
    await _run(
        lambda: _client()
        .table("range_trades")
        .update(upd)
        .eq("trade_id", trade.trade_id)
        .execute()
    )
    await _save_paper_close(
        trade_id      = trade.trade_id,
        exit_price    = exit_price,
        pnl_usd       = pnl_net,
        pnl_pct       = pnl_net / trade.size_usd if trade.size_usd else 0.0,
        close_reason  = reason,
        duration_secs = int(time.time() - trade.open_ts),
    )
