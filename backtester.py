"""
backtester.py — Whale Follower Bot
Motor de backtesting sobre datos OHLCV de 1m.
Simula la detección de Springs y ejecuta trades virtuales.
No conecta a exchanges ni importa config.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# ── Parámetros por defecto (igual que config.py defaults) ────────────────────

DEFAULT_PARAMS: dict = {
    "SIGNAL_SCORE_THRESHOLD": 65,
    "SPRING_DROP_PCT":        0.003,   # 0.3%
    "SPRING_BOUNCE_PCT":      0.002,   # 0.2%
    "VOLUME_MULTIPLIER":      1.5,
    "STOP_LOSS_PCT":          0.005,   # 0.5%
    "RISK_REWARD":            3.0,
    "PAPER_CAPITAL":          10000.0,
}

# ── Sesiones (mismo que context_engine.py) ────────────────────────────────────

_SESSIONS = [
    ("overlap",   13, 16, 2.0),
    ("new_york",  14, 21, 1.5),
    ("london",     3, 12, 1.3),
    ("asia",      21, 27, 0.7),
]


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    entry_idx:        int
    timestamp_ms:     int
    entry_price:      float
    stop_loss:        float
    take_profit:      float
    exit_price:       float
    pnl_usd:          float
    pnl_pct:          float
    won:              bool
    score:            int
    session:          str
    spring_drop_pct:  float
    spring_bounce_pct: float
    vol_ratio:        float
    candles_held:     int
    close_reason:     str   # 'take_profit' | 'stop_loss' | 'timeout'
    week_number:      int


@dataclass
class BacktestResult:
    total_trades:     int   = 0
    winners:          int   = 0
    losers:           int   = 0
    win_rate:         float = 0.0
    profit_factor:    float = 0.0
    max_drawdown:     float = 0.0
    final_capital:    float = 10_000.0
    total_return_pct: float = 0.0
    avg_winner_pct:   float = 0.0
    avg_loser_pct:    float = 0.0
    max_win_streak:   int   = 0
    max_loss_streak:  int   = 0
    trades:           List[TradeRecord] = field(default_factory=list)
    by_session:       Dict[str, Dict]   = field(default_factory=dict)
    by_week:          Dict[int, Dict]   = field(default_factory=dict)
    by_score:         Dict[str, Dict]   = field(default_factory=dict)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_session(ts_ms: int) -> Tuple[str, float]:
    dt   = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    for name, start, end, mult in _SESSIONS:
        if end > 24:
            active = hour >= start or hour < (end - 24)
        else:
            active = start <= hour < end
        if active:
            return name, mult
    return "off_hours", 0.8


def _ema_series(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    emas = [values[0]]
    for v in values[1:]:
        emas.append(v * k + emas[-1] * (1.0 - k))
    return emas


def _detect_spring(
    candles:   List[dict],
    idx:       int,
    params:    dict,
    avg_vol:   float,
) -> Optional[dict]:
    """
    Detecta el patrón Spring en candles OHLCV.
    Retorna dict compatible con ScoringEngine, o None.
    """
    candle = candles[idx]

    # Lookback: últ. 2 velas para calcular la caída
    lookback = min(2, idx)
    prev_candles = candles[idx - lookback: idx]
    high_before = max(c["high"] for c in prev_candles) if prev_candles else candle["open"]

    spring_low    = candle["low"]
    current_price = candle["close"]

    if high_before <= 0 or spring_low <= 0:
        return None

    drop_pct   = (high_before - spring_low) / high_before
    bounce_pct = (current_price - spring_low) / spring_low

    cond_a = drop_pct   >= params["SPRING_DROP_PCT"]
    cond_b = bounce_pct >= params["SPRING_BOUNCE_PCT"]

    # Necesitamos al menos una condición primaria
    if not (cond_a or cond_b):
        return None

    # CVD divergence aproximada: vela(s) previas bajistas + cierre alcista actual
    prev = candles[idx - 1] if idx >= 1 else None
    bearish_before = (prev is not None) and (prev["close"] < prev["open"])
    bullish_now    = current_price > candle["open"]
    cond_c = bearish_before and bullish_now and cond_a

    # Volumen relativo
    vol_ratio = candle["volume"] / avg_vol if avg_vol > 0 else 0.0
    cond_d    = vol_ratio >= params["VOLUME_MULTIPLIER"]

    return {
        "cond_a":          cond_a,
        "cond_b":          cond_b,
        "cond_c":          cond_c,
        "cond_d":          cond_d,
        "drop_pct":        round(drop_pct   * 100, 3),
        "bounce_pct":      round(bounce_pct * 100, 3),
        "drop_intensity":  min(drop_pct   / max(params["SPRING_DROP_PCT"]   * 2, 1e-6), 1.0),
        "bounce_intensity":min(bounce_pct / max(params["SPRING_BOUNCE_PCT"] * 2, 1e-6), 1.0),
        "spring_low":      round(spring_low, 2),
        "current_price":   round(current_price, 2),
        "vol_ratio":       round(vol_ratio, 2),
    }


def _score_spring(
    spring:       dict,
    session_name: str,
    session_mult: float,
    ema200:       float,
    entry_price:  float,
) -> int:
    """
    Scoring simplificado para backtesting (sin funding/OI histórico).
    Mantiene la misma lógica de categorías que ScoringEngine.
    """
    pts = 0

    # ── Primarias (máx 40) ────────────────────────────────────────────────────
    if spring["cond_a"] and spring["cond_b"]:
        pts += 20
    elif spring["cond_a"]:
        pts += 8

    if spring["cond_c"]:            # divergencia CVD aproximada
        pts += 15                   # live da hasta 20; conservador en backtest
    pts = min(pts, 40)

    # ── Volumen (máx 25) ──────────────────────────────────────────────────────
    vol = spring["vol_ratio"]
    if vol >= 1.5:
        pts += 10
    elif vol >= 1.2:
        pts += 5

    if spring["cond_b"] and spring["cond_c"]:   # proxy CVD velocity positiva
        pts += 5
    pts = min(pts, 40 + 25)

    # ── Contexto — sesión (máx 20; sin funding/OI histórico) ─────────────────
    if session_mult >= 2.0:
        pts += 5
    elif session_mult >= 1.5:
        pts += 4
    elif session_mult >= 1.3:
        pts += 3
    pts = min(pts, 40 + 25 + 20)

    # ── Estructura — EMA200 (máx 15) ─────────────────────────────────────────
    if ema200 > 0 and entry_price > 0:
        if entry_price > ema200:
            pts += 7
        elif entry_price > ema200 * 0.98:
            pts += 3
    pts = min(pts, 100)

    # Multiplicador de sesión (±20% máx)
    pts = min(int(pts * session_mult), 100)
    return pts


def _simulate_trade(
    candles:     List[dict],
    entry_idx:   int,
    entry_price: float,
    stop_loss:   float,
    take_profit: float,
    max_hold:    int = 1440,
) -> Tuple[float, bool, str, int]:
    """
    Escanea velas futuras hasta que se alcance TP, SL o timeout.
    Retorna (exit_price, won, close_reason, candles_held).
    """
    end_idx = min(entry_idx + max_hold, len(candles) - 1)
    for i in range(entry_idx + 1, end_idx + 1):
        c = candles[i]
        # SL primero (peor caso dentro de la vela)
        if c["low"] <= stop_loss:
            return stop_loss, False, "stop_loss", i - entry_idx
        # TP
        if c["high"] >= take_profit:
            return take_profit, True, "take_profit", i - entry_idx

    # Timeout: cierre al precio de la última vela
    exit_candle = candles[end_idx]
    exit_price  = exit_candle["close"]
    won         = exit_price > entry_price
    return exit_price, won, "timeout", end_idx - entry_idx


# ── Motor principal ───────────────────────────────────────────────────────────

def run_backtest(klines: List[dict], params: dict = None) -> BacktestResult:
    """
    Ejecuta el backtest completo sobre la lista de velas.
    params: dict con claves de DEFAULT_PARAMS (parcial o completo).
    """
    p = {**DEFAULT_PARAMS, **(params or {})}

    result  = BacktestResult(final_capital=p["PAPER_CAPITAL"])
    capital = p["PAPER_CAPITAL"]
    peak    = capital
    max_dd  = 0.0

    win_streak  = loss_streak  = 0
    max_win_str = max_loss_str = 0

    # EMA200 sobre los cierres
    closes       = [c["close"] for c in klines]
    ema200_vals  = _ema_series(closes, 200)

    # Ventana de volumen promedio rodante (últimas 20 velas)
    vol_window = 20
    cooldown_until = 0   # índice de vela, no generar señales antes de este

    for idx in range(max(200, vol_window), len(klines) - 1441):
        if idx < cooldown_until:
            continue

        # Volumen promedio
        avg_vol = sum(c["volume"] for c in klines[idx - vol_window: idx]) / vol_window

        spring = _detect_spring(klines, idx, p, avg_vol)
        if spring is None:
            continue

        candle        = klines[idx]
        ema200        = ema200_vals[idx]
        session, mult = _get_session(candle["timestamp"])
        entry_price   = candle["close"]

        score = _score_spring(spring, session, mult, ema200, entry_price)
        if score < p["SIGNAL_SCORE_THRESHOLD"]:
            continue

        # ── Entrar al trade ───────────────────────────────────────────────────
        stop_loss  = spring["spring_low"] * (1.0 - p["STOP_LOSS_PCT"])
        risk       = entry_price - stop_loss
        if risk <= 0:
            continue
        take_profit = entry_price + risk * p["RISK_REWARD"]

        exit_price, won, reason, held = _simulate_trade(
            klines, idx, entry_price, stop_loss, take_profit
        )

        # ── P&L ───────────────────────────────────────────────────────────────
        risk_amount = capital * 0.01   # 1% del capital
        if reason == "take_profit":
            pnl_usd = risk_amount * p["RISK_REWARD"]
            pnl_pct = p["RISK_REWARD"] * p["STOP_LOSS_PCT"] * 100
        elif reason == "stop_loss":
            pnl_usd = -risk_amount
            pnl_pct = -p["STOP_LOSS_PCT"] * 100
        else:  # timeout
            move    = (exit_price - entry_price) / entry_price
            pnl_usd = risk_amount * (move / p["STOP_LOSS_PCT"])
            pnl_pct = move * 100

        capital += pnl_usd

        # Drawdown
        if capital > peak:
            peak = capital
        dd = (peak - capital) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd

        # Rachas
        if won:
            win_streak += 1; loss_streak = 0
            max_win_str = max(max_win_str, win_streak)
        else:
            loss_streak += 1; win_streak = 0
            max_loss_str = max(max_loss_str, loss_streak)

        week_num = (idx - 200) // (7 * 1440)

        result.trades.append(TradeRecord(
            entry_idx        = idx,
            timestamp_ms     = candle["timestamp"],
            entry_price      = entry_price,
            stop_loss        = stop_loss,
            take_profit      = take_profit,
            exit_price       = exit_price,
            pnl_usd          = round(pnl_usd, 4),
            pnl_pct          = round(pnl_pct, 4),
            won              = won,
            score            = score,
            session          = session,
            spring_drop_pct  = spring["drop_pct"],
            spring_bounce_pct= spring["bounce_pct"],
            vol_ratio        = spring["vol_ratio"],
            candles_held     = held,
            close_reason     = reason,
            week_number      = week_num,
        ))

        # Cooldown 30 minutos después de cada señal
        cooldown_until = idx + 30

    # ── Agregados ─────────────────────────────────────────────────────────────
    n = len(result.trades)
    result.total_trades  = n
    result.max_drawdown  = max_dd
    result.max_win_streak  = max_win_str
    result.max_loss_streak = max_loss_str
    result.final_capital   = capital
    result.total_return_pct = (capital - p["PAPER_CAPITAL"]) / p["PAPER_CAPITAL"] * 100

    if n == 0:
        return result

    result.winners = sum(1 for t in result.trades if t.won)
    result.losers  = n - result.winners
    result.win_rate = result.winners / n

    gross_profit = sum(t.pnl_usd for t in result.trades if t.pnl_usd > 0)
    gross_loss   = abs(sum(t.pnl_usd for t in result.trades if t.pnl_usd < 0))
    result.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    winners_list = [t for t in result.trades if t.won]
    losers_list  = [t for t in result.trades if not t.won]
    result.avg_winner_pct = (
        sum(t.pnl_pct for t in winners_list) / len(winners_list) if winners_list else 0.0
    )
    result.avg_loser_pct = (
        sum(t.pnl_pct for t in losers_list)  / len(losers_list)  if losers_list  else 0.0
    )

    # Por sesión
    for t in result.trades:
        s = result.by_session.setdefault(t.session, {"trades": 0, "wins": 0})
        s["trades"] += 1
        if t.won:
            s["wins"] += 1

    # Por semana
    for t in result.trades:
        w = result.by_week.setdefault(t.week_number, {"trades": 0, "wins": 0})
        w["trades"] += 1
        if t.won:
            w["wins"] += 1

    # Por score bucket
    for lo, hi, label in [(55, 65, "55-65"), (65, 75, "65-75"),
                           (75, 85, "75-85"), (85, 101, "85-100")]:
        bucket = [t for t in result.trades if lo <= t.score < hi]
        if bucket:
            result.by_score[label] = {
                "trades": len(bucket),
                "wins":   sum(1 for t in bucket if t.won),
            }

    return result
