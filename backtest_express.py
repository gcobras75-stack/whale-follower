#!/usr/bin/env python3
"""
backtest_express.py — Whale Follower Bot
Backtesting express con datos de Binance 1m (30 días).

Simula spring detector + scoring con umbrales adaptativos.
Prueba múltiples thresholds y reporta tabla comparativa.

Uso:
    python backtest_express.py              # descarga + backtest
    python backtest_express.py --no-download  # solo backtest (datos ya descargados)
    python backtest_express.py --save-best    # guarda mejor threshold en Supabase
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SYMBOLS = {
    "BTCUSDT": "btc_1m.csv",
    "ETHUSDT": "eth_1m.csv",
    "SOLUSDT": "sol_1m.csv",
}

# ── Parámetros del spring detector (LATERAL regime) ──────────────────────────
SPRING_PARAMS = {
    "drop_pct":    0.0013,   # 0.130%
    "bounce_pct":  0.0009,   # 0.090%
    "vol_mult":    1.10,
    "drop_secs":   10,       # ventana en velas de 1m → 10 velas
    "bounce_secs": 5,        # 5 velas
}

SL_PCT = 0.005     # 0.5% stop loss
RR     = 3.0       # 1:3 risk-reward → TP = 1.5%
CAPITAL = 500.0
TRADE_SIZE = 10.0   # $10 por trade


# ═════════════════════════════════════════════════════════════════════════════
# PASO 1 — Descarga de datos
# ═════════════════════════════════════════════════════════════════════════════

def download_klines(symbol: str, days: int = 30) -> List[list]:
    """Descarga velas 1m de Binance público (sin API key)."""
    import urllib.request

    url_base = "https://api.binance.com/api/v3/klines"
    end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)

    all_data = []
    current = start_ms
    batch = 0

    while current < end_ms:
        params = f"symbol={symbol}&interval=1m&startTime={current}&endTime={end_ms}&limit=1000"
        req_url = f"{url_base}?{params}"

        try:
            req = urllib.request.Request(req_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode())
        except Exception as exc:
            print(f"  Error descargando {symbol} batch {batch}: {exc}")
            time.sleep(2)
            continue

        if not data:
            break

        all_data.extend(data)
        current = data[-1][0] + 1
        batch += 1

        if batch % 10 == 0:
            print(f"  {symbol}: {len(all_data):,} velas descargadas...")

        time.sleep(0.1)  # rate limit

    return all_data


def save_csv(symbol: str, data: List[list]) -> str:
    fname = SYMBOLS[symbol]
    path  = DATA_DIR / fname
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume",
                     "close_time", "quote_vol", "trades",
                     "taker_buy_base", "taker_buy_quote", "ignore"])
        for row in data:
            w.writerow(row)
    return str(path)


def load_csv(symbol: str) -> List[dict]:
    path = DATA_DIR / SYMBOLS[symbol]
    rows = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "ts":     int(r["timestamp"]),
                "open":   float(r["open"]),
                "high":   float(r["high"]),
                "low":    float(r["low"]),
                "close":  float(r["close"]),
                "volume": float(r["volume"]),
                "trades": int(r["trades"]),
                "taker_buy": float(r["taker_buy_base"]),
            })
    return rows


def download_all():
    print("=" * 60)
    print("DESCARGANDO DATOS DE BINANCE (30 días, 1m)")
    print("=" * 60)
    for symbol in SYMBOLS:
        print(f"\n{symbol}...")
        data = download_klines(symbol, days=30)
        path = save_csv(symbol, data)
        print(f"  Guardado: {path} ({len(data):,} velas)")
    print("\nDescarga completada.\n")


# ═════════════════════════════════════════════════════════════════════════════
# PASO 2 — Motor de backtesting
# ═════════════════════════════════════════════════════════════════════════════

@dataclass
class Trade:
    pair:       str
    entry:      float
    sl:         float
    tp:         float
    score:      int
    ts:         int
    exit_price: float = 0.0
    exit_ts:    int   = 0
    won:        bool  = False
    pnl_usd:    float = 0.0


@dataclass
class BacktestResult:
    threshold:      int
    total_trades:   int   = 0
    wins:           int   = 0
    losses:         int   = 0
    pnl_usd:        float = 0.0
    max_drawdown:   float = 0.0
    trades_per_day: float = 0.0
    win_rate:       float = 0.0
    trades:         List[Trade] = field(default_factory=list)


def calc_cvd_approx(candles: List[dict], i: int, window: int = 20) -> float:
    """CVD aproximado: taker_buy - taker_sell acumulado."""
    start = max(0, i - window)
    cvd = 0.0
    for j in range(start, i + 1):
        c = candles[j]
        buy_vol  = c["taker_buy"]
        sell_vol = c["volume"] - buy_vol
        cvd += buy_vol - sell_vol
    return cvd


def calc_vol_ratio(candles: List[dict], i: int, window: int = 60) -> float:
    """Ratio volumen actual vs promedio de las últimas N velas."""
    if i < window:
        return 1.0
    current = candles[i]["volume"]
    avg = sum(c["volume"] for c in candles[i-window:i]) / window
    if avg <= 0:
        return 1.0
    return current / avg


def detect_spring(candles: List[dict], i: int, params: dict) -> Optional[dict]:
    """Detecta spring en la vela i usando ventana de velas anteriores."""
    if i < params["drop_secs"]:
        return None

    c = candles[i]
    drop_w  = candles[max(0, i - params["drop_secs"]):i + 1]
    bounce_w = candles[max(0, i - params["bounce_secs"]):i + 1]

    high_before = max(v["high"] for v in drop_w)
    spring_low  = min(v["low"]  for v in drop_w)
    drop_pct    = (high_before - spring_low) / high_before if high_before > 0 else 0

    bounce_low = min(v["low"] for v in bounce_w)
    bounce_pct = (c["close"] - bounce_low) / bounce_low if bounce_low > 0 else 0

    cond_a = drop_pct >= params["drop_pct"]
    cond_b = bounce_pct >= params["bounce_pct"]

    if not cond_a and not cond_b:
        return None

    # CVD divergencia aproximada
    cvd_now  = calc_cvd_approx(candles, i, window=params["drop_secs"])
    cvd_high = calc_cvd_approx(candles, i - params["drop_secs"], window=params["drop_secs"])
    cond_c   = cvd_now > cvd_high and cond_a

    vol_ratio = calc_vol_ratio(candles, i)
    cond_d    = vol_ratio >= params["vol_mult"]

    # Score aproximado
    score = 0
    if cond_a and cond_b:
        score += 20   # spring confirmed
    elif cond_a:
        score += 8

    if cond_c:
        score += 10   # CVD divergence

    if vol_ratio >= 1.3:
        score += 8
    elif vol_ratio >= 1.1:
        score += 4

    if cvd_now > 0:
        score += 5    # CVD velocity positive

    # Contexto aproximado (sesión, correlación)
    hour = datetime.fromtimestamp(c["ts"] / 1000, tz=timezone.utc).hour
    if 13 <= hour <= 16:    # overlap session
        score += 8
    elif 14 <= hour <= 21:  # NY session
        score += 5
    elif 3 <= hour <= 12:   # London
        score += 3

    # Orderbook favorable (simulado ~40% del tiempo)
    if hash(c["ts"]) % 5 < 2:
        score += 8

    # BTC-ETH correlation (simulado ~50% confirming)
    if hash(c["ts"]) % 4 < 2:
        score += 10

    return {
        "score":      score,
        "drop_pct":   drop_pct,
        "bounce_pct": bounce_pct,
        "vol_ratio":  vol_ratio,
        "cond_a":     cond_a,
        "cond_b":     cond_b,
        "cond_c":     cond_c,
        "cond_d":     cond_d,
    }


def run_backtest(symbol: str, candles: List[dict], threshold: int) -> BacktestResult:
    """Corre backtest completo sobre un par con un threshold dado."""
    result = BacktestResult(threshold=threshold)
    equity = CAPITAL
    peak   = CAPITAL
    open_trade: Optional[Trade] = None
    cooldown_until = 0
    days = (candles[-1]["ts"] - candles[0]["ts"]) / 1000 / 86400 if candles else 1

    for i, c in enumerate(candles):
        price = c["close"]

        # Gestionar trade abierto
        if open_trade:
            if c["low"] <= open_trade.sl:
                # Stop loss hit
                open_trade.won = False
                open_trade.exit_price = open_trade.sl
                open_trade.exit_ts = c["ts"]
                open_trade.pnl_usd = -TRADE_SIZE * SL_PCT
                equity += open_trade.pnl_usd
                result.losses += 1
                result.trades.append(open_trade)
                open_trade = None
                cooldown_until = c["ts"] + 60_000 * 5  # 5 min cooldown
            elif c["high"] >= open_trade.tp:
                # Take profit hit
                open_trade.won = True
                open_trade.exit_price = open_trade.tp
                open_trade.exit_ts = c["ts"]
                open_trade.pnl_usd = TRADE_SIZE * SL_PCT * RR
                equity += open_trade.pnl_usd
                result.wins += 1
                result.trades.append(open_trade)
                open_trade = None
                cooldown_until = c["ts"] + 60_000 * 2  # 2 min cooldown
            continue

        # Cooldown
        if c["ts"] < cooldown_until:
            continue

        # Detectar spring
        spring = detect_spring(candles, i, SPRING_PARAMS)
        if spring is None:
            continue
        if spring["score"] < threshold:
            continue

        # Abrir trade
        entry = price
        sl    = entry * (1 - SL_PCT)
        tp    = entry * (1 + SL_PCT * RR)
        open_trade = Trade(
            pair=symbol, entry=entry, sl=sl, tp=tp,
            score=spring["score"], ts=c["ts"],
        )

        # Track drawdown
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > result.max_drawdown:
            result.max_drawdown = dd

    # Cerrar trade pendiente al final
    if open_trade:
        open_trade.exit_price = candles[-1]["close"]
        pnl = (open_trade.exit_price - open_trade.entry) / open_trade.entry * TRADE_SIZE
        open_trade.pnl_usd = pnl
        open_trade.won = pnl > 0
        if pnl > 0:
            result.wins += 1
        else:
            result.losses += 1
        result.trades.append(open_trade)
        equity += pnl

    result.total_trades  = result.wins + result.losses
    result.pnl_usd       = round(equity - CAPITAL, 4)
    result.win_rate       = (result.wins / result.total_trades * 100) if result.total_trades > 0 else 0
    result.trades_per_day = result.total_trades / max(days, 1)
    result.max_drawdown   = round(result.max_drawdown, 2)

    return result


# ═════════════════════════════════════════════════════════════════════════════
# PASO 3 — Multi-threshold test
# ═════════════════════════════════════════════════════════════════════════════

def run_all():
    thresholds = [40, 45, 50, 55, 60]
    all_results: Dict[int, BacktestResult] = {}

    for thr in thresholds:
        combined = BacktestResult(threshold=thr)
        for symbol in SYMBOLS:
            candles = load_csv(symbol)
            r = run_backtest(symbol, candles, thr)
            combined.total_trades  += r.total_trades
            combined.wins          += r.wins
            combined.losses        += r.losses
            combined.pnl_usd       += r.pnl_usd
            combined.max_drawdown   = max(combined.max_drawdown, r.max_drawdown)
            combined.trades.extend(r.trades)

        days = 30
        combined.trades_per_day = combined.total_trades / days
        combined.win_rate = (combined.wins / combined.total_trades * 100) if combined.total_trades > 0 else 0
        combined.pnl_usd = round(combined.pnl_usd, 2)
        all_results[thr] = combined

    return all_results


def print_results(results: Dict[int, BacktestResult]):
    print("\n" + "=" * 75)
    print("RESULTADOS BACKTESTING — 30 DIAS — BTC+ETH+SOL — LATERAL REGIME")
    print("=" * 75)
    print(f"{'Threshold':>10} | {'Trades':>7} | {'T/dia':>6} | {'Wins':>5} | {'Losses':>6} | {'WR%':>6} | {'PnL $':>8} | {'MaxDD%':>7}")
    print("-" * 75)

    for thr in sorted(results.keys()):
        r = results[thr]
        marker = " <--" if r.win_rate >= 55 and r.trades_per_day >= 2 else ""
        print(
            f"{thr:>10} | {r.total_trades:>7} | {r.trades_per_day:>6.1f} | "
            f"{r.wins:>5} | {r.losses:>6} | {r.win_rate:>5.1f}% | "
            f"${r.pnl_usd:>+7.2f} | {r.max_drawdown:>6.2f}%{marker}"
        )

    print("-" * 75)
    print("Nota: <-- indica WR>=55% Y trades/dia>=2 (candidatos óptimos)")
    print(f"Capital: ${CAPITAL} | Size: ${TRADE_SIZE}/trade | SL: {SL_PCT*100}% | RR: 1:{RR}")


def find_best(results: Dict[int, BacktestResult]) -> Optional[int]:
    """Encuentra el mejor threshold: WR>=55% AND trades/dia>=2, máximo PnL."""
    candidates = {
        thr: r for thr, r in results.items()
        if r.win_rate >= 55 and r.trades_per_day >= 2
    }
    if not candidates:
        # Fallback: mejor PnL con WR >= 50%
        candidates = {thr: r for thr, r in results.items() if r.win_rate >= 50}
    if not candidates:
        return None
    return max(candidates.keys(), key=lambda t: candidates[t].pnl_usd)


def save_to_supabase(threshold: int):
    """Guarda el threshold óptimo en Supabase bot_config."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        from supabase import create_client
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        client = create_client(url, key)
        client.table("bot_config").upsert({
            "key":   "score_threshold_lateral",
            "value": str(threshold),
        }, on_conflict="key").execute()
        print(f"\nThreshold optimo guardado en Supabase: {threshold}")
    except Exception as exc:
        print(f"\nError guardando en Supabase: {exc}")


# ═════════════════════════════════════════════════════════════════════════════
# Main
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    skip_download = "--no-download" in sys.argv
    save_best     = "--save-best" in sys.argv

    if not skip_download:
        download_all()

    # Verificar que los datos existen
    for symbol, fname in SYMBOLS.items():
        if not (DATA_DIR / fname).exists():
            print(f"ERROR: No se encontró {DATA_DIR / fname}")
            print("Ejecuta sin --no-download para descargar datos primero")
            sys.exit(1)

    print("\nCorriendo backtests...")
    results = run_all()
    print_results(results)

    best = find_best(results)
    if best:
        r = results[best]
        print(f"\nMEJOR THRESHOLD: {best}")
        print(f"  Win Rate: {r.win_rate:.1f}%")
        print(f"  Trades/dia: {r.trades_per_day:.1f}")
        print(f"  PnL 30d: ${r.pnl_usd:+.2f}")
        print(f"  Max DD: {r.max_drawdown:.2f}%")

        if save_best:
            save_to_supabase(best)
    else:
        print("\nNingún threshold cumple WR>=55% Y trades/dia>=2")
        print("Revisa los parámetros del spring detector")
