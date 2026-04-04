"""
auto_calibrator.py — Whale Follower Bot
Grid search de parámetros sobre datos históricos.
Encuentra la combinación que maximiza win_rate × profit_factor.
"""
from __future__ import annotations

import csv
import time
from itertools import product
from pathlib import Path
from typing import Dict, List, Tuple

from backtester import BacktestResult, DEFAULT_PARAMS, run_backtest

DATA_DIR = Path(__file__).parent / "data"

# ── Grid de parámetros a explorar ─────────────────────────────────────────────

PARAM_GRID: Dict[str, List] = {
    "SIGNAL_SCORE_THRESHOLD": [55, 60, 65, 70, 75, 80],
    "SPRING_DROP_PCT":        [0.002, 0.003, 0.004, 0.005],
    "SPRING_BOUNCE_PCT":      [0.0015, 0.002, 0.0025, 0.003],
    "VOLUME_MULTIPLIER":      [1.3, 1.5, 1.8, 2.0],
    "STOP_LOSS_PCT":          [0.003, 0.005, 0.007],
}

# ── Criterios de calificación ─────────────────────────────────────────────────

MIN_WIN_RATE     = 0.60
MIN_PROFIT_FACTOR= 2.0
MAX_DRAWDOWN     = 0.15
MIN_TRADES       = 50


def _composite_score(row: dict) -> float:
    """Métrica compuesta para rankear combinaciones."""
    return row["win_rate"] * row["profit_factor"]


def calibrate(
    klines: List[dict],
    verbose: bool = True,
) -> Tuple[dict, List[dict]]:
    """
    Ejecuta el grid search completo.
    Retorna (best_params dict, lista de top-10 resultados ordenados).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    keys   = list(PARAM_GRID.keys())
    combos = list(product(*[PARAM_GRID[k] for k in keys]))
    total  = len(combos)

    if verbose:
        print(f"[calibrator] Grid search: {total:,} combinaciones")
        print(f"[calibrator] Criterios: win_rate>={MIN_WIN_RATE:.0%}, "
              f"profit_factor>={MIN_PROFIT_FACTOR}, "
              f"max_dd<={MAX_DRAWDOWN:.0%}, trades>={MIN_TRADES}")

    all_results: List[dict] = []
    t_start = time.time()

    for num, combo in enumerate(combos, 1):
        params = dict(zip(keys, combo))

        result = run_backtest(klines, params)

        if result.total_trades < MIN_TRADES:
            continue

        row = {
            **params,
            "total_trades":     result.total_trades,
            "win_rate":         round(result.win_rate, 4),
            "profit_factor":    round(result.profit_factor, 4),
            "max_drawdown":     round(result.max_drawdown, 4),
            "final_capital":    round(result.final_capital, 2),
            "total_return_pct": round(result.total_return_pct, 2),
            "max_loss_streak":  result.max_loss_streak,
        }
        all_results.append(row)

        # Progreso cada 25 combos
        if verbose and (num % 25 == 0 or num == total):
            elapsed   = time.time() - t_start
            remaining = elapsed / num * (total - num)
            best_wr   = max((r["win_rate"] for r in all_results), default=0.0)
            print(
                f"\r[calibrator] {num:4d}/{total} | "
                f"válidas: {len(all_results):4d} | "
                f"mejor WR: {best_wr:.1%} | "
                f"ETA: {remaining:.0f}s   ",
                end="", flush=True,
            )

    elapsed_total = time.time() - t_start
    if verbose:
        print(f"\n[calibrator] Completado en {elapsed_total:.0f}s | "
              f"{len(all_results)} combinaciones con >={MIN_TRADES} trades")

    # ── Guardar CSV completo ──────────────────────────────────────────────────
    results_csv = DATA_DIR / "calibration_results.csv"
    if all_results:
        with open(results_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_results[0].keys())
            writer.writeheader()
            writer.writerows(all_results)
        if verbose:
            print(f"[calibrator] CSV guardado: {results_csv}")

    # ── Filtrar combinaciones calificadas ─────────────────────────────────────
    qualified = [
        r for r in all_results
        if (r["win_rate"]      >= MIN_WIN_RATE
            and r["profit_factor"] >= MIN_PROFIT_FACTOR
            and r["max_drawdown"]  <= MAX_DRAWDOWN)
    ]

    if not qualified:
        if verbose:
            print(f"[calibrator] AVISO: Ninguna combinación califica con criterios estrictos.")
            print(f"[calibrator] Usando top-10 por métrica compuesta...")
        qualified = sorted(all_results, key=_composite_score, reverse=True)[:20]

    # Ordenar por métrica compuesta
    qualified.sort(key=_composite_score, reverse=True)
    top10 = qualified[:10]

    # ── Construir best_params ────────────────────────────────────────────────
    if top10:
        best = top10[0]
        best_params = {k: best[k] for k in PARAM_GRID.keys()}
        # Conservar parámetros no calibrados
        best_params["RISK_REWARD"]   = DEFAULT_PARAMS["RISK_REWARD"]
        best_params["PAPER_CAPITAL"] = DEFAULT_PARAMS["PAPER_CAPITAL"]
        if verbose:
            print(
                f"[calibrator] Mejor combinacion: "
                f"score>={best['SIGNAL_SCORE_THRESHOLD']} | "
                f"drop>={best['SPRING_DROP_PCT']*100:.2f}% | "
                f"bounce>={best['SPRING_BOUNCE_PCT']*100:.2f}% | "
                f"vol>={best['VOLUME_MULTIPLIER']}x | "
                f"sl={best['STOP_LOSS_PCT']*100:.2f}%"
            )
            print(
                f"[calibrator] Resultado: "
                f"WR={best['win_rate']:.1%} | "
                f"PF={best['profit_factor']:.2f} | "
                f"DD={best['max_drawdown']:.1%} | "
                f"Trades={best['total_trades']}"
            )
    else:
        best_params = DEFAULT_PARAMS.copy()
        if verbose:
            print("[calibrator] Sin resultados válidos. Usando parámetros por defecto.")

    return best_params, top10
