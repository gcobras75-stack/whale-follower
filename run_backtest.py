"""
run_backtest.py — Whale Follower Bot
Script principal de backtesting. Ejecutar desde whale-follower/:

    python run_backtest.py

Pasos:
  1. Descarga 60 días de velas 1m desde Binance
  2. Backtest con parámetros actuales
  3. Auto-calibración (grid search)
  4. Backtest con parámetros optimizados
  5. Genera reporte completo
  6. Aplica mejores parámetros al .env
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

# ── Asegurar que el directorio del script esté en sys.path ───────────────────
_HERE = Path(__file__).parent.resolve()
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# Carga .env si existe (para leer parámetros actuales sin importar config.py)
try:
    from dotenv import load_dotenv
    load_dotenv(_HERE / ".env")
except ImportError:
    pass


def _print_box(lines: list[str], width: int = 44) -> None:
    top    = "+" + "=" * width + "+"
    mid    = "+" + "-" * width + "+"
    bottom = "+" + "=" * width + "+"
    print(top)
    for i, line in enumerate(lines):
        padded = f" {line:<{width-1}}"
        print(f"|{padded}|")
        if i == 0:
            print(mid)
    print(bottom)


def _current_params() -> dict:
    """Lee parámetros del .env (ya cargado) con fallbacks."""
    return {
        "SIGNAL_SCORE_THRESHOLD": int(os.getenv("SIGNAL_SCORE_THRESHOLD", "65")),
        "SPRING_DROP_PCT":        float(os.getenv("SPRING_DROP_PCT",   "0.003")),
        "SPRING_BOUNCE_PCT":      float(os.getenv("SPRING_BOUNCE_PCT", "0.002")),
        "VOLUME_MULTIPLIER":      float(os.getenv("VOLUME_MULTIPLIER", "1.5")),
        "STOP_LOSS_PCT":          float(os.getenv("STOP_LOSS_PCT",     "0.005")),
        "RISK_REWARD":            float(os.getenv("RISK_REWARD",       "3.0")),
        "PAPER_CAPITAL":          float(os.getenv("PAPER_CAPITAL",     "10000")),
    }


def main() -> None:
    print()
    print("+=" * 24 + "+")
    print("|   WHALE FOLLOWER BOT -- SISTEMA DE BACKTEST  |")
    print("+=" * 24 + "+")
    print()

    t_global = time.time()

    # ── 1. Descarga de datos históricos ──────────────────────────────────────
    print("-" * 52)
    print("PASO 1/5 — Descarga de datos históricos (60 días)")
    print("-" * 52)
    from historical_downloader import download_historical, load_klines

    filepath = download_historical(days=60, force=False)

    print("Cargando velas en memoria...")
    t0     = time.time()
    klines = load_klines(filepath)
    print(f"OK: {len(klines):,} velas cargadas en {time.time()-t0:.1f}s")

    if len(klines) < 5000:
        print("ERROR: Datos insuficientes para backtest (< 5000 velas). Abortando.")
        sys.exit(1)

    # ── 2. Backtest con parámetros actuales ──────────────────────────────────
    print()
    print("-" * 52)
    print("PASO 2/5 — Backtest con parámetros actuales")
    print("-" * 52)
    from backtester import run_backtest

    current_params = _current_params()
    t0 = time.time()
    baseline = run_backtest(klines, current_params)
    elapsed  = time.time() - t0
    print(
        f"Baseline ({elapsed:.1f}s): "
        f"trades={baseline.total_trades}  "
        f"WR={baseline.win_rate:.1%}  "
        f"PF={baseline.profit_factor:.2f}  "
        f"DD={baseline.max_drawdown:.1%}  "
        f"Retorno={baseline.total_return_pct:+.1f}%"
    )

    # ── 3. Auto-calibración ───────────────────────────────────────────────────
    print()
    print("-" * 52)
    print("PASO 3/5 — Auto-calibración de parámetros (grid search)")
    print("-" * 52)
    from auto_calibrator import calibrate

    t0 = time.time()
    best_params, top_combos = calibrate(klines, verbose=True)
    elapsed = time.time() - t0
    print(f"Calibración completada en {elapsed:.0f}s")

    # ── 4. Backtest con parámetros optimizados ────────────────────────────────
    print()
    print("-" * 52)
    print("PASO 4/5 — Backtest con parámetros optimizados")
    print("-" * 52)
    t0   = time.time()
    best = run_backtest(klines, best_params)
    elapsed = time.time() - t0
    print(
        f"Optimizado ({elapsed:.1f}s): "
        f"trades={best.total_trades}  "
        f"WR={best.win_rate:.1%}  "
        f"PF={best.profit_factor:.2f}  "
        f"DD={best.max_drawdown:.1%}  "
        f"Retorno={best.total_return_pct:+.1f}%"
    )

    # ── 5. Reporte ────────────────────────────────────────────────────────────
    print()
    print("-" * 52)
    print("PASO 5/5 — Generando reporte")
    print("-" * 52)
    from backtest_report import generate_report, REPORT_FILE

    report = generate_report(best, top_combos, best_params)
    # Mostrar primeras 60 líneas del reporte
    for line in report.split("\n")[:65]:
        print(line)
    print(f"... (reporte completo en {REPORT_FILE})")

    # ── Aplicar mejores parámetros ────────────────────────────────────────────
    if top_combos:
        print()
        print("Aplicando parámetros optimizados al .env...")
        from apply_best_params import apply_params

        best_combo = top_combos[0]
        apply_params(
            best_params,
            win_rate      = best_combo["win_rate"],
            profit_factor = best_combo["profit_factor"],
            total_trades  = best_combo["total_trades"],
        )

    # ── Resumen final ─────────────────────────────────────────────────────────
    def _win_rate(d: dict) -> float:
        return d["wins"] / d["trades"] if d.get("trades", 0) > 0 else 0.0

    r = best
    best_session = (
        max(r.by_session.items(), key=lambda x: _win_rate(x[1]))[0]
        if r.by_session else "N/A"
    )

    total_elapsed = time.time() - t_global
    print()
    summary = [
        "WHALE FOLLOWER — BACKTEST COMPLETO",
        f"Período: 60 días  ({len(klines):,} velas 1m)",
        f"Total trades:   {r.total_trades}",
        f"Win Rate:       {r.win_rate:.1%}",
        f"Profit Factor:  {r.profit_factor:.2f}",
        f"Max Drawdown:   {r.max_drawdown:.1%}",
        f"Capital final:  ${r.final_capital:,.2f}",
        f"Retorno:        {r.total_return_pct:+.1f}%",
        f"Mejor sesión:   {best_session}",
        f"Score threshold:{best_params['SIGNAL_SCORE_THRESHOLD']}",
        f"Stop Loss:      {best_params['STOP_LOSS_PCT']*100:.2f}%",
        f"Tiempo total:   {total_elapsed:.0f}s",
    ]
    _print_box(summary, width=44)
    print()
    print(f"Reporte completo: {REPORT_FILE}")
    print()


if __name__ == "__main__":
    main()
