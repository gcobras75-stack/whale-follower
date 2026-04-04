"""
backtest_report.py — Whale Follower Bot
Genera reporte de texto del backtest y lo guarda en data/backtest_report.txt
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from backtester import BacktestResult

REPORT_FILE = Path(__file__).parent / "data" / "backtest_report.txt"


def _bar(ratio: float, width: int = 20) -> str:
    filled = int(ratio * width)
    return "#" * filled + "-" * (width - filled)


def _win_rate(d: dict) -> float:
    return d["wins"] / d["trades"] if d.get("trades", 0) > 0 else 0.0


def generate_report(
    result:      BacktestResult,
    best_combos: List[dict],
    params_used: dict,
) -> str:
    """Genera reporte completo. Lo guarda en disco y lo retorna como string."""
    lines: List[str] = []

    def sep():
        lines.append("=" * 52)

    def h(title: str):
        lines.append("")
        sep()
        lines.append(f"  {title}")
        sep()

    # ── Encabezado ────────────────────────────────────────────────────────────
    sep()
    lines.append("  WHALE FOLLOWER BOT — INFORME DE BACKTESTING")
    sep()
    lines.append(f"Fecha:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"Período:  60 días BTC/USDT (velas de 1 minuto)")
    lines.append(
        f"Params:   threshold={params_used.get('SIGNAL_SCORE_THRESHOLD')} | "
        f"drop={params_used.get('SPRING_DROP_PCT', 0)*100:.2f}% | "
        f"bounce={params_used.get('SPRING_BOUNCE_PCT', 0)*100:.2f}% | "
        f"vol={params_used.get('VOLUME_MULTIPLIER')}x | "
        f"sl={params_used.get('STOP_LOSS_PCT', 0)*100:.2f}%"
    )

    # ── Resumen general ───────────────────────────────────────────────────────
    h("RESUMEN GENERAL")
    r = result
    lines += [
        f"  Total de trades:       {r.total_trades}",
        f"  Ganadores:             {r.winners}",
        f"  Perdedores:            {r.losers}",
        f"  Win Rate:              {r.win_rate:.1%}",
        f"  Profit Factor:         {r.profit_factor:.2f}",
        f"  Max Drawdown:          {r.max_drawdown:.1%}",
        f"  Retorno total:         {r.total_return_pct:+.2f}%",
        f"  Capital final:         ${r.final_capital:,.2f}",
        f"  Racha max. ganadora:   {r.max_win_streak} trades seguidos",
        f"  Racha max. perdedora:  {r.max_loss_streak} trades seguidos",
        f"  Promedio ganador:      {r.avg_winner_pct:+.2f}%",
        f"  Promedio perdedor:     {r.avg_loser_pct:+.2f}%",
    ]

    # ── Win rate por semana ───────────────────────────────────────────────────
    h("WIN RATE POR SEMANA")
    if r.by_week:
        for week in sorted(r.by_week.keys()):
            d  = r.by_week[week]
            wr = _win_rate(d)
            lines.append(
                f"  Semana {week+1:2d}:  {wr:5.1%}  [{_bar(wr)}]  "
                f"{d['trades']:3d} trades  {d['wins']} gan."
            )
    else:
        lines.append("  Sin datos por semana.")

    # ── Win rate por sesión ───────────────────────────────────────────────────
    h("WIN RATE POR SESION")
    session_order = ["overlap", "new_york", "london", "asia", "off_hours"]
    for name in session_order:
        if name not in r.by_session:
            continue
        d  = r.by_session[name]
        wr = _win_rate(d)
        lines.append(
            f"  {name:12s}:  {wr:5.1%}  [{_bar(wr)}]  "
            f"{d['trades']:3d} trades  {d['wins']} gan."
        )
    # Sesiones no listadas
    for name, d in r.by_session.items():
        if name not in session_order:
            wr = _win_rate(d)
            lines.append(
                f"  {name:12s}:  {wr:5.1%}  [{_bar(wr)}]  "
                f"{d['trades']:3d} trades  {d['wins']} gan."
            )

    # ── Win rate por score ────────────────────────────────────────────────────
    h("WIN RATE POR RANGO DE SCORE")
    for bucket in ["55-65", "65-75", "75-85", "85-100"]:
        if bucket not in r.by_score:
            continue
        d  = r.by_score[bucket]
        wr = _win_rate(d)
        lines.append(
            f"  Score {bucket}:  {wr:5.1%}  [{_bar(wr)}]  "
            f"{d['trades']:3d} trades  {d['wins']} gan."
        )

    # ── Mejores combinaciones ─────────────────────────────────────────────────
    if best_combos:
        h("TOP 5 COMBINACIONES DE PARAMETROS")
        for i, combo in enumerate(best_combos[:5], 1):
            lines.append(
                f"  #{i}  WR={combo['win_rate']:.1%}  "
                f"PF={combo['profit_factor']:.2f}  "
                f"DD={combo['max_drawdown']:.1%}  "
                f"Trades={combo['total_trades']}"
            )
            lines.append(
                f"      threshold={combo['SIGNAL_SCORE_THRESHOLD']}  "
                f"drop={combo['SPRING_DROP_PCT']*100:.2f}%  "
                f"bounce={combo['SPRING_BOUNCE_PCT']*100:.2f}%  "
                f"vol={combo['VOLUME_MULTIPLIER']}x  "
                f"sl={combo['STOP_LOSS_PCT']*100:.2f}%"
            )

    # ── Curva de capital por semana ───────────────────────────────────────────
    h("CURVA DE CAPITAL POR SEMANA")
    capital   = 10_000.0
    ini_cap   = capital
    lines.append(f"  Inicio:   ${capital:>10,.2f}")
    for week in sorted(r.by_week.keys()):
        week_trades = [t for t in r.trades if t.week_number == week]
        week_pnl    = sum(t.pnl_usd for t in week_trades)
        capital    += week_pnl
        arrow  = "+" if week_pnl >= 0 else ""
        change = (capital - ini_cap) / ini_cap * 100
        lines.append(
            f"  Semana {week+1:2d}: ${capital:>10,.2f}  "
            f"({arrow}{week_pnl:,.2f} USD | {change:+.1f}% total)"
        )

    # ── Cierre del informe ────────────────────────────────────────────────────
    h("NOTAS")
    lines += [
        "  - CVD aproximado desde OHLCV (sin ticks individuales).",
        "  - Sin datos historicos de Funding Rate / Open Interest.",
        "  - Slippage y comisiones NO incluidos (~0.1% por operacion).",
        "  - Los resultados son indicativos, no garantizan rendimiento futuro.",
        "  - Minimo 50 trades para significancia estadistica.",
    ]
    sep()

    report = "\n".join(lines)

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(report)

    return report
