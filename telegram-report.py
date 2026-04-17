#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
telegram-report.py — Whale Follower Bot — Reporte diario por email
==================================================================
Lee mensajes del bot de Telegram, consulta exchanges (Bybit/OKX) y Supabase,
genera graficos de balance/PnL/trades y envia reporte HTML a Gmail.

Uso:
  python telegram-report.py              # Ejecutar reporte ahora
  python telegram-report.py --schedule   # Programar ejecucion diaria 8AM CST

Requiere:
  pip install python-dotenv ccxt matplotlib requests supabase
  Variable GMAIL_APP_PASSWORD en .env (ver instrucciones al final)
"""
from __future__ import annotations

import base64
import io
import json
import os
import re
import smtplib
import sys
from datetime import datetime, timezone, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# ── Cargar .env ──────────────────────────────────────────────────────────────
_PROJECT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT / ".env")
except (ImportError, ValueError):
    for line in (_PROJECT / ".env").read_text(errors="replace").splitlines():
        line = line.strip().replace("\x00", "")
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip()
        if k and not any(c in k for c in " \t"):
            os.environ.setdefault(k, v)

# ── Configuracion ────────────────────────────────────────────────────────────
TZ_CST = timezone(timedelta(hours=-6))  # CST = UTC-6

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Email
GMAIL_USER = os.environ.get("GMAIL_USER", "gcobras75@gmail.com")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "gcobras75@gmail.com")

# Directorio de reportes
REPORTS_DIR = _PROJECT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# =============================================================================
# 1. LEER MENSAJES DE TELEGRAM
# =============================================================================

def get_telegram_messages(limit: int = 50) -> list[dict]:
    """
    Lee los ultimos mensajes del bot via Telegram Bot API.
    Usa getUpdates para obtener mensajes recientes del chat.
    """
    import requests

    if not TELEGRAM_BOT_TOKEN:
        print("[telegram] ERROR: TELEGRAM_BOT_TOKEN no configurado")
        return []

    messages = []
    try:
        # Obtener updates recientes
        resp = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"limit": limit, "allowed_updates": '["message","channel_post"]'},
            timeout=15,
        )
        data = resp.json()

        if not data.get("ok"):
            print(f"[telegram] Error API: {data.get('description', 'unknown')}")
            return []

        for update in data.get("result", []):
            msg = update.get("message") or update.get("channel_post") or {}
            if msg.get("text"):
                messages.append({
                    "date": datetime.fromtimestamp(msg["date"], tz=timezone.utc),
                    "text": msg["text"],
                    "from": msg.get("from", {}).get("first_name", "Bot"),
                    "is_bot": msg.get("from", {}).get("is_bot", False),
                })

        print(f"[telegram] {len(messages)} mensajes leidos")
    except Exception as e:
        print(f"[telegram] Error: {e}")

    return messages


def parse_telegram_data(messages: list[dict]) -> dict:
    """
    Extrae informacion relevante de los mensajes de Telegram:
    - Balances reportados
    - Trades ejecutados
    - Alertas y senales
    - Reportes diarios previos
    """
    balances = []
    trades = []
    alerts = []
    signals = []
    reports = []

    for msg in messages:
        text = msg["text"]
        dt = msg["date"]

        # Detectar reportes diarios
        if "REPORTE DIARIO" in text or "REPORTE" in text.upper():
            reports.append({"date": dt, "text": text})

        # Detectar balances (patron: $XX.XX USD o USDT)
        bal_matches = re.findall(r'\$?([\d,]+\.?\d*)\s*(?:USD|USDT)', text)
        for b in bal_matches:
            try:
                val = float(b.replace(",", ""))
                if 0.01 < val < 1_000_000:  # filtrar valores razonables
                    balances.append({"date": dt, "value": val, "source": text[:80]})
            except ValueError:
                pass

        # Detectar trades (patron: BUY/SELL, LONG/SHORT, etc.)
        if any(kw in text.upper() for kw in ["BUY", "SELL", "LONG", "SHORT", "FILLED", "EJECUT"]):
            # Extraer par
            pair_match = re.search(r'(BTC|ETH|SOL|BNB|DOGE|XRP|ADA|AVAX|LINK)\s*/?\.?\s*(USDT|USD)', text, re.I)
            pair = pair_match.group(0).upper() if pair_match else "?"

            # Extraer PnL si existe
            pnl_match = re.search(r'[Pp]n[Ll]\s*[:=]?\s*\$?([\-+]?[\d,]+\.?\d*)', text)
            pnl = float(pnl_match.group(1).replace(",", "")) if pnl_match else None

            # Extraer side
            side = "LONG" if any(kw in text.upper() for kw in ["BUY", "LONG"]) else "SHORT"

            trades.append({
                "date": dt,
                "pair": pair,
                "side": side,
                "pnl": pnl,
                "raw": text[:120],
            })

        # Detectar alertas/warnings
        if any(kw in text.upper() for kw in ["ALERTA", "WARNING", "ERROR", "DRAWDOWN", "LIQUIDAT"]):
            alerts.append({"date": dt, "text": text[:200]})

        # Detectar senales de trading
        if any(kw in text.upper() for kw in ["SIGNAL", "SENAL", "SPRING", "WYCKOFF", "SCORE"]):
            score_match = re.search(r'[Ss]core\s*[:=]?\s*(\d+)', text)
            score = int(score_match.group(1)) if score_match else None
            signals.append({"date": dt, "score": score, "text": text[:150]})

    return {
        "balances": balances,
        "trades": trades,
        "alerts": alerts,
        "signals": signals,
        "reports": reports,
        "total_messages": len(messages),
    }


# =============================================================================
# 2. CONSULTAR EXCHANGES (Bybit + OKX)
# =============================================================================

def get_exchange_balances() -> dict:
    """Consulta balances en tiempo real de Bybit y OKX."""
    result = {"bybit": None, "okx": None, "total_usd": 0}

    try:
        import ccxt

        # Bybit
        try:
            bybit = ccxt.bybit({
                "apiKey": os.environ.get("BYBIT_API_KEY", ""),
                "secret": os.environ.get("BYBIT_API_SECRET", ""),
                "options": {"defaultType": "unified"},
            })
            bal = bybit.fetch_balance()
            total = float(bal.get("total", {}).get("USDT", 0))
            result["bybit"] = {"total_usd": total, "detail": {}, "error": None}
            # Detalles por moneda
            for coin, amt in bal.get("total", {}).items():
                amt_f = float(amt) if amt else 0
                if amt_f > 0.001:
                    result["bybit"]["detail"][coin] = amt_f
        except Exception as e:
            result["bybit"] = {"total_usd": 0, "detail": {}, "error": str(e)[:100]}

        # OKX
        try:
            okx = ccxt.okx({
                "apiKey": os.environ.get("OKX_API_KEY", ""),
                "secret": os.environ.get("OKX_SECRET", ""),
                "password": os.environ.get("OKX_PASSPHRASE", ""),
            })
            bal = okx.fetch_balance()
            total = float(bal.get("total", {}).get("USDT", 0))
            detail = {}
            for coin, amt in bal.get("total", {}).items():
                amt_f = float(amt) if amt else 0
                if amt_f > 0.01:
                    detail[coin] = amt_f
            result["okx"] = {"total_usd": total, "detail": detail, "error": None}
        except Exception as e:
            result["okx"] = {"total_usd": 0, "detail": {}, "error": str(e)[:100]}

        bybit_usd = result["bybit"]["total_usd"] if result["bybit"] else 0
        okx_usd = result["okx"]["total_usd"] if result["okx"] else 0
        result["total_usd"] = bybit_usd + okx_usd

    except ImportError:
        print("[exchanges] ccxt no instalado — omitiendo consulta de exchanges")

    return result


# =============================================================================
# 3. CONSULTAR SUPABASE (trades historicos)
# =============================================================================

def get_supabase_trades(days: int = 7) -> list[dict]:
    """Obtiene trades de Supabase de los ultimos N dias."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[supabase] Credenciales no configuradas")
        return []

    try:
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = sb.table("paper_trades") \
            .select("*") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=True) \
            .limit(500) \
            .execute()
        trades = result.data or []
        print(f"[supabase] {len(trades)} trades de los ultimos {days} dias")
        return trades
    except Exception as e:
        print(f"[supabase] Error: {e}")
        return []


def analyze_supabase_trades(trades: list) -> dict:
    """Analiza trades de Supabase y genera metricas."""
    closed = [t for t in trades if t.get("status") == "closed"]
    wins = [t for t in closed if (t.get("pnl_usd") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl_usd") or 0) < 0]
    pnl_total = sum(t.get("pnl_usd", 0) or 0 for t in closed)
    win_rate = (len(wins) / len(closed) * 100) if closed else 0

    # PnL por dia
    daily_pnl = {}
    for t in closed:
        day = (t.get("created_at") or "")[:10]
        if day:
            daily_pnl.setdefault(day, 0)
            daily_pnl[day] += t.get("pnl_usd", 0) or 0

    # PnL por estrategia
    by_strategy = {}
    for t in closed:
        s = t.get("strategy", "unknown")
        by_strategy.setdefault(s, {"count": 0, "wins": 0, "pnl": 0})
        by_strategy[s]["count"] += 1
        if (t.get("pnl_usd") or 0) > 0:
            by_strategy[s]["wins"] += 1
        by_strategy[s]["pnl"] += t.get("pnl_usd", 0) or 0

    # PnL por par
    by_pair = {}
    for t in closed:
        p = t.get("pair", "unknown")
        by_pair.setdefault(p, {"count": 0, "pnl": 0})
        by_pair[p]["count"] += 1
        by_pair[p]["pnl"] += t.get("pnl_usd", 0) or 0

    return {
        "total": len(closed),
        "open": len([t for t in trades if t.get("status") == "open"]),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "pnl_total": pnl_total,
        "daily_pnl": daily_pnl,
        "by_strategy": by_strategy,
        "by_pair": by_pair,
    }


# =============================================================================
# 4. GENERAR GRAFICOS
# =============================================================================

def generate_charts(metrics: dict, exchanges: dict, tg_data: dict) -> dict[str, bytes]:
    """
    Genera graficos PNG en memoria:
    - balance_chart: Balance por exchange (barras)
    - pnl_chart: PnL diario (linea + barras)
    - trades_chart: Distribucion de trades por par (pie)
    - strategy_chart: PnL por estrategia (barras horizontales)
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # backend sin GUI
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        print("[charts] matplotlib no instalado — sin graficos")
        return {}

    charts = {}
    plt.rcParams.update({
        "figure.facecolor": "#1a1a2e",
        "axes.facecolor": "#16213e",
        "axes.edgecolor": "#e94560",
        "text.color": "#eee",
        "axes.labelcolor": "#eee",
        "xtick.color": "#aaa",
        "ytick.color": "#aaa",
        "grid.color": "#333",
        "font.size": 11,
    })

    # ── 1. Balance por exchange ──────────────────────────────────────────────
    try:
        fig, ax = plt.subplots(figsize=(8, 4))
        names, values, colors = [], [], []

        if exchanges.get("bybit") and exchanges["bybit"]["total_usd"] > 0:
            names.append("Bybit")
            values.append(exchanges["bybit"]["total_usd"])
            colors.append("#f7931a")
        if exchanges.get("okx") and exchanges["okx"]["total_usd"] > 0:
            names.append("OKX")
            values.append(exchanges["okx"]["total_usd"])
            colors.append("#00d4aa")

        if names:
            bars = ax.bar(names, values, color=colors, width=0.5, edgecolor="#fff", linewidth=0.5)
            for bar, val in zip(bars, values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                        f"${val:.2f}", ha="center", va="bottom", fontweight="bold", color="#fff")
            ax.set_title("BALANCE POR EXCHANGE", fontweight="bold", fontsize=14, color="#e94560")
            ax.set_ylabel("USD")
            total = sum(values)
            ax.axhline(y=total/len(values), color="#e94560", linestyle="--", alpha=0.5, label=f"Total: ${total:.2f}")
            ax.legend(loc="upper right")
        else:
            ax.text(0.5, 0.5, "Sin datos de exchanges", ha="center", va="center", transform=ax.transAxes, fontsize=14)
            ax.set_title("BALANCE POR EXCHANGE", fontweight="bold", fontsize=14, color="#e94560")

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
        buf.seek(0)
        charts["balance_chart"] = buf.read()
        plt.close(fig)
    except Exception as e:
        print(f"[charts] Error balance: {e}")

    # ── 2. PnL diario ───────────────────────────────────────────────────────
    try:
        daily_pnl = metrics.get("daily_pnl", {})
        if daily_pnl:
            fig, ax = plt.subplots(figsize=(10, 4))
            dates = sorted(daily_pnl.keys())
            values = [daily_pnl[d] for d in dates]
            cumulative = []
            cum = 0
            for v in values:
                cum += v
                cumulative.append(cum)

            bar_colors = ["#00d4aa" if v >= 0 else "#e94560" for v in values]
            ax.bar(dates, values, color=bar_colors, alpha=0.7, label="PnL diario")
            ax.plot(dates, cumulative, color="#f7931a", marker="o", linewidth=2, markersize=5, label="PnL acumulado")
            ax.axhline(y=0, color="#666", linewidth=0.8)
            ax.set_title("PnL DIARIO + ACUMULADO (7 dias)", fontweight="bold", fontsize=14, color="#e94560")
            ax.set_ylabel("USD")
            ax.legend(loc="upper left")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            buf.seek(0)
            charts["pnl_chart"] = buf.read()
            plt.close(fig)
    except Exception as e:
        print(f"[charts] Error PnL: {e}")

    # ── 3. Trades por par (pie chart) ────────────────────────────────────────
    try:
        by_pair = metrics.get("by_pair", {})
        if by_pair:
            fig, ax = plt.subplots(figsize=(6, 6))
            labels = list(by_pair.keys())
            sizes = [by_pair[p]["count"] for p in labels]
            pair_colors = ["#f7931a", "#627eea", "#00d4aa", "#e94560", "#9945ff",
                           "#fff", "#f0b90b", "#2775ca"][:len(labels)]

            wedges, texts, autotexts = ax.pie(
                sizes, labels=labels, autopct="%1.0f%%",
                colors=pair_colors, startangle=90,
                textprops={"color": "#fff", "fontweight": "bold"},
            )
            ax.set_title("TRADES POR PAR", fontweight="bold", fontsize=14, color="#e94560")
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            buf.seek(0)
            charts["trades_chart"] = buf.read()
            plt.close(fig)
    except Exception as e:
        print(f"[charts] Error trades: {e}")

    # ── 4. PnL por estrategia ────────────────────────────────────────────────
    try:
        by_strat = metrics.get("by_strategy", {})
        if by_strat:
            fig, ax = plt.subplots(figsize=(8, 4))
            strats = list(by_strat.keys())
            pnls = [by_strat[s]["pnl"] for s in strats]
            counts = [by_strat[s]["count"] for s in strats]
            bar_colors = ["#00d4aa" if p >= 0 else "#e94560" for p in pnls]

            bars = ax.barh(strats, pnls, color=bar_colors, edgecolor="#fff", linewidth=0.5)
            for bar, pnl, count in zip(bars, pnls, counts):
                x = bar.get_width()
                ax.text(x + 0.001, bar.get_y() + bar.get_height()/2,
                        f"${pnl:+.4f} ({count}t)", va="center", fontsize=9, color="#fff")

            ax.axvline(x=0, color="#666", linewidth=0.8)
            ax.set_title("PnL POR ESTRATEGIA", fontweight="bold", fontsize=14, color="#e94560")
            ax.set_xlabel("USD")
            plt.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            buf.seek(0)
            charts["strategy_chart"] = buf.read()
            plt.close(fig)
    except Exception as e:
        print(f"[charts] Error strategy: {e}")

    print(f"[charts] {len(charts)} graficos generados")
    return charts


# =============================================================================
# 5. GENERAR HTML DEL REPORTE
# =============================================================================

def generate_html_report(
    tg_data: dict,
    exchanges: dict,
    metrics: dict,
    charts: dict[str, bytes],
) -> str:
    """Genera el HTML del reporte con graficos embebidos."""

    now = datetime.now(TZ_CST)
    weekdays = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    months = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
              "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
    date_str = f"{weekdays[now.weekday()]} {now.day} {months[now.month]} {now.year}"

    # Capital
    bybit_usd = exchanges.get("bybit", {}).get("total_usd", 0) if exchanges.get("bybit") else 0
    okx_usd = exchanges.get("okx", {}).get("total_usd", 0) if exchanges.get("okx") else 0
    total_usd = bybit_usd + okx_usd
    initial = 90.0
    drawdown = ((initial - total_usd) / initial * 100) if total_usd < initial else 0
    roi = ((total_usd - initial) / initial * 100) if total_usd > 0 else 0

    # Status color
    if drawdown > 10:
        status_color = "#e94560"
        status_text = f"DRAWDOWN CRITICO: -{drawdown:.1f}%"
        status_icon = "&#9888;"
    elif metrics["pnl_total"] < 0:
        status_color = "#f7931a"
        status_text = f"PnL negativo: ${metrics['pnl_total']:.4f}"
        status_icon = "&#9888;"
    else:
        status_color = "#00d4aa"
        status_text = "Operacion normal"
        status_icon = "&#10004;"

    # Alertas de Telegram
    alerts_html = ""
    if tg_data["alerts"]:
        for a in tg_data["alerts"][-5:]:
            alerts_html += f'<li style="color:#e94560">{a["date"].strftime("%H:%M")} — {a["text"][:100]}</li>\n'
    else:
        alerts_html = '<li style="color:#00d4aa">Sin alertas. Todo normal.</li>'

    # Trades recientes de Telegram
    tg_trades_html = ""
    if tg_data["trades"]:
        for t in tg_data["trades"][-10:]:
            pnl_str = f'${t["pnl"]:+.4f}' if t["pnl"] is not None else "—"
            color = "#00d4aa" if (t["pnl"] or 0) >= 0 else "#e94560"
            tg_trades_html += (
                f'<tr><td>{t["date"].strftime("%m/%d %H:%M")}</td>'
                f'<td>{t["pair"]}</td><td>{t["side"]}</td>'
                f'<td style="color:{color}">{pnl_str}</td></tr>\n'
            )

    # Senales
    signals_html = ""
    if tg_data["signals"]:
        for s in tg_data["signals"][-5:]:
            score_str = f'Score: {s["score"]}' if s["score"] else ""
            signals_html += f'<li>{s["date"].strftime("%H:%M")} — {score_str} {s["text"][:80]}</li>\n'

    # Estrategias
    strategy_rows = ""
    for s, data in metrics.get("by_strategy", {}).items():
        wr = (data["wins"] / data["count"] * 100) if data["count"] > 0 else 0
        color = "#00d4aa" if data["pnl"] >= 0 else "#e94560"
        strategy_rows += (
            f'<tr><td>{s}</td><td>{data["count"]}</td>'
            f'<td>{wr:.0f}%</td><td style="color:{color}">${data["pnl"]:+.4f}</td></tr>\n'
        )

    # Imagenes embebidas como CID
    img_tags = {}
    for name in ["balance_chart", "pnl_chart", "trades_chart", "strategy_chart"]:
        if name in charts:
            img_tags[name] = f'<img src="cid:{name}" style="width:100%;max-width:700px;border-radius:8px;margin:10px 0">'
        else:
            img_tags[name] = '<p style="color:#666;text-align:center">Grafico no disponible</p>'

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="background:#0f0f23;color:#eee;font-family:Consolas,'Courier New',monospace;padding:20px;max-width:800px;margin:0 auto">

<div style="text-align:center;padding:20px 0;border-bottom:2px solid #e94560">
  <h1 style="color:#e94560;margin:0">&#128051; WHALE FOLLOWER</h1>
  <h2 style="color:#f7931a;margin:5px 0">Reporte Diario — {date_str}</h2>
  <p style="color:#888">Generado: {now.strftime('%H:%M:%S')} CST</p>
</div>

<!-- STATUS -->
<div style="background:{status_color}22;border-left:4px solid {status_color};padding:15px;margin:20px 0;border-radius:4px">
  <h3 style="color:{status_color};margin:0">{status_icon} {status_text}</h3>
</div>

<!-- CAPITAL -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#128176; CAPITAL</h3>
  <table style="width:100%;color:#eee">
    <tr><td>Bybit:</td><td style="text-align:right;font-weight:bold">${bybit_usd:,.2f} USD</td></tr>
    <tr><td>OKX:</td><td style="text-align:right;font-weight:bold">${okx_usd:,.2f} USD</td></tr>
    <tr style="border-top:1px solid #333"><td><strong>TOTAL:</strong></td>
        <td style="text-align:right;font-weight:bold;font-size:1.2em;color:#f7931a">${total_usd:,.2f} USD</td></tr>
    <tr><td>Drawdown:</td><td style="text-align:right;color:{'#e94560' if drawdown > 5 else '#00d4aa'}">{drawdown:.1f}%</td></tr>
    <tr><td>ROI total:</td><td style="text-align:right;color:{'#00d4aa' if roi >= 0 else '#e94560'}">{roi:+.1f}%</td></tr>
  </table>
</div>

<!-- GRAFICO BALANCE -->
{img_tags["balance_chart"]}

<!-- METRICAS DE TRADING -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#128200; METRICAS (7 dias)</h3>
  <table style="width:100%;color:#eee">
    <tr><td>Trades cerrados:</td><td style="text-align:right">{metrics['total']}</td></tr>
    <tr><td>Ganados / Perdidos:</td><td style="text-align:right;color:#00d4aa">{metrics['wins']}W</td></tr>
    <tr><td></td><td style="text-align:right;color:#e94560">{metrics['losses']}L</td></tr>
    <tr><td>Win Rate:</td><td style="text-align:right;font-weight:bold">{metrics['win_rate']:.1f}%</td></tr>
    <tr><td>PnL total:</td><td style="text-align:right;font-weight:bold;color:{'#00d4aa' if metrics['pnl_total'] >= 0 else '#e94560'}">${metrics['pnl_total']:+.4f}</td></tr>
    <tr><td>Trades abiertos:</td><td style="text-align:right">{metrics['open']}</td></tr>
  </table>
</div>

<!-- GRAFICO PnL -->
{img_tags["pnl_chart"]}

<!-- PnL POR ESTRATEGIA -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#127919; PnL POR ESTRATEGIA</h3>
  <table style="width:100%;color:#eee;border-collapse:collapse">
    <tr style="border-bottom:1px solid #333"><th style="text-align:left">Estrategia</th><th>Trades</th><th>WR</th><th>PnL</th></tr>
    {strategy_rows if strategy_rows else '<tr><td colspan="4" style="text-align:center;color:#666">Sin datos</td></tr>'}
  </table>
</div>

<!-- GRAFICO ESTRATEGIAS -->
{img_tags["strategy_chart"]}

<!-- GRAFICO TRADES POR PAR -->
{img_tags["trades_chart"]}

<!-- TRADES RECIENTES (Telegram) -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#128202; TRADES RECIENTES (Telegram)</h3>
  <table style="width:100%;color:#eee;border-collapse:collapse">
    <tr style="border-bottom:1px solid #333"><th style="text-align:left">Fecha</th><th>Par</th><th>Side</th><th>PnL</th></tr>
    {tg_trades_html if tg_trades_html else '<tr><td colspan="4" style="text-align:center;color:#666">Sin trades detectados en mensajes</td></tr>'}
  </table>
</div>

<!-- SENALES -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#128225; SENALES RECIENTES</h3>
  <ul style="list-style:none;padding:0">{signals_html if signals_html else '<li style="color:#666">Sin senales detectadas</li>'}</ul>
</div>

<!-- ALERTAS -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#9888; ALERTAS</h3>
  <ul style="list-style:none;padding:0">{alerts_html}</ul>
</div>

<!-- TELEGRAM STATS -->
<div style="background:#16213e;padding:20px;border-radius:8px;margin:15px 0">
  <h3 style="color:#e94560;border-bottom:1px solid #333;padding-bottom:8px">&#128172; TELEGRAM</h3>
  <table style="width:100%;color:#eee">
    <tr><td>Mensajes leidos:</td><td style="text-align:right">{tg_data['total_messages']}</td></tr>
    <tr><td>Trades detectados:</td><td style="text-align:right">{len(tg_data['trades'])}</td></tr>
    <tr><td>Alertas:</td><td style="text-align:right">{len(tg_data['alerts'])}</td></tr>
    <tr><td>Senales:</td><td style="text-align:right">{len(tg_data['signals'])}</td></tr>
    <tr><td>Reportes previos:</td><td style="text-align:right">{len(tg_data['reports'])}</td></tr>
  </table>
</div>

<!-- FOOTER -->
<div style="text-align:center;padding:20px;margin-top:20px;border-top:1px solid #333;color:#666">
  <p>Whale Follower Bot — AFORIA</p>
  <p>Reporte generado automaticamente por Cowork</p>
  <p style="font-size:0.8em">Bybit + OKX + Supabase + Telegram API</p>
</div>

</body>
</html>"""

    return html


# =============================================================================
# 6. ENVIAR EMAIL
# =============================================================================

def send_email(html: str, charts: dict[str, bytes], subject: str | None = None) -> bool:
    """
    Envia reporte HTML con graficos via SMTP de Gmail.
    Requiere GMAIL_APP_PASSWORD en .env.
    """
    if not GMAIL_APP_PASSWORD:
        print("[email] ERROR: GMAIL_APP_PASSWORD no configurado en .env")
        print("[email] Para obtenerlo:")
        print("  1. Ve a https://myaccount.google.com/apppasswords")
        print("  2. Genera una 'Contrasena de aplicacion' para 'Correo'")
        print("  3. Agrega a .env: GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx")
        return False

    now = datetime.now(TZ_CST)
    if not subject:
        subject = f"Whale Follower — Reporte {now.strftime('%Y-%m-%d')}"

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = EMAIL_TO

    # Adjuntar HTML
    msg_alt = MIMEMultipart("alternative")
    msg.attach(msg_alt)
    msg_alt.attach(MIMEText(html, "html", "utf-8"))

    # Adjuntar graficos como CID
    for name, data in charts.items():
        img = MIMEImage(data, _subtype="png")
        img.add_header("Content-ID", f"<{name}>")
        img.add_header("Content-Disposition", "inline", filename=f"{name}.png")
        msg.attach(img)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        print(f"[email] Reporte enviado a {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"[email] Error: {e}")
        return False


# =============================================================================
# 7. GUARDAR REPORTE LOCAL
# =============================================================================

def save_local_report(html: str, charts: dict[str, bytes]) -> Path:
    """Guarda el reporte HTML y graficos en disco."""
    now = datetime.now(TZ_CST)
    date_str = now.strftime("%Y-%m-%d_%H%M")

    # HTML
    html_path = REPORTS_DIR / f"reporte_{date_str}.html"
    html_path.write_text(html, encoding="utf-8")

    # Graficos individuales
    for name, data in charts.items():
        img_path = REPORTS_DIR / f"{name}_{date_str}.png"
        img_path.write_bytes(data)

    print(f"[local] Reporte guardado en {REPORTS_DIR}")
    return html_path


# =============================================================================
# 8. MAIN — EJECUTAR REPORTE
# =============================================================================

def run_report() -> bool:
    """Ejecuta el flujo completo del reporte."""
    print("=" * 60)
    print("  WHALE FOLLOWER — Generando reporte diario")
    print("=" * 60)
    now = datetime.now(TZ_CST)
    print(f"  Fecha: {now.strftime('%Y-%m-%d %H:%M:%S')} CST\n")

    # Paso 1: Leer Telegram
    print("[1/6] Leyendo mensajes de Telegram...")
    messages = get_telegram_messages(limit=50)
    tg_data = parse_telegram_data(messages)

    # Paso 2: Consultar exchanges
    print("[2/6] Consultando balances de exchanges...")
    exchanges = get_exchange_balances()

    # Paso 3: Consultar Supabase
    print("[3/6] Consultando trades de Supabase (7 dias)...")
    sb_trades = get_supabase_trades(days=7)
    metrics = analyze_supabase_trades(sb_trades)

    # Paso 4: Generar graficos
    print("[4/6] Generando graficos...")
    charts = generate_charts(metrics, exchanges, tg_data)

    # Paso 5: Generar HTML
    print("[5/6] Generando reporte HTML...")
    html = generate_html_report(tg_data, exchanges, metrics, charts)

    # Guardar local
    local_path = save_local_report(html, charts)

    # Paso 6: Enviar email
    print("[6/6] Enviando email...")
    sent = send_email(html, charts)

    print("\n" + "=" * 60)
    if sent:
        print(f"  REPORTE ENVIADO a {EMAIL_TO}")
    else:
        print(f"  Reporte guardado localmente: {local_path}")
        print("  (Email no enviado — configura GMAIL_APP_PASSWORD)")
    print("=" * 60)

    return sent


# =============================================================================
# 9. SCHEDULER — Ejecucion diaria a las 8AM CST
# =============================================================================

def run_scheduler():
    """Loop que ejecuta el reporte cada dia a las 8:00 AM CST."""
    import time as _time

    print("=" * 60)
    print("  WHALE FOLLOWER — Scheduler activo")
    print("  Reporte programado: 8:00 AM CST (UTC-6)")
    print("  Ctrl+C para detener")
    print("=" * 60)

    last_date = ""
    while True:
        try:
            now = datetime.now(TZ_CST)
            today = now.strftime("%Y-%m-%d")

            if now.hour == 8 and now.minute == 0 and today != last_date:
                last_date = today
                print(f"\n[scheduler] {now.strftime('%Y-%m-%d %H:%M')} — Ejecutando reporte...")
                run_report()

            _time.sleep(30)  # verificar cada 30 segundos

        except KeyboardInterrupt:
            print("\n[scheduler] Detenido.")
            break
        except Exception as e:
            print(f"[scheduler] Error: {e}")
            _time.sleep(60)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    if "--schedule" in sys.argv:
        run_scheduler()
    else:
        run_report()
