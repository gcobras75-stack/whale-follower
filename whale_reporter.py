#!/usr/bin/env python3
"""
whale_reporter.py — Whale Follower Bot — Sistema de reportes
Consulta exchanges (Bybit/OKX) y Supabase directamente,
genera reportes y los guarda localmente.
Diseñado para ser invocado por cron o manualmente.

Uso:
  python whale_reporter.py daily     # Reporte diario
  python whale_reporter.py weekly    # Reporte semanal (lunes)
  python whale_reporter.py check     # Solo consulta y muestra en consola
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import ccxt
from dotenv import load_dotenv

# Cargar .env del proyecto
_PROJECT = Path(__file__).resolve().parent
# load_dotenv puede fallar si .env tiene caracteres nulos — cargar manualmente
try:
    load_dotenv(_PROJECT / ".env")
except ValueError:
    # Parse manual: leer .env ignorando líneas rotas
    for line in (_PROJECT / ".env").read_text(errors="replace").splitlines():
        line = line.strip().replace("\x00", "")
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip()
        if k and not any(c in k for c in " \t"):
            os.environ.setdefault(k, v)

# ── Configuración ────────────────────────────────────────────────────────────
TZ_MAZATLAN = timezone(timedelta(hours=-7))
REPORTS_DIR = Path(r"C:\Users\ECApro\Whale Follower Reports")
REPORTS_DIR.mkdir(exist_ok=True)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Email config — set GMAIL_APP_PASSWORD in .env to enable SMTP sending
GMAIL_USER = os.environ.get("GMAIL_USER", "gcobras75@gmail.com")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "antonio@automatia.mx")


# ── Exchange helpers ─────────────────────────────────────────────────────────

def get_bybit_balance() -> dict:
    """Consulta balance de Bybit via ccxt."""
    try:
        ex = ccxt.bybit({
            "apiKey": os.environ.get("BYBIT_API_KEY", ""),
            "secret": os.environ.get("BYBIT_API_SECRET", ""),
            "timeout": 5000,
            "options": {"defaultType": "unified"},
        })
        bal = ex.fetch_balance()
        total_usd = float(bal.get("total", {}).get("USDT", 0))
        return {"exchange": "Bybit", "total_usd": total_usd, "detail": bal.get("total", {}), "error": None}
    except Exception as e:
        return {"exchange": "Bybit", "total_usd": 0, "detail": {}, "error": str(e)}


def get_okx_balance() -> dict:
    """Consulta balance de OKX via ccxt."""
    try:
        ex = ccxt.okx({
            "apiKey": os.environ.get("OKX_API_KEY", ""),
            "secret": os.environ.get("OKX_SECRET", ""),
            "password": os.environ.get("OKX_PASSPHRASE", ""),
            "timeout": 15000,
        })
        bal = ex.fetch_balance()
        total_usd = float(bal.get("total", {}).get("USDT", 0))
        # Incluir otras monedas con valor
        detail = {}
        for coin, amt in bal.get("total", {}).items():
            amt_f = float(amt) if amt else 0
            if amt_f > 0.01:
                detail[coin] = amt_f
        return {"exchange": "OKX", "total_usd": total_usd, "detail": detail, "error": None}
    except Exception as e:
        return {"exchange": "OKX", "total_usd": 0, "detail": {}, "error": str(e)}


# ── Supabase helpers ─────────────────────────────────────────────────────────

def get_supabase_trades(days: int = 1) -> list:
    """Consulta trades de Supabase de los últimos N días."""
    try:
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = sb.table("paper_trades") \
            .select("*") \
            .gte("created_at", cutoff) \
            .order("created_at", desc=True) \
            .limit(200) \
            .execute()
        return result.data or []
    except Exception as e:
        print(f"[supabase] Error: {e}")
        return []


def analyze_trades(trades: list) -> dict:
    """Analiza lista de trades y devuelve métricas."""
    if not trades:
        return {
            "total": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "pnl_total": 0, "best_trades": [], "worst_trades": [],
            "by_strategy": {}, "open_count": 0,
        }

    closed = [t for t in trades if t.get("status") == "closed"]
    wins = [t for t in closed if (t.get("pnl_usd") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl_usd") or 0) < 0]
    pnl_total = sum(t.get("pnl_usd", 0) or 0 for t in closed)
    win_rate = (len(wins) / len(closed) * 100) if closed else 0

    # Top trades
    sorted_by_pnl = sorted(closed, key=lambda t: t.get("pnl_usd", 0) or 0, reverse=True)
    best = sorted_by_pnl[:5]
    worst = sorted_by_pnl[-5:] if len(sorted_by_pnl) > 5 else []

    # Por estrategia
    by_strat = {}
    for t in closed:
        s = t.get("strategy", "unknown")
        if s not in by_strat:
            by_strat[s] = {"total": 0, "wins": 0, "pnl": 0}
        by_strat[s]["total"] += 1
        if (t.get("pnl_usd") or 0) > 0:
            by_strat[s]["wins"] += 1
        by_strat[s]["pnl"] += t.get("pnl_usd", 0) or 0

    return {
        "total": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "pnl_total": pnl_total,
        "best_trades": best,
        "worst_trades": worst,
        "by_strategy": by_strat,
        "open_count": len([t for t in trades if t.get("status") == "open"]),
    }


# ── Telegram: enviar comandos al bot y leer respuesta ────────────────────────

def send_telegram_command(command: str) -> str | None:
    """Envía un comando al chat de Telegram via Bot API y espera respuesta."""
    import requests

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return None

    # Enviar comando
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": command},
            timeout=10,
        )
    except Exception as e:
        print(f"[telegram] Error enviando {command}: {e}")
        return None

    # Esperar respuesta (el bot responde al polling)
    time.sleep(5)

    # Leer últimos mensajes
    try:
        resp = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"offset": -5, "limit": 5},
            timeout=10,
        )
        updates = resp.json().get("result", [])
        # Buscar la respuesta del bot (from.is_bot == true)
        for u in reversed(updates):
            msg = u.get("message", {})
            if msg.get("from", {}).get("is_bot"):
                return msg.get("text", "")
    except Exception as e:
        print(f"[telegram] Error leyendo respuesta: {e}")

    return None


# ── Generación de reportes ───────────────────────────────────────────────────

def generate_daily_report() -> str:
    """Genera el reporte diario completo."""
    now = datetime.now(TZ_MAZATLAN)
    date_str = now.strftime("%Y-%m-%d")

    print(f"[reporter] Generando reporte diario {date_str}...")

    # 1. Consultar exchanges
    bybit = get_bybit_balance()
    okx = get_okx_balance()
    total_capital = bybit["total_usd"] + okx["total_usd"]

    # 2. Consultar trades del día
    trades = get_supabase_trades(days=1)
    metrics = analyze_trades(trades)

    # 3. Calcular drawdown (vs capital inicial de $90)
    initial_capital = 90.0
    drawdown = ((initial_capital - total_capital) / initial_capital * 100) if total_capital < initial_capital else 0

    # 4. Detectar alertas
    alerts = []
    if bybit["error"]:
        alerts.append(f"Error conectando Bybit: {bybit['error']}")
    if okx["error"]:
        alerts.append(f"Error conectando OKX: {okx['error']}")
    if drawdown > 15:
        alerts.append(f"DRAWDOWN CRITICO: -{drawdown:.1f}%")
    if drawdown > 5:
        alerts.append(f"Drawdown elevado: -{drawdown:.1f}%")
    if metrics["total"] == 0:
        alerts.append("Sin trades cerrados en las ultimas 24h")
    if total_capital == 0:
        alerts.append("No se pudo obtener balance de ningun exchange")

    # 5. Mejores trades
    best_lines = []
    for t in metrics["best_trades"][:5]:
        pnl = t.get("pnl_usd", 0) or 0
        pair = t.get("pair", "?")
        strat = t.get("strategy", "?")
        best_lines.append(f"  {pair} ({strat}): +${pnl:.4f}")

    # 6. Recomendación
    if metrics["win_rate"] > 60 and metrics["pnl_total"] > 0:
        recommendation = "Rendimiento positivo. Bot operando bien. Mantener estrategia actual."
    elif drawdown > 10:
        recommendation = "Drawdown elevado. Considerar reducir tamano de posiciones o pausar bot."
    elif metrics["total"] == 0:
        recommendation = "Sin actividad de trades. Verificar que el bot esta corriendo en Railway y que el mercado tiene volatilidad suficiente."
    elif metrics["pnl_total"] < 0:
        recommendation = f"PnL negativo (${metrics['pnl_total']:.4f}). Revisar logs del bot y parametros de entrada."
    else:
        recommendation = "Operacion normal. Mercado lateral, bot escaneando oportunidades."

    # 7. P&L por estrategia
    strat_lines = []
    for s, data in metrics["by_strategy"].items():
        wr = (data["wins"] / data["total"] * 100) if data["total"] > 0 else 0
        strat_lines.append(f"  {s}: {data['total']} trades, WR {wr:.0f}%, PnL ${data['pnl']:.4f}")

    # OKX detail
    okx_detail_lines = []
    for coin, amt in okx.get("detail", {}).items():
        if coin != "USDT" and amt > 0.01:
            okx_detail_lines.append(f"  OKX {coin}: {amt:.4f}")

    report = f"""WHALE FOLLOWER — Reporte {date_str}
{'='*50}

RESUMEN EJECUTIVO
-----------------
Capital total: ${total_capital:.2f} USD en {1 + (1 if okx['total_usd'] > 0 else 0)} exchanges
Trades cerrados hoy: {metrics['total']} ({metrics['wins']}W / {metrics['losses']}L)
PnL del dia: ${metrics['pnl_total']:+.4f} USD

GANANCIAS DEL DIA
-----------------
PnL total: ${metrics['pnl_total']:+.4f} USD
Trades ganados: {metrics['wins']}
Trades perdidos: {metrics['losses']}
Win rate: {metrics['win_rate']:.1f}%
Trades abiertos: {metrics['open_count']}

P&L POR ESTRATEGIA
-------------------
{chr(10).join(strat_lines) if strat_lines else '  (sin trades cerrados)'}

ESTADO DEL CAPITAL
------------------
Bybit: ${bybit['total_usd']:.2f} USD{' (ERROR: ' + bybit['error'] + ')' if bybit['error'] else ''}
OKX:   ${okx['total_usd']:.2f} USD{' (ERROR: ' + okx['error'] + ')' if okx['error'] else ''}
{chr(10).join(okx_detail_lines)}
Total: ${total_capital:.2f} USD
Drawdown: {drawdown:.1f}%

MEJORES TRADES
--------------
{chr(10).join(best_lines) if best_lines else '  (sin trades)'}

ALERTAS IMPORTANTES
-------------------
{chr(10).join('  ' + a for a in alerts) if alerts else '  Sin alertas. Todo normal.'}

RECOMENDACION DEL DIA
---------------------
{recommendation}

{'='*50}
Generado: {now.strftime('%Y-%m-%d %H:%M:%S')} MST (America/Mazatlan)
Bot corriendo en Railway — datos de Bybit API + OKX API + Supabase
"""
    return report


def generate_weekly_report() -> str:
    """Genera reporte semanal (7 días)."""
    now = datetime.now(TZ_MAZATLAN)
    week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    date_str = now.strftime("%Y-%m-%d")

    print(f"[reporter] Generando reporte semanal {week_start} a {date_str}...")

    bybit = get_bybit_balance()
    okx = get_okx_balance()
    total_capital = bybit["total_usd"] + okx["total_usd"]

    trades = get_supabase_trades(days=7)
    metrics = analyze_trades(trades)

    # Agrupar por día
    daily_pnl = {}
    for t in trades:
        if t.get("status") != "closed":
            continue
        day = t.get("created_at", "")[:10]
        if day not in daily_pnl:
            daily_pnl[day] = {"pnl": 0, "count": 0}
        daily_pnl[day]["pnl"] += t.get("pnl_usd", 0) or 0
        daily_pnl[day]["count"] += 1

    best_day = max(daily_pnl.items(), key=lambda x: x[1]["pnl"]) if daily_pnl else ("N/A", {"pnl": 0, "count": 0})
    worst_day = min(daily_pnl.items(), key=lambda x: x[1]["pnl"]) if daily_pnl else ("N/A", {"pnl": 0, "count": 0})

    # Proyección mensual
    daily_avg = metrics["pnl_total"] / 7 if metrics["total"] > 0 else 0
    monthly_projection = daily_avg * 30

    daily_lines = []
    for day in sorted(daily_pnl.keys()):
        d = daily_pnl[day]
        daily_lines.append(f"  {day}: {d['count']} trades, PnL ${d['pnl']:+.4f}")

    report = f"""WHALE FOLLOWER — Reporte Semanal
{'='*50}
Periodo: {week_start} a {date_str}

RESUMEN SEMANAL
---------------
Ganancia total semana: ${metrics['pnl_total']:+.4f} USD
Total trades: {metrics['total']} ({metrics['wins']}W / {metrics['losses']}L)
Win rate semanal: {metrics['win_rate']:.1f}%

DESGLOSE DIARIO
----------------
{chr(10).join(daily_lines) if daily_lines else '  (sin datos)'}

Mejor dia:  {best_day[0]} (${best_day[1]['pnl']:+.4f}, {best_day[1]['count']} trades)
Peor dia:   {worst_day[0]} (${worst_day[1]['pnl']:+.4f}, {worst_day[1]['count']} trades)

PROYECCION MENSUAL
-------------------
Promedio diario: ${daily_avg:+.4f}
Proyeccion 30 dias: ${monthly_projection:+.4f} USD
ROI proyectado: {(monthly_projection / total_capital * 100) if total_capital > 0 else 0:.2f}%

CAPITAL ACTUAL
--------------
Bybit: ${bybit['total_usd']:.2f}
OKX:   ${okx['total_usd']:.2f}
Total: ${total_capital:.2f}

{'='*50}
Generado: {now.strftime('%Y-%m-%d %H:%M:%S')} MST
"""
    return report


# ── Email SMTP ───────────────────────────────────────────────────────────────

def send_email(subject: str, body: str, to: str = EMAIL_TO) -> bool:
    """Envía email via Gmail SMTP. Requiere GMAIL_APP_PASSWORD en .env."""
    import smtplib
    from email.mime.text import MIMEText

    if not GMAIL_APP_PASSWORD:
        print("[email] GMAIL_APP_PASSWORD no configurado — saltando envio SMTP")
        return False

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = to

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as s:
            s.starttls()
            s.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            s.sendmail(GMAIL_USER, [to], msg.as_string())
        print(f"[email] Enviado a {to}: {subject}")
        return True
    except Exception as e:
        print(f"[email] Error enviando: {e}")
        return False


def send_telegram_report(text: str) -> bool:
    """Envía reporte completo al chat de Telegram via Bot API."""
    import requests

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    # Telegram tiene limite de 4096 chars por mensaje — dividir si necesario
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    ok = True
    for chunk in chunks:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk},
                timeout=15,
            )
            if r.status_code != 200:
                print(f"[telegram] Error {r.status_code}: {r.text[:200]}")
                ok = False
        except Exception as e:
            print(f"[telegram] Error: {e}")
            ok = False
    if ok:
        print("[telegram] Reporte enviado a Telegram")
    return ok


# ── Guardar reporte local ────────────────────────────────────────────────────

def save_report(report: str, prefix: str = "reporte") -> Path:
    """Guarda reporte en la carpeta local."""
    now = datetime.now(TZ_MAZATLAN)
    filename = f"{prefix}_{now.strftime('%Y-%m-%d')}.txt"
    filepath = REPORTS_DIR / filename
    filepath.write_text(report, encoding="utf-8")
    print(f"[reporter] Guardado: {filepath}")
    return filepath


# ── Output para Claude Code (JSON para que el cron job lo use) ───────────────

def run_daily():
    """Ejecuta reporte diario: genera, guarda, envía email + Telegram."""
    report = generate_daily_report()
    filepath = save_report(report, "reporte")
    now = datetime.now(TZ_MAZATLAN)
    subject = f"Whale Follower — Reporte {now.strftime('%Y-%m-%d')}"

    # Enviar email
    email_ok = send_email(subject, report)

    # Enviar a Telegram como backup
    send_telegram_report(report)

    # Output JSON para integración con cron
    output = {
        "type": "daily",
        "date": now.strftime("%Y-%m-%d"),
        "subject": subject,
        "body": report,
        "file": str(filepath),
        "email_sent": email_ok,
    }
    print("---REPORT_JSON_START---")
    print(json.dumps(output, ensure_ascii=False))
    print("---REPORT_JSON_END---")
    return output


def run_weekly():
    """Ejecuta reporte semanal: genera, guarda, envía email + Telegram."""
    report = generate_weekly_report()
    filepath = save_report(report, "semanal")
    now = datetime.now(TZ_MAZATLAN)
    subject = f"Whale Follower — Reporte Semanal {now.strftime('%Y-%m-%d')}"

    email_ok = send_email(subject, report)
    send_telegram_report(report)

    output = {
        "type": "weekly",
        "date": now.strftime("%Y-%m-%d"),
        "subject": subject,
        "body": report,
        "file": str(filepath),
        "email_sent": email_ok,
    }
    print("---REPORT_JSON_START---")
    print(json.dumps(output, ensure_ascii=False))
    print("---REPORT_JSON_END---")
    return output


def run_check():
    """Solo consulta y muestra en consola (sin guardar ni enviar)."""
    bybit = get_bybit_balance()
    okx = get_okx_balance()
    print(f"\nBybit: ${bybit['total_usd']:.2f} {'ERROR: '+bybit['error'] if bybit['error'] else 'OK'}")
    print(f"OKX:   ${okx['total_usd']:.2f} {'ERROR: '+okx['error'] if okx['error'] else 'OK'}")
    print(f"Total: ${bybit['total_usd'] + okx['total_usd']:.2f}\n")

    trades = get_supabase_trades(days=1)
    m = analyze_trades(trades)
    print(f"Trades hoy: {m['total']} | PnL: ${m['pnl_total']:+.4f} | WR: {m['win_rate']:.1f}%")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    if cmd == "daily":
        run_daily()
    elif cmd == "weekly":
        run_weekly()
    elif cmd == "check":
        run_check()
    else:
        print(f"Uso: python {sys.argv[0]} [daily|weekly|check]")
        sys.exit(1)
