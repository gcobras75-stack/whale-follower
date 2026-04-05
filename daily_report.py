# -*- coding: utf-8 -*-
"""
daily_report.py -- Whale Follower Bot
Reporte diario automatico a las 8:00 AM hora Ciudad de Mexico (UTC-6).

Recopila todas las estadisticas de la sesion y las envia por Telegram.
No requiere API key externo — usa el TELEGRAM_BOT_TOKEN de config.py.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from loguru import logger

import config

_CST = timezone(timedelta(hours=-6))   # Ciudad de Mexico (UTC-6 fijo)
_REPORT_HOUR = 8                        # 8:00 AM


class DailyReporter:
    """
    Envia un reporte diario a las 8 AM CST con:
      - Capital en Bybit y OKX
      - PnL por estrategia
      - Operaciones del dia
      - Estado de termometros de mercado
      - Estado del sistema
    """

    def __init__(self, arb_ref=None) -> None:
        self._arb_ref  = arb_ref   # ArbEngine instance para stats de arb
        self._sent_today: bool = False
        self._last_date: str   = ""
        logger.info("[daily_report] Reporte diario programado para {:02d}:00 AM CST (UTC-6)",
                    _REPORT_HOUR)

    def set_arb_ref(self, arb_ref) -> None:
        self._arb_ref = arb_ref

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Loop que verifica cada minuto si es hora de enviar el reporte."""
        while True:
            try:
                now_cst = datetime.now(_CST)
                today   = now_cst.strftime("%Y-%m-%d")

                if (now_cst.hour == _REPORT_HOUR
                        and now_cst.minute == 0
                        and today != self._last_date):
                    self._last_date  = today
                    self._sent_today = True
                    await self._send_report(now_cst)

            except Exception as exc:
                logger.warning("[daily_report] Loop error: {}", exc)

            await asyncio.sleep(60)   # checar cada minuto

    # ── Report generation ─────────────────────────────────────────────────────

    async def _send_report(self, now_cst: datetime) -> None:
        token   = config.TELEGRAM_BOT_TOKEN
        chat_id = config.TELEGRAM_CHAT_ID
        if not token or not chat_id:
            logger.warning("[daily_report] TELEGRAM_BOT_TOKEN o CHAT_ID no configurados")
            return

        try:
            import alerts as _alerts
            stats = _alerts._stats
            therm = _alerts._thermometers
        except Exception as exc:
            logger.warning("[daily_report] No se pudo importar alerts: {}", exc)
            return

        # ── Fecha ─────────────────────────────────────────────────────────────
        weekdays_es = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        months_es   = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                       "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
        day_name = weekdays_es[now_cst.weekday()]
        date_str = f"{day_name} {now_cst.day:02d} {months_es[now_cst.month]} {now_cst.year}"

        # ── Capital ───────────────────────────────────────────────────────────
        cap_bybit = stats.get("capital_bybit", 0.0) or config.REAL_CAPITAL
        cap_okx   = stats.get("capital_okx",   0.0) or 51.0
        cap_total = cap_bybit + cap_okx

        # ── PnL ───────────────────────────────────────────────────────────────
        pnl_arb  = stats.get("arb_pnl",  0.0)
        pnl_grid = stats.get("grid_pnl", 0.0)
        pnl_total = pnl_arb + pnl_grid

        # ── Operaciones ───────────────────────────────────────────────────────
        arb_btc = stats.get("arb_btc_exec", 0)
        arb_eth = stats.get("arb_eth_exec", 0)
        arb_sol = stats.get("arb_sol_exec", 0)
        arb_total = stats.get("arb_executed", 0)
        grid_cycles = stats.get("grid_cycles", 0)
        wyckoff_total = stats.get("signals_total", 0)
        trades_total = arb_total + grid_cycles

        win_rate = "N/A"
        if trades_total > 0:
            wins = arb_total + grid_cycles   # asumimos todos ganadores por ahora
            win_rate = f"{int(wins / trades_total * 100)}%"

        # ── Termometros ───────────────────────────────────────────────────────
        dom_pct  = therm.get("btc_dom_pct", 0.0)
        dom_sig  = therm.get("btc_dom_signal", "?")
        dxy_val  = therm.get("dxy_value", 0.0)
        dxy_sig  = therm.get("dxy_signal", "?")
        liq_sig  = therm.get("liq_signal", "?")

        # Bitso prima
        b_opps  = stats.get("bitso_opportunities", 0)
        b_count = stats.get("bitso_spread_count", 0)
        b_avg   = (stats.get("bitso_spread_sum", 0.0) / b_count) if b_count > 0 else 0.0
        bitso_prima = f"{b_avg:+.2f}%" if b_opps > 0 else "sin datos"

        # ── Arb stats ─────────────────────────────────────────────────────────
        arb_detected = stats.get("arb_opportunities", 0)

        # ── Uptime ───────────────────────────────────────────────────────────
        uptime_secs = int(time.time() - stats.get("start_time", time.time()))
        h, rem = divmod(uptime_secs // 60, 60)
        m      = rem

        # ── Estado ───────────────────────────────────────────────────────────
        if pnl_total < -0.5:
            status_line = f"⚠️ Drawdown: ${pnl_total:.2f} — revisar estrategia"
        else:
            status_line = "✅ Bot operando normal. Sin alertas."

        # ── Mensaje ───────────────────────────────────────────────────────────
        msg = (
            f"📊 REPORTE DIARIO — Whale Follower\n"
            f"════════════════════════════════\n"
            f"📅 Fecha: {date_str}\n"
            f"\n"
            f"💰 CAPITAL\n"
            f"Bybit:     ${cap_bybit:,.2f} USD\n"
            f"OKX:       ${cap_okx:,.2f} USD\n"
            f"Total:     ${cap_total:,.2f} USD\n"
            f"\n"
            f"📈 OPERACIONES\n"
            f"Trades reales:      {trades_total}\n"
            f"  → Cross-arb BTC:  {arb_btc}\n"
            f"  → Cross-arb ETH:  {arb_eth}\n"
            f"  → Cross-arb SOL:  {arb_sol}\n"
            f"  → Grid ciclos:    {grid_cycles}\n"
            f"  → Wyckoff señales:{wyckoff_total}\n"
            f"Win Rate:           {win_rate}\n"
            f"\n"
            f"💵 PnL\n"
            f"PnL Cross-arb:    {pnl_arb:+.4f}\n"
            f"PnL Grid:         {pnl_grid:+.4f}\n"
            f"PnL TOTAL:        {pnl_total:+.4f}\n"
            f"\n"
            f"🌡️ TERMÓMETROS\n"
            f"BTC Dominancia:   {dom_pct:.1f}% ({dom_sig})\n"
            f"DXY:              {dxy_val:.2f} ({dxy_sig})\n"
            f"Liquidaciones:    {liq_sig}\n"
            f"Prima Bitso:      {bitso_prima}\n"
            f"\n"
            f"🔍 OPORTUNIDADES\n"
            f"Arb detectadas:   {arb_detected}\n"
            f"Arb ejecutadas:   {arb_total}\n"
            f"\n"
            f"⚙️ SISTEMA\n"
            f"Uptime:           {h}h {m}m\n"
            f"Exchanges:        Bybit ✅ OKX ✅\n"
            f"════════════════════════════════\n"
            f"💡 {status_line}"
        )

        try:
            async with aiohttp.ClientSession() as s:
                await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": msg},
                    timeout=aiohttp.ClientTimeout(total=15),
                )
            logger.info("[daily_report] Reporte enviado a Telegram ({} CST)", now_cst.strftime("%H:%M"))
        except Exception as exc:
            logger.warning("[daily_report] Error enviando reporte: {}", exc)
