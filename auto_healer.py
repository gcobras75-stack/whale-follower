# -*- coding: utf-8 -*-
"""
auto_healer.py — Whale Follower Bot
Auto-monitoreo 24/7: detecta problemas en el bot y aplica fixes automáticos.

Corre cada 5 minutos. Detecta 8 patrones de error y se auto-corrige.
Reporte a Telegram cada 6 horas.

Límites: NUNCA toca credenciales, dinero, trades, ni stop losses.
Solo ajusta parámetros internos, reinicia módulos y manda alertas.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from loguru import logger


_CHECK_INTERVAL    = 300      # 5 minutos
_REPORT_INTERVAL   = 6 * 3600  # 6 horas
_THRESHOLD_FLOOR   = 45      # nunca bajar debajo de esto
_THRESHOLD_CEILING = 65      # máximo para LATERAL


@dataclass
class Fix:
    timestamp: float
    issue:     str
    action:    str


class AutoHealer:
    """Monitoreo continuo + auto-corrección del bot."""

    def __init__(self, bot_refs: Optional[Dict[str, Any]] = None) -> None:
        self._refs = bot_refs or {}
        self._checks_total   = 0
        self._issues_found   = 0
        self._fixes_applied: List[Fix] = []
        self._last_report    = time.time()

        # Contadores de errores por tipo
        self._bybit_403_streak   = 0
        self._grid_cancel_count  = 0
        self._ctx_zero_streak    = 0
        self._last_spring_log_ts = time.time()
        self._last_tg_send_ts    = time.time()

        logger.info("[healer] AutoHealer iniciado | check cada {}s", _CHECK_INTERVAL)

    # ── Background loop ──────────────────────────────────────────────────────

    async def run_loop(self) -> None:
        await asyncio.sleep(120)  # 2 min warmup
        while True:
            try:
                await self._check_and_heal()
            except Exception as exc:
                logger.warning("[healer] Error en ciclo: {}", exc)
            await asyncio.sleep(_CHECK_INTERVAL)

    # ── Ingesta de eventos (llamados desde otros módulos) ────────────────────

    def on_bybit_403(self) -> None:
        self._bybit_403_streak += 1

    def on_bybit_ok(self) -> None:
        self._bybit_403_streak = 0

    def on_grid_cancel(self) -> None:
        self._grid_cancel_count += 1

    def on_spring_eval(self) -> None:
        self._last_spring_log_ts = time.time()

    def on_telegram_sent(self) -> None:
        self._last_tg_send_ts = time.time()

    def on_ctx_score(self, ctx_pts: int) -> None:
        if ctx_pts == 0:
            self._ctx_zero_streak += 1
        else:
            self._ctx_zero_streak = 0

    # ── Check principal ──────────────────────────────────────────────────────

    async def _check_and_heal(self) -> None:
        self._checks_total += 1
        now = time.time()

        await self._check_threshold_too_high()
        await self._check_grid_paralyzed()
        await self._check_telegram_stale()
        await self._check_okx_balance_low()
        await self._check_bybit_403()
        await self._check_websocket_dead()
        await self._check_ctx_zero()

        # Reset contadores cíclicos
        self._grid_cancel_count = 0

        # Reporte periódico
        if now - self._last_report >= _REPORT_INTERVAL:
            await self._send_report()
            self._last_report = now

    # ── 1. Threshold demasiado alto ──────────────────────────────────────────

    async def _check_threshold_too_high(self) -> None:
        try:
            from threshold_optimizer import get_optimizer
            opt = get_optimizer()
            thr = opt.get_threshold("LATERAL")
            if thr > _THRESHOLD_CEILING:
                old = thr
                opt._thresholds["LATERAL"] = 55
                await opt._save_threshold("LATERAL", 55)
                self._record_fix(
                    f"threshold_alto ({old}>{_THRESHOLD_CEILING})",
                    f"reset LATERAL {old}->55",
                )
        except Exception:
            pass

    # ── 2. Grid paralizado por reserva ───────────────────────────────────────

    async def _check_grid_paralyzed(self) -> None:
        if self._grid_cancel_count < 10:
            return
        try:
            import okx_grid
            current = okx_grid._OKX_WYCKOFF_RESERVE
            if current > 30:
                okx_grid._OKX_WYCKOFF_RESERVE = 30.0
                self._record_fix(
                    f"grid_paralizado ({self._grid_cancel_count} cancelaciones)",
                    f"reserva {current}->{okx_grid._OKX_WYCKOFF_RESERVE}",
                )
        except Exception:
            pass

    # ── 3. Telegram stale ────────────────────────────────────────────────────

    async def _check_telegram_stale(self) -> None:
        elapsed = time.time() - self._last_tg_send_ts
        if elapsed < 7200:  # < 2 horas OK
            return
        try:
            import alerts
            # Verificar si hay rate limit activo
            if alerts._tg_retry_after > time.time():
                wait = alerts._tg_retry_after - time.time()
                logger.info("[healer] Telegram rate limit activo, {}s restantes", int(wait))
                return
            # Intentar enviar test
            await alerts._send_telegram(
                "Auto-healer: verificando conexion Telegram",
                priority="critical",
            )
            self._last_tg_send_ts = time.time()
            self._record_fix("telegram_stale", "test enviado OK")
        except Exception:
            pass

    # ── 4. OKX balance bajo ──────────────────────────────────────────────────

    async def _check_okx_balance_low(self) -> None:
        try:
            okx_exec = self._refs.get("okx_wyckoff")
            if not okx_exec or not okx_exec.enabled:
                return
            bal = await asyncio.wait_for(okx_exec.get_balance(), timeout=8)
            if bal < 20:
                # Pausar grid
                try:
                    import okx_grid
                    for pair, grid in getattr(okx_grid, '_instance', {}).items():
                        if hasattr(grid, '_grids'):
                            for g in grid._grids.values():
                                if not g.paused:
                                    g.paused = True
                                    g.pause_reason = f"USDT bajo ${bal:.0f}"
                except Exception:
                    pass
                self._record_fix(
                    f"okx_balance_bajo (${bal:.0f})",
                    "grid pausado",
                )
                import alerts
                await alerts._send_telegram(
                    f"OKX USDT bajo (${bal:.0f}) — grid pausado temporalmente",
                    priority="critical",
                )
        except Exception:
            pass

    # ── 5. Bybit 403 persistente ─────────────────────────────────────────────

    async def _check_bybit_403(self) -> None:
        if self._bybit_403_streak < 5:
            return
        try:
            import aiohttp
            ip = "desconocida"
            async with aiohttp.ClientSession() as s:
                async with s.get("https://api.ipify.org?format=json",
                                 timeout=aiohttp.ClientTimeout(total=5)) as r:
                    data = await r.json()
                    ip = data.get("ip", "desconocida")
            import alerts
            await alerts._send_telegram(
                f"Bybit 403 persistente ({self._bybit_403_streak} seguidos)\n"
                f"IP actual: {ip}\n"
                f"Ir a: bybit.com → API Management → Edit → No IP restriction",
                priority="critical",
            )
            self._record_fix(
                f"bybit_403 (x{self._bybit_403_streak})",
                f"alerta enviada, IP={ip}",
            )
            self._bybit_403_streak = 0  # reset para no spamear
        except Exception:
            pass

    # ── 6. WebSocket desconectado ────────────────────────────────────────────

    async def _check_websocket_dead(self) -> None:
        elapsed = time.time() - self._last_spring_log_ts
        if elapsed < 120:  # < 2 min OK
            return
        self._record_fix(
            f"websocket_dead ({int(elapsed)}s sin spring eval)",
            "alerta registrada — reconexion depende de aggregator",
        )

    # ── 7. Score ctx=0 persistente ───────────────────────────────────────────

    async def _check_ctx_zero(self) -> None:
        if self._ctx_zero_streak < 20:
            return
        self._record_fix(
            f"ctx_zero_persistente (x{self._ctx_zero_streak})",
            "alerta registrada — revisar context_engine y ExtendedContext",
        )
        self._ctx_zero_streak = 0

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _record_fix(self, issue: str, action: str) -> None:
        self._issues_found += 1
        self._fixes_applied.append(Fix(
            timestamp=time.time(),
            issue=issue,
            action=action,
        ))
        # Mantener solo últimos 50 fixes
        if len(self._fixes_applied) > 50:
            self._fixes_applied = self._fixes_applied[-50:]
        logger.info("[healer] Fix: {} → {}", issue, action)

    async def _send_report(self) -> None:
        recent = [f for f in self._fixes_applied
                  if time.time() - f.timestamp < _REPORT_INTERVAL]

        fix_lines = ""
        if recent:
            for f in recent[-5:]:  # últimos 5
                fix_lines += f"\n  {f.issue} → {f.action}"
        else:
            fix_lines = "\n  (ninguno)"

        # Estado actual
        bybit_ok = self._bybit_403_streak == 0
        ws_ok    = (time.time() - self._last_spring_log_ts) < 120

        msg = (
            f"Auto-Healer Report\n"
            f"Checks: {self._checks_total}\n"
            f"Issues: {self._issues_found}\n"
            f"Fixes: {len(self._fixes_applied)}\n"
            f"\nFixes recientes:{fix_lines}\n"
            f"\nEstado:\n"
            f"  Bybit: {'OK' if bybit_ok else '403'}\n"
            f"  WebSocket: {'OK' if ws_ok else 'revisar'}\n"
            f"  ctx_zero: {self._ctx_zero_streak}"
        )
        try:
            import alerts
            await alerts._send_telegram(msg, priority="normal")
        except Exception:
            pass


# ── Singleton ────────────────────────────────────────────────────────────────

_instance: Optional[AutoHealer] = None


def get_healer() -> AutoHealer:
    global _instance
    if _instance is None:
        _instance = AutoHealer()
    return _instance
