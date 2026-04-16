# -*- coding: utf-8 -*-
"""
security_monitor.py — Whale Follower Bot
Monitors and blocks unauthorized Telegram access attempts.
Logs incidents to Supabase, alerts owner, generates reports.
"""
from __future__ import annotations

import time
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
from loguru import logger

import config

AUTHORIZED_CHAT_IDS = [5985169283]
MAX_ATTEMPTS_PER_MINUTE = 5
ALERT_EMAIL = "antonio@automatia.mx"

# In-memory rate limiter for attackers
_attempt_log: Dict[int, List[float]] = defaultdict(list)
_incidents: List[dict] = []


def is_authorized(chat_id: int) -> bool:
    """Check if chat_id is in the whitelist."""
    return chat_id in AUTHORIZED_CHAT_IDS


async def handle_unauthorized(chat_id: int, username: Optional[str], command: str) -> None:
    """Full incident response for unauthorized access attempt."""
    now = time.time()
    ts = datetime.now(timezone.utc)

    # Rate limit check (don't spam ourselves with alerts)
    _attempt_log[chat_id] = [t for t in _attempt_log[chat_id] if now - t < 60]
    _attempt_log[chat_id].append(now)
    attempt_count = len(_attempt_log[chat_id])

    incident = {
        "timestamp": ts.isoformat(),
        "attacker_chat_id": str(chat_id),
        "attacker_username": username or "desconocido",
        "command_attempted": command,
        "attempt_number": attempt_count,
    }
    _incidents.append(incident)

    logger.warning(
        "[security] Acceso no autorizado: chat_id={} username={} cmd={} attempt=#{}",
        chat_id, username, command, attempt_count,
    )

    # Only alert on first attempt or every 5th attempt (avoid spam)
    if attempt_count == 1 or attempt_count % 5 == 0:
        await _alert_owner(incident)

    # Always save to Supabase
    await _save_to_supabase(incident)

    # Generate report on first attempt
    if attempt_count == 1:
        _generate_report(incident)


async def _save_to_supabase(incident: dict) -> None:
    """Save incident to security_incidents table."""
    try:
        client = _get_supabase()
        if not client:
            return
        client.table("security_incidents").insert({
            "attacker_chat_id": incident["attacker_chat_id"],
            "attacker_username": incident["attacker_username"],
            "command_attempted": incident["command_attempted"],
        }).execute()
        logger.info("[security] Incidente guardado en Supabase")
    except Exception as exc:
        logger.warning("[security] Error guardando en Supabase: {}", exc)


async def _alert_owner(incident: dict) -> None:
    """Send alert to bot owner via Telegram."""
    try:
        import tg_sender
        msg = (
            f"INTENTO DE ACCESO NO AUTORIZADO\n"
            f"{'=' * 35}\n"
            f"Chat ID atacante: {incident['attacker_chat_id']}\n"
            f"Username: @{incident['attacker_username']}\n"
            f"Comando: {incident['command_attempted']}\n"
            f"Hora: {incident['timestamp']}\n"
            f"Intento #{incident['attempt_number']}\n\n"
            f"Evidencia guardada en Supabase.\n"
            f"Total incidentes sesion: {len(_incidents)}"
        )
        await tg_sender.send(msg, priority="critical")
    except Exception as exc:
        logger.error("[security] Error enviando alerta: {}", exc)


def _generate_report(incident: dict) -> str:
    """Generate incident report text for potential legal action."""
    report = f"""
REPORTE DE INCIDENTE DE SEGURIDAD
Automatia Negocios Inteligentes
{'=' * 40}

Fecha: {incident['timestamp']}
Sistema afectado: Whale Follower Bot
Tipo: Intento de acceso no autorizado

DATOS DEL INCIDENTE:
- Chat ID: {incident['attacker_chat_id']}
- Username Telegram: @{incident['attacker_username']}
- Comando intentado: {incident['command_attempted']}

CONTACTOS PARA DENUNCIA:
- Policia Cibernetica CDMX:
  buzon.cibernetica@ssp.cdmx.gob.mx
  Tel: 55 5242-5100 ext. 5086
- CERT-MX: cert-mx@gsic.gob.mx
- Telegram Abuse: abuse@telegram.org

DECLARACION:
Yo, Antonio Gutierrez, titular de Automatia
Negocios Inteligentes, denuncio el intento
de acceso no autorizado a mis sistemas
automatizados de trading.
{'=' * 40}
"""
    logger.info("[security] Reporte de incidente generado para chat_id={}", incident["attacker_chat_id"])
    return report


def get_incidents_summary() -> str:
    """Get summary of all incidents this session."""
    if not _incidents:
        return "Sin incidentes de seguridad."
    unique_attackers = len(set(i["attacker_chat_id"] for i in _incidents))
    return (
        f"Incidentes: {len(_incidents)} | "
        f"Atacantes unicos: {unique_attackers} | "
        f"Ultimo: {_incidents[-1]['timestamp']}"
    )


def _get_supabase():
    """Get Supabase client."""
    try:
        from supabase import create_client
        return create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    except Exception:
        return None
