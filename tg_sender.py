# -*- coding: utf-8 -*-
"""
tg_sender.py — Centralised Telegram sender with global rate limiting.

ALL modules must use send() instead of direct aiohttp calls.
Handles 429 retry_after globally — one 429 blocks ALL sends.
"""
from __future__ import annotations

import os
import time
from typing import List

import aiohttp
from loguru import logger

_MAX_PER_HOUR = 15       # conservador para evitar ban (Telegram limit ~30/min)
_rate_window: List[float] = []
_retry_after: float = 0.0  # unix timestamp hasta cuando estamos bloqueados


async def send(text: str, priority: str = "normal") -> bool:
    """Envía mensaje a Telegram. Retorna True si se envió.

    priority:
      "critical" — respeta 429 pero no rate limit propio
      "normal"   — sujeto a ambos
    """
    global _retry_after

    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return False

    now = time.time()

    # SIEMPRE respetar 429 — incluso critical
    if now < _retry_after:
        remaining = int(_retry_after - now)
        if remaining % 300 == 0 or remaining < 60:  # log cada 5 min o último minuto
            logger.debug("[tg] 429 activo, {}s restantes", remaining)
        return False

    # Rate limit propio (excepto critical)
    if priority != "critical":
        _rate_window[:] = [t for t in _rate_window if now - t < 3600]
        if len(_rate_window) >= _MAX_PER_HOUR:
            logger.debug("[tg] Rate limit propio ({}/h) — omitido", _MAX_PER_HOUR)
            return False

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 429:
                    body = await resp.json()
                    retry_secs = body.get("parameters", {}).get("retry_after", 3600)
                    _retry_after = now + retry_secs
                    logger.warning("[tg] 429 — bloqueado {}s ({:.1f}h)",
                                   retry_secs, retry_secs / 3600)
                    return False
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("[tg] HTTP {}: {}", resp.status, body[:200])
                    return False
                _rate_window.append(now)
                return True
    except Exception as exc:
        logger.error("[tg] Error: {}", exc)
        return False


def is_blocked() -> bool:
    """True si hay un 429 activo."""
    return time.time() < _retry_after


def block_remaining_secs() -> int:
    """Segundos restantes del bloqueo 429."""
    return max(0, int(_retry_after - time.time()))
