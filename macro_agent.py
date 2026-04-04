# -*- coding: utf-8 -*-
"""
macro_agent.py — Whale Follower Bot — Sprint 6
Agente de macrofundamentales en tiempo real (costo $0).

Fuentes:
  1. Forex Factory JSON  — calendario económico USD de alto impacto
  2. CryptoPanic RSS     — noticias crypto importantes
  3. CoinGecko Fear&Greed — ya existe en fear_greed.py (usado como hook)

Expone:
  is_paused()           -> (bool, str)   # bloquea la señal si hay evento macro
  sentiment_adjustment() -> int          # ajuste de score (-15 a +10)
  run()                 -> coro          # tarea de fondo asyncio
"""
from __future__ import annotations

import asyncio
import datetime
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from loguru import logger

# ── Configuración ──────────────────────────────────────────────────────────────
_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_CRYPTOPANIC_RSS = "https://cryptopanic.com/news/rss/"
_REFRESH_CALENDAR = 3600      # cada hora
_REFRESH_NEWS     = 300       # cada 5 minutos
_PRE_EVENT_SECS   = 1800      # pausar 30 min ANTES del evento
_POST_EVENT_SECS  = 900       # pausar 15 min DESPUÉS del evento

# Eventos USD de alto impacto que mueven cripto significativamente
_HIGH_IMPACT_KEYWORDS = [
    "CPI", "NFP", "Non-Farm", "FOMC", "Federal Funds",
    "PCE", "GDP", "PPI", "Unemployment", "Retail Sales",
    "Interest Rate", "Powell", "Fed Chair", "Treasury",
    "Inflation", "Consumer Price",
]

# Keywords de noticias que indican riesgo para longs en cripto
_BEARISH_KEYWORDS = [
    "ban", "banned", "hack", "hacked", "exploit", "stolen",
    "SEC", "lawsuit", "arrest", "seized", "shutdown",
    "crash", "collapse", "bankrupt", "fraud", "scam",
    "regulation", "restricted", "blocked", "sanction",
    "tariff", "recession", "panic",
]

# Keywords que refuerzan sentimiento alcista
_BULLISH_KEYWORDS = [
    "ETF", "approved", "institutional", "adoption", "partnership",
    "reserve", "treasury", "accumulate", "strategic",
    "breakthrough", "upgrade", "milestone",
]


# ── Dataclass de estado ────────────────────────────────────────────────────────
@dataclass
class MacroSnapshot:
    # Calendario económico
    paused_for_event: bool = False
    event_name: str = ""
    event_time_utc: Optional[datetime.datetime] = None
    mins_to_event: Optional[int] = None

    # Noticias CryptoPanic
    bearish_headline: str = ""
    bullish_headline: str = ""
    news_sentiment_adj: int = 0    # -15 a +10

    # Meta
    last_calendar_update: float = 0.0
    last_news_update: float = 0.0
    calendar_stale: bool = True
    news_stale: bool = True


# ── Motor principal ────────────────────────────────────────────────────────────
class MacroAgent:
    """
    Corre como tarea asyncio de fondo.
    Los métodos públicos (is_paused, sentiment_adjustment) son seguros
    para llamar desde el loop principal sin bloquear.
    """

    def __init__(self) -> None:
        self._snap = MacroSnapshot()
        self._upcoming_events: list[dict] = []   # cache de eventos esta semana
        self._recent_headlines: list[dict] = []  # cache de noticias recientes

    # ── API pública ────────────────────────────────────────────────────────────

    def is_paused(self) -> tuple[bool, str]:
        """
        Retorna (True, reason) si hay un evento macro de alto impacto
        en ventana de ±30/15 min.  Nunca bloquea en caso de fallo de API.
        """
        if self._snap.paused_for_event:
            return True, f"macro_pause: {self._snap.event_name}"
        return False, ""

    def sentiment_adjustment(self) -> int:
        """
        Retorna delta de score:
          -15  si hay noticia bearish grave (hack, ban, SEC)
          -8   si hay noticia bearish moderada
          +5   si hay noticia bullish fuerte (ETF, reserve)
           0   en ausencia de señal
        """
        return self._snap.news_sentiment_adj

    def snapshot(self) -> MacroSnapshot:
        return self._snap

    # ── Loop de fondo ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Tarea de fondo: corre calendar y news en paralelo."""
        logger.info("[macro_agent] iniciando — calendario económico + CryptoPanic RSS")
        await asyncio.gather(
            self._calendar_loop(),
            self._news_loop(),
        )

    # ── Calendario económico ───────────────────────────────────────────────────

    async def _calendar_loop(self) -> None:
        while True:
            await self._fetch_calendar()
            self._evaluate_calendar()
            await asyncio.sleep(_REFRESH_CALENDAR)

    async def _fetch_calendar(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_CALENDAR_URL) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            # Filtrar: solo USD + High impact + keywords relevantes
            filtered = []
            for event in data:
                if event.get("country", "").upper() != "USD":
                    continue
                if event.get("impact", "").lower() != "high":
                    continue
                title = event.get("title", "")
                if not any(kw.lower() in title.lower() for kw in _HIGH_IMPACT_KEYWORDS):
                    continue
                filtered.append(event)

            self._upcoming_events = filtered
            self._snap.last_calendar_update = time.time()
            self._snap.calendar_stale = False
            logger.info(
                "[macro_agent] calendario: {} eventos USD High-Impact esta semana",
                len(filtered),
            )
        except Exception as exc:
            logger.warning("[macro_agent] calendario fallo (no bloquea): {}", exc)
            self._snap.calendar_stale = True

    def _evaluate_calendar(self) -> None:
        """Compara hora actual con eventos próximos. Activa pausa si corresponde."""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        nearest_name: str = ""
        nearest_dt: Optional[datetime.datetime] = None
        nearest_delta: Optional[float] = None

        for event in self._upcoming_events:
            dt = _parse_ff_datetime(event.get("date", ""), event.get("time", ""))
            if dt is None:
                continue
            delta_secs = (dt - now_utc).total_seconds()

            # Ventana: -PRE_EVENT_SECS antes hasta +POST_EVENT_SECS después
            if -_POST_EVENT_SECS <= delta_secs <= _PRE_EVENT_SECS:
                if nearest_delta is None or abs(delta_secs) < abs(nearest_delta):
                    nearest_delta = delta_secs
                    nearest_name = event.get("title", "evento")
                    nearest_dt = dt

        if nearest_name:
            self._snap.paused_for_event = True
            self._snap.event_name = nearest_name
            self._snap.event_time_utc = nearest_dt
            self._snap.mins_to_event = int((nearest_dt - now_utc).total_seconds() / 60) if nearest_dt else None
            logger.warning(
                "[macro_agent] PAUSA ACTIVA — {} en ~{} min",
                nearest_name,
                self._snap.mins_to_event,
            )
        else:
            if self._snap.paused_for_event:
                logger.info("[macro_agent] pausa liberada — sin eventos inmediatos")
            self._snap.paused_for_event = False
            self._snap.event_name = ""
            self._snap.event_time_utc = None
            self._snap.mins_to_event = None

    # ── Noticias CryptoPanic RSS ───────────────────────────────────────────────

    async def _news_loop(self) -> None:
        while True:
            await self._fetch_news()
            self._evaluate_news()
            await asyncio.sleep(_REFRESH_NEWS)

    async def _fetch_news(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_CRYPTOPANIC_RSS) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    text = await resp.text()

            items = _parse_rss(text)
            # Solo los últimos 30 min
            cutoff = time.time() - 1800
            recent = [i for i in items if i.get("pub_ts", 0) >= cutoff]
            self._recent_headlines = recent
            self._snap.last_news_update = time.time()
            self._snap.news_stale = False
            logger.debug("[macro_agent] noticias: {} en últimos 30 min", len(recent))
        except Exception as exc:
            logger.warning("[macro_agent] noticias fallo (no bloquea): {}", exc)
            self._snap.news_stale = True

    def _evaluate_news(self) -> None:
        """Analiza headlines y calcula sentiment_adjustment."""
        bearish_hit: Optional[str] = None
        bullish_hit: Optional[str] = None
        bearish_weight = 0
        bullish_weight = 0

        for item in self._recent_headlines:
            title = (item.get("title") or "").lower()

            for kw in _BEARISH_KEYWORDS:
                if kw.lower() in title:
                    # Peso mayor si es hack/ban/SEC
                    w = 2 if kw in ("hack", "hacked", "ban", "banned", "SEC", "exploit") else 1
                    bearish_weight += w
                    if bearish_hit is None:
                        bearish_hit = item.get("title", "")
                    break

            for kw in _BULLISH_KEYWORDS:
                if kw.lower() in title:
                    bullish_weight += 1
                    if bullish_hit is None:
                        bullish_hit = item.get("title", "")
                    break

        # Calcular ajuste neto
        if bearish_weight >= 4:
            adj = -15
        elif bearish_weight >= 2:
            adj = -8
        elif bearish_weight == 1:
            adj = -5
        elif bullish_weight >= 2:
            adj = 10
        elif bullish_weight == 1:
            adj = 5
        else:
            adj = 0

        prev_adj = self._snap.news_sentiment_adj
        self._snap.news_sentiment_adj = adj
        self._snap.bearish_headline = bearish_hit or ""
        self._snap.bullish_headline = bullish_hit or ""

        if adj != prev_adj:
            if adj < 0:
                logger.warning(
                    "[macro_agent] sentimiento BEARISH adj={} — {}",
                    adj, bearish_hit or "múltiples noticias",
                )
            elif adj > 0:
                logger.info(
                    "[macro_agent] sentimiento BULLISH adj=+{} — {}",
                    adj, bullish_hit or "múltiples noticias",
                )
            else:
                logger.info("[macro_agent] sentimiento NEUTRO")


# ── Parsers auxiliares ─────────────────────────────────────────────────────────

def _parse_ff_datetime(date_str: str, time_str: str) -> Optional[datetime.datetime]:
    """
    Parsea fecha y hora de Forex Factory.
    date_str ejemplo: "2026-04-04T00:00:00-05:00"   (EST)
    time_str ejemplo: "8:30am"
    Retorna datetime en UTC o None si falla.
    """
    try:
        # Extraer sólo la parte de fecha
        date_part = date_str[:10]  # "YYYY-MM-DD"
        year, month, day = map(int, date_part.split("-"))

        # Parsear hora: "8:30am", "12:00pm", "All Day"
        time_str = time_str.strip().lower()
        if "all day" in time_str or time_str == "":
            # Eventos sin hora específica: usar mediodía EST
            hour, minute = 12, 0
        else:
            m = re.match(r"(\d+):(\d+)(am|pm)", time_str)
            if not m:
                return None
            hour, minute, ampm = int(m.group(1)), int(m.group(2)), m.group(3)
            if ampm == "pm" and hour != 12:
                hour += 12
            if ampm == "am" and hour == 12:
                hour = 0

        # Forex Factory usa EST (UTC-5)
        est_dt = datetime.datetime(year, month, day, hour, minute,
                                   tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
        return est_dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def _parse_rss(xml_text: str) -> list[dict]:
    """Parsea RSS XML y retorna lista de {title, pub_ts}."""
    items = []
    try:
        root = ET.fromstring(xml_text)
        channel = root.find("channel")
        if channel is None:
            return []
        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            pub_date_str = (item.findtext("pubDate") or "").strip()
            pub_ts = _parse_rss_date(pub_date_str)
            items.append({"title": title, "pub_ts": pub_ts})
    except Exception as exc:
        logger.debug("[macro_agent] RSS parse error: {}", exc)
    return items


def _parse_rss_date(date_str: str) -> float:
    """Parsea fecha RSS como 'Fri, 04 Apr 2026 12:00:00 +0000' a timestamp."""
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).timestamp()
    except Exception:
        return 0.0
