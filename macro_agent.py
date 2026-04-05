# -*- coding: utf-8 -*-
"""
macro_agent.py — Whale Follower Bot — Sprint 6
Agente de macrofundamentales en tiempo real (costo $0).

Fuentes:
  1. Forex Factory JSON   — calendario económico USD de alto impacto
  2. CryptoPanic RSS      — noticias crypto importantes
  3. Reddit JSON API      — r/CryptoCurrency, r/Bitcoin, r/ethereum (sin auth)
  4. X / Twitter via Nitter RSS — cuentas clave: Elon, Saylor, CZ, Fed

Expone:
  is_paused()            -> (bool, str)   # bloquea la señal si hay evento macro
  sentiment_adjustment() -> int           # ajuste de score (-20 a +15)
  run()                  -> coro          # tarea de fondo asyncio
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
_CALENDAR_URL     = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_CRYPTOPANIC_RSS  = "https://cryptopanic.com/news/rss/"
_REFRESH_CALENDAR = 3600   # cada hora
_REFRESH_NEWS     = 300    # cada 5 minutos
_REFRESH_REDDIT   = 180    # cada 3 minutos
_REFRESH_TWITTER  = 240    # cada 4 minutos
_REFRESH_DXY      = 300    # cada 5 minutos

# DXY — Yahoo Finance (sin API key, aiohttp directo)
_DXY_URL         = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=5m&range=1h"
_DXY_UP_THRESHOLD = 0.30   # % cambio en 5 min para considerar tendencia alcista fuerte
_PRE_EVENT_SECS   = 1800   # pausar 30 min ANTES del evento
_POST_EVENT_SECS  = 900    # pausar 15 min DESPUÉS del evento
_ARTICLE_WINDOW   = 1800   # solo noticias de los últimos 30 min

# ── Reddit — subreddits a monitorear ──────────────────────────────────────────
_REDDIT_FEEDS = [
    "https://www.reddit.com/r/CryptoCurrency/new.json?limit=25",
    "https://www.reddit.com/r/Bitcoin/new.json?limit=25",
    "https://www.reddit.com/r/ethereum/new.json?limit=25",
    "https://www.reddit.com/r/CryptoMarkets/new.json?limit=15",
]
_REDDIT_USER_AGENT = "WhaleFollowerBot/1.0 (market monitoring)"

# ── X / Twitter via Nitter RSS — cuentas de alto impacto ─────────────────────
# Nitter es un frontend open-source de Twitter que expone RSS público sin auth
_NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
]
_TWITTER_ACCOUNTS = [
    "elonmusk",      # mueve mercados con un tweet
    "saylor",        # MicroStrategy — señal de acumulación institucional
    "cz_binance",    # CEO Binance — noticias exchange clave
    "federalreserve",# Fed oficial — política monetaria
    "POTUS",         # Presidencia — aranceles, regulación
    "realDonaldTrump", # TruthSocial / X — impacto cripto directo
]

# ── Keywords bearish ──────────────────────────────────────────────────────────
_BEARISH_KEYWORDS = [
    "ban", "banned", "hack", "hacked", "exploit", "stolen",
    "SEC", "lawsuit", "arrest", "seized", "shutdown",
    "crash", "collapse", "bankrupt", "fraud", "scam",
    "regulation", "restricted", "blocked", "sanction",
    "tariff", "recession", "panic", "dump", "rugpull",
    "investigation", "fined", "penalty", "delistment",
]

# ── Keywords bullish ──────────────────────────────────────────────────────────
_BULLISH_KEYWORDS = [
    "ETF", "approved", "institutional", "adoption", "partnership",
    "reserve", "treasury", "accumulate", "strategic", "buy",
    "breakthrough", "upgrade", "milestone", "all-time", "ATH",
    "halving", "bullrun", "moon", "rally",
]

# ── Eventos macro USD de alto impacto ────────────────────────────────────────
_HIGH_IMPACT_KEYWORDS = [
    "CPI", "NFP", "Non-Farm", "FOMC", "Federal Funds",
    "PCE", "GDP", "PPI", "Unemployment", "Retail Sales",
    "Interest Rate", "Powell", "Fed Chair", "Treasury",
    "Inflation", "Consumer Price",
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
    cp_bearish_headline: str = ""
    cp_bullish_headline: str = ""

    # Reddit
    reddit_bearish_headline: str = ""
    reddit_bullish_headline: str = ""
    reddit_score: int = 0          # upvotes del post más relevante

    # X / Twitter
    twitter_bearish_tweet: str = ""
    twitter_bullish_tweet: str = ""
    twitter_account_hit: str = ""

    # Ajuste total de sentimiento
    news_sentiment_adj: int = 0    # -20 a +15

    # DXY — Índice Dólar
    dxy_value:      float = 0.0
    dxy_change_5m:  float = 0.0    # % cambio últimos 5 minutos
    dxy_strong_up:  bool  = False  # True si DXY subió > 0.30% en 5 min

    # Meta
    last_calendar_update: float = 0.0
    last_news_update: float = 0.0
    last_reddit_update: float = 0.0
    last_twitter_update: float = 0.0
    last_dxy_update: float = 0.0


# ── Motor principal ────────────────────────────────────────────────────────────
class MacroAgent:
    """
    Corre como tarea asyncio de fondo.
    Los métodos públicos (is_paused, sentiment_adjustment) son seguros
    para llamar desde el loop principal sin bloquear.
    """

    def __init__(self) -> None:
        self._snap = MacroSnapshot()
        self._upcoming_events: list[dict] = []
        self._cp_headlines: list[dict] = []
        self._reddit_posts: list[dict] = []
        self._tweets: list[dict] = []

    # ── API pública ────────────────────────────────────────────────────────────

    def is_paused(self) -> tuple[bool, str]:
        if self._snap.paused_for_event:
            return True, f"macro_pause: {self._snap.event_name}"
        return False, ""

    def sentiment_adjustment(self) -> int:
        return self._snap.news_sentiment_adj

    def dxy_trend(self) -> tuple[float, bool]:
        """Retorna (change_pct_5m, strong_up) del DXY. Sin bloqueo."""
        return self._snap.dxy_change_5m, self._snap.dxy_strong_up

    def snapshot(self) -> MacroSnapshot:
        return self._snap

    # ── Loop de fondo ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(
            "[macro_agent] iniciando — calendario + CryptoPanic + Reddit + X/Nitter + DXY"
        )
        await asyncio.gather(
            self._calendar_loop(),
            self._news_loop(),
            self._reddit_loop(),
            self._twitter_loop(),
            self._dxy_loop(),
        )

    # ── 1. Calendario económico ────────────────────────────────────────────────

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
            logger.info(
                "[macro_agent] calendario: {} eventos USD High esta semana", len(filtered)
            )
        except Exception as exc:
            logger.warning("[macro_agent] calendario fallo (no bloquea): {}", exc)

    def _evaluate_calendar(self) -> None:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        nearest_name = ""
        nearest_dt: Optional[datetime.datetime] = None
        nearest_delta: Optional[float] = None

        for event in self._upcoming_events:
            dt = _parse_ff_datetime(event.get("date", ""), event.get("time", ""))
            if dt is None:
                continue
            delta_secs = (dt - now_utc).total_seconds()
            if -_POST_EVENT_SECS <= delta_secs <= _PRE_EVENT_SECS:
                if nearest_delta is None or abs(delta_secs) < abs(nearest_delta):
                    nearest_delta = delta_secs
                    nearest_name = event.get("title", "evento")
                    nearest_dt = dt

        if nearest_name:
            self._snap.paused_for_event = True
            self._snap.event_name = nearest_name
            self._snap.event_time_utc = nearest_dt
            mins = int((nearest_dt - now_utc).total_seconds() / 60) if nearest_dt else None
            self._snap.mins_to_event = mins
            logger.warning(
                "[macro_agent] PAUSA ACTIVA — {} en ~{} min", nearest_name, mins
            )
        else:
            if self._snap.paused_for_event:
                logger.info("[macro_agent] pausa liberada — sin eventos inmediatos")
            self._snap.paused_for_event = False
            self._snap.event_name = ""
            self._snap.event_time_utc = None
            self._snap.mins_to_event = None

    # ── 2. CryptoPanic RSS ────────────────────────────────────────────────────

    async def _news_loop(self) -> None:
        while True:
            await self._fetch_cryptopanic()
            self._recalculate_sentiment()
            await asyncio.sleep(_REFRESH_NEWS)

    async def _fetch_cryptopanic(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(_CRYPTOPANIC_RSS) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    text = await resp.text()

            items = _parse_rss(text)
            cutoff = time.time() - _ARTICLE_WINDOW
            self._cp_headlines = [i for i in items if i.get("pub_ts", 0) >= cutoff]
            self._snap.last_news_update = time.time()
            logger.debug(
                "[macro_agent] CryptoPanic: {} noticias en últimos 30 min",
                len(self._cp_headlines),
            )
        except Exception as exc:
            logger.warning("[macro_agent] CryptoPanic fallo (no bloquea): {}", exc)

    # ── 3. Reddit ─────────────────────────────────────────────────────────────

    async def _reddit_loop(self) -> None:
        while True:
            await self._fetch_reddit()
            self._recalculate_sentiment()
            await asyncio.sleep(_REFRESH_REDDIT)

    async def _fetch_reddit(self) -> None:
        timeout = aiohttp.ClientTimeout(total=12)
        headers = {"User-Agent": _REDDIT_USER_AGENT}
        posts: list[dict] = []
        cutoff = time.time() - _ARTICLE_WINDOW

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            for url in _REDDIT_FEEDS:
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json(content_type=None)

                    children = data.get("data", {}).get("children", [])
                    for child in children:
                        p = child.get("data", {})
                        created = p.get("created_utc", 0)
                        if created < cutoff:
                            continue
                        posts.append({
                            "title":   p.get("title", ""),
                            "score":   p.get("score", 0),
                            "subreddit": p.get("subreddit", ""),
                            "pub_ts":  created,
                        })
                except Exception as exc:
                    logger.debug("[macro_agent] Reddit {} fallo: {}", url, exc)

        self._reddit_posts = posts
        self._snap.last_reddit_update = time.time()
        if posts:
            logger.debug(
                "[macro_agent] Reddit: {} posts en últimos 30 min", len(posts)
            )

    # ── 4. X / Twitter via Nitter RSS ────────────────────────────────────────

    async def _twitter_loop(self) -> None:
        # Espera inicial para no saturar al arrancar junto con los otros loops
        await asyncio.sleep(30)
        while True:
            await self._fetch_twitter()
            self._recalculate_sentiment()
            await asyncio.sleep(_REFRESH_TWITTER)

    async def _fetch_twitter(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        tweets: list[dict] = []
        cutoff = time.time() - _ARTICLE_WINDOW

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for account in _TWITTER_ACCOUNTS:
                fetched = False
                for nitter in _NITTER_INSTANCES:
                    if fetched:
                        break
                    url = f"{nitter}/{account}/rss"
                    try:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            text = await resp.text()

                        items = _parse_rss(text)
                        recent = [i for i in items if i.get("pub_ts", 0) >= cutoff]
                        for item in recent:
                            item["account"] = account
                            tweets.append(item)
                        fetched = True
                        if recent:
                            logger.debug(
                                "[macro_agent] @{}: {} tweets recientes", account, len(recent)
                            )
                    except Exception:
                        continue   # intentar siguiente instancia Nitter

        self._tweets = tweets
        self._snap.last_twitter_update = time.time()

    # ── 5. DXY — Índice Dólar en tiempo real ──────────────────────────────

    async def _dxy_loop(self) -> None:
        await asyncio.sleep(15)   # espera inicial para no saturar arranque
        while True:
            await self._fetch_dxy()
            await asyncio.sleep(_REFRESH_DXY)

    async def _fetch_dxy(self) -> None:
        """Obtiene datos DXY de Yahoo Finance (sin API key). No bloquea si falla."""
        timeout = aiohttp.ClientTimeout(total=12)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; WhaleBot/1.0)"}
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(_DXY_URL) as resp:
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    data = await resp.json(content_type=None)

            result = data.get("chart", {}).get("result", [{}])[0]
            closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
            closes = [c for c in closes if c is not None]

            if len(closes) < 2:
                return

            current   = closes[-1]
            prev_5m   = closes[-2]
            change    = (current - prev_5m) / prev_5m * 100

            self._snap.dxy_value     = current
            self._snap.dxy_change_5m = change
            self._snap.dxy_strong_up = change >= _DXY_UP_THRESHOLD
            self._snap.last_dxy_update = time.time()

            if self._snap.dxy_strong_up:
                logger.info(
                    "[macro_agent] DXY alcista fuerte: {:.4f} ({:+.3f}% en 5m) "
                    "→ multiplicador Long 0.85x activo",
                    current, change,
                )
            else:
                logger.debug(
                    "[macro_agent] DXY: {:.4f} ({:+.3f}% en 5m)", current, change
                )
        except Exception as exc:
            logger.debug("[macro_agent] DXY fetch fallo (no bloquea): {}", exc)

    # ── Recalcular sentimiento (combina todas las fuentes) ────────────────────

    def _recalculate_sentiment(self) -> None:
        """Combina señales de CryptoPanic + Reddit + Twitter en un único score."""
        bearish_weight = 0
        bullish_weight = 0
        bearish_hit: Optional[str] = None
        bullish_hit: Optional[str] = None
        cp_bearish = ""
        cp_bullish = ""
        reddit_bearish = ""
        reddit_bullish = ""
        twitter_bearish = ""
        twitter_bullish = ""
        twitter_account = ""
        reddit_max_score = 0

        # ── CryptoPanic ───────────────────────────────────────────────────────
        for item in self._cp_headlines:
            title = (item.get("title") or "").lower()
            for kw in _BEARISH_KEYWORDS:
                if kw.lower() in title:
                    w = 3 if kw in ("hack", "hacked", "ban", "banned", "SEC", "exploit") else 1
                    bearish_weight += w
                    if not cp_bearish:
                        cp_bearish = item.get("title", "")
                    break
            for kw in _BULLISH_KEYWORDS:
                if kw.lower() in title:
                    bullish_weight += 1
                    if not cp_bullish:
                        cp_bullish = item.get("title", "")
                    break

        # ── Reddit ────────────────────────────────────────────────────────────
        for post in self._reddit_posts:
            title = (post.get("title") or "").lower()
            score = post.get("score", 0)
            # Posts con muchos upvotes pesan más
            multiplier = 2 if score > 500 else 1

            for kw in _BEARISH_KEYWORDS:
                if kw.lower() in title:
                    bearish_weight += multiplier
                    if not reddit_bearish or score > reddit_max_score:
                        reddit_bearish = post.get("title", "")
                        reddit_max_score = score
                    break
            for kw in _BULLISH_KEYWORDS:
                if kw.lower() in title:
                    bullish_weight += multiplier
                    if not reddit_bullish:
                        reddit_bullish = post.get("title", "")
                    break

        # ── Twitter / X ────────────────────────────────────────────────────────
        for tweet in self._tweets:
            title = (tweet.get("title") or "").lower()
            account = tweet.get("account", "")
            # Elon, Saylor, CZ tienen mayor impacto — peso 3x
            high_impact_account = account in ("elonmusk", "saylor", "cz_binance")
            w = 3 if high_impact_account else 1

            for kw in _BEARISH_KEYWORDS:
                if kw.lower() in title:
                    bearish_weight += w
                    if not twitter_bearish:
                        twitter_bearish = f"@{account}: {tweet.get('title','')[:80]}"
                        twitter_account = account
                    break
            for kw in _BULLISH_KEYWORDS:
                if kw.lower() in title:
                    bullish_weight += w
                    if not twitter_bullish:
                        twitter_bullish = f"@{account}: {tweet.get('title','')[:80]}"
                    break

        # ── Calcular ajuste neto ───────────────────────────────────────────────
        if bearish_weight >= 8:
            adj = -20
        elif bearish_weight >= 5:
            adj = -15
        elif bearish_weight >= 3:
            adj = -10
        elif bearish_weight >= 1:
            adj = -5
        elif bullish_weight >= 4:
            adj = 15
        elif bullish_weight >= 2:
            adj = 10
        elif bullish_weight >= 1:
            adj = 5
        else:
            adj = 0

        prev = self._snap.news_sentiment_adj
        self._snap.news_sentiment_adj    = adj
        self._snap.cp_bearish_headline   = cp_bearish
        self._snap.cp_bullish_headline   = cp_bullish
        self._snap.reddit_bearish_headline = reddit_bearish
        self._snap.reddit_bullish_headline = reddit_bullish
        self._snap.twitter_bearish_tweet = twitter_bearish
        self._snap.twitter_bullish_tweet = twitter_bullish
        self._snap.twitter_account_hit   = twitter_account

        if adj != prev:
            source_summary = []
            if cp_bearish or cp_bullish:
                source_summary.append("CryptoPanic")
            if reddit_bearish or reddit_bullish:
                source_summary.append("Reddit")
            if twitter_bearish or twitter_bullish:
                source_summary.append(f"X/@{twitter_account}" if twitter_account else "X")

            if adj < 0:
                hit = twitter_bearish or reddit_bearish or cp_bearish or ""
                logger.warning(
                    "[macro_agent] BEARISH adj={} fuentes={} — {}",
                    adj, "+".join(source_summary) or "ninguna", hit[:80],
                )
            elif adj > 0:
                hit = twitter_bullish or reddit_bullish or cp_bullish or ""
                logger.info(
                    "[macro_agent] BULLISH adj=+{} fuentes={} — {}",
                    adj, "+".join(source_summary) or "ninguna", hit[:80],
                )
            else:
                logger.info("[macro_agent] sentimiento NEUTRO")


# ── Parsers auxiliares ─────────────────────────────────────────────────────────

def _parse_ff_datetime(date_str: str, time_str: str) -> Optional[datetime.datetime]:
    """Parsea fecha y hora de Forex Factory a datetime UTC."""
    try:
        date_part = date_str[:10]
        year, month, day = map(int, date_part.split("-"))
        time_str = time_str.strip().lower()
        if "all day" in time_str or time_str == "":
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
        est_dt = datetime.datetime(
            year, month, day, hour, minute,
            tzinfo=datetime.timezone(datetime.timedelta(hours=-5)),
        )
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
    """Parsea fecha RSS a Unix timestamp."""
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str).timestamp()
    except Exception:
        return 0.0
