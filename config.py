"""
config.py — Whale Follower Bot
Centralized configuration and environment variable loading.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]

# ── Railway / HTTP ────────────────────────────────────────────────────────────
PORT: int = int(os.getenv("PORT", "8080"))

# ── Trading parameters ────────────────────────────────────────────────────────
TRADING_PAIR = os.getenv("TRADING_PAIR", "BTC/USDT")   # display name
SIGNAL_SCORE_THRESHOLD: int = int(os.getenv("SIGNAL_SCORE_THRESHOLD", "65"))

# ── Exchange enable flags ─────────────────────────────────────────────────────
ENABLE_BINANCE: bool = os.getenv("ENABLE_BINANCE", "true").lower() == "true"
ENABLE_BYBIT: bool   = os.getenv("ENABLE_BYBIT",   "true").lower() == "true"
ENABLE_OKX: bool     = os.getenv("ENABLE_OKX",     "true").lower() == "true"

# ── Spring detection thresholds (all adjustable) ──────────────────────────────
SPRING_DROP_PCT: float    = float(os.getenv("SPRING_DROP_PCT",   "0.003"))   # 0.3%
SPRING_BOUNCE_PCT: float  = float(os.getenv("SPRING_BOUNCE_PCT", "0.002"))   # 0.2%
SPRING_DROP_SECS: int     = int(os.getenv("SPRING_DROP_SECS",    "10"))
SPRING_BOUNCE_SECS: int   = int(os.getenv("SPRING_BOUNCE_SECS",  "5"))
VOLUME_MULTIPLIER: float  = float(os.getenv("VOLUME_MULTIPLIER", "1.5"))
WINDOW_SECS: int          = int(os.getenv("WINDOW_SECS",         "60"))

# ── Stop-cascade detection ────────────────────────────────────────────────────
CASCADE_SELL_COUNT: int   = int(os.getenv("CASCADE_SELL_COUNT",  "200"))
CASCADE_SECS: int         = int(os.getenv("CASCADE_SECS",        "5"))

# ── Trade parameters ──────────────────────────────────────────────────────────
STOP_LOSS_PCT: float  = float(os.getenv("STOP_LOSS_PCT",  "0.005"))   # 0.5%
RISK_REWARD: float    = float(os.getenv("RISK_REWARD",    "3.0"))      # 1:3

# ── Reconnection back-off ─────────────────────────────────────────────────────
BACKOFF_BASE: float = 1.0
BACKOFF_MAX: float  = 60.0
