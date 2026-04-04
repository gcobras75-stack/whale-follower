"""
config.py — Whale Follower Bot — Sprint 2
Centralized configuration and environment variable loading.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str   = os.environ["TELEGRAM_CHAT_ID"]

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL: str = os.environ["SUPABASE_URL"]
SUPABASE_KEY: str = os.environ["SUPABASE_KEY"]

# ── Railway / HTTP ────────────────────────────────────────────────────────────
PORT: int = int(os.getenv("PORT", "8080"))

# ── Trading parameters ────────────────────────────────────────────────────────
TRADING_PAIR             = os.getenv("TRADING_PAIR", "BTC/USDT")
SIGNAL_SCORE_THRESHOLD:  int   = int(os.getenv("SIGNAL_SCORE_THRESHOLD", "65"))
HIGH_CONFIDENCE_SCORE:   int   = int(os.getenv("HIGH_CONFIDENCE_SCORE",  "80"))

# ── Exchange enable flags ─────────────────────────────────────────────────────
ENABLE_BINANCE: bool = os.getenv("ENABLE_BINANCE", "true").lower() == "true"
ENABLE_BYBIT:   bool = os.getenv("ENABLE_BYBIT",   "true").lower() == "true"
ENABLE_OKX:     bool = os.getenv("ENABLE_OKX",     "true").lower() == "true"

# ── Spring detection thresholds ───────────────────────────────────────────────
SPRING_DROP_PCT:   float = float(os.getenv("SPRING_DROP_PCT",   "0.003"))
SPRING_BOUNCE_PCT: float = float(os.getenv("SPRING_BOUNCE_PCT", "0.002"))
SPRING_DROP_SECS:  int   = int(os.getenv("SPRING_DROP_SECS",    "10"))
SPRING_BOUNCE_SECS: int  = int(os.getenv("SPRING_BOUNCE_SECS",  "5"))
VOLUME_MULTIPLIER: float = float(os.getenv("VOLUME_MULTIPLIER", "1.5"))
WINDOW_SECS:       int   = int(os.getenv("WINDOW_SECS",         "60"))

# ── Stop-cascade detection ────────────────────────────────────────────────────
CASCADE_SELL_COUNT: int = int(os.getenv("CASCADE_SELL_COUNT", "200"))
CASCADE_SECS:       int = int(os.getenv("CASCADE_SECS",       "5"))

# ── Trade parameters ──────────────────────────────────────────────────────────
STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "0.005"))
RISK_REWARD:   float = float(os.getenv("RISK_REWARD",   "3.0"))

# ── Bybit Testnet (paper trading) ─────────────────────────────────────────────
BYBIT_TESTNET_API_KEY: str = os.getenv("BYBIT_TESTNET_API_KEY", "")
BYBIT_TESTNET_SECRET:  str = os.getenv("BYBIT_TESTNET_SECRET",  "")
PAPER_CAPITAL:        float = float(os.getenv("PAPER_CAPITAL", "10000"))

# ── Reconnection back-off ─────────────────────────────────────────────────────
BACKOFF_BASE: float = 1.0
BACKOFF_MAX:  float = 60.0
