"""
historical_downloader.py — Whale Follower Bot
Descarga 60 días de velas 1m BTC/USDT desde Binance API pública (sin API key).
Guarda en data/historical/btcusdt_1m.csv
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

DATA_DIR   = Path(__file__).parent / "data" / "historical"
KLINES_CSV = DATA_DIR / "btcusdt_1m.csv"
BASE_URL   = "https://api.binance.com/api/v3/klines"


def _fetch_batch(start_ms: int, end_ms: int, limit: int = 1000) -> list:
    params = urlencode({
        "symbol":    "BTCUSDT",
        "interval":  "1m",
        "startTime": start_ms,
        "endTime":   end_ms,
        "limit":     limit,
    })
    url = f"{BASE_URL}?{params}"
    try:
        with urlopen(url, timeout=30) as resp:
            return json.loads(resp.read())
    except URLError as e:
        raise RuntimeError(f"HTTP error: {e}") from e


def download_historical(days: int = 60, force: bool = False) -> Path:
    """
    Descarga días días de velas 1m. Usa caché si el archivo ya existe.
    Devuelve el Path al CSV.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Verificar caché
    if KLINES_CSV.exists() and not force:
        with open(KLINES_CSV, encoding="utf-8") as f:
            line_count = sum(1 for _ in f) - 1  # excluye header
        expected = days * 1440
        if line_count >= expected * 0.95:
            print(f"[downloader] Cache OK: {line_count:,} velas en {KLINES_CSV.name}")
            return KLINES_CSV
        print(f"[downloader] Cache incompleta ({line_count:,} velas). Descargando de nuevo...")

    now       = datetime.now(timezone.utc)
    end_ms    = int(now.timestamp() * 1000)
    start_ms  = int((now - timedelta(days=days)).timestamp() * 1000)
    expected  = days * 1440

    print(f"[downloader] Descargando {days} días de velas 1m (~{expected:,} velas)...")
    print(f"[downloader] Desde: {datetime.utcfromtimestamp(start_ms/1000).strftime('%Y-%m-%d')}"
          f"  Hasta: {datetime.utcfromtimestamp(end_ms/1000).strftime('%Y-%m-%d')}")

    total_written = 0
    current_start = start_ms
    batch_num     = 0
    retries       = 0

    with open(KLINES_CSV, "w", encoding="utf-8") as f:
        f.write("timestamp,open,high,low,close,volume\n")

        while current_start < end_ms:
            batch_num += 1
            try:
                data = _fetch_batch(current_start, end_ms)
            except Exception as exc:
                retries += 1
                if retries > 5:
                    print(f"\n[downloader] Demasiados errores. Último: {exc}")
                    break
                print(f"\n[downloader] Error en batch {batch_num}: {exc}. Reintentando en 3s...")
                time.sleep(3)
                continue

            if not data:
                break

            for k in data:
                # k = [openTime, open, high, low, close, volume, ...]
                f.write(f"{k[0]},{k[1]},{k[2]},{k[3]},{k[4]},{k[5]}\n")

            total_written += len(data)
            pct = min(total_written / expected * 100, 100.0)
            bar = "#" * int(pct / 5) + "-" * (20 - int(pct / 5))
            print(
                f"\r[downloader] [{bar}] {pct:5.1f}% | "
                f"{total_written:,}/{expected:,} velas | batch {batch_num}",
                end="", flush=True,
            )

            current_start = data[-1][0] + 60_000  # siguiente minuto
            retries = 0
            time.sleep(0.12)  # respetar rate limit de Binance

    print(f"\n[downloader] Descarga completa: {total_written:,} velas -> {KLINES_CSV}")
    return KLINES_CSV


def load_klines(filepath: Path = None) -> list[dict]:
    """Carga el CSV de velas. Devuelve lista de dicts."""
    filepath = filepath or KLINES_CSV
    klines = []
    with open(filepath, encoding="utf-8") as f:
        next(f)  # saltar header
        for line in f:
            line = line.strip()
            if not line:
                continue
            ts, o, h, l, c, v = line.split(",")
            klines.append({
                "timestamp": int(ts),
                "open":      float(o),
                "high":      float(h),
                "low":       float(l),
                "close":     float(c),
                "volume":    float(v),
            })
    return klines


if __name__ == "__main__":
    download_historical(days=60, force=False)
