# -*- coding: utf-8 -*-
"""
latency_probe.py — Whale Follower Bot
Sonda de latencia ultra-baja para seniales de trading via WebSocket.

Propósito:
  Medir cuánto tiempo pasa entre que Binance emite un trade y que
  nosotros emitimos una señal. Usa todas las optimizaciones de bajo
  nivel disponibles en Python puro para minimizar overhead propio.

Optimizaciones implementadas (ver comentarios en código):
  1. uvloop      — event loop alternativo basado en libuv (C), 2-4x más rápido
  2. orjson      — parser JSON en Rust, 3-10x más rápido que stdlib
  3. Buffer O(1) — EMA incremental, sin recalcular la ventana completa
  4. WS múltiplex — un solo socket para múltiples streams (evita N handshakes TCP)
  5. time_ns()   — precisión nanosegundo para medir latencia real
  6. No I/O en hot path — prints solo al generar señal, nunca dentro del parse

NOTA: Binance está geo-bloqueado en Railway. Ejecutar localmente o desde
un VPS con acceso a Binance (Europa / Asia). En Railway usar Bybit streams.

Instalación:
  pip install uvloop orjson websockets

Uso:
  python latency_probe.py
  python latency_probe.py --no-uvloop   # comparar con asyncio puro
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional

# ── Optimización 1: orjson (Rust) en lugar de json (CPython) ─────────────────
# orjson.loads es 3-10x más rápido en strings cortos (<1KB) como ticks de trade.
# Fallback a json estándar si no está instalado.
try:
    import orjson as _json_lib
    _loads = _json_lib.loads
    _JSON_LIB = "orjson"
except ImportError:
    import json as _json_lib          # type: ignore[no-redef]
    _loads = _json_lib.loads
    _JSON_LIB = "json (stdlib)"

# ── Optimización 2: uvloop (libuv) en lugar del event loop CPython ────────────
# uvloop implementa el event loop usando libuv (la misma biblioteca que Node.js).
# Reduce overhead de select()/epoll() y scheduling de corrutinas en ~2ms/ciclo.
try:
    import uvloop
    _UVLOOP_AVAILABLE = True
except ImportError:
    _UVLOOP_AVAILABLE = False

import websockets
from websockets.exceptions import ConnectionClosed

# ── Configuración ─────────────────────────────────────────────────────────────

# WebSocket multiplexado Binance: un solo socket, múltiples streams
# Optimización 4: evita N handshakes TCP y N conexiones TLS
_BINANCE_WS = "wss://stream.binance.com:9443/stream"
_STREAMS    = ["btcusdt@trade", "ethusdt@trade"]

# Tamaño de ventanas para EMA rápida y lenta
_EMA_FAST  = 5     # últimos 5 ticks (~250ms con BTC activo)
_EMA_SLOW  = 20    # últimos 20 ticks (~1s)

# Umbral mínimo de diferencia EMA para emitir señal (evita ruido)
_MIN_SPREAD_PCT = 0.003   # 0.003% de diferencia entre EMA rápida y lenta

# Reconexión: backoff exponencial
_BACKOFF_BASE = 0.5
_BACKOFF_MAX  = 30.0


# ── Optimización 3: EMA incremental O(1) ─────────────────────────────────────
# Una EMA (Exponential Moving Average) se calcula como:
#   ema_t = alpha * precio_t + (1 - alpha) * ema_{t-1}
# donde alpha = 2 / (N + 1)
# Esto es O(1) por tick: no necesitamos almacenar los últimos N precios.
# Ventaja sobre SMA circular: misma complejidad, pero más peso a precios recientes
# → reacciona más rápido a cambios → mejor para señales de latencia baja.

@dataclass
class EMAState:
    """Estado de una EMA incremental. Un float, actualización O(1)."""
    period: int
    value:  Optional[float] = None
    alpha:  float = field(init=False)

    def __post_init__(self) -> None:
        # alpha determina qué tan rápido "olvida" precios viejos
        self.alpha = 2.0 / (self.period + 1)

    def update(self, price: float) -> float:
        if self.value is None:
            self.value = price          # primer tick: inicializar
        else:
            # Operación O(1): una multiplicación y una suma
            self.value = self.alpha * price + (1 - self.alpha) * self.value
        return self.value


@dataclass
class PairState:
    """Estado completo de señal para un par (EMA rápida + lenta + última señal)."""
    pair:        str
    ema_fast:    EMAState = field(default_factory=lambda: EMAState(_EMA_FAST))
    ema_slow:    EMAState = field(default_factory=lambda: EMAState(_EMA_SLOW))
    last_signal: str      = "none"   # "buy" | "sell" | "none"
    ticks:       int      = 0

    def process(self, price: float, recv_ns: int) -> Optional[dict]:
        """
        Actualizar EMAs y decidir señal.
        Retorna dict con señal o None si no hay cruce.
        Todo O(1) — sin loops, sin sumas de arrays.
        """
        self.ticks += 1

        fast = self.ema_fast.update(price)
        slow = self.ema_slow.update(price)

        # Necesitamos al menos `period_slow` ticks para que las EMAs converjan
        if self.ticks < _EMA_SLOW:
            return None

        spread_pct = (fast - slow) / slow * 100

        if spread_pct > _MIN_SPREAD_PCT:
            signal = "BUY"
        elif spread_pct < -_MIN_SPREAD_PCT:
            signal = "SELL"
        else:
            return None

        # Solo emitir cuando cambia la dirección (evitar duplicados)
        if signal == self.last_signal:
            return None

        self.last_signal = signal

        # Optimización 5: time_ns() tiene precisión nanosegundo
        # La diferencia recv_ns → now_ns mide el overhead de procesamiento puro
        emit_ns  = time.time_ns()
        proc_ms  = (emit_ns - recv_ns) / 1_000_000   # ns → ms

        return {
            "signal":    signal,
            "pair":      self.pair,
            "price":     price,
            "ema_fast":  round(fast, 4),
            "ema_slow":  round(slow, 4),
            "spread":    round(spread_pct, 5),
            "proc_ms":   round(proc_ms, 4),
            "recv_ns":   recv_ns,
        }


# ── Parser de mensaje Binance ─────────────────────────────────────────────────

def _parse_trade(raw: bytes | str) -> Optional[tuple[str, float, int]]:
    """
    Extrae (pair, price, exchange_ts_ms) del mensaje multiplexado de Binance.

    El formato del stream combinado es:
      {"stream": "btcusdt@trade", "data": {"p": "65123.45", "T": 1714000000000, ...}}

    Usamos orjson que retorna directamente desde bytes sin decodificar UTF-8,
    ahorrando ~0.1ms en strings grandes. Aquí el mensaje es pequeño (~200 bytes)
    pero en alta frecuencia (>1000 ticks/s) el ahorro acumula.
    """
    try:
        msg  = _loads(raw)
        data = msg.get("data", {})
        if data.get("e") != "trade":
            return None
        pair     = data["s"]                    # eg "BTCUSDT"
        price    = float(data["p"])             # precio como string en Binance
        exch_ts  = int(data["T"])              # timestamp exchange en ms
        return pair, price, exch_ts
    except Exception:
        return None


# ── Estimación de latencia de red ─────────────────────────────────────────────

def _estimate_network_ms(exchange_ts_ms: int) -> float:
    """
    Latencia aproximada de red = hora_local - hora_exchange.
    Requiere que el reloj local esté sincronizado (NTP).
    Negativo significa reloj local adelantado o exchange retrasado.
    """
    local_ms = time.time() * 1000
    return local_ms - exchange_ts_ms


# ── Lógica principal de WebSocket ─────────────────────────────────────────────

async def _stream_loop(states: Dict[str, PairState], stats: dict) -> None:
    """
    Conecta al stream multiplexado de Binance y procesa ticks.

    Optimización 4 (WS multiplex): un solo URI combina todos los streams:
      /stream?streams=btcusdt@trade/ethusdt@trade
    En vez de abrir dos conexiones WebSocket separadas, una sola maneja
    ambos pares → 1 handshake TLS, 1 buffer de red, menos overhead de OS.
    """
    uri = f"{_BINANCE_WS}?streams=" + "/".join(_STREAMS)

    while True:
        try:
            # ping_interval=20 mantiene la conexión viva sin overhead visible
            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=10,
                max_size=2**18,          # 256KB max frame, suficiente para trades
                compression=None,        # deshabilitar deflate — ahorra CPU en hot path
            ) as ws:
                stats["connected"] = True
                stats["reconnects"] += 1
                print(
                    f"[OK] Conectado a Binance | streams: {', '.join(_STREAMS)}\n"
                    f"     JSON lib: {_JSON_LIB} | uvloop: {stats['uvloop']}\n"
                    f"     EMA fast={_EMA_FAST} slow={_EMA_SLOW} "
                    f"threshold={_MIN_SPREAD_PCT}%\n"
                    f"{'─'*60}",
                    flush=True,
                )

                async for raw_msg in ws:
                    # Optimización 5: capturar recv_ns INMEDIATAMENTE al recibir
                    # Cualquier instrucción antes de esta línea añade latencia medible.
                    recv_ns = time.time_ns()

                    parsed = _parse_trade(raw_msg)
                    if parsed is None:
                        continue

                    pair, price, exch_ts_ms = parsed
                    stats["ticks"] += 1

                    # Latencia de red (exchange → nosotros)
                    net_ms = _estimate_network_ms(exch_ts_ms)

                    state  = states.get(pair)
                    if state is None:
                        continue

                    # Procesar señal — O(1) por diseño
                    result = state.process(price, recv_ns)

                    # Optimización 6: print SOLO cuando hay señal
                    # En el hot path (sin señal) no hay I/O → latencia mínima.
                    # I/O es la operación más costosa después de la red.
                    if result is not None:
                        stats["signals"] += 1
                        direction = "▲ BUY " if result["signal"] == "BUY" else "▼ SELL"
                        print(
                            f"{direction} {pair:>8} "
                            f"price={price:>12,.4f}  "
                            f"fast={result['ema_fast']:>12,.4f}  "
                            f"slow={result['ema_slow']:>12,.4f}  "
                            f"spread={result['spread']:>+8.4f}%  "
                            f"net={net_ms:>6.1f}ms  "
                            f"proc={result['proc_ms']:>5.3f}ms",
                            flush=True,
                        )

        except ConnectionClosed as exc:
            stats["connected"] = False
            print(f"[WS] Conexión cerrada: {exc.code} {exc.reason}", file=sys.stderr)
        except Exception as exc:
            stats["connected"] = False
            print(f"[WS] Error: {exc}", file=sys.stderr)

        # Optimización implícita: backoff exponencial evita flood de reconexiones
        # que consumirían CPU y enmascararían problemas de red
        delay = min(_BACKOFF_BASE * (2 ** stats["reconnects"]), _BACKOFF_MAX)
        stats["reconnects"] = min(stats["reconnects"], 10)   # cap para no crecer infinito
        print(f"[WS] Reconectando en {delay:.1f}s...", file=sys.stderr)
        await asyncio.sleep(delay)


# ── Stats periódicas ──────────────────────────────────────────────────────────

async def _print_stats(stats: dict, interval: float = 30.0) -> None:
    """Imprime throughput y señales cada N segundos para diagnóstico."""
    start = time.time()
    prev_ticks = 0
    while True:
        await asyncio.sleep(interval)
        elapsed   = time.time() - start
        ticks_now = stats["ticks"]
        rate      = (ticks_now - prev_ticks) / interval
        prev_ticks = ticks_now
        print(
            f"\n[STATS] uptime={elapsed:.0f}s  "
            f"ticks={ticks_now}  rate={rate:.1f}/s  "
            f"signals={stats['signals']}  "
            f"connected={stats['connected']}\n",
            flush=True,
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Sonda de latencia WebSocket")
    parser.add_argument("--no-uvloop", action="store_true",
                        help="Usar asyncio puro en lugar de uvloop (para comparar)")
    args = parser.parse_args()

    use_uvloop = _UVLOOP_AVAILABLE and not args.no_uvloop

    # Optimización 1: instalar uvloop ANTES de crear el event loop
    if use_uvloop:
        uvloop.install()
        print(f"[OK] uvloop activo (libuv C backend)")
    else:
        if not _UVLOOP_AVAILABLE:
            print("[!]  uvloop no instalado — usando asyncio puro. "
                  "Instalar: pip install uvloop")
        else:
            print("[!]  uvloop deshabilitado por --no-uvloop")

    # Estado por par
    states: Dict[str, PairState] = {
        "BTCUSDT": PairState("BTCUSDT"),
        "ETHUSDT": PairState("ETHUSDT"),
    }

    stats = {
        "ticks":     0,
        "signals":   0,
        "reconnects": 0,
        "connected": False,
        "uvloop":    use_uvloop,
    }

    async def _run() -> None:
        await asyncio.gather(
            _stream_loop(states, stats),
            _print_stats(stats),
        )

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        elapsed = time.time()
        print(
            f"\n[FIN] ticks={stats['ticks']}  "
            f"signals={stats['signals']}  "
            f"reconexiones={stats['reconnects']}",
        )


if __name__ == "__main__":
    main()
