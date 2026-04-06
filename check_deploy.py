#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_deploy.py -- Diagnóstico pre-deploy para Whale Follower Bot
Ejecutar: python check_deploy.py
"""
import sys
import os

print(f"=== WHALE FOLLOWER DEPLOY DIAGNOSTICS ===")
print(f"Python: {sys.version}")
print(f"Platform: {sys.platform}\n")

# ── 1. Variables de entorno críticas ──────────────────────────────────────────
print("── VARIABLES DE ENTORNO ──")
required_vars = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "OKX_API_KEY",
    "OKX_SECRET",
    "OKX_PASSPHRASE",
    "SUPABASE_URL",
    "PRODUCTION",
]
optional_vars = [
    "BYBIT_API_KEY",
    "BYBIT_API_SECRET",
    "ENABLE_CROSS_ARB_REAL",
    "DISABLE_CROSS_ARB",
    "PORT",
]
all_ok = True
for var in required_vars:
    val = os.getenv(var, "")
    if val:
        masked = val[:4] + "****" if len(val) > 4 else "****"
        print(f"  [OK]  {var} = {masked}")
    else:
        print(f"  [MISSING] {var} ← REQUERIDA")
        all_ok = False

for var in optional_vars:
    val = os.getenv(var, "")
    masked = (val[:4] + "****") if len(val) > 4 else (val or "no definida")
    print(f"  [opt] {var} = {masked}")

print()

# ── 2. Imports críticos ───────────────────────────────────────────────────────
print("── IMPORTS CRÍTICOS ──")
imports_to_test = [
    ("asyncio",          "stdlib"),
    ("aiohttp",          "requirements.txt"),
    ("websockets",       "requirements.txt"),
    ("loguru",           "requirements.txt"),
    ("supabase",         "requirements.txt"),
    ("dotenv",           "python-dotenv"),
    ("xgboost",          "requirements.txt"),
    ("sklearn",          "scikit-learn"),
    ("numpy",            "requirements.txt"),
    ("pandas",           "requirements.txt"),
    ("requests",         "requirements.txt"),
    ("aiofiles",         "requirements.txt"),
]
# uvloop solo en Linux
if sys.platform != "win32":
    imports_to_test.append(("uvloop", "requirements.txt"))

for mod, source in imports_to_test:
    try:
        __import__(mod)
        print(f"  [OK]  {mod}  ({source})")
    except ImportError as e:
        print(f"  [FAIL] {mod}  ({source}) ← {e}")
        all_ok = False

# pybit (opcional, solo si BYBIT habilitado)
try:
    import pybit
    print(f"  [OK]  pybit  (requirements.txt)")
except ImportError:
    print(f"  [warn] pybit  no instalado (cross-arb deshabilitado = OK)")

print()

# ── 3. Módulos del bot ────────────────────────────────────────────────────────
print("── MÓDULOS DEL BOT ──")
bot_modules = [
    "config",
    "alerts",
    "okx_executor",
    "bybit_utils",
    "grid_trader",
    "spring_detector",
    "momentum_strategy",
    "delta_neutral",
    "mean_reversion",
    "ofi_strategy",
    "btc_dominance",
    "dxy_monitor",
    "liquidations_global",
    "daily_report",
]
for mod in bot_modules:
    try:
        __import__(mod)
        print(f"  [OK]  {mod}.py")
    except Exception as e:
        print(f"  [FAIL] {mod}.py ← {e}")
        all_ok = False

# cross_arb (debe importar pero estar deshabilitado)
try:
    import cross_exchange_arb as _ca
    disabled = getattr(_ca, "DISABLE_CROSS_ARB", None)
    enabled  = getattr(_ca, "CROSS_ARB_ENABLED", None)
    status   = "DESHABILITADO ✅" if disabled else "⚠️ ACTIVO"
    print(f"  [OK]  cross_exchange_arb.py — DISABLE_CROSS_ARB={disabled} CROSS_ARB_ENABLED={enabled} → {status}")
except Exception as e:
    print(f"  [FAIL] cross_exchange_arb.py ← {e}")

print()

# ── 4. Healthcheck endpoint ───────────────────────────────────────────────────
print("── HEALTHCHECK ──")
port = os.getenv("PORT", "8080")
print(f"  Puerto esperado: {port}")
print(f"  Endpoint:        http://localhost:{port}/health")
print()

# ── 5. Resumen ────────────────────────────────────────────────────────────────
if all_ok:
    print("✅ DIAGNÓSTICO OK — El bot debería deployar correctamente")
else:
    print("❌ HAY ERRORES — Revisa los ítems marcados con [MISSING] o [FAIL]")
