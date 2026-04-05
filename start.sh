#!/bin/bash
# start.sh — Whale Follower Bot VPS Singapore
# Uso: bash start.sh

set -e

cd /root/whale-follower

echo "=== Whale Follower Bot — Iniciando ==="
echo "Directorio: $(pwd)"
echo "Hora: $(date)"

# 1. Pull últimos cambios de GitHub
echo "[1/4] git pull..."
git pull origin main

# 2. Instalar/actualizar dependencias
echo "[2/4] pip install requirements..."
pip install -r requirements.txt --break-system-packages --quiet 2>&1 | tail -5

# 3. Matar proceso anterior si existe
echo "[3/4] Deteniendo bot anterior..."
pkill -f "python3 main.py" || true
pkill -f "python main.py" || true
sleep 2

# 4. Iniciar bot
echo "[4/4] Iniciando bot..."
nohup python3 main.py > bot.log 2>&1 &
BOT_PID=$!
echo "Bot iniciado PID=$BOT_PID"
echo $BOT_PID > /tmp/whale_bot.pid

# Tail del log
sleep 3
echo "=== Log (Ctrl+C para salir — el bot seguirá corriendo) ==="
tail -f bot.log
