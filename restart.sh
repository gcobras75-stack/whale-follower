#!/bin/bash
# restart.sh — Whale Follower Bot VPS Singapore
# Uso: bash restart.sh

cd /root/whale-follower

echo "=== Whale Follower Bot — Reiniciando ==="
echo "Hora: $(date)"

# Detener proceso actual
echo "[1/2] Deteniendo bot..."
pkill -f "python3 main.py" || true
pkill -f "python main.py" || true
sleep 2

# Iniciar de nuevo
echo "[2/2] Iniciando bot..."
nohup python3 main.py > bot.log 2>&1 &
BOT_PID=$!
echo "Bot reiniciado PID=$BOT_PID"
echo $BOT_PID > /tmp/whale_bot.pid
