#!/bin/bash
# restart.sh — Whale Follower Bot VPS Singapore
# Uso: bash restart.sh

cd /root/whale-follower

echo "=== Whale Follower Bot — Reiniciando ==="
echo "Hora: $(date)"

# 1. Pull código actualizado
echo "[1/4] git pull..."
git pull origin main 2>&1 || echo "[WARN] git pull falló — usando código existente"

# 2. Actualizar dependencias
echo "[2/4] pip install..."
pip install -r requirements.txt --break-system-packages --quiet 2>&1 | tail -3 || true

# 3. Detener proceso actual
echo "[3/4] Deteniendo bot..."
pkill -f "python3 main.py" 2>/dev/null || true
pkill -f "python main.py" 2>/dev/null || true
sleep 2

# 4. Iniciar de nuevo
echo "[4/4] Iniciando bot..."
nohup python3 main.py >> /tmp/bot.log 2>&1 &
BOT_PID=$!
echo "Bot reiniciado PID=$BOT_PID"
echo $BOT_PID > /tmp/whale_bot.pid
echo "Log: /tmp/bot.log"
tail -5 /tmp/bot.log
