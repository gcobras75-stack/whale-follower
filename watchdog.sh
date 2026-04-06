#!/bin/bash
# watchdog.sh — Whale Follower Bot VPS Singapore
# Monitorea el proceso y lo reinicia si muere.
# Instalar en crontab para auto-recuperación:
#   crontab -e
#   */5 * * * * /bin/bash /root/whale-follower/watchdog.sh >> /tmp/watchdog.log 2>&1
#   @reboot sleep 30 && /bin/bash /root/whale-follower/start.sh >> /tmp/bot.log 2>&1

BOT_DIR="/root/whale-follower"
LOG_FILE="/tmp/bot.log"
PID_FILE="/tmp/whale_bot.pid"
MAX_LOG_MB=50

# ── Rotar log si supera MAX_LOG_MB ──────────────────────────────────────────
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE_MB=$(du -m "$LOG_FILE" 2>/dev/null | cut -f1)
    if [ "${LOG_SIZE_MB:-0}" -ge "$MAX_LOG_MB" ]; then
        mv "$LOG_FILE" "${LOG_FILE}.bak"
        echo "$(date) [watchdog] Log rotado (${LOG_SIZE_MB}MB)" > "$LOG_FILE"
    fi
fi

# ── Verificar si el bot está corriendo ───────────────────────────────────────
BOT_RUNNING=false

if [ -f "$PID_FILE" ]; then
    STORED_PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$STORED_PID" ] && kill -0 "$STORED_PID" 2>/dev/null; then
        BOT_RUNNING=true
    fi
fi

# Doble verificación por nombre de proceso
if pgrep -f "python3 main.py" > /dev/null 2>&1; then
    BOT_RUNNING=true
fi

# ── Reiniciar si no está corriendo ───────────────────────────────────────────
if [ "$BOT_RUNNING" = false ]; then
    echo "$(date) [watchdog] Bot no encontrado — reiniciando..."
    cd "$BOT_DIR"

    # Pull código actualizado (silencioso)
    git pull origin main > /dev/null 2>&1 || true

    # Reiniciar
    nohup python3 main.py >> "$LOG_FILE" 2>&1 &
    NEW_PID=$!
    echo $NEW_PID > "$PID_FILE"
    echo "$(date) [watchdog] Bot reiniciado PID=$NEW_PID"
else
    # Bot corriendo — log silencioso cada hora (min 0)
    MINUTE=$(date +%M)
    if [ "$MINUTE" = "00" ]; then
        echo "$(date) [watchdog] Bot OK (PID=$(cat $PID_FILE 2>/dev/null || echo '?'))"
    fi
fi
