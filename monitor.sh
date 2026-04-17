#!/bin/bash
# monitor.sh — Whale Follower Bot health check
# Corre cada 3 horas via cron: 0 */3 * * * /bin/bash /root/whale-follower/monitor.sh
# Log: /root/monitor.log

LOGFILE="/root/monitor.log"
BOTNAME="whale-follower"
MAX_DISCONNECTS=5
MAX_LOG_LINES=100

LOG=$(pm2 logs "$BOTNAME" --lines $MAX_LOG_LINES --nostream 2>&1)
DISCONNECTED=$(echo "$LOG" | grep -c "Disconnected")
SIGNALS=$(echo "$LOG" | grep -c "SIGNAL #")
RECHAZADOS=$(echo "$LOG" | grep -c "RECHAZADO")
ERRORS=$(echo "$LOG" | grep -c -iE "Traceback|CRITICAL|OOM|MemoryError")
OKX_OK=$(echo "$LOG" | grep -c "\[okx\] Connected")
BYBIT_OK=$(echo "$LOG" | grep -c "\[bybit\] Connected")

NOW=$(date '+%Y-%m-%d %H:%M:%S')

# 1. Reiniciar si demasiadas desconexiones
if [ "$DISCONNECTED" -gt "$MAX_DISCONNECTS" ]; then
    pm2 restart "$BOTNAME"
    echo "$NOW: RESTART — $DISCONNECTED desconexiones en ultimas $MAX_LOG_LINES lineas" >> "$LOGFILE"
fi

# 2. Reiniciar si errores criticos
if [ "$ERRORS" -gt 0 ]; then
    pm2 restart "$BOTNAME"
    echo "$NOW: RESTART — $ERRORS errores criticos detectados" >> "$LOGFILE"
fi

# 3. Verificar que el proceso existe
if ! pm2 pid "$BOTNAME" | grep -q "[0-9]"; then
    pm2 start /root/whale-follower/main.py --name "$BOTNAME" --interpreter python3
    echo "$NOW: RESTART — proceso no encontrado, reiniciado" >> "$LOGFILE"
fi

# 4. Log de estado
echo "$NOW: signals=$SIGNALS rechazados=$RECHAZADOS disconnects=$DISCONNECTED okx=$OKX_OK bybit=$BYBIT_OK" >> "$LOGFILE"

# 5. Mantener log limpio (max 500 lineas)
if [ -f "$LOGFILE" ] && [ "$(wc -l < "$LOGFILE")" -gt 500 ]; then
    tail -200 "$LOGFILE" > "${LOGFILE}.tmp" && mv "${LOGFILE}.tmp" "$LOGFILE"
fi
