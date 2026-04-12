#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
# deploy_contabo.sh — Whale Follower Bot → Contabo VPS
#
# Uso desde tu máquina local:
#   ssh root@147.93.159.82 'bash -s' < deploy_contabo.sh
#
# O copiar al VPS y ejecutar:
#   scp deploy_contabo.sh root@147.93.159.82:~/
#   ssh root@147.93.159.82 'chmod +x ~/deploy_contabo.sh && ~/deploy_contabo.sh'
# ═══════════════════════════════════════════════════════════════════

set -e
echo "═══════════════════════════════════════════"
echo " Whale Follower Bot — Deploy Contabo VPS"
echo "═══════════════════════════════════════════"

# ── 1. Dependencias del sistema ──────────────────────────────────
echo ""
echo "[1/7] Instalando dependencias del sistema..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv git nodejs npm > /dev/null 2>&1
npm install -g pm2 > /dev/null 2>&1
echo "  Python: $(python3 --version)"
echo "  PM2: $(pm2 --version)"

# ── 2. Clonar repositorio ───────────────────────────────────────
echo ""
echo "[2/7] Clonando repositorio..."
cd /root
if [ -d "whale-follower" ]; then
    echo "  Directorio existe — haciendo pull..."
    cd whale-follower
    git pull origin main
else
    git clone https://github.com/gcobras75-stack/whale-follower.git
    cd whale-follower
fi
echo "  Commit: $(git log --oneline -1)"

# ── 3. Entorno virtual + dependencias Python ────────────────────
echo ""
echo "[3/7] Instalando dependencias Python..."
python3 -m venv venv 2>/dev/null || true
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q 2>/dev/null || {
    echo "  requirements.txt no encontrado, instalando paquetes base..."
    pip install aiohttp websockets loguru supabase python-dotenv xgboost numpy -q
}
echo "  Paquetes instalados OK"

# ── 4. Crear .env ────────────────────────────────────────────────
echo ""
echo "[4/7] Configurando .env..."
cat > .env << 'ENVEOF'
# ── Telegram ─────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN=8620020996:AAFL6r5zhlA7mdfwXlc2jgxIPna3aLT2pa4
TELEGRAM_CHAT_ID=5985169283

# ── Supabase ─────────────────────────────────────────────────────
SUPABASE_URL=https://dunlkxbcsxzwnencrklx.supabase.co
SUPABASE_KEY=PENDIENTE_AGREGAR_MANUALMENTE

# ── Bybit (IP fija en Contabo — puede funcionar) ────────────────
BYBIT_API_KEY=FvuTfWOpEDEIf6lELK
BYBIT_API_SECRET=IrYqWBtNvzQ7S5TQRJm6jBXtXE68iMzqNqfp
BYBIT_ENABLED=true
BYBIT_ORDERS_BLOCKED=false
PRODUCTION=true
REAL_CAPITAL=90

# ── OKX ──────────────────────────────────────────────────────────
OKX_API_KEY=0faf49ea-ccde-47e4-89eb-7c347e469b1a
OKX_SECRET=11D03B681A925119042B5D9808C333C7
OKX_PASSPHRASE=Automatia2026$

# ── MEXC ─────────────────────────────────────────────────────────
MEXC_API_KEY=mx0vglbRXpteALmday
MEXC_API_SECRET=dca73459b8854508a2ab83ec625976aa

# ── Trading ──────────────────────────────────────────────────────
SIGNAL_SCORE_THRESHOLD=55
PREFERRED_PAIR=ETHUSDT
MAX_ORDER_PCT_CAPITAL=0.35
MAX_LEVERAGE=1
MAX_TRADES_OPEN=2
RISK_PER_TRADE=0.01
DAILY_LOSS_LIMIT=0.05
RISK_REWARD=2.0
MIN_TRADE_SIZE_USD=10
MAX_TRADE_SIZE_USD=50
ENVEOF

echo "  .env creado — EDITAR SUPABASE_KEY manualmente:"
echo "  nano /root/whale-follower/.env"

# ── 5. Test rápido de imports ────────────────────────────────────
echo ""
echo "[5/7] Verificando imports..."
python3 -c "
import sys
ok = 0
for mod in ['aiohttp', 'websockets', 'loguru', 'dotenv']:
    try:
        __import__(mod)
        ok += 1
    except ImportError:
        print(f'  FALTA: {mod}')
print(f'  {ok}/4 módulos OK')
" 2>/dev/null

# ── 6. Configurar PM2 ───────────────────────────────────────────
echo ""
echo "[6/7] Configurando PM2..."

# Detener instancia anterior si existe
pm2 delete whale-follower 2>/dev/null || true

# Crear ecosystem file
cat > ecosystem.config.js << 'PM2EOF'
module.exports = {
  apps: [{
    name: 'whale-follower',
    script: 'venv/bin/python3',
    args: 'main.py',
    cwd: '/root/whale-follower',
    interpreter: 'none',
    autorestart: true,
    max_restarts: 10,
    restart_delay: 30000,
    env: {
      PYTHONUNBUFFERED: '1',
    },
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    error_file: '/root/whale-follower/logs/error.log',
    out_file: '/root/whale-follower/logs/out.log',
    merge_logs: true,
  }]
};
PM2EOF

mkdir -p logs
pm2 start ecosystem.config.js
pm2 save
pm2 startup systemd -u root --hp /root 2>/dev/null || true

echo "  PM2 configurado — auto-restart on reboot"

# ── 7. Verificar ─────────────────────────────────────────────────
echo ""
echo "[7/7] Verificando..."
sleep 5
pm2 list
echo ""
echo "═══════════════════════════════════════════"
echo " Deploy completado!"
echo ""
echo " Comandos útiles:"
echo "   pm2 logs whale-follower --lines 50"
echo "   pm2 restart whale-follower"
echo "   pm2 stop whale-follower"
echo ""
echo " IMPORTANTE:"
echo "   1. Editar SUPABASE_KEY en .env:"
echo "      nano /root/whale-follower/.env"
echo "   2. Agregar IP del VPS a Bybit whitelist:"
echo "      IP: 147.93.159.82"
echo "   3. Después de editar .env:"
echo "      pm2 restart whale-follower"
echo "═══════════════════════════════════════════"
