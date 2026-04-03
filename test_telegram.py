"""
test_telegram.py — Whale Follower Bot
Verifica que el bot de Telegram puede enviar mensajes al chat configurado.

Uso:
    python test_telegram.py
"""
import sys
import io
import urllib.request
import urllib.parse
import urllib.error
import json

# Forzar UTF-8 en la salida estándar (necesario en Windows con cp1252)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

try:
    import config
except KeyError as e:
    print(f"[ERROR] Falta variable de entorno {e}")
    print("   Asegurate de tener TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID en .env")
    sys.exit(1)


def run_test() -> None:
    token   = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID

    print("Verificando bot token con Telegram API...")

    # ── 1. Verificar que el token es válido (getMe) ───────────────────────────
    url_me = f"https://api.telegram.org/bot{token}/getMe"
    try:
        with urllib.request.urlopen(url_me, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a Telegram API: {e}")
        print("   Verifica que TELEGRAM_BOT_TOKEN es correcto.")
        sys.exit(1)

    if not data.get("ok"):
        print(f"[ERROR] Token invalido: {data.get('description')}")
        sys.exit(1)

    bot_username = data["result"]["username"]
    print(f"   Bot verificado: @{bot_username}")

    # ── 2. Enviar mensaje de prueba ───────────────────────────────────────────
    message = (
        "🐋 *Whale Follower Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ Conexión exitosa\n"
        "Listo para detectar ballenas."
    )

    payload = json.dumps({
        "chat_id":    chat_id,
        "text":       message,
        "parse_mode": "Markdown",
    }).encode("utf-8")

    url_send = f"https://api.telegram.org/bot{token}/sendMessage"
    req = urllib.request.Request(
        url_send,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    print(f"Enviando mensaje de prueba al chat {chat_id}...")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[ERROR] HTTP {e.code}: {body}")
        print("   Verifica que TELEGRAM_CHAT_ID es correcto y que enviaste /start al bot.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    if not result.get("ok"):
        print(f"[ERROR] {result.get('description')}")
        sys.exit(1)

    msg_id = result["result"]["message_id"]
    print(f"   Mensaje enviado correctamente (message_id={msg_id})")
    print()
    print("✅ Telegram conectado correctamente")
    print(f"   Bot: @{bot_username}")
    print(f"   Chat ID: {chat_id}")


if __name__ == "__main__":
    try:
        run_test()
    except Exception as e:
        print(f"[ERROR] Inesperado: {e}")
        sys.exit(1)
