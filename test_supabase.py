"""
test_supabase.py — Whale Follower Bot
Verifica que la conexión a Supabase funciona correctamente:
  1. Inserta un registro de prueba en whale_signals
  2. Lo lee de vuelta
  3. Lo borra (limpieza)
  4. Imprime resultado claro

Uso:
    python test_supabase.py
"""
import sys
import io

# Forzar UTF-8 en la salida estándar (necesario en Windows con cp1252)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

try:
    import config
except KeyError as e:
    print(f"[ERROR] Falta variable de entorno {e}")
    print("   Asegurate de tener un archivo .env con SUPABASE_URL y SUPABASE_KEY")
    sys.exit(1)

from supabase import create_client


def run_test() -> None:
    # ── 1. Conectar ───────────────────────────────────────────────────────────
    print("Conectando a Supabase...")
    try:
        client = create_client(config.SUPABASE_URL, config.SUPABASE_KEY)
    except Exception as e:
        print(f"[ERROR] No se pudo crear cliente Supabase: {e}")
        sys.exit(1)

    # ── 2. Insertar registro de prueba ────────────────────────────────────────
    test_row = {
        "pair":               "TEST/USDT",
        "score":              99,
        "price_entry":        99999.99,
        "stop_loss":          99500.00,
        "take_profit":        101499.97,
        "conditions_met":     {"test": True, "cond_a": True, "cond_b": True},
        "strongest_exchange": "test_exchange",
        "cvd_velocity":       0.1234,
    }

    print("Insertando registro de prueba...")
    try:
        insert_resp = client.table("whale_signals").insert(test_row).execute()
    except Exception as e:
        print(f"[ERROR] Al insertar: {e}")
        print("   -> Ejecutaste supabase_migration.sql en el SQL Editor?")
        print("   -> El SQL actualizado incluye los GRANT necesarios.")
        sys.exit(1)

    if not insert_resp.data:
        print(f"[ERROR] La insercion no devolvio datos: {insert_resp}")
        sys.exit(1)

    inserted_id = insert_resp.data[0]["id"]
    print(f"   Insertado con id={inserted_id}")

    # ── 3. Leer de vuelta ─────────────────────────────────────────────────────
    print("Leyendo registro de vuelta...")
    try:
        select_resp = (
            client.table("whale_signals")
            .select("*")
            .eq("id", inserted_id)
            .execute()
        )
    except Exception as e:
        print(f"[ERROR] Al leer: {e}")
        sys.exit(1)

    if not select_resp.data:
        print("[ERROR] No se encontro el registro recien insertado")
        sys.exit(1)

    row = select_resp.data[0]
    assert row["pair"]  == "TEST/USDT",        "pair no coincide"
    assert row["score"] == 99,                 "score no coincide"
    assert row["strongest_exchange"] == "test_exchange", "exchange no coincide"
    print(f"   Leido correctamente: pair={row['pair']} score={row['score']}")

    # ── 4. Borrar (limpieza) ──────────────────────────────────────────────────
    print("Borrando registro de prueba...")
    try:
        client.table("whale_signals").delete().eq("id", inserted_id).execute()
    except Exception as e:
        print(f"[ERROR] Al borrar: {e}")
        print(f"   Borra manualmente el id={inserted_id} en el dashboard")
        sys.exit(1)

    print("   Borrado correctamente.")

    # ── 5. Resultado ──────────────────────────────────────────────────────────
    print()
    print("✅ Supabase conectado correctamente")
    print(f"   URL: {config.SUPABASE_URL}")
    print("   Tabla whale_signals: INSERT / SELECT / DELETE operativos")


if __name__ == "__main__":
    try:
        run_test()
    except AssertionError as e:
        print(f"[ERROR] Validacion: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Inesperado: {e}")
        sys.exit(1)
