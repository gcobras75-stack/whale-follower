"""
apply_best_params.py — Whale Follower Bot
Aplica los parámetros optimizados al archivo .env.
Hace commit a git con el resumen de resultados.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ENV_FILE = Path(__file__).parent / ".env"

# Mapa param → nombre en .env
PARAM_TO_ENV = {
    "SIGNAL_SCORE_THRESHOLD": "SIGNAL_SCORE_THRESHOLD",
    "SPRING_DROP_PCT":        "SPRING_DROP_PCT",
    "SPRING_BOUNCE_PCT":      "SPRING_BOUNCE_PCT",
    "VOLUME_MULTIPLIER":      "VOLUME_MULTIPLIER",
    "STOP_LOSS_PCT":          "STOP_LOSS_PCT",
}


def _format_value(key: str, value) -> str:
    if isinstance(value, float):
        # Mantener 4 decimales para los porcentajes
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(int(value))


def apply_params(
    best_params:    dict,
    win_rate:       float = 0.0,
    profit_factor:  float = 0.0,
    total_trades:   int   = 0,
) -> None:
    """
    Actualiza .env con los mejores parámetros y hace git commit.
    """
    if not ENV_FILE.exists():
        print(f"[apply] AVISO: No se encontró {ENV_FILE}")
        return

    env_content = ENV_FILE.read_text(encoding="utf-8")
    changed = []

    for param_key, env_key in PARAM_TO_ENV.items():
        if param_key not in best_params:
            continue

        value   = best_params[param_key]
        val_str = _format_value(param_key, value)
        pattern = rf"^({env_key}=).*$"
        new_line = f"{env_key}={val_str}"

        if re.search(pattern, env_content, re.MULTILINE):
            new_content = re.sub(pattern, new_line, env_content, flags=re.MULTILINE)
            if new_content != env_content:
                changed.append(f"{env_key}={val_str}")
                env_content = new_content
        else:
            env_content += f"\n{new_line}"
            changed.append(f"{env_key}={val_str}")

    if not changed:
        print("[apply] Sin cambios en .env (parámetros ya actualizados).")
        return

    ENV_FILE.write_text(env_content, encoding="utf-8")
    print(f"[apply] .env actualizado: {', '.join(changed)}")

    # ── Git commit ────────────────────────────────────────────────────────────
    commit_msg = (
        f"Auto-calibracion: WR={win_rate:.0%} PF={profit_factor:.2f} "
        f"trades={total_trades} — parametros optimizados"
    )
    repo_dir = Path(__file__).parent

    try:
        subprocess.run(
            ["git", "add", str(ENV_FILE)],
            cwd=repo_dir, capture_output=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=repo_dir, capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"[apply] Commit creado: '{commit_msg}'")
        else:
            print(f"[apply] Sin cambios pendientes para commit.")
    except FileNotFoundError:
        print("[apply] git no disponible — actualizacion solo en .env.")
    except Exception as exc:
        print(f"[apply] Error en git (ignorado): {exc}")
