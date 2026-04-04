"""
Crea un Railway Project Token (GraphQL) y lo sube como secret RAILWAY_TOKEN en GitHub.
Uso local: python scripts/sync_github_actions_railway_secret.py

El PAT de GitHub se obtiene de `git credential fill` (Credential Manager), no se imprime.
"""
from __future__ import annotations

import base64
import json
import re
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

from nacl import encoding, public

RAILWAY_CONFIG = Path(r"C:\Users\ECApro\.railway\config.json")
CREDENTIALS_TXT = Path(r"C:\Users\ECApro\mis-credenciales.txt")
GIT_EXE = Path(r"C:\Program Files\Git\bin\git.exe")
REPO = "gcobras75-stack/whale-follower"
PROJECT_ID = "d3f6357f-019c-4a69-bfe8-0870201fd18e"
ENVIRONMENT_ID = "3bf62174-af9c-4f4b-8885-a4c6dbac7e64"
RAILWAY_GQL = "https://backboard.railway.com/graphql/v2"
TOKEN_NAME_PREFIX = "gha-whale-follower"


def _github_pat_from_git(repo_root: Path) -> str:
    if not GIT_EXE.is_file():
        raise SystemExit(f"Git no encontrado en {GIT_EXE}")
    proc = subprocess.run(
        [str(GIT_EXE), "credential", "fill"],
        input=b"protocol=https\nhost=github.com\n\n",
        capture_output=True,
        timeout=60,
        cwd=str(repo_root),
    )
    text = proc.stdout.decode(errors="replace")
    m = re.search(r"^password=(.+)$", text, re.MULTILINE)
    if not m:
        raise SystemExit("No se pudo leer password= de git credential fill")
    return m.group(1).strip()


def _gql(bearer: str, query: str, variables: dict | None = None) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        RAILWAY_GQL,
        data=body,
        headers={
            "Authorization": f"Bearer {bearer}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) sync-actions-secret",
        },
        method="POST",
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
        return json.loads(r.read().decode())


def _encrypt_github_secret(public_key_b64: str, secret_value: str) -> str:
    pk = public.PublicKey(public_key_b64.encode("ascii"), encoding.Base64Encoder())
    box = public.SealedBox(pk)
    return base64.b64encode(box.encrypt(secret_value.encode("utf-8"))).decode("ascii")


def _github_api(method: str, url: str, github_pat: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {github_pat}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "whale-follower-setup",
        },
        method=method,
    )
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            raw = r.read().decode() or "{}"
            return r.status, json.loads(raw) if raw.strip() else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode(errors="replace")
        try:
            parsed = json.loads(err_body) if err_body.strip() else {}
        except json.JSONDecodeError:
            parsed = {"message": err_body[:500]}
        return e.code, parsed


def _append_railway_token_to_credentials(railway_token: str) -> None:
    marker = "=== RAILWAY — GitHub Actions (whale-follower production) ==="
    block = (
        f"\n\n{marker}\n"
        f"RAILWAY_PROJECT_TOKEN_WHALE_FOLLOWER={railway_token}\n"
        "# Project token (env production). Rotar si se expone.\n"
    )
    existing = CREDENTIALS_TXT.read_text(encoding="utf-8") if CREDENTIALS_TXT.is_file() else ""
    if "RAILWAY_PROJECT_TOKEN_WHALE_FOLLOWER=" in existing:
        out, n = re.subn(
            r"RAILWAY_PROJECT_TOKEN_WHALE_FOLLOWER=.*",
            f"RAILWAY_PROJECT_TOKEN_WHALE_FOLLOWER={railway_token}",
            existing,
            count=1,
        )
        if n:
            CREDENTIALS_TXT.write_text(out, encoding="utf-8")
            print("mis-credenciales.txt: RAILWAY_PROJECT_TOKEN_WHALE_FOLLOWER actualizado.")
            return
    CREDENTIALS_TXT.write_text(existing.rstrip() + block, encoding="utf-8")
    print("mis-credenciales.txt: bloque Railway añadido/actualizado.")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    github_pat = _github_pat_from_git(root)
    cfg = json.loads(RAILWAY_CONFIG.read_text(encoding="utf-8"))
    rw_session = cfg["user"]["accessToken"]

    from datetime import datetime, timezone

    name = f"{TOKEN_NAME_PREFIX}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    mut = """
    mutation ($input: ProjectTokenCreateInput!) {
      projectTokenCreate(input: $input)
    }
    """
    data = _gql(
        rw_session,
        mut,
        {
            "input": {
                "projectId": PROJECT_ID,
                "environmentId": ENVIRONMENT_ID,
                "name": name,
            }
        },
    )
    if data.get("errors"):
        print("Railway GraphQL errors:", data["errors"])
        raise SystemExit(1)
    project_token = data.get("data", {}).get("projectTokenCreate")
    if not project_token or not isinstance(project_token, str):
        print("Respuesta inesperada:", data)
        raise SystemExit(1)
    print(f"Railway project token creado (nombre={name}, len={len(project_token)}).")

    _append_railway_token_to_credentials(project_token)

    pk_url = f"https://api.github.com/repos/{REPO}/actions/secrets/public-key"
    status, key_json = _github_api("GET", pk_url, github_pat)
    if status != 200:
        print("GitHub public-key:", status, key_json)
        raise SystemExit(1)
    enc = _encrypt_github_secret(key_json["key"], project_token)
    put_url = f"https://api.github.com/repos/{REPO}/actions/secrets/RAILWAY_TOKEN"
    st2, body2 = _github_api(
        "PUT",
        put_url,
        github_pat,
        {"encrypted_value": enc, "key_id": key_json["key_id"]},
    )
    if st2 not in (201, 204):
        print("PUT RAILWAY_TOKEN:", st2, body2)
        raise SystemExit(1)
    print("GitHub secret RAILWAY_TOKEN configurado (201/204).")


if __name__ == "__main__":
    main()
