"""Access-token authentication.

The dashboard can submit and cancel jobs as the user who runs it, and login
nodes are shared: any other user on the node can reach a port bound to
127.0.0.1. Every request therefore needs a secret token.

Flow: the startup message prints a URL containing the token. Opening it stores
the token in an HttpOnly cookie and redirects to a clean URL. API clients can
send ``Authorization: Bearer <token>`` instead.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
from pathlib import Path
from typing import Optional

from flask import Flask, Response, jsonify, redirect, request

logger = logging.getLogger(__name__)

CSRF_HEADER = "X-Slurm-Dashboard"
UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

LOGIN_PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Slurm Dashboard</title>
<style>
body{font-family:system-ui,sans-serif;max-width:34rem;margin:15vh auto;padding:0 1.5rem;
color:#1f2933;line-height:1.5}
code{background:#eef1f4;padding:.1rem .35rem;border-radius:4px}
input{width:100%;padding:.55rem;font:inherit;box-sizing:border-box;margin:.75rem 0}
button{padding:.5rem 1rem;font:inherit;cursor:pointer}
@media (prefers-color-scheme:dark){body{background:#14181d;color:#e4e7eb}
code,input{background:#232a32;color:inherit;border:1px solid #3a444f}}
</style></head><body>
<h1>Access token required</h1>
<p>Open the link that <code>slurm-dashboard</code> printed when it started, or paste
the token here. The token is also stored in <code>{token_file}</code> on the host
that runs the dashboard.</p>
<form method="get" action="/"><input name="token" placeholder="Token" autofocus
autocomplete="off"><button type="submit">Open dashboard</button></form>
</body></html>"""


def load_or_create_token(state_dir: Path, explicit: Optional[str] = None) -> str:
    """Return the access token, creating and persisting one on first run.

    Persisting keeps bookmarked URLs valid across restarts. The file is only
    readable by its owner.
    """
    explicit = explicit or os.environ.get("SLURM_DASHBOARD_TOKEN")
    if explicit:
        return explicit
    token_file = state_dir / "token"
    try:
        token = token_file.read_text().strip()
        if len(token) >= 32:
            return token
    except OSError:
        pass
    token = secrets.token_urlsafe(32)
    try:
        state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(str(token_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write(token + "\n")
    except OSError as exc:
        logger.warning(
            "Could not store the token in %s (%s); it changes on restart", token_file, exc
        )
    return token


def install_auth(app: Flask, token: Optional[str], token_file: Path) -> None:
    """Require the token on every request and guard against cross-site writes.

    Args:
        app: Flask application.
        token: The secret, or None to disable authentication (--no-auth).
        token_file: Shown on the login page so users know where to find it.
    """
    cookie_name = "slurmdash_" + hashlib.sha256((token or "").encode()).hexdigest()[:10]

    def token_ok(candidate: Optional[str]) -> bool:
        return bool(candidate) and hmac.compare_digest(candidate.encode(), (token or "").encode())

    @app.before_request
    def guard() -> Optional[Response]:
        if request.method in UNSAFE_METHODS:
            # Browsers cannot attach a custom header cross-origin without a CORS
            # preflight, which this server never approves. Cookies alone are not
            # enough: every port on localhost counts as the same site.
            if request.headers.get(CSRF_HEADER) != "1":
                return _deny("Missing request header", 403)
            origin = request.headers.get("Origin")
            if origin and origin.split("://", 1)[-1] != request.host:
                return _deny("Cross-origin request refused", 403)

        if token is None:
            return None

        supplied = request.args.get("token")
        if supplied is not None and request.method == "GET" and not request.path.startswith("/api"):
            if not token_ok(supplied):
                return _login(token_file, 401)
            response = redirect(request.path or "/")
            response.set_cookie(
                cookie_name, token, httponly=True, samesite="Strict", max_age=90 * 86400
            )
            return response

        bearer = request.headers.get("Authorization", "")
        if bearer.startswith("Bearer ") and token_ok(bearer[7:].strip()):
            return None
        if token_ok(request.cookies.get(cookie_name)):
            return None
        if request.path.startswith("/api") or request.path.startswith("/stream"):
            return _deny("Not authenticated. Open the URL printed by slurm-dashboard.", 401)
        return _login(token_file, 401)

    @app.after_request
    def harden(response: Response) -> Response:
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        return response


def _deny(message: str, status: int) -> Response:
    response = jsonify({"error": message})
    response.status_code = status
    return response


def _login(token_file: Path, status: int) -> Response:
    page = LOGIN_PAGE.replace("{token_file}", str(token_file))
    return Response(page, status=status, mimetype="text/html")
