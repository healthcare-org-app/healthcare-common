"""JWT verification.

Tokens are issued by `auth-service` (RS256, kid in header). In real deployments
we'd fetch the JWKS from `auth-service` and cache it. Here we verify against a
shared secret (dev) or a public key file (prod).

    from healthcare_common.auth import require_auth

    @app.get("/api/patients/<int:pid>")
    @require_auth(scopes=["patients.read"])
    def read(pid):
        ...

    # Inside the handler, `flask.g.principal` is the decoded token claims.
"""
from __future__ import annotations

import logging
import os
from functools import wraps
from typing import Optional

import jwt
from flask import g, jsonify, request

log = logging.getLogger("healthcare_common.auth")


def _secret() -> str:
    return os.environ.get("JWT_SECRET", "dev-secret-do-not-use-in-prod")


def _algorithm() -> str:
    return os.environ.get("JWT_ALGORITHM", "HS256")


def verify_jwt(token: str) -> Optional[dict]:
    """Return decoded claims or None if invalid."""
    try:
        return jwt.decode(token, _secret(), algorithms=[_algorithm()],
                          options={"require": ["exp", "sub"]})
    except jwt.PyJWTError as e:
        log.debug("jwt verification failed: %s", e)
        return None


def require_auth(scopes: Optional[list[str]] = None):
    """Flask decorator enforcing a valid JWT and (optionally) required scopes.

    Auth in dev can be bypassed by setting AUTH_DISABLED=1 — useful for tests
    and local smoke runs. Never set this in prod.
    """
    scopes = scopes or []

    def _decorator(fn):
        @wraps(fn)
        def _wrapped(*args, **kwargs):
            if os.environ.get("AUTH_DISABLED") == "1":
                g.principal = {"sub": "dev-user", "scopes": ["*"]}
                return fn(*args, **kwargs)

            header = request.headers.get("Authorization", "")
            if not header.startswith("Bearer "):
                return jsonify({"error": "missing bearer token"}), 401
            claims = verify_jwt(header[7:])
            if not claims:
                return jsonify({"error": "invalid token"}), 401

            token_scopes = set(claims.get("scopes", []))
            if scopes and "*" not in token_scopes and not set(scopes).issubset(token_scopes):
                return jsonify({"error": "insufficient scope",
                                "required": scopes,
                                "have": sorted(token_scopes)}), 403

            g.principal = claims
            return fn(*args, **kwargs)
        return _wrapped
    return _decorator
