"""Correlation-id propagation across HTTP + Kafka.

Every inbound HTTP request either carries an `X-Request-ID` header or gets one
assigned. That id is stashed in Flask's `g` for the duration of the request,
attached to outbound HTTP calls (see `http.ServiceClient`), and included in
every Kafka event we publish. Downstream services do the same, so an id
traversing five hops still lands in every log line.
"""
from __future__ import annotations

import uuid
from typing import Optional

from flask import g, request

HEADER = "X-Request-ID"


def _new_id() -> str:
    return uuid.uuid4().hex


def request_id_middleware(app):
    """Register before/after hooks that carry the request id through the app."""

    @app.before_request
    def _attach():
        rid = request.headers.get(HEADER) or _new_id()
        g.request_id = rid

    @app.after_request
    def _echo(response):
        rid = getattr(g, "request_id", None)
        if rid:
            response.headers[HEADER] = rid
        return response

    return app


def current_request_id() -> Optional[str]:
    """Return the request id for the current request, or None outside a request context."""
    try:
        return getattr(g, "request_id", None)
    except RuntimeError:
        return None
