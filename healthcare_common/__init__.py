"""Shared runtime for healthcare-org services."""
from .http import ServiceClient, ServiceUnavailable
from .events import EventBus
from .auth import verify_jwt, require_auth
from .tracing import request_id_middleware, current_request_id
from .db import db_pool, DBPool
from .audit import emit_audit
from .bootstrap import create_service, Service

__version__ = "0.1.0"

__all__ = [
    "ServiceClient", "ServiceUnavailable",
    "EventBus",
    "verify_jwt", "require_auth",
    "request_id_middleware", "current_request_id",
    "db_pool", "DBPool",
    "emit_audit",
    "create_service", "Service",
]
