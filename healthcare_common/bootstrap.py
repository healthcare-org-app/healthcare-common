"""One-line service factory.

Every service's `app/main.py` starts with:

    from healthcare_common.bootstrap import create_service

    svc = create_service("patients-service")
    app, bus, db = svc.app, svc.bus, svc.db

`create_service`:
  1. Reads `service.yaml` from the service's working directory to discover
     the port, http_deps, publishes, subscribes.
  2. Creates a Flask app with the request-id middleware installed.
  3. Builds an `EventBus` (Kafka producer + consumer thread).
  4. Builds a `DBPool` from DATABASE_URL.
  5. Pre-instantiates a `ServiceClient` per HTTP dependency, accessible via
     `svc.clients["identity-service"]`.
  6. Registers `/health` and `/ready` endpoints.

The service is responsible for:
  - importing/registering blueprints on `svc.app`,
  - calling `svc.bus.on(...)` to register consumers before `svc.start()`,
  - calling `svc.start()` right before `svc.app.run()`.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from flask import Flask, jsonify

from .db import DBPool, db_pool
from .events import EventBus
from .http import ServiceClient
from .tracing import request_id_middleware

log = logging.getLogger("healthcare_common.bootstrap")


@dataclass
class Service:
    name: str
    app: Flask
    bus: EventBus
    db: DBPool
    clients: dict[str, ServiceClient] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def start(self) -> None:
        """Start the Kafka consumer thread (call once, right before app.run)."""
        self.bus.start()

    def run(self, *, host: str = "0.0.0.0") -> None:
        port = int(self.config.get("port") or os.environ.get("PORT") or 8000)
        self.start()
        self.app.run(host=host, port=port, threaded=True, debug=False)


def _load_service_yaml(service_dir: Path, service_name: str) -> dict:
    candidate = service_dir / "service.yaml"
    if candidate.exists():
        with candidate.open() as f:
            return yaml.safe_load(f) or {}
    log.warning("no service.yaml found at %s; using defaults", candidate)
    return {"name": service_name}


def _register_health(app: Flask, bus: EventBus, db: DBPool, name: str) -> None:
    @app.get("/health")
    def _health():
        return jsonify({"status": "ok", "service": name})

    @app.get("/ready")
    def _ready():
        problems: list[str] = []
        try:
            db.query("SELECT 1")
        except Exception as e:
            problems.append(f"db: {e}")
        # Kafka readiness is looser: producer is created lazily.
        return (jsonify({"status": "ok" if not problems else "degraded",
                         "service": name, "problems": problems}),
                200 if not problems else 503)


def create_service(name: str, *, service_dir: Optional[Path] = None,
                   configure_logging: bool = True) -> Service:
    if configure_logging:
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", "INFO"),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

    service_dir = service_dir or Path.cwd()
    cfg = _load_service_yaml(service_dir, name)

    app = Flask(name)
    request_id_middleware(app)

    bus = EventBus(service_name=name)
    db = db_pool()

    clients: dict[str, ServiceClient] = {
        dep: ServiceClient(dep) for dep in cfg.get("http_deps") or []
    }

    _register_health(app, bus, db, name)

    return Service(name=name, app=app, bus=bus, db=db, clients=clients, config=cfg)
