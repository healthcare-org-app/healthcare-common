"""Cross-service event bus.

This module used to wrap Kafka via confluent-kafka. It's now backed by
Postgres LISTEN/NOTIFY — same interface (`publish`, `on`, `start`, `stop`)
but no external broker required. The shared Postgres instance we already
have is the substrate.

Semantics vs Kafka:
  - Broadcast: every listening subscriber receives every notification. No
    consumer groups, no partitioning. Fits the healthcare-org shape (each
    service that cares about a topic runs one process that subscribes).
  - No persistence: a NOTIFY delivered while the subscriber is disconnected
    is lost. Acceptable for demo. If durability matters, layer an outbox
    table + polling on top.
  - Ordering: FIFO per publisher connection.
  - Payload limit: 8 kB per notification (Postgres hard limit). Envelopes
    exceeding 7.5 kB are replaced with a truncated stub and logged.

Channel names are derived from Kafka-style topic names:
    `patient.created`   ->   `evt_patient_created`
The `evt_` prefix keeps our channels namespaced away from any accidental
Postgres LISTEN clashes.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional

import psycopg

from .tracing import current_request_id

log = logging.getLogger("healthcare_common.events")

Handler = Callable[[dict], None]

MAX_PAYLOAD_BYTES = 7500  # PG hard limit is 8000; leave headroom for the envelope wrap


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _channel(topic: str) -> str:
    """Turn a Kafka-style topic into a Postgres channel identifier."""
    safe = re.sub(r"[^a-zA-Z0-9]", "_", topic).lower()
    return f"evt_{safe}"


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set — required for LISTEN/NOTIFY event bus")
    return dsn


class EventBus:
    """One-per-service Postgres LISTEN/NOTIFY bus. Thread-safe for `publish`."""

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        self._handlers: dict[str, Handler] = {}
        self._consumer_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    # ── publish ──────────────────────────────────────────────────

    def publish(self, topic: str, *, key: Optional[str], value: dict) -> None:
        envelope = {
            "id": uuid.uuid4().hex,
            "event_type": topic,
            "occurred_at": _now_iso(),
            "producer": self.service_name,
            "request_id": current_request_id(),
            "key": key,
            "data": value,
        }
        payload = json.dumps(envelope, default=str)
        if len(payload) > MAX_PAYLOAD_BYTES:
            log.warning(
                "event payload %d bytes exceeds NOTIFY limit for %s; publishing stub",
                len(payload), topic,
            )
            envelope["data"] = {"_truncated": True, "original_size": len(payload)}
            payload = json.dumps(envelope, default=str)

        try:
            # A fresh connection per publish keeps this thread-safe without a
            # dedicated pool. Publish is rare enough that connection overhead
            # doesn't matter for a demo.
            with psycopg.connect(_dsn(), autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_notify(%s, %s)", (_channel(topic), payload))
        except Exception as e:
            # Never propagate — mirrors the old Kafka producer's error-swallowing
            # so a broker (er, DB) hiccup doesn't crash the request path.
            log.error("publish failed topic=%s: %s", topic, e)

    # ── subscribe ────────────────────────────────────────────────

    def on(self, topic: str) -> Callable[[Handler], Handler]:
        """Register a handler for `topic`. One handler per topic per service."""
        def _decorator(fn: Handler) -> Handler:
            if topic in self._handlers:
                raise ValueError(f"handler already registered for {topic!r}")
            self._handlers[topic] = fn
            return fn
        return _decorator

    def start(self) -> None:
        """Spawn the consumer thread. No-op if no handlers registered."""
        if not self._handlers:
            log.info("%s: no event handlers, skipping consumer", self.service_name)
            return
        self._consumer_thread = threading.Thread(
            target=self._consume_loop,
            name=f"{self.service_name}-consumer",
            daemon=True,
        )
        self._consumer_thread.start()
        atexit.register(self.stop)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_flag.set()
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=timeout)

    # ── internals ────────────────────────────────────────────────

    def _consume_loop(self) -> None:
        channel_to_topic = {_channel(t): t for t in self._handlers}
        while not self._stop_flag.is_set():
            try:
                with psycopg.connect(_dsn(), autocommit=True) as conn:
                    with conn.cursor() as cur:
                        for topic in self._handlers:
                            cur.execute(f"LISTEN {_channel(topic)}")
                    log.info(
                        "%s: LISTENing on %d channel(s): %s",
                        self.service_name,
                        len(self._handlers),
                        list(channel_to_topic.keys()),
                    )
                    while not self._stop_flag.is_set():
                        # psycopg 3's notifies() yields Notify objects as they
                        # arrive; timeout returns control so we can check _stop.
                        for notify in conn.notifies(timeout=1.0):
                            topic = channel_to_topic.get(notify.channel)
                            if not topic:
                                continue
                            try:
                                envelope = json.loads(notify.payload)
                                self._handlers[topic](envelope)
                            except Exception:
                                log.exception("handler failed for topic=%s", topic)
            except Exception as e:
                if self._stop_flag.is_set():
                    return
                log.warning("%s: consumer connection lost; reconnecting: %s",
                            self.service_name, e)
                self._stop_flag.wait(2.0)
