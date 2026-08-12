"""Kafka producer + consumer wrapper.

The `EventBus` is the single object every service uses to talk to Kafka.

    bus = EventBus(service_name="patients-service")

    # publish
    bus.publish("patient.created", key=str(patient["id"]), value=patient)

    # subscribe (one handler per topic)
    @bus.on("identity.user.created")
    def link_identity(event):
        ...

    bus.start()   # spawns consumer threads
    bus.stop()    # graceful drain (registered as atexit in bootstrap.create_service)

Envelope schema (every message):

    {
        "id": "<uuid>",
        "event_type": "patient.created",
        "occurred_at": "2026-08-07T12:00:00Z",
        "producer": "patients-service",
        "request_id": "<correlation id from tracing>",
        "data": {...}
    }
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, Producer

from .tracing import current_request_id

log = logging.getLogger("healthcare_common.events")

Handler = Callable[[dict], None]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _brokers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092")


def _kafka_auth_config() -> dict:
    """Add SASL_SSL config when KAFKA_SASL_USERNAME is present.

    Set to talk to Upstash Kafka / Confluent Cloud / anything using SASL:
      KAFKA_BOOTSTRAP=cluster.upstash.io:9092
      KAFKA_SASL_USERNAME=...
      KAFKA_SASL_PASSWORD=...
      KAFKA_SASL_MECHANISM=SCRAM-SHA-256   # optional; SCRAM-SHA-256 default
    """
    user = os.environ.get("KAFKA_SASL_USERNAME")
    if not user:
        return {}
    return {
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": user,
        "sasl.password": os.environ.get("KAFKA_SASL_PASSWORD", ""),
    }


class EventBus:
    """One-per-service Kafka wrapper. Thread-safe for `publish`."""

    def __init__(self, service_name: str) -> None:
        self.service_name = service_name
        self._producer = Producer({
            "bootstrap.servers": _brokers(),
            "client.id": f"{service_name}-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
            "retries": 5,
            **_kafka_auth_config(),
        })
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
            "data": value,
        }
        payload = json.dumps(envelope, default=str).encode("utf-8")
        self._producer.produce(topic, key=(key.encode() if key else None), value=payload,
                               on_delivery=self._on_delivery)
        # Poll non-blocking to trigger delivery callbacks; a periodic flush happens
        # via the background flusher started in `start()`.
        self._producer.poll(0)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            log.error("kafka delivery failed topic=%s: %s", msg.topic(), err)

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
        """Spawn the consumer thread and register a shutdown hook."""
        if not self._handlers:
            log.info("%s: no event handlers, skipping consumer", self.service_name)
        else:
            self._consumer_thread = threading.Thread(
                target=self._consume_loop, name=f"{self.service_name}-consumer", daemon=True
            )
            self._consumer_thread.start()
        threading.Thread(target=self._flush_loop, name=f"{self.service_name}-flusher", daemon=True).start()
        atexit.register(self.stop)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_flag.set()
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=timeout)
        try:
            self._producer.flush(timeout)
        except Exception as e:
            log.warning("producer flush failed: %s", e)

    # ── internals ────────────────────────────────────────────────

    def _flush_loop(self) -> None:
        while not self._stop_flag.wait(1.0):
            try:
                self._producer.poll(0)
            except Exception:
                pass

    def _consume_loop(self) -> None:
        consumer = Consumer({
            "bootstrap.servers": _brokers(),
            "group.id": self.service_name,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            **_kafka_auth_config(),
        })
        consumer.subscribe(list(self._handlers.keys()))
        log.info("%s: subscribed to %s", self.service_name, list(self._handlers.keys()))

        try:
            while not self._stop_flag.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("consumer error: %s", msg.error())
                    continue
                topic = msg.topic()
                try:
                    envelope = json.loads(msg.value())
                    handler = self._handlers[topic]
                    handler(envelope)
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as e:
                    log.exception("handler %s failed for topic %s: %s", topic, topic, e)
                    # commit anyway to avoid poison-pill loops; a real system
                    # would DLQ. That's a future task in `libs/`.
                    consumer.commit(message=msg, asynchronous=False)
                    # backoff briefly to avoid tight-loop on repeated failures
                    time.sleep(0.5)
        finally:
            consumer.close()
