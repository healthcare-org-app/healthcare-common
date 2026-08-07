"""Audit event publisher.

Every service emits audit events to Kafka topic `audit.event`. The
`audit-log-service` is the sole consumer and durably persists them.

    emit_audit(bus, action="patient.read", actor=g.principal["sub"],
               target=f"patient:{pid}", details={"fields": ["dob"]})
"""
from __future__ import annotations

from typing import Any, Optional

from .events import EventBus

AUDIT_TOPIC = "audit.event"


def emit_audit(
    bus: EventBus,
    *,
    action: str,
    actor: str,
    target: Optional[str] = None,
    details: Optional[dict[str, Any]] = None,
    outcome: str = "success",
) -> None:
    """Publish an audit event. Never raises — audit must not break primary flows."""
    try:
        bus.publish(
            AUDIT_TOPIC,
            key=actor,
            value={
                "action": action,
                "actor": actor,
                "target": target,
                "outcome": outcome,
                "details": details or {},
            },
        )
    except Exception:
        # Best-effort. Errors are logged by the producer.
        pass
