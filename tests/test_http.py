"""Smoke tests for ServiceClient — retries, breaker, request-id propagation."""
from __future__ import annotations

import os
import time

import pytest
import responses

from healthcare_common.http import ServiceClient, ServiceUnavailable, _breaker_for


@pytest.fixture(autouse=True)
def _configure_env(monkeypatch):
    monkeypatch.setenv("SERVICE_URL_TEST_SERVICE", "http://test-service.local")
    monkeypatch.setenv("SERVICE_HOST_MODE", "localhost")
    yield
    # Reset breaker state between tests to keep them independent.
    b = _breaker_for("test-service")
    b.failures = 0
    b.open_until = 0.0


@responses.activate
def test_get_ok():
    responses.add(responses.GET, "http://test-service.local/api/x",
                  json={"ok": True}, status=200)
    c = ServiceClient("test-service", max_retries=0)
    assert c.get("/api/x").json() == {"ok": True}


@responses.activate
def test_retry_on_5xx_then_success():
    responses.add(responses.GET, "http://test-service.local/api/x", status=500)
    responses.add(responses.GET, "http://test-service.local/api/x",
                  json={"ok": True}, status=200)
    c = ServiceClient("test-service", max_retries=2)
    resp = c.get("/api/x")
    assert resp.status_code == 200
    assert len(responses.calls) == 2


@responses.activate
def test_breaker_opens_after_repeated_failures():
    for _ in range(10):
        responses.add(responses.GET, "http://test-service.local/api/x", status=500)
    c = ServiceClient("test-service", max_retries=0)
    for _ in range(5):
        with pytest.raises(ServiceUnavailable):
            c.get("/api/x")
    # 6th call should fail-fast without hitting the network.
    calls_before = len(responses.calls)
    with pytest.raises(ServiceUnavailable, match="circuit open"):
        c.get("/api/x")
    assert len(responses.calls) == calls_before
