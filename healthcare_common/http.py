"""Retryable, circuit-broken HTTP client for peer-service calls.

`ServiceClient` resolves peers by name using either the CONSUL registry (in
docker) or a static hostname map (in local dev). It:

  - retries on connection errors and 5xx up to `max_retries` with jitter,
  - opens a circuit breaker after N consecutive failures (fails fast until
    the cooldown elapses), and
  - propagates `X-Request-ID` from `tracing.current_request_id()`.

Usage:

    identity = ServiceClient("identity-service")
    user = identity.get(f"/api/users/{user_id}").json()
"""
from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Optional

import requests

from .tracing import HEADER as REQUEST_ID_HEADER, current_request_id

log = logging.getLogger("healthcare_common.http")


class ServiceUnavailable(RuntimeError):
    """Raised when a peer service is unreachable or has an open circuit."""


@dataclass
class _Breaker:
    fail_threshold: int = 5
    cooldown_s: float = 20.0
    failures: int = 0
    open_until: float = 0.0
    lock: Lock = field(default_factory=Lock)

    def allow(self) -> bool:
        with self.lock:
            return time.time() >= self.open_until

    def record_success(self) -> None:
        with self.lock:
            self.failures = 0
            self.open_until = 0.0

    def record_failure(self) -> None:
        with self.lock:
            self.failures += 1
            if self.failures >= self.fail_threshold:
                self.open_until = time.time() + self.cooldown_s


_BREAKERS: dict[str, _Breaker] = {}
_BREAKERS_LOCK = Lock()


def _breaker_for(name: str) -> _Breaker:
    with _BREAKERS_LOCK:
        b = _BREAKERS.get(name)
        if b is None:
            b = _Breaker()
            _BREAKERS[name] = b
        return b


def _resolve(service_name: str) -> str:
    """Return the base URL for `service_name`.

    Precedence:
      1. env var SERVICE_URL_<UPPER_UNDERSCORED_NAME>
      2. docker DNS: http://<service-name>:<port from env>
      3. localhost:<port from PORT_<UPPER_UNDERSCORED_NAME>>
    """
    key = service_name.upper().replace("-", "_")
    explicit = os.environ.get(f"SERVICE_URL_{key}")
    if explicit:
        return explicit.rstrip("/")
    port = os.environ.get(f"PORT_{key}")
    host = os.environ.get("SERVICE_HOST_MODE", "localhost")
    if host == "docker":
        return f"http://{service_name}:{port or 80}".rstrip("/")
    return f"http://localhost:{port or 80}".rstrip("/")


class ServiceClient:
    """One instance per peer service. Thread-safe."""

    def __init__(
        self,
        service_name: str,
        *,
        timeout: float = 5.0,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.name = service_name
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self._breaker = _breaker_for(service_name)

    def _url(self, path: str) -> str:
        return f"{_resolve(self.name)}{path if path.startswith('/') else '/' + path}"

    def _headers(self, extra: Optional[dict] = None) -> dict:
        headers = {"Accept": "application/json"}
        rid = current_request_id()
        if rid:
            headers[REQUEST_ID_HEADER] = rid
        if extra:
            headers.update(extra)
        return headers

    def _call(self, method: str, path: str, **kwargs) -> requests.Response:
        if not self._breaker.allow():
            raise ServiceUnavailable(f"circuit open for {self.name}")

        kwargs.setdefault("timeout", self.timeout)
        kwargs["headers"] = self._headers(kwargs.pop("headers", None))
        url = self._url(path)

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.request(method, url, **kwargs)
                if resp.status_code >= 500:
                    raise requests.HTTPError(f"{resp.status_code} from {self.name}")
                self._breaker.record_success()
                return resp
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                last_exc = e
                self._breaker.record_failure()
                if attempt == self.max_retries:
                    break
                sleep_s = min(2 ** attempt, 4) + random.uniform(0, 0.2)
                log.warning("%s %s failed (attempt %d): %s; retry in %.2fs",
                            method, url, attempt + 1, e, sleep_s)
                time.sleep(sleep_s)

        raise ServiceUnavailable(f"{self.name} unreachable: {last_exc}") from last_exc

    def get(self, path: str, **kw) -> requests.Response:      return self._call("GET", path, **kw)
    def post(self, path: str, json=None, **kw) -> requests.Response: return self._call("POST", path, json=json, **kw)
    def put(self, path: str, json=None, **kw) -> requests.Response:  return self._call("PUT", path, json=json, **kw)
    def patch(self, path: str, json=None, **kw) -> requests.Response: return self._call("PATCH", path, json=json, **kw)
    def delete(self, path: str, **kw) -> requests.Response:   return self._call("DELETE", path, **kw)


def json_or_raise(resp: requests.Response) -> Any:
    """Parse JSON, raising a readable error on 4xx."""
    if resp.status_code >= 400:
        raise ServiceUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}")
    return resp.json()
