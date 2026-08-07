# py-healthcare-common

Shared runtime for every Python service in healthcare-org.

## Install

Local dev (from each service's directory):

```bash
pip install -e ../../libs/py-healthcare-common
```

In containers this library is copied in by each service's Dockerfile.

## What's in here

| Module      | Purpose                                                                 |
|-------------|-------------------------------------------------------------------------|
| `http`      | `ServiceClient` — retryable, circuit-broken HTTP client for peer calls  |
| `events`    | `EventBus` — Kafka producer + consumer wrapper with graceful shutdown   |
| `auth`      | `verify_jwt`, `require_auth` Flask decorator                            |
| `tracing`   | Correlation-id middleware; propagates `X-Request-ID` across HTTP + Kafka |
| `db`        | `db_pool(dsn)` — psycopg connection pool + `query`/`execute` helpers    |
| `audit`     | `emit_audit(action, actor, target)` — one-liner to publish audit events |
| `bootstrap` | `create_service(name)` — wires all of the above into a Flask app        |

## Typical service main.py

```python
from healthcare_common.bootstrap import create_service

svc = create_service("patients-service")

@svc.app.get("/api/patients/<int:pid>")
def read(pid):
    return svc.db.query_one("SELECT data FROM patients WHERE id=%s", (pid,))
```
