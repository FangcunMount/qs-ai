"""Bounded, allowlisted diagnostics. Never a business receipt or retry trigger."""

import json
import logging
import math
import re
import threading
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import TextIO
from uuid import uuid4

_FIELDS = frozenset(
    {
        "correlation_id",
        "request_id",
        "command_id",
        "session_id",
        "run_id",
        "invocation_id",
        "delivery_event_id",
        "task_kind",
        "stage",
        "status",
        "error_type",
        "error_code",
        "publication_id",
        "policy_version",
        "provider",
        "model",
        "attempt",
        "failures",
        "duration_ms",
        "retryable",
        "backoff_seconds",
    }
)
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}\Z")
_context: ContextVar[dict[str, str | int | float | bool] | None] = ContextVar(
    "log_context", default=None
)


def safe_fields(fields: dict[str, object]) -> dict[str, str | int | float | bool]:
    result: dict[str, str | int | float | bool] = {}
    for key, value in fields.items():
        if key not in _FIELDS:
            continue
        if isinstance(value, str) and _TOKEN.fullmatch(value):
            result[key] = value
        elif (
            isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
        ):
            result[key] = value
        elif type(value) is bool:
            result[key] = value
    return result


@contextmanager
def context(**fields: object) -> Iterator[None]:
    token = _context.set({**(_context.get() or {}), **safe_fields(fields)})
    try:
        yield
    finally:
        _context.reset(token)


def emit(event: str, component: str, *, level: int = logging.INFO, **fields: object) -> None:
    """Only structured fields cross the sink; arbitrary messages are not accepted."""
    try:
        if not _TOKEN.fullmatch(event) or not _TOKEN.fullmatch(component):
            return
        logging.getLogger("qs_ai.structured").log(
            level,
            "",
            extra={
                "diagnostic": {
                    "event": event,
                    "component": component,
                    **(_context.get() or {}),
                    **safe_fields(fields),
                }
            },
        )
    except Exception:
        pass


class StructuredHandler(logging.Handler):
    """One daemon writer, bounded queue, no output I/O on the event loop."""

    def __init__(
        self,
        stream: TextIO,
        *,
        environment: str,
        release: str,
        capacity: int = 1024,
        reserved: int = 128,
        max_bytes: int = 16384,
    ) -> None:
        super().__init__()
        if not 0 <= reserved < capacity or max_bytes < 1024:
            raise ValueError("Invalid diagnostic queue limits")
        self.stream = stream
        self.capacity, self.reserved, self.max_bytes = capacity, reserved, max_bytes
        self.environment = environment if _TOKEN.fullmatch(environment) else "unknown"
        self.release_sha = release if _TOKEN.fullmatch(release) else "unknown"
        self.instance = str(uuid4())
        self._condition = threading.Condition()
        self._queue: deque[str] = deque()
        self._stopping = False
        self.dropped = 0
        self.failed = 0
        self._writer = threading.Thread(target=self._write, name="qs-ai-log", daemon=True)
        self._writer.start()

    def emit(self, record: logging.LogRecord) -> None:
        diagnostic = getattr(record, "diagnostic", None)
        # Third-party free-text and exception objects never reach the output.
        if record.name != "qs_ai.structured" or not isinstance(diagnostic, dict):
            return
        try:
            event, component = diagnostic.get("event"), diagnostic.get("component")
            if not isinstance(event, str) or not isinstance(component, str):
                return
            if not _TOKEN.fullmatch(event) or not _TOKEN.fullmatch(component):
                return
            payload = {
                "schema_version": 1,
                "timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "service": "qs-ai",
                "environment": self.environment,
                "release": self.release_sha,
                "instance_id": self.instance,
                "log_event_id": str(uuid4()),
                "event": event,
                "component": component,
                **safe_fields(diagnostic),
            }
            line = json.dumps(payload, ensure_ascii=True, allow_nan=False)
            if len(line.encode()) > self.max_bytes:
                payload = {key: value for key, value in payload.items() if key not in _FIELDS}
                payload["truncated"] = True
                line = json.dumps(payload)
            with self._condition:
                limit = (
                    self.capacity
                    if record.levelno >= logging.WARNING
                    else (self.capacity - self.reserved)
                )
                if self._stopping or len(self._queue) >= limit:
                    self.dropped += 1
                    return
                self._queue.append(line)
                self._condition.notify()
        except Exception:
            self.failed += 1

    def _write(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._queue or self._stopping)
                if not self._queue:
                    return
                line = self._queue.popleft()
            try:
                self.stream.write(line + "\n")
                self.stream.flush()
            except Exception:
                self.failed += 1

    def shutdown(self, timeout: float = 2) -> bool:
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        self._writer.join(max(0, timeout))
        return not self._writer.is_alive()

    def snapshot(self) -> dict[str, int]:
        with self._condition:
            return {"queued": len(self._queue), "dropped": self.dropped, "failed": self.failed}


def render_process_metrics() -> str:
    """Independent of database availability; never logs its own failures."""
    handler = next(
        (item for item in logging.getLogger().handlers if isinstance(item, StructuredHandler)), None
    )
    values = handler.snapshot() if handler else {"queued": 0, "dropped": 0, "failed": 0}
    lines: list[str] = []
    for name, value in values.items():
        suffix = name if name == "queued" else name + "_total"
        kind = "gauge" if name == "queued" else "counter"
        metric = "qs_ai_logging_" + suffix
        lines.extend((f"# TYPE {metric} {kind}", f"{metric} {value}"))
    return "\n".join(lines) + "\n"
