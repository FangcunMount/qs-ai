"""Bounded output adapter for application diagnostic events."""

import json
import logging
import threading
from collections import deque
from datetime import UTC, datetime
from time import monotonic
from typing import TextIO
from uuid import uuid4

from qs_ai.application.operations.diagnostics import _FIELDS, _TOKEN, safe_fields


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
        repeat_seconds: float = 30,
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
        self.repeat_seconds = repeat_seconds
        self._repeats: dict[str, tuple[float, int]] = {}
        self.suppressed = 0
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
            suppressed = 0
            # Only idle-loop infrastructure errors are collapsed; business transitions never are.
            if event == "attempt_failed" and self.repeat_seconds > 0:
                key = component
                now = monotonic()
                previous, count = self._repeats.get(key, (now - self.repeat_seconds, 0))
                if now - previous < self.repeat_seconds:
                    self._repeats[key] = (previous, count + 1)
                    self.suppressed += 1
                    return
                if len(self._repeats) >= 64 and key not in self._repeats:
                    self._repeats.pop(next(iter(self._repeats)))
                self._repeats[key] = (now, 0)
                suppressed = count
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
            if suppressed:
                payload["suppressed_count"] = suppressed
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
            return {
                "queued": len(self._queue),
                "dropped": self.dropped,
                "failed": self.failed,
                "suppressed": self.suppressed,
            }
