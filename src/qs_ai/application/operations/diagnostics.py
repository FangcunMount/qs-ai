"""Application diagnostic context and events; no persistence or transport dependencies."""

import asyncio
import logging
import math
import re
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from pathlib import PurePath
from time import monotonic
from uuid import uuid4

from qs_ai.application.execution.errors import LeaseLost
from qs_ai.application.interpretation.ports import AccessDenied, DependencyUnavailable
from qs_ai.domain.interpretation.model import RuleViolation

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
        "error_location",
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


def classify(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    if isinstance(error, LeaseLost):
        return "lease_lost"
    if isinstance(error, AccessDenied):
        return "access_denied"
    if isinstance(error, (DependencyUnavailable, ConnectionError, TimeoutError)):
        return "dependency_unavailable"
    if isinstance(error, RuleViolation):
        return "rule_violation"
    if isinstance(error, ValueError):
        return "invalid_input"
    return "unexpected_error"


@contextmanager
def operation(name: str, component: str, **fields: object) -> Iterator[None]:
    started = monotonic()
    with context(**fields):
        emit(name + ".started", component)
        try:
            yield
        except BaseException as error:
            code = classify(error)
            location = "unknown"
            tb = error.__traceback__
            while tb is not None:
                filename = tb.tb_frame.f_code.co_filename
                if "/qs_ai/" in filename:
                    location = (
                        f"{PurePath(filename).name}:{tb.tb_lineno}:{tb.tb_frame.f_code.co_name}"
                    )
                tb = tb.tb_next
            emit(
                name + ".failed",
                component,
                level=logging.ERROR if code == "unexpected_error" else logging.WARNING,
                error_code=code,
                error_type=type(error).__name__,
                error_location=location,
                duration_ms=(monotonic() - started) * 1000,
            )
            raise
        else:
            emit(name + ".completed", component, duration_ms=(monotonic() - started) * 1000)


def attempt_context(**fields: object) -> AbstractContextManager[None]:
    return context(correlation_id=str(uuid4()), **fields)


def outgoing_metadata() -> tuple[tuple[str, str], ...]:
    value = (_context.get() or {}).get("correlation_id")
    return (("x-correlation-id", value),) if isinstance(value, str) else ()


def _empty_snapshot() -> dict[str, int]:
    return {"queued": 0, "dropped": 0, "failed": 0}


_snapshot: Callable[[], dict[str, int]] = _empty_snapshot


def register_log_metrics(snapshot: Callable[[], dict[str, int]]) -> None:
    global _snapshot
    _snapshot = snapshot


def render_process_metrics() -> str:
    lines: list[str] = []
    for name, value in _snapshot().items():
        if name not in {"queued", "dropped", "failed"}:
            continue
        suffix = name if name == "queued" else name + "_total"
        kind = "gauge" if name == "queued" else "counter"
        metric = "qs_ai_logging_" + suffix
        lines.extend((f"# TYPE {metric} {kind}", f"{metric} {value}"))
    return "\n".join(lines) + "\n"
