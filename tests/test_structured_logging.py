import asyncio
import io
import json
import logging
import threading

import pytest

from qs_ai.infrastructure.observability.structured import StructuredHandler, context, emit


@pytest.fixture
def sink():
    output = io.StringIO()
    handler = StructuredHandler(output, environment="local", release="abc123")
    logger = logging.getLogger("qs_ai.structured")
    previous = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    yield handler, output
    logger.removeHandler(handler)
    handler.shutdown()
    logger.setLevel(previous)


async def test_concurrent_context_isolation(sink):
    handler, output = sink

    async def work(index):
        with context(request_id=f"request-{index}"):
            await asyncio.sleep(0)
            emit("completed", "worker", run_id=f"run-{index}")

    await asyncio.gather(*(work(index) for index in range(100)))
    emit("outside", "worker")
    assert handler.shutdown()
    rows = [json.loads(line) for line in output.getvalue().splitlines()]
    assert len(rows) == 101
    for row in rows[:-1]:
        assert row["request_id"].split("-")[1] == row["run_id"].split("-")[1]
        assert row["schema_version"] == 1
    assert "request_id" not in rows[-1]


def test_payloads_and_exception_text_never_escape(sink):
    handler, output = sink
    emit(
        "failed",
        "worker",
        prompt="SECRET_PROMPT",
        token="SECRET_TOKEN",
        error_type="ConnectionError",
        error_code="bad\nSECRET_BODY",
    )
    record = logging.LogRecord("httpx", logging.ERROR, "", 1, "SECRET_URL", (), None)
    handler.handle(record)
    assert handler.shutdown()
    assert "SECRET" not in output.getvalue()
    assert json.loads(output.getvalue())["error_type"] == "ConnectionError"


def test_blocked_output_bounded_queue_and_shutdown():
    entered, release = threading.Event(), threading.Event()

    class Blocked:
        def write(self, value):
            entered.set()
            release.wait(5)

        def flush(self):
            pass

    handler = StructuredHandler(
        Blocked(), environment="local", release="abc", capacity=4, reserved=1
    )
    logger = logging.getLogger("qs_ai.structured")
    old = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        emit("first", "worker")
        assert entered.wait(1)
        for _ in range(20):
            emit("normal", "worker")
        emit("failure", "worker", level=logging.ERROR)
        assert handler.snapshot()["queued"] == 4
        assert handler.snapshot()["dropped"] == 17
        assert not handler.shutdown(0.01)
    finally:
        release.set()
        assert handler.shutdown(1)
        logger.removeHandler(handler)
        logger.setLevel(old)


def test_broken_sink_does_not_raise(sink):
    handler, output = sink
    output.close()
    emit("committed", "worker")
    assert handler.shutdown()
    assert handler.snapshot()["failed"] == 1


def test_invalid_fields_do_not_stringify_objects(sink):
    handler, output = sink

    class Sensitive:
        def __str__(self):
            raise AssertionError("Must not serialize arbitrary objects")

    emit("safe", "worker", error_type=Sensitive(), duration_ms=float("nan"))
    assert handler.shutdown()
    row = json.loads(output.getvalue())
    assert "error_type" not in row
    assert "duration_ms" not in row


def test_metrics_have_fixed_names_without_identifiers():
    from qs_ai.infrastructure.observability.structured import render_process_metrics

    value = render_process_metrics()
    assert "qs_ai_logging_dropped_total" in value
    assert "qs_ai_logging_failed_total" in value
    assert "qs_ai_logging_queued" in value
    assert "request_id" not in value
