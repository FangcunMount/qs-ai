"""Refusals are durable receipts; uncertain failures never become false refusals."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from qs_ai.application.interpretation.ports import AdmissionRejected, DependencyUnavailable
from qs_ai.application.interpretation.service import InterpretationService
from qs_ai.domain.interpretation.model import Actor
from tests.test_input_binding import bound_case


class Uow:
    def __init__(self, failure=None):
        self.failure = failure
        self.previous = None
        self.jobs = 0
        self.commits = 0
        self.rejections = 0
        self.events = []

    async def reserve(self, *args):
        return self.previous

    async def add(self, session):
        self.session = session

    async def bind_request(self, *args):
        pass

    async def add_evidence(self, *args):
        pass

    async def bind_configuration(self, *args):
        if self.failure:
            raise self.failure

    async def enqueue(self, *args):
        self.jobs += 1

    async def reject_run(self, session):
        self.rejections += 1

    async def save(self, session):
        self.events.append((session.status, session.failure_code))

    async def receipt(self, scope, key, receipt):
        self.previous = receipt

    async def commit(self):
        self.commits += 1

    @asynccontextmanager
    async def open(self):
        yield self


@pytest.mark.parametrize(
    "code",
    ["configuration_unavailable", "admission_input_invalid", "admission_configuration_invalid"],
)
async def test_known_refusal_is_committed_and_replays_after_configuration_recovers(code):
    uow = Uow(AdmissionRejected(code))
    service = InterpretationService(uow, SimpleNamespace())
    args = (Actor("1", "2"), "7", ("42",), "goal", str(uuid4()), bound_case()[1].items)
    receipt = await service.start_external(*args)
    assert receipt.status == "blocked" and receipt.version == 4
    assert uow.events == [("blocked", code)]
    assert uow.jobs == 0 and uow.commits == uow.rejections == 1
    uow.failure = None
    assert await service.start_external(*args) == receipt
    assert uow.jobs == 0 and uow.commits == 1


async def test_missing_snapshot_is_durable_refusal_without_job():
    uow = Uow()
    receipt = await InterpretationService(uow, SimpleNamespace()).start_external(
        Actor("1", "2"), "7", ("42",), "goal", str(uuid4())
    )
    assert receipt.status == "blocked"
    assert uow.events == [("blocked", "admission_input_invalid")]
    assert uow.jobs == 0 and uow.commits == 1


@pytest.mark.parametrize(
    "error", [DependencyUnavailable(), RuntimeError("unknown transaction outcome")]
)
async def test_uncertain_failure_propagates_without_false_refusal(error):
    uow = Uow(error)
    with pytest.raises(type(error)):
        await InterpretationService(uow, SimpleNamespace()).start_external(
            Actor("1", "2"), "7", ("42",), "goal", str(uuid4()), bound_case()[1].items
        )
    assert uow.commits == uow.rejections == uow.jobs == 0
    assert uow.events == []
