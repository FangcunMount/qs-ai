import asyncio
from time import monotonic
from unittest.mock import AsyncMock, Mock

import pytest
from reliable_messaging.transaction import ShutdownIncompleteError

from qs_ai.bootstrap.transaction_messaging import TransactionAdmissionClosed, TransactionRuntime
from qs_ai.bootstrap.transaction_observation import ProtocolMonitor
from qs_ai.bootstrap.transaction_protection import (
    ProtocolProtection,
    ProtocolProtectionError,
    ProtocolSample,
)


def observe(policy, now, sample=None, *, failed=False, full=False):
    sample = sample if sample is not None else ProtocolSample()
    policy.observe(now, sample, failed=failed, binding_conflict=False, full=full, freshness=12)


def test_current_gaps_need_review_but_untracked_branches_are_not_known_backlog():
    p = ProtocolProtection()
    assert not p.allowed(0, 12)
    observe(p, 0, ProtocolSample(confirmation_age=10000, untracked_aborted=3))
    assert p.allowed(0, 12)
    observe(p, 0, ProtocolSample(gaps=1, confirmation_age=300))
    assert p.reason == "confirmation_gap" and not p.allowed(0, 12)
    generation = p.generation
    observe(p, 1)
    assert not p.allowed(1, 12)
    for epoch, name, ready, full in (
        (generation + 1, "review-1", True, False),
        (generation, "", True, False),
        (generation, "review-1", False, False),
        (generation, "review-1", True, True),
    ):
        with pytest.raises(ProtocolProtectionError):
            p.review(1, 12, epoch, name, ready=ready, full=full)
    with pytest.raises(ProtocolProtectionError):
        p.review(14, 12, generation, "review-1", ready=True, full=False)
    p.review(1, 12, generation, "review-1", ready=True, full=False)
    assert p.allowed(1, 12) and not p.allowed(14, 12)


@pytest.mark.parametrize(
    "sample,reason",
    [
        (ProtocolSample(untracked_aborted=2), ""),
        (ProtocolSample(gaps=1, unaged=1), "confirmation_age_unavailable"),
        (ProtocolSample(unobserved_committed=1), "committed_half_unobserved"),
        (ProtocolSample(conflicts=1), "metadata_conflict"),
        (ProtocolSample(preparing=1, expiry_age=300), "preparation_expired"),
        (ProtocolSample(gaps=-1), "invalid_observation"),
    ],
)
def test_protocol_protection_evidence_categories(sample, reason):
    p = ProtocolProtection()
    observe(p, 0, sample)
    assert p.reason == reason


def test_continuous_saturation_and_unavailability_do_not_invent_zero():
    p = ProtocolProtection()
    for step in range(25):
        observe(p, step * 5, full=True)
        if step < 24:
            assert not p.reason
    assert p.reason == "checker_saturated"
    missing = ProtocolProtection()
    observe(missing, 0, full=True)
    observe(missing, 121, full=True)
    assert not missing.reason
    unavailable = ProtocolProtection()
    observe(unavailable, 0, ProtocolSample(gaps=1, confirmation_age=10))
    for step in range(1, 26):
        observe(unavailable, step * 5, failed=True)
    assert unavailable.reason == "observation_unavailable"
    assert unavailable.sample.gaps == 1 and not unavailable.available


async def test_sampler_keeps_noncooperative_read_after_budget_and_waiter_cancellation():
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    reads = 0

    async def read(budget):
        nonlocal reads
        reads += 1
        assert asyncio.get_running_loop() is loop and budget == 0.01
        scope = asyncio.timeout(budget)
        async with scope:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()
        if scope.expired():
            raise TimeoutError("read actually returned after its budget")
        return ProtocolSample()

    m = ProtocolMonitor(read, lambda: {"ready": True}, workers=1, queue=16, budget=0.01)
    assert reads == 0
    m.start()
    async with asyncio.timeout(1):
        await entered.wait()
        await cancelled.wait()
    assert m.inflight
    with pytest.raises(ShutdownIncompleteError) as error:
        await m.close(grace_seconds=0)
    assert error.value.counts["sampler_inflight"] is True
    waiter = asyncio.create_task(m.wait())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert m.inflight and m._task is not None and not m._task.done()
    release.set()
    await m.wait()
    assert reads == 1 and not m.inflight and m._task.done()


async def test_python_pending_checks_includes_running_and_invalid_deltas_are_latched_once():
    raw = {"ready": True, "running_checks": 1, "pending_checks": 16, "invalid_checks": 0}

    async def read(budget):
        return ProtocolSample()

    m = ProtocolMonitor(read, lambda: raw, workers=1, queue=16)
    m.start()
    await m.wait_initial()
    assert m.policy.saturated_since is None and m.allowed()
    raw["invalid_checks"] = 1
    assert not m.allowed(), "new invalid evidence allowed admission before the next poll"
    m.policy.latch("invalid_check")
    m._seen_invalid = 1  # Unit boundary: production sampler records this after its next read.
    m.review(m.policy.generation, "review-1")
    assert m.allowed(), "cumulative invalid history permanently blocked reviewed recovery"
    assert m.policy.saturated_since is None
    await m.close(grace_seconds=1)


async def test_host_start_waits_for_first_sample_and_plain_open_cannot_bypass_protection():
    driver = Mock()
    driver.start, driver.close, driver.wait = AsyncMock(), AsyncMock(), AsyncMock()
    driver.reserve, driver.execute = AsyncMock(), AsyncMock()
    driver.stats.return_value = {"ready": True}
    entered, release = asyncio.Event(), asyncio.Event()

    async def read(budget):
        entered.set()
        await release.wait()
        return ProtocolSample(gaps=1, confirmation_age=300)

    monitor = ProtocolMonitor(read, driver.stats, workers=1, queue=16)
    runtime = TransactionRuntime(driver, admitted=True, monitor=monitor)
    started = asyncio.create_task(runtime.start())
    await entered.wait()
    assert not started.done() and not runtime.stats()["ready"]
    release.set()
    await started
    with pytest.raises(TransactionAdmissionClosed):
        runtime.set_admission(True)
    with pytest.raises(TransactionAdmissionClosed):
        await runtime.execute(None, None)
    driver.execute.assert_not_called()
    driver.stop_admission.assert_not_called()
    observe(monitor.policy, monotonic())
    with pytest.raises(TransactionAdmissionClosed):
        runtime.set_admission(True)
    runtime.review_protection(monitor.policy.generation, "review-1")
    assert runtime.stats()["ready"]
    runtime.set_admission(False)
    assert not runtime.stats()["ready"] and driver.stats.return_value["ready"]
    await runtime.close(grace_seconds=1)
