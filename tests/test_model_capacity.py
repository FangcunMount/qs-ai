import asyncio

import pytest

from qs_ai.application.execution.model_capacity import ModelCapacity, ProviderCapacity


def capacity():
    return ModelCapacity({p: ProviderCapacity(4, 1) for p in ("deepseek", "zhipu")}, 3)


def test_evaluations_share_global_limit_and_preserve_generation_capacity():
    pool = capacity()
    tokens = [pool.try_acquire("deepseek", evaluation=True) for _ in range(3)]
    assert all(tokens)
    assert pool.try_acquire("deepseek", evaluation=True) is None
    assert pool.try_acquire("zhipu", evaluation=True) is None
    generation = pool.try_acquire("deepseek", evaluation=False)
    assert generation is not None
    assert pool.try_acquire("deepseek", evaluation=False) is None
    tokens[0].release()
    tokens[0].release()  # Duplicate cleanup must not manufacture capacity.
    replacement = pool.try_acquire("zhipu", evaluation=True)
    assert replacement is not None
    assert pool.try_acquire("zhipu", evaluation=True) is None
    for token in tokens:
        token.release()
    generation.release()
    replacement.release()
    assert all(pool.try_acquire("deepseek", evaluation=True) for _ in range(3))


async def test_overlapping_tasks_reach_three_and_cancellation_releases_capacity():
    pool = capacity()
    entered = asyncio.Event()
    wait = asyncio.Event()
    active = 0
    peak = 0

    async def call():
        nonlocal active, peak
        token = pool.try_acquire("deepseek", evaluation=True)
        assert token is not None
        with token:
            active += 1
            peak = max(peak, active)
            if active == 3:
                entered.set()
            try:
                await wait.wait()
            finally:
                active -= 1

    tasks = [asyncio.create_task(call()) for _ in range(3)]
    await asyncio.wait_for(entered.wait(), 1)
    assert peak == 3
    assert pool.try_acquire("deepseek", evaluation=True) is None
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert active == 0
    assert all(pool.try_acquire("deepseek", evaluation=True) for _ in range(3))


@pytest.mark.parametrize("total,reserved", [(0, 0), (4, 4), (4, -1), (True, 0), (33, 1)])
def test_invalid_provider_limits(total, reserved):
    with pytest.raises(ValueError):
        ProviderCapacity(total, reserved)


def test_unknown_provider_is_not_unlimited():
    with pytest.raises(ValueError, match="not configured"):
        capacity().try_acquire("unknown", evaluation=True)


async def test_generation_waits_until_shared_capacity_is_released():
    pool = capacity()
    held = [pool.try_acquire("deepseek", evaluation=False) for _ in range(4)]
    waiter = asyncio.create_task(pool.acquire_generation("deepseek"))
    await asyncio.sleep(0)
    assert not waiter.done()
    held[0].release()
    token = await asyncio.wait_for(waiter, 1)
    assert pool.try_acquire("deepseek", evaluation=False) is None
    token.release()
    for item in held:
        item.release()


async def test_cancelled_generation_waiter_does_not_consume_capacity():
    pool = capacity()
    held = [pool.try_acquire("deepseek", evaluation=False) for _ in range(4)]
    waiter = asyncio.create_task(pool.acquire_generation("deepseek"))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    for item in held:
        item.release()
    tokens = [pool.try_acquire("deepseek", evaluation=False) for _ in range(4)]
    assert all(tokens)
    assert pool.try_acquire("deepseek", evaluation=False) is None
    for token in tokens:
        token.release()
