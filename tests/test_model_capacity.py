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
