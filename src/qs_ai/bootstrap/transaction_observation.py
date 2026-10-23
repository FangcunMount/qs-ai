"""One host-loop sampler; actual DB work remains owned until it returns."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from time import monotonic

from reliable_messaging.transaction import BindingError, SchemaError, ShutdownIncompleteError

from qs_ai.bootstrap.transaction_protection import (
    ProtocolProtection,
    ProtocolProtectionError,
    ProtocolSample,
)


class ProtocolMonitor:
    def __init__(
        self,
        read: Callable[[float], Awaitable[ProtocolSample]],
        stats: Callable[[], dict[str, int | bool]],
        *,
        workers: int,
        queue: int,
        interval: float = 5.0,
        budget: float = 2.0,
    ) -> None:
        if (
            workers < 1
            or queue < 0
            or not math.isfinite(interval)
            or interval <= 0
            or not math.isfinite(budget)
            or budget <= 0
        ):
            raise ValueError("Explicit bounded protocol sampling budgets required")
        self.read, self.stats = read, stats
        self.workers, self.queue = workers, queue
        self.interval, self.budget = interval, budget
        self.freshness = 2 * interval + budget
        self.policy = ProtocolProtection()
        self.inflight = False
        self._task: asyncio.Task[None] | None = None
        self._stop: asyncio.Event | None = None
        self._stopped = False
        self._initial: asyncio.Event | None = None
        self._seen_invalid = 0

    def start(self) -> None:
        if self._task is not None or self._stopped:
            raise RuntimeError("Protocol sampler already started or stopped")
        loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        self._initial = asyncio.Event()
        # Uses the running host loop; never the pinned client's separate I/O loop.
        self._task = loop.create_task(self._run(), name="ai.mysql-protocol-observation")

    async def wait_initial(self) -> None:
        if self._initial is None or self._task is None:
            raise RuntimeError("Protocol sampler not started")
        await self._initial.wait()
        if self._task.done():
            self._task.result()
            raise RuntimeError("Protocol sampler stopped during startup")
        if self._stopped:
            raise RuntimeError("Protocol sampler stopped during startup")

    def stop(self) -> None:
        self._stopped = True
        if self._stop is not None:
            self._stop.set()

    async def _run(self) -> None:
        assert self._initial is not None
        try:
            await self._sample_loop()
        finally:
            self._initial.set()

    async def _sample_loop(self) -> None:
        assert self._stop is not None
        while not self._stop.is_set():
            self.inflight = True
            sample = ProtocolSample()
            failed = binding_conflict = False
            try:
                # The SDK observer owns the sole database timeout. A second
                # cancellation scope could interrupt session cleanup early.
                sample = await self.read(self.budget)
            except asyncio.CancelledError:
                self.policy.available = False
                raise
            except Exception as error:
                failed = True
                binding_conflict = isinstance(error, (BindingError, SchemaError))
            finally:
                self.inflight = False
            if self._stop.is_set():
                return
            stats = self.stats()
            # SDK pending_checks already includes running Futures. Adding
            # running_checks again would falsely report a full queue early.
            full = int(stats.get("pending_checks", 0)) >= self.workers + self.queue
            invalid = int(stats.get("invalid_checks", 0))
            if invalid > self._seen_invalid:
                self.policy.latch("invalid_check")
            self._seen_invalid = invalid
            self.policy.observe(
                monotonic(),
                sample,
                failed=failed,
                binding_conflict=binding_conflict,
                full=full,
                freshness=self.freshness,
            )
            assert self._initial is not None
            self._initial.set()
            try:
                async with asyncio.timeout(self.interval):
                    await self._stop.wait()
            except TimeoutError:
                pass

    def allowed(self) -> bool:
        return (
            not self._stopped
            and self._task is not None
            and not self._task.done()
            and int(self.stats().get("invalid_checks", 0)) <= self._seen_invalid
            and self.policy.allowed(monotonic(), self.freshness)
        )

    def review(self, generation: int, review: str) -> None:
        if self._stopped or self._task is None or self._task.done():
            raise ProtocolProtectionError("Stopping runtime cannot be reviewed open")
        stats = self.stats()
        if int(stats.get("invalid_checks", 0)) > self._seen_invalid:
            raise ProtocolProtectionError("New invalid check requires a fresh review")
        full = int(stats.get("pending_checks", 0)) >= self.workers + self.queue
        self.policy.review(
            monotonic(),
            self.freshness,
            generation,
            review,
            ready=bool(stats.get("ready")),
            full=full,
        )

    async def close(self, *, grace_seconds: float) -> None:
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("Finite nonnegative sampler shutdown budget required")
        self.stop()
        if self._task is None:
            return
        assert self._stop is not None
        self._stop.set()
        # wait() never cancels the owned task on timeout. A driver ignoring
        # cancellation cannot disappear from in-flight/shutdown accounting.
        done, _ = await asyncio.wait({self._task}, timeout=grace_seconds)
        if not done:
            raise ShutdownIncompleteError(
                "Protocol sampler still owns work",
                {"runtime_pending": True, "sampler_inflight": self.inflight},
            )
        self._task.result()

    async def wait(self) -> None:
        self.stop()
        if self._task is not None:
            assert self._stop is not None
            self._stop.set()
            await asyncio.shield(self._task)

    def drained(self) -> bool:
        return self._stopped and (self._task is None or self._task.done())
