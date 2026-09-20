"""Own process lifetime, not business state. Components never install signals."""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

from qs_ai.application.operations.diagnostics import emit


def event(name: str, component: str, **fields: str | int | float) -> None:
    level = logging.INFO
    if name in {"service_failed", "startup_or_runtime_failure", "component_stopped_with_error"}:
        level = logging.ERROR
    elif name == "drain_timeout":
        level = logging.WARNING
    emit(name, component, level=level, **fields)


@dataclass
class RuntimeState:
    ready: bool = False
    failed: bool = False
    tasks: dict[str, asyncio.Task[None]] = field(default_factory=dict)

    @property
    def healthy(self) -> bool:
        return not self.failed and all(not task.done() for task in self.tasks.values())


@dataclass(frozen=True)
class Component:
    name: str
    run: Callable[[], Coroutine[Any, Any, None]]
    stop: Callable[[], Awaitable[None]]
    started: Callable[[], bool]
    shutdown_seconds: float


async def supervise(
    components: list[Component],
    state: RuntimeState,
    stop: asyncio.Event,
    *,
    startup_seconds: float = 30,
) -> None:
    """Fail the process if any mandatory component exits, including clean exits."""
    stopper = asyncio.create_task(stop.wait())
    try:
        for component in components:
            state.tasks[component.name] = asyncio.create_task(component.run(), name=component.name)
            event("component_starting", component.name)
        async with asyncio.timeout(startup_seconds):
            while not stop.is_set():
                if not state.healthy:
                    raise RuntimeError("Component exited during startup")
                if all(component.started() for component in components):
                    state.ready = True
                    event("service_ready", "qs-ai")
                    break
                await asyncio.sleep(0.01)
        if not stop.is_set():
            await asyncio.wait(
                [stopper, *state.tasks.values()], return_when=asyncio.FIRST_COMPLETED
            )
            if not stop.is_set():
                raise RuntimeError("Required component exited unexpectedly")
    except BaseException:
        state.failed = True
        event("service_failed", "qs-ai")
        raise
    finally:
        state.ready = False
        stop.set()
        event("service_draining", "qs-ai")

        async def drain(component: Component) -> None:
            task = state.tasks.get(component.name)
            try:
                async with asyncio.timeout(component.shutdown_seconds):
                    await component.stop()
                    if task is not None:
                        await asyncio.shield(task)
            except TimeoutError:
                event("drain_timeout", component.name)
            except (Exception, asyncio.CancelledError):
                # Never log exception text: drivers can embed credentials and payloads.
                event("component_stopped_with_error", component.name)
            finally:
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                event("component_stopped", component.name)

        await asyncio.gather(*(drain(component) for component in components))
        stopper.cancel()
        await asyncio.gather(stopper, return_exceptions=True)
