"""Single process: one event loop, dependency container and lifecycle owner."""

import asyncio
import logging
import signal
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import uvicorn
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from dishka import AsyncContainer, Provider, Scope, provide

from qs_ai.application.execution.worker import ExecuteNext
from qs_ai.application.integration.events import DeliverResults, ResultReceiver
from qs_ai.bootstrap.api import create_app
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.daemon import run_loop
from qs_ai.bootstrap.grpc_server import create_grpc_server
from qs_ai.bootstrap.lifecycle import Component, RuntimeState, event, supervise
from qs_ai.bootstrap.providers.evaluation import EvaluationProvider
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.infrastructure.persistence.mysql.evaluation_worker import EvaluationWorker
from qs_ai.infrastructure.qs_server.report_probe import mtls_channel
from qs_ai.infrastructure.workflow_transport.results import GRPCResultReceiver


class DeliveryProvider(Provider):
    @provide(scope=Scope.APP, provides=ResultReceiver, override=True)
    async def receiver(self, settings: Settings) -> AsyncIterator[ResultReceiver]:
        options = settings.grpc
        if not options.result_address or not all(
            (options.ca_file, options.cert_file, options.key_file)
        ):
            raise ValueError("Result address and TLS files are required")
        ca, cert, key = await tls_bytes(settings)
        async with mtls_channel(options.result_address, ca, key, cert) as channel:
            yield GRPCResultReceiver(channel, options.request_timeout_seconds)


async def tls_bytes(settings: Settings) -> tuple[bytes, bytes, bytes]:
    options = settings.grpc
    if not all((options.ca_file, options.cert_file, options.key_file)):
        raise ValueError("TLS files are required")
    values = await asyncio.gather(
        *(
            asyncio.to_thread(Path(path).read_bytes)
            for path in (options.ca_file, options.cert_file, options.key_file)
            if path
        )
    )
    return values[0], values[1], values[2]


def validate_tls(settings: Settings) -> None:
    options = settings.grpc
    if not options.ca_file or not options.cert_file or not options.key_file:
        raise ValueError("TLS files are required")
    context = ssl.create_default_context(cafile=options.ca_file)
    context.load_cert_chain(options.cert_file, options.key_file)


async def preflight(container: AsyncContainer, settings: Settings) -> None:
    # Use the application pool; startup must not construct a second engine.
    database = await container.get(Database)
    if database.engine is None:
        raise ValueError("Database is required")
    scripts = await asyncio.to_thread(ScriptDirectory.from_config, Config("alembic.ini"))
    async with asyncio.timeout(10):
        async with database.engine.connect() as connection:
            heads = await connection.run_sync(
                lambda conn: MigrationContext.configure(conn).get_current_heads()
            )
    if sorted(heads) != sorted(scripts.get_heads()):
        raise ValueError("Database migration version does not match the image")
    async with container() as operation:
        await operation.get(DeliverResults)
        if settings.generation.enabled:
            await operation.get(ExecuteNext)
        if settings.evaluation.enabled:
            await operation.get(EvaluationWorker)


class HTTPServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        # Only run() owns SIGTERM/SIGINT.
        yield


def background(
    name: str,
    attempt: Callable[[], Awaitable[int | bool]],
    stop: asyncio.Event,
    options: dict[str, float | int],
) -> Component:
    started = False

    async def run() -> None:
        nonlocal started
        started = True
        await run_loop(
            attempt,
            stop,
            concurrency=int(options["concurrency"]),
            idle_seconds=options["idle_seconds"],
            max_backoff_seconds=options["max_backoff_seconds"],
            shutdown_seconds=options["shutdown_seconds"],
        )

    async def halt() -> None:
        stop.set()

    return Component(name, run, halt, lambda: started, options["shutdown_seconds"] + 1)


async def serve(settings: Settings, stop: asyncio.Event) -> None:
    await asyncio.to_thread(validate_tls, settings)
    ca, cert, key = await tls_bytes(settings)
    container = create_container(settings, DeliveryProvider(), EvaluationProvider())
    state = RuntimeState()
    grpc_server = None
    try:
        await preflight(container, settings)
        grpc_server = create_grpc_server(container, settings, ca, cert, key)
        grpc_started = False

        async def run_grpc() -> None:
            nonlocal grpc_started
            await grpc_server.start()
            grpc_started = True
            await grpc_server.wait_for_termination()

        async def stop_grpc() -> None:
            await grpc_server.stop(settings.grpc.shutdown_grace_seconds)

        async def generate() -> bool:
            async with container() as operation:
                return await (await operation.get(ExecuteNext)).once(settings.worker.lease_seconds)

        async def evaluate() -> bool:
            async with container() as operation:
                return await (await operation.get(EvaluationWorker)).once()

        async def deliver() -> int:
            async with container() as operation:
                return await (await operation.get(DeliverResults)).once(
                    settings.delivery.batch_size
                )

        components = [
            Component(
                "grpc",
                run_grpc,
                stop_grpc,
                lambda: grpc_started,
                settings.grpc.shutdown_grace_seconds + 1,
            )
        ]
        for name, attempt, options, excluded, enabled in (
            (
                "generation",
                generate,
                settings.worker,
                {"lease_seconds"},
                settings.generation.enabled,
            ),
            (
                "evaluation",
                evaluate,
                settings.evaluation,
                {"enabled", "daily_provider_calls", "max_active_runs"},
                settings.evaluation.enabled,
            ),
            ("delivery", deliver, settings.delivery, {"batch_size", "max_retry_seconds"}, True),
        ):
            if enabled:
                values = options.model_dump(exclude=excluded)
                components.append(background(name, attempt, stop, values))
        app = create_app(settings, container=container, runtime=state)
        http = HTTPServer(
            uvicorn.Config(app, **settings.http.model_dump(), timeout_graceful_shutdown=5)
        )

        async def run_http() -> None:
            try:
                await http.serve()
            except SystemExit:
                # Uvicorn bind failure must reach supervisor rather than kill the loop.
                raise RuntimeError("HTTP startup failed") from None

        async def stop_http() -> None:
            try:
                # Keep readiness available (503) during background drain.
                await asyncio.gather(
                    *(asyncio.shield(task) for name, task in state.tasks.items() if name != "http"),
                    return_exceptions=True,
                )
            finally:
                http.should_exit = True

        deadline = max(component.shutdown_seconds for component in components) + 4
        components.append(Component("http", run_http, stop_http, lambda: http.started, deadline))
        await supervise(components, state, stop)
    finally:
        if grpc_server is not None:
            await grpc_server.stop(0)
        async with asyncio.timeout(5):
            await container.close()


async def run() -> None:
    settings = Settings()
    logging.basicConfig(
        level="DEBUG" if settings.http.log_level == "trace" else settings.http.log_level.upper(),
        format="%(message)s",
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=4, thread_name_prefix="qs-ai-io"))
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await serve(settings, stop)
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)


def main() -> None:
    try:
        asyncio.run(run())
    except Exception as error:
        event("startup_or_runtime_failure", "qs-ai", error_type=type(error).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
