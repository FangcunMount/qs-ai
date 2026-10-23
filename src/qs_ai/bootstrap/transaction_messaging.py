"""Explicit transaction assembly; the host owns Start, admission and DB lifetime."""

import math
import os
from time import monotonic
from typing import cast

from reliable_messaging.rocketmq_transaction import (
    TransactionalSender,
    TransactionConfig,
    TransactionRoute,
)
from reliable_messaging.transaction import (
    Binding,
    Execution,
    FrozenPlan,
    LocalExecutor,
    ReplanAuthorization,
    Request,
    Reservation,
    Result,
    RetryAuthorization,
)
from reliable_messaging.transaction_mysql import MySQLTransactionStore
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.application.operations.metrics import TransactionMetricsSnapshot
from qs_ai.bootstrap.messaging_config import read_rocketmq_config
from qs_ai.bootstrap.transaction_observation import ProtocolMonitor
from qs_ai.bootstrap.transaction_protection import ProtocolProtectionError, ProtocolSample
from qs_ai.config import MessagingOptions
from qs_ai.infrastructure.workflow_transport.messaging import EVENTS
from qs_ai.infrastructure.workflow_transport.transaction_root import (
    TransactionAdmissionClosed as TransactionAdmissionClosed,
)


class TransactionRuntime:
    """Pause only new sends; final StopAdmission and drain remain SDK-owned."""

    def __init__(
        self, sender: TransactionalSender, *, admitted: bool, monitor: ProtocolMonitor | None = None
    ) -> None:
        self._sender = sender
        self._admitted = admitted
        self._stopping = False
        self._monitor = monitor

    @property
    def config(self) -> TransactionConfig:
        return self._sender.config

    @property
    def store(self) -> MySQLTransactionStore:
        return cast(MySQLTransactionStore, self._sender.store)

    async def start(self) -> None:
        try:
            # Observe before registration can resolve historical half messages;
            # a protected host still starts its idle Checker afterward.
            if self._monitor is not None:
                self._monitor.start()
                await self._monitor.wait_initial()
            await self._sender.start()
        except BaseException:
            self.stop_admission()
            if self._monitor is not None:
                self._monitor.stop()
            raise

    def _is_admitted(self) -> bool:
        return (
            self._admitted
            and not self._stopping
            and bool(self._sender.stats()["ready"])
            and (self._monitor is None or self._monitor.allowed())
        )

    def set_admission(self, opened: bool) -> None:
        if self._stopping or (
            opened
            and (
                not self._sender.stats()["ready"]
                or (self._monitor is not None and not self._monitor.allowed())
            )
        ):
            raise TransactionAdmissionClosed("Transaction runtime is not ready for admission")
        self._admitted = opened

    async def reserve(self, request: Request, candidate: FrozenPlan) -> Reservation:
        if not self._is_admitted():
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.reserve(request, candidate)

    async def execute(self, execution: Execution, local: LocalExecutor) -> Result:
        if not self._is_admitted():
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.execute(execution, local)

    async def retry(self, authorization: RetryAuthorization) -> Reservation:
        if not self._is_admitted():
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.retry(authorization)

    async def replan(self, authorization: ReplanAuthorization) -> Reservation:
        if not self._is_admitted():
            raise TransactionAdmissionClosed("Host transaction admission closed")
        return await self._sender.replan(authorization)

    def stop_admission(self) -> None:
        self._admitted = False
        self._stopping = True
        self._sender.stop_admission()

    async def close(self, *, grace_seconds: float = 30) -> None:
        if not math.isfinite(grace_seconds) or grace_seconds < 0:
            raise ValueError("Finite nonnegative transaction shutdown budget required")
        deadline = monotonic() + grace_seconds
        self.stop_admission()
        await self._sender.close(grace_seconds=grace_seconds)
        if self._monitor is not None:
            await self._monitor.close(grace_seconds=max(0.0, deadline - monotonic()))

    async def wait(self) -> None:
        await self._sender.wait()
        if self._monitor is not None:
            await self._monitor.wait()

    def review_protection(self, generation: int, review: str) -> None:
        if self._stopping or self._monitor is None:
            raise ProtocolProtectionError("Stopping or unobserved runtime cannot be reviewed open")
        self._monitor.review(generation, review)

    def protection_stats(self) -> dict[str, int | bool | str]:
        monitor = self._monitor
        if monitor is None:
            return {
                "available": False,
                "fresh": False,
                "protected": False,
                "generation": 0,
                "reason": "unconfigured",
                "observation_errors": 0,
                "sampler_running": False,
                "sampler_inflight": False,
            }
        p = monitor.policy
        return {
            "available": p.available,
            "fresh": p.fresh(monotonic(), monitor.freshness),
            "protected": bool(p.reason),
            "generation": p.generation,
            "reason": p.reason,
            "observation_errors": p.errors,
            "sampler_running": monitor._task is not None and not monitor._task.done(),
            "sampler_inflight": monitor.inflight,
        }

    def transaction_metrics_snapshot(self) -> TransactionMetricsSnapshot | None:
        monitor = self._monitor
        if monitor is None:
            return None
        p, s = monitor.policy, monitor.policy.sample
        raw = self._sender.stats()
        running = int(raw.get("running_checks", 0))
        # pending_checks is total admitted Futures, including running workers.
        queued = max(0, int(raw.get("pending_checks", 0)) - running)
        values: dict[str, int | float] = {
            "sampler_running": int(monitor._task is not None and not monitor._task.done()),
            "sdk_ready": int(bool(raw.get("ready"))),
            "sdk_stopping": int(bool(raw.get("stopping"))),
            "sdk_stopped": int(bool(raw.get("stopped"))),
            "observation_inflight": int(monitor.inflight),
            "observation_available": int(p.available),
            "observation_fresh": int(p.fresh(monotonic(), monitor.freshness)),
            "observation_errors_total": p.errors,
            "reserved_executions": s.reserved,
            "preparing_executions": s.preparing,
            "prepared_expiry_age_seconds": s.expiry_age,
            "confirmation_gap_branches": s.gaps,
            "oldest_confirmation_observation_age_seconds": s.confirmation_age,
            "unaged_confirmation_gap_branches": s.unaged,
            "untracked_aborted_branches": s.untracked_aborted,
            "unobserved_committed_branches": s.unobserved_committed,
            "metadata_conflicts": s.conflicts,
            "admission_protected": int(bool(p.reason)),
            "protection_generation": p.generation,
            "admission_open": int(self._is_admitted()),
            "active_sends": int(raw.get("active_sends", 0)),
            "running_checks": running,
            "queued_checks": queued,
            "check_workers_limit": monitor.workers,
            "check_queue_limit": monitor.queue,
            "reconciled_executions_total": int(raw.get("reconciled", 0)),
        }
        outcomes = {
            (stage, name): int(raw.get(key, 0))
            for stage, name, key in (
                ("half", "prepared", "half_prepared_results"),
                ("half", "rejected", "half_rejected_results"),
                ("half", "unknown", "half_unknown_results"),
                ("local", "committed", "local_committed_results"),
                ("local", "aborted", "local_aborted_results"),
                ("local", "unknown", "local_unknown_results"),
                ("final", "commit_confirmed", "final_commit_results"),
                ("final", "rollback_confirmed", "final_rollback_results"),
                ("final", "unknown", "final_unknown_results"),
            )
        }
        checks = {
            name: int(raw.get(key, 0))
            for name, key in (
                ("commit", "check_commit"),
                ("rollback", "check_rollback"),
                ("unknown", "check_unknown"),
                ("invalid", "invalid_checks"),
                ("rejected", "check_rejected"),
                ("database_error", "database_errors"),
                ("callback_error", "callback_errors"),
            )
        }
        return TransactionMetricsSnapshot(
            values,
            outcomes,
            checks,
            {
                name: int(raw.get("recovery_" + name + "_confirmed", 0))
                for name in ("commit", "rollback")
            },
            {
                name: int(raw.get("recovery_audit_" + key, 0))
                for name, key in (
                    ("recorded", "recorded"),
                    ("error", "errors"),
                    ("unsupported", "unsupported"),
                )
            },
        )

    def stats(self) -> dict[str, int | bool]:
        result = dict(self._sender.stats())
        result["ready"] = bool(result["ready"] and self._is_admitted())
        if self._monitor is not None:
            result["stopped"] = bool(result.get("stopped") and self._monitor.drained())
        return result


def make_transaction_sender(
    options: MessagingOptions, sessions: async_sessionmaker[AsyncSession]
) -> TransactionRuntime:
    """Borrow a primary session factory without opening a session/thread/client.

    The host gate honors publish_enabled; callers explicitly manage sender
    Start/StopAdmission/Close/Wait before disposing the application's pool/loop.
    No constructor or Start installs the transaction schema.
    """
    if options.provider != "rocketmq" or set(options.transactions) != {"ai.mysql"}:
        raise ValueError("Explicit ai.mysql transaction configuration required")
    if options.rocketmq is None or (
        os.environ.get("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH") != options.rocketmq.ca_file
    ):
        raise ValueError("Transaction client requires the host's configured verified CA")
    binding = options.transactions["ai.mysql"]
    config = read_rocketmq_config(options)
    routes = {
        r.physical_topic: TransactionRoute(r.topic, r.physical_topic, r.route_revision)
        for r in config.routes
        if r.message_type == "TRANSACTION"
        and r.owner == "ai.mysql"
        and r.route_revision == binding.route_revision
        and r.topic == EVENTS
    }
    if not routes:
        raise ValueError("Required ai.mysql transaction event route absent")
    store = MySQLTransactionStore(
        sessions, Binding("ai.mysql", binding.store_id, binding.writer_epoch)
    )
    sender = TransactionalSender(
        TransactionConfig(
            config.endpoint,
            config.access_key,
            config.secret_key,
            "ai.mysql",
            tuple(routes.values()),
            binding.limits,
        ),
        store,
    )

    async def read(budget: float) -> ProtocolSample:
        observation = await store.observe_protocol(budget_seconds=budget)
        return ProtocolSample(
            reserved=observation.reserved_executions,
            preparing=observation.preparing_executions,
            gaps=observation.confirmation_gap_branches,
            unaged=observation.unaged_confirmation_gap_branches,
            untracked_aborted=observation.untracked_aborted_branches,
            unobserved_committed=observation.unobserved_committed_branches,
            conflicts=observation.metadata_conflicts,
            expiry_age=observation.prepared_expiry_age_seconds,
            confirmation_age=observation.oldest_confirmation_observation_age_seconds,
        )

    monitor = ProtocolMonitor(
        read,
        sender.stats,
        workers=binding.limits.check_workers,
        queue=binding.limits.check_queue,
        interval=binding.limits.reconcile_interval,
        budget=binding.limits.check_timeout,
    )
    return TransactionRuntime(sender, admitted=options.publish_enabled, monitor=monitor)
