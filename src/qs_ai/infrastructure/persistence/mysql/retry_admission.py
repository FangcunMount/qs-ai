"""Original retry rules with explicit read projection or frozen root resources.

Neither adapter authorizes another retry/model call. The original application
still requires participant access and explicit unknown-result risk consent.
The frozen adapter borrows the caller's transaction and commit responsibility.
"""

import hashlib
import json
from datetime import datetime
from typing import Any

from sqlalchemy import Select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.application.integration.events import StateEvent
from qs_ai.application.interpretation.ports import UnitOfWork
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import (
    ConfigurationPlanConflict,
)
from qs_ai.infrastructure.persistence.mysql.interpretation import MySQLUnitOfWork
from qs_ai.infrastructure.persistence.mysql.interpretation_admission import (
    ReadOnlyInterpretationMutation,
)
from qs_ai.infrastructure.persistence.mysql.participant_retries import MySQLParticipantRetries
from qs_ai.infrastructure.workflow_transport.messaging import valid_id


def audit_digest(values: dict[str, Any]) -> bytes:
    raw = json.dumps(
        values, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    return hashlib.sha256(b"qs-ai-participant-retry-audit/v1\0" + raw).digest()


class ReadOnlyParticipantRetries(MySQLParticipantRetries):
    def __init__(
        self,
        transactions: Transactions,
        capacity: ParticipantCapacityPolicy,
        quota_baseline: QuotaBaseline | None = None,
        *,
        at: datetime,
        run_id: str,
        state_event_id: str,
    ) -> None:
        super().__init__(transactions, capacity, quota_baseline)
        if (
            not isinstance(at, datetime)
            or at.utcoffset() is None
            or not valid_id(run_id)
            or not valid_id(state_event_id)
        ):
            raise ValueError("Frozen retry projection resources required")
        self._at, self._run_id, self._event_id = at, run_id, state_event_id
        self._projection: ReadOnlyInterpretationMutation | None = None
        self._audit_digest: bytes | None = None

    @property
    def state(self) -> StateEvent | None:
        return None if self._projection is None else self._projection.state

    @property
    def audit_digest(self) -> bytes | None:
        return self._audit_digest

    def _new_run(self) -> str:
        return self._run_id

    def _current(self, statement: Select[Any]) -> Select[Any]:
        return statement

    def _unit(self, db: AsyncSession) -> UnitOfWork:
        if self._projection is not None:
            raise RuntimeError("One original retry projection per command required")
        self._projection = ReadOnlyInterpretationMutation(
            db,
            at=self._at,
            run_id=self._run_id,
            state_event_id=self._event_id,
            capacity=self.capacity,
            quota_baseline=self.quota_baseline,
        )
        return self._projection

    async def _record_retry(self, db: AsyncSession, values: dict[str, Any]) -> None:
        if (
            self._projection is None
            or self._projection.db is not db
            or self._audit_digest is not None
        ):
            raise RuntimeError("One original retry audit projection required")
        self._audit_digest = audit_digest(values)


class FrozenParticipantRetries(MySQLParticipantRetries):
    def __init__(
        self,
        transactions: Transactions,
        capacity: ParticipantCapacityPolicy,
        quota_baseline: QuotaBaseline | None = None,
        *,
        at: datetime,
        run_id: str,
        job_id: str,
        state_event_id: str,
        expected_audit: bytes | None,
    ) -> None:
        super().__init__(transactions, capacity, quota_baseline)
        if (
            not isinstance(at, datetime)
            or at.utcoffset() is None
            or any(not valid_id(v) for v in (run_id, job_id, state_event_id))
            or (
                expected_audit is not None
                and (type(expected_audit) is not bytes or len(expected_audit) != 32)
            )
        ):
            raise ValueError("Frozen original retry resources and audit required")
        self._at, self._run_id, self._job_id, self._event_id = at, run_id, job_id, state_event_id
        self._expected_audit = expected_audit

    def _new_run(self) -> str:
        return self._run_id

    def _unit(self, db: AsyncSession) -> UnitOfWork:
        return MySQLUnitOfWork(
            db,
            self.capacity,
            self.quota_baseline,
            commit=self.transactions.commit,
            job_id=self._job_id,
            state_event_id=self._event_id,
            at=self._at,
        )

    async def _record_retry(self, db: AsyncSession, values: dict[str, Any]) -> None:
        if self._expected_audit is None or audit_digest(values) != self._expected_audit:
            raise ConfigurationPlanConflict("Original retry audit differs from frozen source")
        await super()._record_retry(db, values)
