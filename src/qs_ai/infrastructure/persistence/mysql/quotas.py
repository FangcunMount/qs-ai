"""Organization quota history and CAS updates under the existing admission locks."""

import hashlib
import json
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import insert, select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.capacity import EvaluationCapacityPolicy
from qs_ai.application.execution.capacity import ParticipantCapacityPolicy
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import QuotaBaseline, QuotaValues
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.prompt_draft import DraftConflict, valid_reason
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_admission_locks,
    participant_admission_locks,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    organization_quota_commands as commands,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    organization_quota_pointers as pointers,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    organization_quota_versions as versions,
)


def encode(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def digest(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


def checked(raw: str, checksum: str) -> Any:
    if digest(raw) != checksum:
        raise ValueError("Quota snapshot checksum mismatch")
    return json.loads(raw)


async def configuration(db: AsyncSession, organization_id: int) -> tuple[QuotaValues | None, int]:
    # Locking/current read: admission may already have established a repeatable-read snapshot.
    revision = (
        await db.execute(
            select(pointers.c.revision)
            .where(pointers.c.organization_id == organization_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if revision is None:
        return None, 0
    row = (
        (
            await db.execute(
                select(versions)
                .where(
                    versions.c.organization_id == organization_id, versions.c.revision == revision
                )
                .with_for_update()
            )
        )
        .mappings()
        .one()
    )
    return QuotaValues.parse(checked(row["definition_json"], row["definition_sha256"])), revision


async def effective(
    db: AsyncSession, organization_id: int, baseline: QuotaBaseline
) -> dict[str, Any]:
    configured, revision = await configuration(db, organization_id)
    return baseline.resolve(configured, revision)


async def lock_configuration(db: AsyncSession, organization_id: int) -> None:
    # No Session/Run locks are acquired by configuration management.
    for table in (participant_admission_locks, evaluation_admission_locks):
        stmt = mysql_insert(table).values(organization_id=organization_id)
        await db.execute(stmt.on_duplicate_key_update(organization_id=organization_id))


class MySQLQuotas:
    def __init__(self, transactions: Transactions, baseline: QuotaBaseline) -> None:
        self.transactions, self.baseline = transactions, baseline

    async def get(self, scope: DraftScope) -> dict[str, Any]:
        async with self.transactions.open() as db:
            return await effective(db, scope.organization_id, self.baseline)

    async def _receipt(
        self, db: AsyncSession, scope: DraftScope, command_id: UUID, request: str | None = None
    ) -> dict[str, Any] | None:
        row = (
            (
                await db.execute(
                    select(commands)
                    .where(
                        commands.c.organization_id == scope.organization_id,
                        commands.c.command_id == str(command_id),
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        if row["operator_user_id"] != scope.operator_user_id:
            raise NotFound("Quota command unavailable")
        if request is not None and row["request_json"] != request:
            raise DraftConflict("Quota command identity already used")
        value = checked(row["receipt_json"], row["receipt_sha256"])
        if not isinstance(value, dict):
            raise ValueError("Invalid quota receipt")
        return value

    async def receipt(self, scope: DraftScope, command_id: UUID) -> dict[str, Any]:
        async with self.transactions.open() as db:
            result = await self._receipt(db, scope, command_id)
            if result is None:
                raise NotFound("Quota command unavailable")
            return result

    async def history(self, scope: DraftScope, before_revision: int = 0) -> dict[str, Any]:
        if type(before_revision) is not int or before_revision < 0:
            raise ValueError("Invalid history cursor")
        query = select(versions).where(versions.c.organization_id == scope.organization_id)
        if before_revision:
            query = query.where(versions.c.revision < before_revision)
        async with self.transactions.open() as db:
            rows = (
                (await db.execute(query.order_by(versions.c.revision.desc()).limit(21)))
                .mappings()
                .all()
            )
            items = [
                {
                    "revision": row["revision"],
                    "values": asdict(
                        QuotaValues.parse(checked(row["definition_json"], row["definition_sha256"]))
                    ),
                    "operator_user_id": str(row["operator_user_id"]),
                    "reason": row["reason"],
                    "created_at": row["created_at"].replace(tzinfo=UTC).isoformat(),
                }
                for row in rows[:20]
            ]
            return {"items": items, "next_revision": rows[19]["revision"] if len(rows) > 20 else 0}

    async def apply(
        self,
        scope: DraftScope,
        command_id: UUID,
        expected_revision: int,
        reason: str,
        at: datetime,
        *,
        values: QuotaValues | None = None,
        target_revision: int | None = None,
    ) -> dict[str, Any]:
        if (
            not command_id.int
            or type(expected_revision) is not int
            or expected_revision < 0
            or not valid_reason(reason)
            or at.tzinfo is None
            or (values is None) == (target_revision is None)
        ):
            raise ValueError("Complete quota command required")
        if target_revision is not None and (
            type(target_revision) is not int or target_revision < 1
        ):
            raise ValueError("Positive rollback revision required")
        request = encode(
            {
                "expected_revision": expected_revision,
                "reason": reason,
                "values": asdict(values) if values else None,
                "target_revision": target_revision,
            }
        )
        async with self.transactions.open() as db:
            await lock_configuration(db, scope.organization_id)
            prior = await self._receipt(db, scope, command_id, request)
            if prior is not None:
                return prior
            _, revision = await configuration(db, scope.organization_id)
            if expected_revision != revision:
                raise DraftConflict("Quota revision changed")
            if target_revision is not None:
                row = (
                    (
                        await db.execute(
                            select(versions).where(
                                versions.c.organization_id == scope.organization_id,
                                versions.c.revision == target_revision,
                            )
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise NotFound("Quota version unavailable")
                values = QuotaValues.parse(
                    checked(row["definition_json"], row["definition_sha256"])
                )
            assert values is not None
            values.validate_write(self.baseline.ceilings)
            raw = encode(asdict(values))
            revision += 1
            await db.execute(
                insert(versions).values(
                    organization_id=scope.organization_id,
                    revision=revision,
                    definition_json=raw,
                    definition_sha256=digest(raw),
                    operator_user_id=scope.operator_user_id,
                    reason=reason,
                    created_at=at.astimezone(UTC).replace(tzinfo=None),
                )
            )
            stmt = mysql_insert(pointers).values(
                organization_id=scope.organization_id, revision=revision
            )
            await db.execute(stmt.on_duplicate_key_update(revision=revision))
            result = {
                **self.baseline.resolve(values, revision),
                "command_id": str(command_id),
                "reason": reason,
                "operator_user_id": str(scope.operator_user_id),
                "created_at": at.astimezone(UTC).isoformat(),
            }
            receipt = encode(result)
            await db.execute(
                insert(commands).values(
                    organization_id=scope.organization_id,
                    command_id=str(command_id),
                    operator_user_id=scope.operator_user_id,
                    request_json=request,
                    receipt_json=receipt,
                    receipt_sha256=digest(receipt),
                )
            )
            await db.commit()
            return result


async def participant_policy(
    db: AsyncSession,
    organization_id: int,
    defaults: ParticipantCapacityPolicy,
    baseline: QuotaBaseline | None = None,
) -> tuple[ParticipantCapacityPolicy, dict[str, Any]]:
    if baseline is None:
        values = QuotaValues(defaults, EvaluationCapacityPolicy())
        baseline = QuotaBaseline(values, values)
    snapshot = await effective(db, organization_id, baseline)
    return ParticipantCapacityPolicy(**snapshot["effective"]["participant"]), snapshot


async def evaluation_policy(
    db: AsyncSession,
    organization_id: int,
    defaults: EvaluationCapacityPolicy,
    baseline: QuotaBaseline | None = None,
) -> tuple[EvaluationCapacityPolicy, dict[str, Any]]:
    if baseline is None:
        values = QuotaValues(ParticipantCapacityPolicy(), defaults)
        baseline = QuotaBaseline(values, values)
    snapshot = await effective(db, organization_id, baseline)
    return EvaluationCapacityPolicy(**snapshot["effective"]["evaluation"]), snapshot
