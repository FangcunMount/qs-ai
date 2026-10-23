"""Primary-bound deny-only notices, separate from all message/witness state."""

import asyncio
import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any, cast

import sqlalchemy as sa
from reliable_messaging.transaction import Binding, BindingError, SchemaError
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from qs_ai.infrastructure.persistence.mysql.native_limit_schema import notices
from qs_ai.infrastructure.persistence.mysql.transaction_schema import binding as binding_table
from qs_ai.infrastructure.workflow_transport.native_limit import (
    NativeLimitEvidence,
    native_notice_key,
)


class MySQLNativeLimits:
    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], binding: Binding, revision: str
    ) -> None:
        if (
            not isinstance(sessions, async_sessionmaker)
            or binding.owner != "ai.mysql"
            or any(
                not isinstance(v, str) or not v.strip() or len(v.encode()) > 128
                for v in (binding.owner, binding.store_id, binding.writer_epoch)
            )
            or revision != "m3-1"
        ):
            raise BindingError("Explicit native-limit primary binding required")
        self._sessions, self.binding, self.revision = sessions, binding, revision

    async def _require_binding(self, db: AsyncSession, *, writer: bool = False) -> None:
        query = sa.select(binding_table).where(binding_table.c.owner == self.binding.owner.encode())
        if writer:
            query = query.with_for_update(read=True)
        row = (await db.execute(query)).mappings().one_or_none()
        if row is None:
            raise BindingError("Native-limit primary binding absent")
        if type(row["schema_version"]) is not int or row["schema_version"] != 1:
            raise SchemaError("Native-limit original SDK schema differs")
        if (
            row["store_id"] != self.binding.store_id.encode()
            or row["writer_epoch"] != self.binding.writer_epoch.encode()
        ):
            raise BindingError("Native-limit primary binding mismatch")

    def _validate(self, row: Mapping[str, Any]) -> bytes:
        if type(row["schema_version"]) is not int or row["schema_version"] != 1:
            raise SchemaError("Native-limit notice schema differs")
        if (
            row["owner"] != self.binding.owner.encode()
            or row["store_id"] != self.binding.store_id.encode()
            or row["writer_epoch"] != self.binding.writer_epoch.encode()
            or row["route_revision"] != self.revision.encode()
            or type(row["native_index_sha256"]) is not bytes
            or len(row["native_index_sha256"]) != 32
            or not any(row["native_index_sha256"])
            or type(row["evidence_sha256"]) is not bytes
            or len(row["evidence_sha256"]) != 32
            or not any(row["evidence_sha256"])
            or not self._valid_resolution(row)
        ):
            raise BindingError("Native-limit notice binding mismatch")
        expected = native_notice_key(
            self.binding.owner,
            self.binding.store_id,
            self.binding.writer_epoch,
            self.revision,
            row["native_index_sha256"],
        )
        if row["notice_key"] != expected:
            raise BindingError("Native-limit notice identity mismatch")
        return cast(bytes, row["native_index_sha256"])

    @staticmethod
    def _valid_resolution(row: Mapping[str, Any]) -> bool:
        at, receipt = row["resolved_at"], row["resolution_receipt_sha256"]
        if row["status"] == b"blocked":
            return at is None and receipt is None
        return (
            row["status"] == b"resolved"
            and type(at) is datetime
            and at.replace(tzinfo=None) != datetime.min
            and type(receipt) is bytes
            and len(receipt) == 32
            and any(receipt)
        )

    @staticmethod
    def _budget(value: float) -> float:
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise ValueError("Finite positive native-limit database budget required")
        return value

    async def record(self, evidence: NativeLimitEvidence, *, budget_seconds: float = 2.0) -> None:
        """No business transaction or Broker confirmation is changed.

        Approval/provenance is a separate host responsibility. This deny-only
        batch has no automatic resolution, delete or admission-reopen method.
        A reviewed resolved row keeps its first evidence and stays resolved
        when the same physical index is imported again.
        """
        budget_seconds = self._budget(budget_seconds)
        if not isinstance(evidence, NativeLimitEvidence):
            raise ValueError("Validated native-limit evidence required")
        values = []
        keys = set()
        for index in evidence.indices:
            key = native_notice_key(
                self.binding.owner,
                self.binding.store_id,
                self.binding.writer_epoch,
                self.revision,
                index,
            )
            keys.add(key)
            values.append(
                dict(
                    notice_key=key,
                    owner=self.binding.owner.encode(),
                    store_id=self.binding.store_id.encode(),
                    writer_epoch=self.binding.writer_epoch.encode(),
                    route_revision=self.revision.encode(),
                    native_index_sha256=index,
                    evidence_sha256=evidence.digest,
                    status=b"blocked",
                    schema_version=1,
                    recorded_at=sa.func.utc_timestamp(6),
                )
            )
        async with asyncio.timeout(budget_seconds), self._sessions() as db, db.begin():
            await self._require_binding(db, writer=True)
            query = insert(notices).values(values)
            await db.execute(query.on_duplicate_key_update(notice_key=notices.c.notice_key))
            rows = (
                (
                    await db.execute(
                        sa.select(notices).where(notices.c.notice_key.in_(keys)).with_for_update()
                    )
                )
                .mappings()
                .all()
            )
            remaining = set(keys)
            for row in rows:
                self._validate(dict(row))
                if row["notice_key"] not in remaining:
                    raise BindingError("Native-limit notice identity mismatch")
                remaining.remove(row["notice_key"])
            if remaining:
                raise BindingError("Native-limit notice set incomplete")

    async def read_native_limit(self, budget_seconds: float = 2.0) -> bool:
        budget_seconds = self._budget(budget_seconds)
        async with asyncio.timeout(budget_seconds), self._sessions() as db:
            await self._require_binding(db)
            row = (
                (
                    await db.execute(
                        sa.select(notices)
                        .where(
                            notices.c.owner == self.binding.owner.encode(),
                            notices.c.status != b"resolved",
                        )
                        .limit(1)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            self._validate(dict(row))
            return True
