"""Atomic global selector publication, replacement, disable and retained rollback.

Run/checkpoint locks precede the selector lock. Consistent reads start only after
both locks are held. Missing selector reservations roll back with any failed
validation. Commands never invoke models or silently retry a conflict.
"""

import hashlib
from dataclasses import asdict
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from sqlalchemy import insert, select, update
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.evaluation.management import ManagementScope
from qs_ai.application.governance.publication import (
    MovePublication,
    PublicationHistoryPage,
    PublicationHistoryQuery,
    PublicationReceipt,
    PublicationScope,
    PublishConfiguration,
    valid_uuid,
)
from qs_ai.application.governance.publication_codec import canonical, publication_json, request_json
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.publication import (
    PublicationAudit,
    PublicationConflict,
    PublicationPointer,
    PublishedConfiguration,
    ReleaseSelector,
    change_publication,
)
from qs_ai.infrastructure.persistence.mysql import publication_history
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import lock_run_in_transaction
from qs_ai.infrastructure.persistence.mysql.publication_evidence import publication_evidence
from qs_ai.infrastructure.persistence.mysql.publication_records import (
    load_pointer,
    load_publication,
    load_receipt,
    receipt_json,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_changes as changes,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers as pointers,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publications as publications,
)


async def apply_publication(
    db: AsyncSession,
    scope: PublicationScope,
    command: PublishConfiguration | MovePublication,
    at: datetime,
) -> PublicationReceipt:
    """Caller passes QS-authorized scope and server time, and owns the commit."""
    audit = PublicationAudit(scope.actor, command.reason, at)
    await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
    target = None
    run = None
    run_scope = None
    if isinstance(command, PublishConfiguration):
        run_scope = ManagementScope(command.run_id, scope.organization_id, scope.operator_user_id)
        run = await lock_run_in_transaction(db, run_scope, command.run_version)
    elif command.target_id is not None:
        # The catalog is global, matching QS. The stored record supplies the original
        # evaluation organization; callers cannot substitute another Run or its bytes.
        target, original_org = await load_publication(db, command.target_id, lock=True)
        if target.evidence.selector != command.selector:
            raise PublicationConflict("Rollback target occupies another selector")
        run_scope = ManagementScope(target.evidence.run_id, original_org, scope.operator_user_id)
        run = await lock_run_in_transaction(db, run_scope, target.evidence.run_version)

    key = command.selector.key()
    reservation = mysql_insert(pointers).values(
        selector_key=key,
        selector_json=canonical(asdict(command.selector)),
        version=0,
    )
    await db.execute(reservation.on_duplicate_key_update(selector_key=key))
    row = (
        (await db.execute(select(pointers).where(pointers.c.selector_key == key).with_for_update()))
        .mappings()
        .one()
    )
    request = request_json(scope, command)
    accepted = (
        (await db.execute(select(changes).where(changes.c.command_id == str(command.command_id))))
        .mappings()
        .one_or_none()
    )
    if accepted is not None:
        if accepted["request_json"] != request:
            raise PublicationConflict("Command identity already used for another request or scope")
        return await load_receipt(db, accepted)

    current = await load_pointer(db, row)
    action: Literal["publish", "rollback", "disable"]
    if isinstance(command, PublishConfiguration):
        assert run is not None and run_scope is not None
        proof = await publication_evidence(db, run_scope, run, command.release_fingerprint)
        target = PublishedConfiguration(uuid4(), proof, audit)
        action = "publish"
    elif target is not None:
        assert run is not None and run_scope is not None
        proof = await publication_evidence(
            db, run_scope, run, target.evidence.release.fingerprint()
        )
        if proof != target.evidence:
            raise ValueError("Rollback evidence differs from original publication")
        action = "rollback"
    else:
        # Stopping a bad configuration must not require that its quality still passes.
        action = "disable"
    change = change_publication(
        current,
        target,
        action,
        audit,
        expected_version=command.expected_version,
        expected_active_id=command.expected_active_id,
    )
    if action == "publish":
        assert target is not None
        raw = publication_json(target)
        await db.execute(
            insert(publications).values(
                publication_id=str(target.publication_id),
                selector_key=key,
                run_id=str(target.evidence.run_id),
                run_version=target.evidence.run_version,
                organization_id=scope.organization_id,
                content_json=raw,
                content_sha256=hashlib.sha256(raw.encode()).hexdigest(),
            )
        )
    await db.execute(
        update(pointers)
        .where(pointers.c.selector_key == key)
        .values(
            version=change.current.version,
            active_publication_id=str(target.publication_id) if target else None,
            changed_at=audit.at.isoformat(),
        )
    )
    receipt = PublicationReceipt(command.command_id, change)
    await db.execute(
        insert(changes).values(
            command_id=str(command.command_id),
            selector_key=key,
            version=change.current.version,
            organization_id=scope.organization_id,
            operator_user_id=scope.operator_user_id,
            request_json=request,
            receipt_json=receipt_json(receipt),
        )
    )
    return receipt


class MySQLPublications:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def apply(
        self,
        scope: PublicationScope,
        command: PublishConfiguration | MovePublication,
        at: datetime,
    ) -> PublicationReceipt:
        try:
            async with self.transactions.open() as db:
                result = await apply_publication(db, scope, command, at)
                await db.commit()
                return result
        except IntegrityError as error:
            if error.orig is None or not error.orig.args or error.orig.args[0] != 1062:
                raise
            # Concurrent reuse across selectors loses at the command PK. All writes
            # above rolled back; only the exact accepted request may be replayed.
            async with self.transactions.open() as db:
                accepted = (
                    (
                        await db.execute(
                            select(changes).where(changes.c.command_id == str(command.command_id))
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if accepted is not None and accepted["request_json"] == request_json(
                    scope, command
                ):
                    return await load_receipt(db, accepted)
            raise PublicationConflict("Concurrent publication command conflict") from None

    async def get_receipt(self, scope: PublicationScope, command_id: UUID) -> PublicationReceipt:
        """Reconcile an unknown outcome without accepting a new mutation."""
        if not valid_uuid(command_id):
            raise ValueError("Canonical publication command identity required")
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        select(changes).where(
                            changes.c.command_id == str(command_id),
                            changes.c.organization_id == scope.organization_id,
                            changes.c.operator_user_id == scope.operator_user_id,
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise NotFound("Publication command unavailable in operator scope")
            return await load_receipt(db, row)

    async def get(self, selector: ReleaseSelector) -> PublicationPointer:
        """Read the global management catalog; this does not authorize generation."""
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        select(pointers).where(pointers.c.selector_key == selector.key())
                    )
                )
                .mappings()
                .one_or_none()
            )
            return PublicationPointer(selector) if row is None else await load_pointer(db, row)

    async def list_history(self, query: PublicationHistoryQuery) -> PublicationHistoryPage:
        async with self.transactions.open() as db:
            return await publication_history.list_history(db, query)

    async def get_history(self, selector: ReleaseSelector, version: int) -> PublicationReceipt:
        async with self.transactions.open() as db:
            return await publication_history.get_history(db, selector, version)
