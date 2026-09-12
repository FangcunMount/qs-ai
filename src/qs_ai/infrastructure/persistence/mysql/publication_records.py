"""Read retained publications and verify their indexed pointer/audit bindings."""

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.governance.publication import PublicationReceipt, PublishConfiguration
from qs_ai.application.governance.publication_codec import canonical, read_publication, read_request
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.publication import (
    PublicationAudit,
    PublicationPointer,
    PublishedConfiguration,
    ReleaseSelector,
    change_publication,
    valid_version,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_changes as changes,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publications as publications,
)


async def load_publication(
    db: AsyncSession, publication_id: UUID, *, lock: bool = False
) -> tuple[PublishedConfiguration, int]:
    query = select(publications).where(publications.c.publication_id == str(publication_id))
    row = (await db.execute(query.with_for_update() if lock else query)).mappings().one_or_none()
    if row is None:
        raise NotFound("Retained publication not found")
    value = read_publication(row["content_json"])
    if (
        hashlib.sha256(row["content_json"].encode()).hexdigest() != row["content_sha256"]
        or value.publication_id != publication_id
        or value.evidence.selector.key() != row["selector_key"]
        or str(value.evidence.run_id) != row["run_id"]
        or value.evidence.run_version != row["run_version"]
        or not valid_version(row["organization_id"])
    ):
        raise ValueError("Publication differs from its retained index")
    return value, row["organization_id"]


def pointer_record(pointer: PublicationPointer) -> dict:
    return {
        "version": pointer.version,
        "active_id": str(pointer.active.publication_id) if pointer.active else None,
        "changed_at": pointer.changed_at.isoformat() if pointer.changed_at else None,
    }


def receipt_json(receipt: PublicationReceipt) -> str:
    change = receipt.change
    return canonical(
        {
            "schema_version": "qs-ai-publication-change/v1",
            "command_id": str(receipt.command_id),
            "selector": asdict(change.current.selector),
            "previous": pointer_record(change.previous),
            "current": pointer_record(change.current),
            "action": change.action,
            "audit": asdict(change.audit),
        }
    )


async def read_pointer_record(
    db: AsyncSession, selector: ReleaseSelector, value: dict
) -> PublicationPointer:
    active = None
    if value["active_id"] is not None:
        active, _ = await load_publication(db, UUID(value["active_id"]))
    return PublicationPointer(
        selector,
        value["version"],
        active,
        datetime.fromisoformat(value["changed_at"]) if value["changed_at"] else None,
    )


async def load_receipt(db: AsyncSession, row: RowMapping) -> PublicationReceipt:
    data = json.loads(row["receipt_json"])
    selector = ReleaseSelector(**data["selector"])
    previous = await read_pointer_record(db, selector, data["previous"])
    current = await read_pointer_record(db, selector, data["current"])
    audit = data["audit"]
    change = change_publication(
        previous,
        current.active,
        data["action"],
        PublicationAudit(audit["actor"], audit["reason"], datetime.fromisoformat(audit["at"])),
        expected_version=previous.version,
        expected_active_id=previous.active.publication_id if previous.active else None,
    )
    receipt = PublicationReceipt(UUID(row["command_id"]), change)
    if (
        change.current != current
        or selector.key() != row["selector_key"]
        or current.version != row["version"]
        or change.audit.actor != f"user:{row['operator_user_id']}"
        or receipt_json(receipt) != row["receipt_json"]
    ):
        raise ValueError("Publication receipt differs from retained transition")
    scope, command = read_request(row["request_json"])
    if (
        scope.organization_id != row["organization_id"]
        or scope.operator_user_id != row["operator_user_id"]
        or command.command_id != receipt.command_id
        or command.selector != selector
        or command.action != change.action
        or command.reason != change.audit.reason
        or command.expected_version != previous.version
        or command.expected_active_id
        != (previous.active.publication_id if previous.active else None)
    ):
        raise ValueError("Publication audit differs from confirmed request")
    if isinstance(command, PublishConfiguration):
        if current.active is None or (
            command.run_id,
            command.run_version,
            command.release_fingerprint,
        ) != (
            current.active.evidence.run_id,
            current.active.evidence.run_version,
            current.active.evidence.release.fingerprint(),
        ):
            raise ValueError("Publication differs from confirmed evaluation")
    elif command.target_id != (current.active.publication_id if current.active else None):
        raise ValueError("Pointer change differs from confirmed target")
    return receipt


async def load_pointer(db: AsyncSession, row: RowMapping) -> PublicationPointer:
    selector = ReleaseSelector(**json.loads(row["selector_json"]))
    if selector.key() != row["selector_key"] or canonical(asdict(selector)) != row["selector_json"]:
        raise ValueError("Publication selector differs from index")
    pointer = await read_pointer_record(
        db,
        selector,
        {
            "version": row["version"],
            "active_id": row["active_publication_id"],
            "changed_at": row["changed_at"],
        },
    )
    if pointer.version:
        audit = (
            (
                await db.execute(
                    select(changes).where(
                        changes.c.selector_key == selector.key(),
                        changes.c.version == pointer.version,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if audit is None or (await load_receipt(db, audit)).change.current != pointer:
            raise ValueError("Publication pointer lacks matching committed audit")
    return pointer
