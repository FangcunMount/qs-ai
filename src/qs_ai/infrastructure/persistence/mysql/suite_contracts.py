"""Suite contract bindings are immutable; absence is not an invitation to choose latest."""

import hashlib

from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.infrastructure.persistence.mysql.schema import evaluation_suites

CONTRACTS = TypeAdapter(SuiteContracts)


def encode(value: SuiteContracts) -> tuple[str, str]:
    raw = CONTRACTS.dump_json(value).decode()
    return raw, hashlib.sha256(raw.encode()).hexdigest()


def decode(raw: str | None, checksum: str | None) -> SuiteContracts:
    if not raw or len(raw.encode()) > 8192 or hashlib.sha256(raw.encode()).hexdigest() != checksum:
        raise ValueError("Suite contract binding unavailable or changed")
    value = CONTRACTS.validate_json(raw, strict=True)
    if encode(value)[0] != raw:
        raise ValueError("Noncanonical suite contract binding")
    return value


async def read(
    db: AsyncSession, reference: FrozenContractRef, organization_id: int
) -> SuiteContracts:
    row = (
        (
            await db.execute(
                select(evaluation_suites).where(
                    evaluation_suites.c.suite_id == reference.id,
                    evaluation_suites.c.suite_version == reference.version,
                    evaluation_suites.c.organization_id.in_((0, organization_id)),
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or row["fingerprint"] != reference.fingerprint:
        raise ValueError("Exact scoped suite unavailable")
    value = decode(row["contracts_json"], row["contracts_sha256"])
    if value.semantic_owner_organization_id not in (0, organization_id):
        raise ValueError("Suite semantic owner differs from caller organization")
    return value
