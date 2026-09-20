"""One controlled, atomic initialization of original suite bytes and provable bindings."""

import argparse
import asyncio
import hashlib
import json

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.bootstrap.import_evaluation_assets import baseline_assets
from qs_ai.config import Settings
from qs_ai.domain.evaluation.assets import PolicyKind
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.evaluation.suite_contracts import SuiteContracts
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import AssetSnapshotReader
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import (
    read_policy,
)
from qs_ai.infrastructure.persistence.mysql.evaluation_suites import (
    decode_record,
    load_registered_suite,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_runs,
    evaluation_suites,
    schema_assets,
    semantic_prompt_assets,
)
from qs_ai.infrastructure.persistence.mysql.suite_contracts import decode, encode
from qs_ai.infrastructure.qs_server.evaluation_suite import V6_PUBLISHED, FrozenSuite, load_suite
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


def baseline() -> tuple[str, FrozenSuite, SuiteContracts]:
    source, policies, semantic, _ = baseline_assets()
    suite = load_suite(V6_PUBLISHED)
    refs = {p.kind.value: p.reference for p in policies}
    contracts = SuiteContracts(
        refs["execution"],
        refs["gate"],
        semantic.reference,
        load_semantic_assets().output_schema,
    )
    return source, suite, contracts


async def install_baseline(
    db: AsyncSession, source: str, suite: FrozenSuite, contracts: SuiteContracts, imported_by: str
) -> bool:
    raw, checksum = encode(contracts)
    row = (
        (
            await db.execute(
                select(evaluation_suites).where(
                    evaluation_suites.c.suite_id == suite.reference.id,
                    evaluation_suites.c.suite_version == suite.reference.version,
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is not None:
        if (
            row["definition_json"],
            row["fingerprint"],
            row["organization_id"],
            row["contracts_json"],
            row["contracts_sha256"],
        ) != (suite.definition_json, suite.reference.fingerprint, 0, raw, checksum):
            raise ValueError("Original initialized suite differs")
        return False
    await db.execute(
        insert(evaluation_suites).values(
            suite_id=suite.reference.id,
            suite_version=suite.reference.version,
            fingerprint=suite.reference.fingerprint,
            definition_json=suite.definition_json,
            organization_id=0,
            source_ref=source,
            imported_by=imported_by,
            contracts_json=raw,
            contracts_sha256=checksum,
        )
    )
    return True


async def run(imported_by: str) -> dict[str, int]:
    if not imported_by.strip() or len(imported_by) > 128:
        raise ValueError("Initializer identity required")
    source, baseline_suite, contracts = await asyncio.to_thread(baseline)
    raw_contracts, checksum = encode(contracts)
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database required")
    database = Database(settings.database_url.get_secret_value())
    try:
        async with Transactions(database).open() as db:
            await db.connection(execution_options={"isolation_level": "SERIALIZABLE"})
            await read_policy(db, PolicyKind.EXECUTION, contracts.execution_policy)
            await read_policy(db, PolicyKind.GATE, contracts.gate_policy)
            prompt = (
                await db.execute(
                    select(semantic_prompt_assets.c.markdown).where(
                        semantic_prompt_assets.c.organization_id == 0,
                        semantic_prompt_assets.c.asset_id == contracts.semantic_prompt.id,
                        semantic_prompt_assets.c.version == contracts.semantic_prompt.version,
                        semantic_prompt_assets.c.fingerprint
                        == contracts.semantic_prompt.fingerprint,
                    )
                )
            ).scalar_one_or_none()
            if (
                prompt is None
                or "sha256:" + hashlib.sha256(prompt.encode()).hexdigest()
                != contracts.semantic_prompt.fingerprint
            ):
                raise ValueError("Initialize original semantic prompt before suites")
            schema_id, schema_version = contracts.semantic_output_schema.version.rsplit("/", 1)
            schema = await AssetSnapshotReader(db, schema_assets, SchemaAsset).get(
                schema_id, schema_version
            )
            if schema is None or schema.fingerprint != contracts.semantic_output_schema.fingerprint:
                raise ValueError("Initialize original semantic schema before suites")
            rows = (await db.execute(select(evaluation_suites).with_for_update())).mappings().all()
            runs = (await db.execute(select(evaluation_runs.c.definition_json))).scalars().all()
            inserted = int(
                await install_baseline(db, source, baseline_suite, contracts, imported_by)
            )
            # Existing evidence must agree before the original static source can be bound.
            for raw in runs:
                creation = json.loads(raw)
                suite_ref = FrozenContractRef(**creation["release"]["suite"])
                known = suite_ref == baseline_suite.reference or any(
                    (r["suite_id"], r["suite_version"], r["fingerprint"])
                    == (suite_ref.id, suite_ref.version, suite_ref.fingerprint)
                    for r in rows
                )
                if not known:
                    raise ValueError("Run references an unaccounted suite")
                existing = next(
                    (
                        r
                        for r in rows
                        if (r["suite_id"], r["suite_version"], r["fingerprint"])
                        == (suite_ref.id, suite_ref.version, suite_ref.fingerprint)
                    ),
                    None,
                )
                binding = (
                    contracts
                    if existing is None or existing["contracts_json"] is None
                    else decode(existing["contracts_json"], existing["contracts_sha256"])
                )
                wanted = json.loads(encode(binding)[0])
                for field in (
                    "execution_policy",
                    "gate_policy",
                    "semantic_prompt",
                    "semantic_output_schema",
                ):
                    if creation["release"][field] != wanted[field]:
                        raise ValueError("Historical Run requires a different contract binding")
            bound = 0
            for row in rows:
                reference = FrozenContractRef(
                    row["suite_id"], row["suite_version"], row["fingerprint"]
                )
                if row["contracts_json"] is not None:
                    decode(row["contracts_json"], row["contracts_sha256"])
                    if reference != baseline_suite.reference:
                        await load_registered_suite(
                            db, reference, organization_id=row["organization_id"]
                        )
                    continue
                if reference == baseline_suite.reference:
                    if (
                        row["definition_json"] != baseline_suite.definition_json
                        or row["organization_id"] != 0
                    ):
                        raise ValueError("Original suite source collides with another asset")
                else:
                    # The original validator verifies every inherited byte against its fixed source.
                    await asyncio.to_thread(decode_record, row, baseline_suite)
                await db.execute(
                    update(evaluation_suites)
                    .where(
                        evaluation_suites.c.suite_id == row["suite_id"],
                        evaluation_suites.c.suite_version == row["suite_version"],
                    )
                    .values(contracts_json=raw_contracts, contracts_sha256=checksum)
                )
                bound += 1
            await db.commit()
            return {"inserted": inserted, "bindings_added": bound, "runs_checked": len(runs)}
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--imported-by", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(run(args.imported_by))
    except Exception as error:
        print(json.dumps({"import": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"import": "complete", "activated": False, **result}))


if __name__ == "__main__":
    main()
