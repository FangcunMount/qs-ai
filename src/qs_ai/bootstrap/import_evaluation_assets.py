"""Initialize exact evaluation assets without approving, publishing or changing active releases."""

import argparse
import asyncio
import json

from qs_ai.config import Settings
from qs_ai.domain.evaluation.assets import PolicyAsset, PolicyKind, SemanticPromptAsset
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import MySQLEvaluationAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.qs_server.evaluation_policies import (
    evaluation_directory,
    load_execution_policy,
    load_gate_policy,
)
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


def baseline_assets() -> tuple[str, tuple[PolicyAsset, ...], SemanticPromptAsset, SchemaAsset]:
    manifest = json.loads((evaluation_directory() / "manifest.json").read_bytes())
    execution, gate, semantic = load_execution_policy(), load_gate_policy(), load_semantic_assets()
    schema_id, version = semantic.output_schema.version.rsplit("/", 1)
    return (
        f"qs-server:{manifest['commit']}",
        (
            PolicyAsset(
                PolicyKind.EXECUTION,
                FrozenContractRef(execution.policy_id, execution.version, execution.fingerprint),
                execution.definition_json,
            ),
            PolicyAsset(PolicyKind.GATE, gate.reference, gate.definition_json),
        ),
        SemanticPromptAsset(semantic.prompt, semantic.prompt_markdown),
        SchemaAsset(
            schema_id, version, semantic.output_schema.fingerprint, semantic.output_schema_json
        ),
    )


async def run(imported_by: str) -> int:
    source, policies, prompt, schema = await asyncio.to_thread(baseline_assets)
    settings = Settings()
    if settings.database_url is None:
        raise ValueError("Database is not configured")
    database = Database(settings.database_url.get_secret_value())
    try:
        transactions = Transactions(database)
        store = MySQLEvaluationAssets(transactions)
        inserted = 0
        for policy in policies:
            inserted += await store.put_policy(policy, source, imported_by)
        inserted += await store.put_semantic_prompt(prompt, source, imported_by)
        inserted += await MySQLSchemaAssets(transactions).put(schema, source, imported_by)
        return inserted
    finally:
        await database.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--imported-by", required=True)
    args = parser.parse_args()
    try:
        inserted = asyncio.run(run(args.imported_by))
    except Exception as error:
        print(json.dumps({"import": "failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({"import": "complete", "inserted": inserted, "activated": False}))


if __name__ == "__main__":
    main()
