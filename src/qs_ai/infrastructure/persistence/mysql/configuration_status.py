"""Public configuration projection: never hash or serialize secret-bearing Settings."""

import hashlib
import json
from typing import Any

from sqlalchemy import select

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import QuotaBaseline
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.quotas import effective
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_policy_assets,
    evaluation_suites,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
    semantic_prompt_assets,
)


def fingerprint(value: Any) -> str:
    return (
        "sha256:"
        + hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )


def deployment_status(settings: Settings, baseline: QuotaBaseline) -> list[dict[str, Any]]:
    transport = {
        "grpc_enabled": True,
        "governance_enabled": settings.grpc.governance_enabled,
        "tls_configured": all(
            (settings.grpc.ca_file, settings.grpc.cert_file, settings.grpc.key_file)
        ),
        "access_configured": bool(settings.grpc.access_address),
        "delivery_configured": bool(settings.grpc.result_address),
    }
    public = {
        "database_pool": settings.database.model_dump(),
        "communication_tls": transport,
        "execution_lifecycle": {
            "worker": settings.worker.model_dump(),
            "evaluation": settings.evaluation.model_dump(),
            "delivery": settings.delivery.model_dump(),
            "generation_enabled": settings.generation.enabled,
            "grpc_shutdown_seconds": settings.grpc.shutdown_grace_seconds,
        },
        "governance_models": list(settings.governance_models),
        "quota_baseline": baseline.resolve(None, 0)["defaults"],
        "quota_ceilings": baseline.resolve(None, 0)["ceilings"],
    }
    return [
        {
            "category": name,
            "source": "deployment",
            "version": fingerprint(value),
            "version_scope": "public_fields_only",
            "applies_to": "restart",
            "values": value,
        }
        for name, value in public.items()
    ]


class MySQLConfigurationStatus:
    def __init__(
        self, transactions: Transactions, settings: Settings, baseline: QuotaBaseline
    ) -> None:
        self.transactions, self.settings, self.baseline = transactions, settings, baseline

    async def get(self, scope: DraftScope) -> dict[str, Any]:
        categories = deployment_status(self.settings, self.baseline)
        async with self.transactions.open() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            quota = await effective(db, scope.organization_id, self.baseline)
            categories.append(
                {
                    "category": "organization_quotas",
                    "source": quota["source"],
                    "version": quota["revision"],
                    "applies_to": "next_admission_or_slot",
                    "values": quota,
                }
            )
            for name, table in (
                ("profiles", profile_assets),
                ("prompts", prompt_assets),
                ("model_routes", route_assets),
                ("protocol_schemas", schema_assets),
                ("evaluation_suites", evaluation_suites),
                ("evaluation_policies", evaluation_policy_assets),
                ("semantic_prompts", semantic_prompt_assets),
            ):
                keys = list(table.primary_key.columns)
                stmt = select(*keys, table.c.fingerprint).order_by(*keys)
                if "organization_id" in table.c:
                    stmt = stmt.where(table.c.organization_id.in_((0, scope.organization_id)))
                digest, count = hashlib.sha256(), 0
                rows = await db.stream(stmt)
                async for row in rows:
                    digest.update(json.dumps(list(row), separators=(",", ":")).encode() + b"\n")
                    count += 1
                categories.append(
                    {
                        "category": name,
                        "source": "mysql",
                        "version": "sha256:" + digest.hexdigest(),
                        "version_scope": "visible_immutable_asset_inventory",
                        "asset_count": count,
                        "applies_to": "frozen_evaluation_or_published_task",
                        "editable": name not in ("evaluation_policies", "protocol_schemas"),
                    }
                )
        return {"organization_id": scope.organization_id, "categories": categories}
