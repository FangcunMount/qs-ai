import hashlib
import json
from dataclasses import asdict, replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import delete, insert, update

from qs_ai.application.governance.asset_references import ReferenceQuery
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.quotas import default_baseline
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.asset_references import MySQLPolicyReferences
from qs_ai.infrastructure.persistence.mysql.configuration_status import MySQLConfigurationStatus
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import configuration_publications
from tests.integration.test_evaluation_runs import setup_run as setup_run

pytestmark = pytest.mark.integration


async def test_exact_scoped_usage_of_run_and_publication(setup_run):
    tx, run_id, release = setup_run
    ids = [str(uuid4()), str(uuid4())]
    content = json.dumps({"evidence": {"release": asdict(release)}})
    async with tx.open() as db:
        await create_run(db, run_id, release, 1, "operator:42", "引用查询测试", datetime.now(UTC))
        for publication_id, org in zip(ids, (1, 2), strict=True):
            await db.execute(
                insert(configuration_publications).values(
                    publication_id=publication_id,
                    selector_key="0" * 64,
                    run_id=str(run_id),
                    run_version=1,
                    organization_id=org,
                    content_json=content,
                    content_sha256=hashlib.sha256(content.encode()).hexdigest(),
                )
            )
        await db.commit()
    try:
        store = MySQLPolicyReferences(tx)
        scope = DraftScope(1, 42)
        query = ReferenceQuery("execution_policy", release.execution_policy, "evaluation")
        assert str(run_id) in [v["identity"] for v in (await store.get(scope, query))["items"]]
        assert str(run_id) not in [
            v["identity"] for v in (await store.get(DraftScope(2, 43), query))["items"]
        ]
        page = await store.get(scope, replace(query, usage_kind="publication"))
        assert ids[0] in [v["identity"] for v in page["items"]]
        assert ids[1] not in [v["identity"] for v in page["items"]]
        with pytest.raises(ValueError):
            await store.get(
                scope,
                replace(
                    query, reference=replace(query.reference, fingerprint="sha256:" + "0" * 64)
                ),
            )
        async with tx.open() as db:
            await db.execute(
                update(configuration_publications)
                .where(configuration_publications.c.publication_id == ids[0])
                .values(content_sha256="0" * 64)
            )
            await db.commit()
        with pytest.raises(ValueError, match="checksum mismatch"):
            await store.get(scope, replace(query, usage_kind="publication"))
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(configuration_publications).where(
                    configuration_publications.c.publication_id.in_(ids)
                )
            )
            await db.commit()


async def test_status_is_scoped_and_contains_only_version_metadata(setup_run):
    tx, _, _ = setup_run
    value = await MySQLConfigurationStatus(tx, Settings(), default_baseline()).get(
        DraftScope(1, 42)
    )
    assert value["organization_id"] == 1
    categories = {item["category"]: item for item in value["categories"]}
    assert categories["evaluation_policies"]["editable"] is False
    assert categories["evaluation_policies"]["asset_count"] >= 2
    assert categories["organization_quotas"]["applies_to"] == "next_admission_or_slot"
    assert "definition_json" not in json.dumps(value)
