"""Real asset/Run transactions; model replies and report cases remain synthetic."""

import hashlib
import json
from dataclasses import replace

import pytest
from sqlalchemy import delete, select, update

from qs_ai.bootstrap.import_schemas import baseline_assets
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_dispatches,
    evaluation_runs,
    profile_assets,
    prompt_assets,
    route_assets,
    schema_assets,
)
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_evaluation_step import AT, Gateway, step
from tests.integration.test_evaluation_step import ready as ready

pytestmark = pytest.mark.integration


@pytest.fixture
async def frozen_creation(persisted_assets, monkeypatch):
    from tests.integration import test_evaluation_step

    async def create(db, run_id, release):
        schema = baseline_assets()[1][0]
        release = replace(
            release,
            input_schema=FrozenContractRef(
                schema.schema_id, schema.schema_id + "/" + schema.version, schema.fingerprint
            ),
        )
        _, manifest = await generation_snapshot(db, release)
        return await create_run(
            db, run_id, release, 1, "actor:1", "冻结评测资产", AT, generation_manifest=manifest
        )

    monkeypatch.setattr(test_evaluation_step, "create", create)


@pytest.fixture
async def frozen_ready(frozen_creation, ready):
    return ready


def forbid_legacy(monkeypatch):
    from qs_ai.infrastructure.qs_server import evaluation_case

    def fail(*args, **kwargs):
        raise AssertionError("Manifest-bound execution loaded a legacy file")

    monkeypatch.setattr(evaluation_case, "load_prompt", fail)
    monkeypatch.setattr(evaluation_case, "load_migrated_release", fail)
    from qs_ai.infrastructure.qs_server import routes

    monkeypatch.setattr(routes, "load_route", fail)


async def test_generation_acceptance_and_semantic_use_stored_assets(frozen_ready, monkeypatch):
    forbid_legacy(monkeypatch)
    gateway = Gateway(frozen_ready)
    state = await step(frozen_ready, gateway)
    state = await step(frozen_ready, gateway, state.version)
    assert state.version == 9 and state.checkpoint is None and gateway.calls == 2


async def alter_prompt(tx):
    async with tx.open() as db:
        row = (
            (await db.execute(select(prompt_assets).where(prompt_assets.c.version == "v6")))
            .mappings()
            .one()
        )
        raw = row["package_json"] + " "
        await db.execute(
            update(prompt_assets)
            .where(prompt_assets.c.version == "v6")
            .values(package_json=raw, package_sha256=hashlib.sha256(raw.encode()).hexdigest())
        )
        await db.commit()


@pytest.mark.parametrize(
    "damage",
    [
        "prompt",
        "profile",
        "route",
        "schema",
        "package_bytes",
        "manifest_json",
        "manifest_fingerprint",
        "manifest_half",
        "manifest_missing",
        "suite",
        "release_fingerprint",
    ],
)
async def test_invalid_or_missing_frozen_assets_never_dispatch(frozen_ready, monkeypatch, damage):
    tx, run_id, *_ = frozen_ready
    forbid_legacy(monkeypatch)
    if damage == "package_bytes":
        await alter_prompt(tx)
    else:
        async with tx.open() as db:
            tables = {
                "prompt": prompt_assets,
                "profile": profile_assets,
                "route": route_assets,
                "schema": schema_assets,
            }
            if damage in tables:
                await db.execute(delete(tables[damage]))
            else:
                raw = await db.scalar(
                    select(evaluation_runs.c.definition_json).where(
                        evaluation_runs.c.run_id == str(run_id)
                    )
                )
                doc = json.loads(raw)
                if damage == "manifest_missing":
                    del doc["generation_manifest_json"]
                    del doc["generation_manifest_fingerprint"]
                elif damage == "manifest_half":
                    del doc["generation_manifest_json"]
                elif damage == "manifest_json":
                    doc["generation_manifest_json"] += " "
                elif damage == "manifest_fingerprint":
                    doc["generation_manifest_fingerprint"] = "sha256:" + "0" * 64
                elif damage == "suite":
                    doc["suite_json"] += " "
                else:
                    doc["release_fingerprint"] = "sha256:" + "0" * 64
                await db.execute(
                    update(evaluation_runs)
                    .where(evaluation_runs.c.run_id == str(run_id))
                    .values(definition_json=json.dumps(doc))
                )
            await db.commit()
    gateway = Gateway(frozen_ready)
    with pytest.raises(ValueError):
        await step(frozen_ready, gateway)
    assert gateway.calls == 0
    assert (await rows(tx, run_id))[2]["version"] == 3
    async with tx.open() as db:
        assert not (
            await db.execute(
                select(evaluation_dispatches).where(evaluation_dispatches.c.run_id == str(run_id))
            )
        ).all()


async def test_asset_change_after_dispatch_cannot_be_accepted(frozen_ready):
    tx, run_id, *_ = frozen_ready

    class Mutating(Gateway):
        async def generate_messages(self, *args):
            response = await super().generate_messages(*args)
            await alter_prompt(tx)
            return response

    gateway = Mutating(frozen_ready)
    with pytest.raises(ValueError):
        await step(frozen_ready, gateway)
    checkpoint = (await rows(tx, run_id))[2]
    assert gateway.calls == 1 and checkpoint["version"] == 5
    assert checkpoint["checkpoint_json"]["phase"] == "dispatching"
