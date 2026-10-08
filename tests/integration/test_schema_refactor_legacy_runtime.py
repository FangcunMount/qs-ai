"""The pinned original code reads and writes facts after a full reverse conversion."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from qs_ai.application.governance.prompt_drafts import (
    CreatePromptDraft,
    DraftScope,
    RevisePromptDraft,
)
from qs_ai.application.governance.semantic_drafts import CreateSemanticDraft, ReviseSemanticDraft
from qs_ai.bootstrap.import_evaluation_assets import baseline_assets as evaluation_baseline
from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.evaluation_asset_registry import MySQLEvaluationAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets
from qs_ai.infrastructure.persistence.mysql.semantic_drafts import MySQLSemanticDrafts
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.conversion import manifest
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier
from qs_ai.maintenance.schema_refactor.validation import require_schema

pytestmark = pytest.mark.integration
LEGACY_COMMIT = "a533afef3f390a0726ff26657c93c045d07300b7"


def canonical(value):
    return json.loads(json.dumps(asdict(value), ensure_ascii=False, default=str))


async def new_domain_facts(url):
    database = Database(url)
    transactions = Transactions(database)
    try:
        raw = json.loads(baseline_assets()[1][0].package_json)
        raw["Ref"]["TemplateID"] = "reverse-proof-" + uuid4().hex
        package = json.dumps(raw, ensure_ascii=False)
        asset = PromptAsset(
            raw["Ref"]["TemplateID"],
            raw["Ref"]["Version"],
            raw["Ref"]["Fingerprint"],
            hashlib.sha256(package.encode()).hexdigest(),
            package,
        )
        assert await MySQLPromptAssets(transactions).put(
            asset, "new-runtime-compatibility", "new-runtime-writer"
        )
        scope = DraftScope(uuid4().int % (2**60) + 1, 42)
        at = datetime.now(UTC)
        drafts = MySQLPromptDrafts(transactions)
        create = CreatePromptDraft(
            uuid4(),
            uuid4(),
            AssetReference(
                asset.template_id, asset.version, asset.fingerprint, asset.package_sha256
            ),
            asset.template_id,
            "reverse-v2",
            "新版运行时创建草稿",
        )
        first = await drafts.apply(scope, create, at)
        second = await drafts.apply(
            scope,
            RevisePromptDraft(
                first.draft_id,
                uuid4(),
                1,
                replace(first.content, system_message="新版写入，逆迁移后由原始代码读取😀"),
                "新版运行时保存第二版",
            ),
            at + timedelta(seconds=1),
        )
        source, _, semantic_prompt, schema = evaluation_baseline()
        await MySQLEvaluationAssets(transactions).put_semantic_prompt(
            semantic_prompt, source, "new-runtime-compatibility"
        )
        await MySQLSchemaAssets(transactions).put(schema, source, "new-runtime-compatibility")
        semantics = MySQLSemanticDrafts(transactions)
        semantic_create = CreateSemanticDraft(
            uuid4(),
            uuid4(),
            semantic_prompt.reference,
            load_semantic_assets().output_schema,
            0,
            "reverse-semantic-v2",
            "新版裁判草稿",
        )
        semantic_first = await semantics.apply(scope, semantic_create, at)
        semantic_second = await semantics.apply(
            scope,
            ReviseSemanticDraft(
                semantic_first.draft_id,
                uuid4(),
                1,
                semantic_first.markdown + "\n新版保存，旧版完整读取😀",
                "新版裁判第二版",
            ),
            at + timedelta(seconds=1),
        )
        return dict(
            asset=canonical(asset),
            scope=canonical(scope),
            prompt=[canonical(first), canonical(second)],
            semantic=[canonical(semantic_first), canonical(semantic_second)],
        )
    finally:
        await database.close()


LEGACY_READER = r"""
import asyncio, json, os
from dataclasses import asdict
from pathlib import Path
from uuid import UUID
import qs_ai
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions, MySQLProbe
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.prompt_drafts import MySQLPromptDrafts
from qs_ai.infrastructure.persistence.mysql.semantic_drafts import MySQLSemanticDrafts
from qs_ai.infrastructure.persistence.mysql.runtime import MySQLRuntimeReader
from qs_ai.maintenance.messaging_audit import inventory

assert Path(qs_ai.__file__).resolve().is_relative_to(
    Path(os.environ['QS_AI_LEGACY_SOURCE']).resolve() / 'src'
), 'Reader imported a different checkout'
expected = json.loads(Path(os.environ['QS_AI_LEGACY_FACTS']).read_text())
def canonical(value):
    return json.loads(json.dumps(asdict(value), ensure_ascii=False, default=str))

async def main():
    url = os.environ['QS_AI_TEST_MYSQL_DSN'].replace('mysql://', 'mysql+asyncmy://', 1)
    database = Database(url)
    transactions = Transactions(database)
    try:
        assert (await MySQLProbe(database).check()).ready
        scope = DraftScope(**expected['scope'])
        asset = expected['asset']
        assert canonical(await MySQLPromptAssets(transactions).get(
            asset['template_id'], asset['version']
        )) == asset
        prompts = MySQLPromptDrafts(transactions)
        for value in expected['prompt']:
            assert canonical(await prompts.get(
                scope, UUID(value['draft_id']), value['revision']
            )) == value
            assert canonical(await prompts.get_receipt(
                scope, UUID(value['command_id'])
            )) == value
        assert canonical(await prompts.get(
            scope, UUID(expected['prompt'][-1]['draft_id'])
        )) == expected['prompt'][-1]
        semantics = MySQLSemanticDrafts(transactions)
        for value in expected['semantic']:
            assert canonical(await semantics.get(
                scope, UUID(value['draft_id']), value['revision']
            )) == value
            assert canonical(await semantics.receipt(
                scope, UUID(value['command_id'])
            )) == value
        report = await inventory(transactions)
        assert report['schema_heads'] == ['0038_messaging_observations']
        assert report['handoff_schema_compatible'] and report['complete']
        health = await MySQLRuntimeReader(transactions, Settings(
            database_url=url, messaging={'enabled': False}
        )).health(scope)
        assert health['availability'] == 'available' and not health['partial']
        print(json.dumps(dict(legacy_readers='passed', schema_head=report['schema_heads'][0],
            prompt_revisions=2, semantic_revisions=2, runtime_health=health['availability'])))
    finally:
        await database.close()

asyncio.run(main())
"""


def test_pinned_old_runtime_reads_new_domain_facts_and_operates_after_reverse(tmp_path):
    url, legacy = os.getenv("QS_AI_SCHEMA_TEST_SERVER"), os.getenv("QS_AI_LEGACY_SOURCE")
    if not url or not legacy:
        pytest.skip("Requires disposable MySQL and the pinned original qs-ai source checkout")
    original = Path(legacy).resolve()
    identity = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=original,
        capture_output=True,
        text=True,
        check=True,
    )
    assert identity.stdout.strip() == LEGACY_COMMIT, (
        "Original checkout is not the approved baseline"
    )
    subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=original, check=True)
    environment = {
        **os.environ,
        "PYTHONPATH": str(original / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "QS_AI_LEGACY_SOURCE": str(original),
    }
    engine = sa.create_engine(url)
    suffix = uuid4().hex[:12]
    source, target, archive = (
        "qs_ai_legacy_" + suffix,
        "ai_refactor_" + suffix,
        "ai_backup_" + suffix,
    )
    state = {}
    try:
        with engine.connect() as conn:
            try:
                conn.execute(
                    sa.text(
                        f"CREATE DATABASE {identifier(source)} CHARACTER SET utf8mb4 "
                        "COLLATE utf8mb4_0900_ai_ci"
                    )
                )
                conn.execute(sa.text(f"USE {identifier(source)}"))
                config = Config("alembic.ini")
                config.attributes["connection"] = conn
                command.upgrade(config, OLD_HEAD)
                conn.commit()
                state = control.plan(
                    conn,
                    source,
                    target,
                    archive,
                    "sha256:" + "1" * 64,
                    "sha256:" + "2" * 64,
                )
                journal = tmp_path / "receipt.json"
                control.save(journal, state)
                control.prepare(conn, state, journal)
                control.copy(conn, state, True, datetime.now(UTC).isoformat(), journal)
                conn.commit()
                control.verified(conn, state, True)
                control.switch(conn, state, True, journal)
                conn.commit()
                require_schema(conn, NEW_HEAD, source)
                source_url = sa.engine.make_url(url).set(database=source)
                facts = asyncio.run(
                    new_domain_facts(
                        source_url.set(drivername="mysql+asyncmy").render_as_string(
                            hide_password=False
                        )
                    )
                )
                # Adapter writes use independent transactions; refresh the maintenance read view.
                conn.commit()
                accepted = manifest(conn, source, NEW_HEAD)
                assert accepted != state["source_manifest"]
                conn.commit()
                control.rollback(conn, state, True, datetime.now(UTC).isoformat(), journal=journal)
                conn.commit()
                require_schema(conn, OLD_HEAD, source)
                assert contracts.head(conn, source) == OLD_HEAD
                assert manifest(conn, source, OLD_HEAD) == accepted
                conn.commit()
                proof = tmp_path / "domain-facts.json"
                proof.write_text(json.dumps(facts, ensure_ascii=False))
                proof.chmod(0o600)
                environment.update(
                    QS_AI_TEST_MYSQL_DSN=source_url.set(drivername="mysql").render_as_string(
                        hide_password=False
                    ),
                    QS_AI_LEGACY_FACTS=str(proof),
                )
                reader = subprocess.run(
                    [sys.executable, "-c", LEGACY_READER],
                    cwd=original,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                assert reader.returncode == 0, reader.stdout + reader.stderr
                assert json.loads(reader.stdout)["legacy_readers"] == "passed"
                tests = [
                    "tests/integration/test_prompt_assets.py",
                    "tests/integration/test_prompt_drafts.py",
                    "tests/integration/test_semantic_drafts.py::test_create_revise_freeze_and_original_receipts",
                    "tests/integration/test_semantic_drafts.py::test_scope_cas_and_command_conflicts",
                    "tests/integration/test_semantic_drafts.py::test_invalid_placeholder_cannot_freeze_or_create_asset",
                    "tests/integration/test_evaluation_completions.py",
                    "tests/integration/test_evaluation_recovery.py",
                ]
                adapters = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        "-q",
                        "-p",
                        "no:cacheprovider",
                        "--basetemp=" + str(tmp_path / "legacy-pytest"),
                        *tests,
                    ],
                    cwd=original,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=240,
                )
                assert adapters.returncode == 0, adapters.stdout + adapters.stderr
                assert "skipped" not in adapters.stdout, adapters.stdout
                print(reader.stdout.strip())
                print(adapters.stdout.strip())
                subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=original, check=True)
            finally:
                conn.rollback()
                conn.execute(sa.text("USE mysql"))
                names = {source, target, archive}
                names.update(
                    state[key] for key in ("rollback_target", "rollback_archive") if key in state
                )
                for name in names:
                    conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(name)}"))
    finally:
        engine.dispose()
