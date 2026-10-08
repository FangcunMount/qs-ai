"""Run the actual maintenance readers and new-facts inverse on disposable MySQL."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa

from qs_ai.maintenance.schema_refactor import backup, control
from qs_ai.maintenance.schema_refactor.conversion import manifest
from qs_ai.maintenance.schema_refactor.layouts import NEW_HEAD, OLD_HEAD, identifier
from tests.integration.test_schema_refactor_conversion import cloned as cloned
from tests.integration.test_schema_refactor_conversion import cutover
from tests.test_deployment import load

pytestmark = pytest.mark.integration


def reader(payload, schema, *arguments, source=None, fenced=False):
    url = (
        sa.make_url(os.environ["QS_AI_SCHEMA_TEST_SERVER"])
        .set(drivername="mysql+asyncmy", database=schema)
        .render_as_string(hide_password=False)
    )
    environment = {
        **os.environ,
        "QS_AI_DATABASE_URL": url,
        "PYTHONDONTWRITEBYTECODE": "1",
        "MAINTENANCE_FENCED": "1" if fenced else "0",
    }
    if source:
        environment["PYTHONPATH"] = str(source / "src")
    return subprocess.run(
        [sys.executable, "-c", payload, *arguments],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_actual_followup_probe_checks_four_identities_and_real_writer_fence(cloned):
    conn, state, journal = cloned
    cutover(conn, state, journal)
    conn.execute(sa.text("USE mysql"))
    conn.commit()
    module = load("deploy/serverA/schema_followup.py")
    payload = module.VALIDATION_READER.replace("/maintenance/binding-forward.json", str(journal))
    result = reader(payload, state["source"], fenced=True)
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout.strip().splitlines()[-1])
    assert proof["validated"] and proof["head"] == NEW_HEAD
    assert len(proof["auto_increment_columns"]) == 4
    engine = sa.create_engine(
        sa.make_url(os.environ["QS_AI_SCHEMA_TEST_SERVER"]).set(database=state["source"])
    )
    try:
        with engine.connect() as blocker:
            blocker.execute(sa.text("SELECT 1"))
            assert reader(payload, state["source"], fenced=True).returncode != 0
            assert reader(payload, state["source"], fenced=False).returncode == 0
    finally:
        engine.dispose()


def test_actual_fixture_changes_survive_inverse_and_original_runtime_reader(cloned, tmp_path):
    legacy_name = os.environ.get("QS_AI_LEGACY_SOURCE")
    if not legacy_name:
        pytest.skip("Requires the pinned original 0038 runtime source")
    legacy = Path(legacy_name)
    conn, state, journal = cloned
    cutover(conn, state, journal)
    conn.rollback()
    token = uuid4().hex[:12]
    clone = "ai_refactor_followup_" + token
    target, archive = "ai_rollback_fixture_" + token, "ai_failed_fixture_" + token
    try:
        receipt = backup.backup(conn, state["source"], tmp_path / "snapshot.gz")
        backup.restore(
            conn, state["source"], tmp_path / "snapshot.gz", clone, receipt["backup_sha256"]
        )
        conn.commit()
        module = load("deploy/serverA/schema_followup.py")
        result = reader(module.FIXTURE_WRITE, clone, token)
        assert result.returncode == 0, result.stderr
        fixture = json.loads(result.stdout.strip().splitlines()[-1])
        assert fixture["fixture"] == "passed"
        assert [fixture[k] for k in ("inserted", "updated", "deleted")] == [1, 1, 1]
        current = reader(module.FIXTURE_READ, clone, fixture["template_id"], fixture["version"])
        assert current.returncode == 0, current.stderr
        inherited = {**state, "source": clone}
        reverse = control.plan(
            conn,
            clone,
            target,
            archive,
            state["old_image"],
            state["new_image"],
            inherited=inherited,
        )
        control.prepare(conn, reverse)
        control.copy(conn, reverse, True, datetime.now(UTC).isoformat())
        conn.commit()
        control.verified(conn, reverse, True)
        assert manifest(conn, clone, NEW_HEAD) == manifest(conn, target, OLD_HEAD)
        conn.rollback()
        original = reader(
            module.FIXTURE_READ, target, fixture["template_id"], fixture["version"], source=legacy
        )
        assert original.returncode == 0, original.stderr
        assert (
            json.loads(original.stdout.strip().splitlines()[-1])["prompt_sha256"]
            == fixture["prompt_sha256"]
        )
        assert (
            json.loads(current.stdout.strip().splitlines()[-1])["prompt_sha256"]
            == fixture["prompt_sha256"]
        )
    finally:
        conn.rollback()
        for schema in (clone, target, archive):
            conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(schema)}"))
        conn.commit()
