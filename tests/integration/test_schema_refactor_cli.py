"""Exercise the shipped maintenance entrypoint with an isolated disposable schema."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.layouts import OLD_HEAD, identifier, qualified
from qs_ai.maintenance.schema_refactor.validation import require_schema

pytestmark = pytest.mark.integration


def test_cli_cutover_and_reverse_preserve_post_start_facts(tmp_path):
    url = os.environ.get("QS_AI_SCHEMA_TEST_SERVER")
    if not url:
        pytest.skip("Requires an explicitly disposable schema-refactor MySQL server")
    engine = sa.create_engine(url)
    suffix = uuid4().hex[:10]
    source, target, archive = (
        "qs_ai_cli_" + suffix,
        "ai_refactor_cli_" + suffix,
        "ai_backup_cli_" + suffix,
    )
    journal = tmp_path / "receipt.json"
    env = {
        **os.environ,
        "QS_AI_DATABASE_URL": sa.make_url(url)
        .set(drivername="mysql+asyncmy")
        .render_as_string(hide_password=False),
    }

    def invoke(name, *args):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "qs_ai.maintenance.schema_refactor",
                name,
                "--journal",
                str(journal),
                *args,
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    try:
        with engine.connect() as conn:
            contracts.create_schema(conn, source, "mysql")
            conn.execute(sa.text(f"USE {identifier(source)}"))
            config = Config("alembic.ini")
            config.attributes["connection"] = conn
            command.upgrade(config, OLD_HEAD)
            conn.commit()
        # Release every fixture connection before the real writer-fencing checks.
        engine.dispose()
        invoke(
            "plan",
            "--source",
            source,
            "--target",
            target,
            "--archive",
            archive,
            "--old-image",
            "sha256:" + "1" * 64,
            "--new-image",
            "sha256:" + "2" * 64,
        )
        invoke("prepare")
        stopped_at = datetime.now(UTC).isoformat()
        invoke("copy", "--writers-stopped", "--stopped-at", stopped_at)
        invoke("verify", "--writers-stopped")
        assert invoke("switch", "--writers-stopped")["phase"] == "switched"
        with engine.connect() as conn:
            conn.execute(
                sa.text(
                    f"UPDATE {qualified(source, 'messaging_observations')} "
                    "SET recorded_count=recorded_count+17"
                )
            )
            conn.commit()
        engine.dispose()
        assert (
            invoke("rollback", "--writers-stopped", "--stopped-at", datetime.now(UTC).isoformat())[
                "phase"
            ]
            == "rolled_back"
        )
        with engine.connect() as conn:
            state = control.read(journal)
            conn.info["source_column_collations"] = state["source_column_collations"]
            require_schema(conn, OLD_HEAD, source)
            assert (
                conn.scalar(
                    sa.text(
                        "SELECT SUM(recorded_count) FROM "
                        f"{qualified(source, 'ai_messaging_observations')}"
                    )
                )
                == 8 * 17
            )
        assert journal.stat().st_mode & 0o777 == 0o600
        assert "password" not in journal.read_text() and "mysql+" not in journal.read_text()
    finally:
        with engine.connect() as conn:
            conn.execute(sa.text("USE mysql"))
            state = control.read(journal) if journal.exists() else {}
            names = {source, target, archive, *state.get("owned_schemas", [])}
            for name in names:
                conn.execute(sa.text(f"DROP DATABASE IF EXISTS {identifier(name)}"))
        engine.dispose()
