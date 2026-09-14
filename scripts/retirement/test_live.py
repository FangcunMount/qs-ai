"""Explicit disposable database test; CI invokes this separately from application shards."""

import json
import os
from uuid import uuid4

import pymysql
import pytest
from pymongo import MongoClient
from sqlalchemy.engine import make_url

from scripts.retirement import core
from scripts.retirement.policy import COLLECTIONS, EVENTS, SHARED_TABLES
from scripts.retirement.stores import MongoStore, MySQLStore


@pytest.fixture
def stores():
    uri = os.environ["M5_TEST_MONGO_URI"]
    url = make_url(os.environ["M5_TEST_MYSQL_URL"])
    admin = pymysql.connect(
        host=url.host,
        port=url.port or 3306,
        user=url.username,
        password=url.password,
        autocommit=True,
    )
    qs, ai, mongo = ["m5_test_" + uuid4().hex for _ in range(3)]
    client = MongoClient(uri)
    with admin.cursor() as cursor:
        for database in (qs, ai):
            cursor.execute("CREATE DATABASE `" + database + "`")
        cursor.execute("USE `" + qs + "`")
        for table in SHARED_TABLES:
            cursor.execute(
                f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, event_type VARCHAR(128), "
                "payload_json LONGTEXT, created_at DATETIME(3))"
            )
            for key, event in enumerate((*EVENTS, "assessment.scored", "unrelated"), 1):
                cursor.execute(
                    f"INSERT INTO `{table}` VALUES (%s,%s,%s,NOW(3))",
                    (key, event, json.dumps({"type": event, "data": "private synthetic data"})),
                )
        cursor.execute("CREATE TABLE ai_bridge_requests (id BIGINT PRIMARY KEY, payload TEXT)")
        cursor.execute("INSERT INTO ai_bridge_requests VALUES (1,'preserve UUID and result')")
        cursor.execute("USE `" + ai + "`")
        for table in ("execution_leases", "evaluation_checkpoints", "checkpoint_leases"):
            cursor.execute(f"CREATE TABLE `{table}` (id INT PRIMARY KEY, fence INT)")
            cursor.execute(f"INSERT INTO `{table}` VALUES (1,42)")
    for collection in COLLECTIONS:
        client[mongo][collection].insert_one({"_id": "old", "payload": "synthetic private data"})
        client[mongo][collection].create_index("payload", name="legacy_lookup")
    client[mongo]["reports"].insert_one({"_id": 1, "standard_report": "keep"})
    for key, event in enumerate((*EVENTS, "assessment.scored")):
        client[mongo]["domain_event_outbox"].insert_one({"_id": key, "event_type": event})
    adapters = [
        MongoStore(uri, mongo),
        MySQLStore("qs_mysql", url.set(database=qs).render_as_string(hide_password=False)),
        MySQLStore("ai_mysql", url.set(database=ai).render_as_string(hide_password=False)),
    ]
    try:
        yield adapters
    finally:
        for adapter in adapters:
            adapter.close()
        client.drop_database(mongo)
        client.close()
        with admin.cursor() as cursor:
            for database in (qs, ai):
                cursor.execute("DROP DATABASE `" + database + "`")
        admin.close()


def test_backup_restore_and_targeted_deletion_preserve_shared_data(tmp_path, stores):
    directory = tmp_path / "backup"
    sha = core.plan(directory, stores)
    receipt = core.backup(directory, stores, sha)
    assert receipt["backup_sha256"]
    before = core.read(directory / "backup.json")["snapshots"]
    proof = dict(
        acceptance_verified=True,
        writers_stopped=True,
        old_events_drained=True,
        qs_sha="isolated-qs",
        ai_sha="isolated-ai",
        evidence_reference="local-test-only",
    )
    core.apply(directory, stores, sha, proof)
    for store in stores:
        store.verify_deleted(before[store.name])
        store.verify_restore(before[store.name])
    assert not set(COLLECTIONS) & set(stores[0].db.list_collection_names())
    for table in SHARED_TABLES:
        assert stores[1].snapshot()[table]["protected_count"] == 2
    assert stores[2].snapshot() == before["ai_mysql"]


def test_live_drift_blocks_every_database_before_first_drop(tmp_path, stores):
    directory = tmp_path / "backup"
    sha = core.plan(directory, stores)
    core.backup(directory, stores, sha)
    stores[0].db["reports"].insert_one({"_id": 2, "new_reference": "keep"})
    proof = dict(
        acceptance_verified=True,
        writers_stopped=True,
        old_events_drained=True,
        qs_sha="isolated-qs",
        ai_sha="isolated-ai",
        evidence_reference="local-test-only",
    )
    with pytest.raises(core.Stop, match="changed before deletion"):
        core.apply(directory, stores, sha, proof)
    assert set(COLLECTIONS) <= set(stores[0].db.list_collection_names())


@pytest.mark.parametrize("store_index", [0, 1])
def test_failed_partial_restore_removes_only_its_scratch_database(stores, store_index):
    import binascii
    from copy import deepcopy

    store = stores[store_index]
    original = store.snapshot()
    snapshot = deepcopy(original)
    if store_index == 0:
        name = COLLECTIONS[-1]
        snapshot[name]["rows"] = ["invalid BSON"]
        before = set(store.client.list_database_names())
    else:
        snapshot[SHARED_TABLES[-1]]["rows"][0]["insert"] = "INSERT INTO missing_table VALUES (1)"
        with store.connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            before = cursor.fetchall()
    with pytest.raises((binascii.Error, pymysql.err.ProgrammingError)):
        store.verify_restore(snapshot)
    if store_index == 0:
        assert set(store.client.list_database_names()) == before
    else:
        with store.connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            assert cursor.fetchall() == before
    assert store.snapshot() == original


def test_incoming_cross_database_reference_prevents_deletion(stores):
    qs, ai = stores[1:]
    with ai.connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE retirement_reference (id BIGINT PRIMARY KEY, "
            f"FOREIGN KEY (id) REFERENCES `{qs.database}`.domain_event_outbox(id) "
            "ON DELETE CASCADE)"
        )
        cursor.execute("INSERT INTO retirement_reference VALUES (1)")
    try:
        with pytest.raises(core.Stop, match="references"):
            qs.snapshot()
        with qs.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM domain_event_outbox")
            assert cursor.fetchone()["n"] == 8
        with ai.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM retirement_reference")
            assert cursor.fetchone()["n"] == 1
    finally:
        with ai.connection.cursor() as cursor:
            cursor.execute("DROP TABLE retirement_reference")


def seed_prompts(store):
    from scripts.retirement.prompt_policy import TEMPLATE_ID

    with store.connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE prompt_assets (template_id VARCHAR(255), version VARCHAR(128), "
            "fingerprint VARCHAR(71), package_sha256 VARCHAR(64), package_json LONGTEXT, "
            "PRIMARY KEY(template_id, version))"
        )
        cursor.execute("CREATE TABLE profile_assets (id INT PRIMARY KEY, definition_json LONGTEXT)")
        for version in ("v1", "v2", "v6"):
            cursor.execute(
                "INSERT INTO prompt_assets VALUES (%s,%s,%s,%s,%s)",
                (
                    TEMPLATE_ID,
                    version,
                    version + "-fingerprint",
                    version + "-digest",
                    json.dumps({"Ref": {"TemplateID": TEMPLATE_ID, "Version": version}}),
                ),
            )
        cursor.execute(
            "INSERT INTO profile_assets VALUES (1,%s)",
            (json.dumps({"prompt": {"id": TEMPLATE_ID, "version": "v2"}}),),
        )


def test_prompt_backup_restore_deletes_only_unreferenced_exact_versions(tmp_path, stores):
    from scripts.retirement.prompt_policy import TEMPLATE_ID

    ai = stores[2]
    seed_prompts(ai)
    before = ai.snapshot()
    selected = before["prompt_assets"]
    assert [row["id"] for row in selected["rows"]] == [[TEMPLATE_ID, "v1"]]
    assert selected["protected_count"] == 2
    assert selected["retained_candidates"] == [
        {
            "id": [TEMPLATE_ID, "v2"],
            "reason": "foreign_key_reference_or_incomplete_identity",
            "reference_tables": ["profile_assets"],
        }
    ]
    directory = tmp_path / "prompt-backup"
    sha = core.plan(directory, [ai])
    assert core.read(directory / "plan.json")["objects"]["ai_mysql"]["prompt_assets"][
        "selected_ids"
    ] == [[TEMPLATE_ID, "v1"]]
    core.backup(directory, [ai], sha)
    ai.apply(before)
    ai.verify_deleted(before)
    ai.verify_restore(before)
    with ai.connection.cursor() as cursor:
        cursor.execute("SELECT version FROM prompt_assets ORDER BY version")
        assert [row["version"] for row in cursor.fetchall()] == ["v2", "v6"]


def test_new_prompt_reference_blocks_apply_without_deleting_any_asset(stores):
    from scripts.retirement.prompt_policy import TEMPLATE_ID

    ai = stores[2]
    seed_prompts(ai)
    before = ai.snapshot()
    with ai.connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO profile_assets VALUES (2,%s)",
            (json.dumps({"prompt": {"id": TEMPLATE_ID, "version": "v1"}}),),
        )
    with pytest.raises(core.Stop, match="changed"):
        ai.apply(before)
    with ai.connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS n FROM prompt_assets")
        assert cursor.fetchone()["n"] == 3


def test_prompt_cross_database_foreign_key_preserves_all_candidates(stores):
    from scripts.retirement.prompt_policy import TEMPLATE_ID

    qs, ai = stores[1:]
    seed_prompts(ai)
    with qs.connection.cursor() as cursor:
        cursor.execute(
            "CREATE TABLE prompt_reference (template_id VARCHAR(255), version VARCHAR(128), "
            f"FOREIGN KEY (template_id, version) REFERENCES `{ai.database}`.prompt_assets "
            "(template_id, version) ON DELETE CASCADE)"
        )
        cursor.execute("INSERT INTO prompt_reference VALUES (%s,'v1')", (TEMPLATE_ID,))
    try:
        snapshot = ai.snapshot()
        assert snapshot["prompt_assets"]["action"] == "preserve"
        assert snapshot["prompt_assets"]["count"] == 0
        assert snapshot["prompt_assets"]["protected_count"] == 3
        ai.apply(snapshot)
        with qs.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM prompt_reference")
            assert cursor.fetchone()["n"] == 1
    finally:
        with qs.connection.cursor() as cursor:
            cursor.execute("DROP TABLE prompt_reference")
