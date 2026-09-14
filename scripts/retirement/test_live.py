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
