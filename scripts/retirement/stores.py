"""Fixed-scope synchronous maintenance adapters. Credentials never enter artifacts."""

import base64
import hashlib
import re
from uuid import uuid4

from scripts.retirement.core import Stop, canonical, digest
from scripts.retirement.policy import COLLECTIONS, SHARED_TABLES, TECHNICAL_TABLES, old_event
from scripts.retirement.prompt_policy import candidate, reference_tables, unreferenced


class ProtectedRows:
    """Hash a deterministically ordered JSON array without retaining its rows."""

    def __init__(self):
        self.count = 0
        self.hash = hashlib.sha256(b"[")

    def append(self, row):
        if self.count:
            self.hash.update(b",")
        self.hash.update(canonical(row))
        self.count += 1

    def hexdigest(self):
        result = self.hash.copy()
        result.update(b"]")
        return result.hexdigest()


def record(action, rows, protected, metadata):
    return {
        "action": action,
        "rows": rows,
        "count": len(rows),
        "sha256": digest(rows),
        "protected_count": protected.count,
        "protected_sha256": protected.hexdigest(),
        "metadata": metadata,
    }


def compare_remaining(before, after):
    expected_names = {name for name, item in before.items() if item["action"] != "drop"}
    if set(after) != expected_names:
        raise Stop("unexpected database objects after application")
    for name in expected_names:
        old, new = before[name], after[name]
        if (
            new["count"]
            or old["protected_count"] != new["protected_count"]
            or old["protected_sha256"] != new["protected_sha256"]
            or old["metadata"] != new["metadata"]
        ):
            raise Stop("retirement incomplete or protected data/schema changed")


class MongoStore:
    name = "qs_mongo"

    def __init__(self, uri, database, *, restore_uri=None):
        from pymongo import MongoClient
        from pymongo.uri_parser import parse_uri

        self.client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        self.db = self.client[database]
        self.restore_uri = restore_uri
        parsed = parse_uri(uri)
        self.identity = digest({"nodes": parsed["nodelist"], "database": database})

    def snapshot(self):
        collections = sorted(self.db.list_collections(), key=lambda item: item["name"])
        # Validate before reading large business collections. The reviewed profiler
        # collection is protected; it is never a drop/restore target.
        for info in collections:
            if info["name"].startswith("system.") and info["name"] != "system.profile":
                raise Stop("system collection requires a separate reviewed inventory")
        if any(info["name"] == "system.profile" for info in collections):
            if self.db.command("profile", -1)["was"] != 0:
                raise Stop("Mongo profiling must be paused before stable inventory")
        return {info["name"]: self._snapshot_collection(info) for info in collections}

    def _snapshot_collection(self, info):
        from bson import BSON

        name = info["name"]
        collection = self.db[name]
        action = "drop" if name in COLLECTIONS else "preserve"
        if name == "domain_event_outbox":
            action = "delete_rows"
        selected, protected = [], ProtectedRows()
        order = "$natural" if name == "system.profile" else "_id"
        for row in collection.find().sort(order, 1):
            encoded = base64.b64encode(BSON.encode(row)).decode()
            target = action == "drop" or (action == "delete_rows" and old_event(row))
            (selected if target else protected).append(encoded)
        metadata = {
            "options": info.get("options", {}),
            "indexes": [dict(index) for index in collection.list_indexes()],
        }
        # Canonical BSON preserves options containing BSON-only values too.
        metadata = base64.b64encode(BSON.encode(metadata)).decode()
        return record(action, selected, protected, metadata)

    def restore(self, snapshot, database):
        from bson import BSON

        if database in self.client.list_database_names():
            raise Stop("restore destination must be absent")
        if not re.fullmatch(r"m5_restore_[a-f0-9]{32}", database):
            raise Stop("restore destination must be an isolated generated database")
        db = self.client[database]
        try:
            for name, item in snapshot.items():
                if item["action"] == "preserve":
                    continue
                metadata = BSON(base64.b64decode(item["metadata"])).decode()
                collection = db.create_collection(name, **metadata["options"])
                rows = [BSON(base64.b64decode(row)).decode() for row in item["rows"]]
                if rows:
                    collection.insert_many(rows)
                for index in metadata["indexes"]:
                    if index["name"] == "_id_":
                        continue
                    index = dict(index)
                    keys = list(index.pop("key").items())
                    index.pop("v", None)
                    index.pop("ns", None)
                    collection.create_index(keys, **index)
        except BaseException:
            self.client.drop_database(database)
            raise
        return db

    def verify_restore(self, snapshot, *, keep=False):
        if self.restore_uri:
            target = MongoStore(self.restore_uri, self.db.name)
            try:
                return target.verify_restore(snapshot, keep=keep)
            finally:
                target.close()
        name = "m5_restore_" + uuid4().hex
        restored = self.restore(snapshot, name)
        original = self.db
        verified = False
        try:
            self.db = restored
            actual = self.snapshot()
            for key, item in snapshot.items():
                if item["action"] == "preserve":
                    continue
                for field in ("count", "sha256", "metadata"):
                    if actual[key][field] != item[field]:
                        raise Stop("Mongo restoration verification failed")
            verified = True
        finally:
            self.db = original
            if not keep or not verified:
                self.client.drop_database(name)
        return {"database": name, "target": self.identity, "kept": keep}

    def apply(self, snapshot):
        from bson import BSON

        # M5 requires stopped writers. Mongo collection drops are not transactional.
        # Each successful drop remains recoverable from the verified BSON archive.
        for name, item in snapshot.items():
            if item["action"] == "preserve":
                continue
            if name not in COLLECTIONS and name != "domain_event_outbox":
                raise Stop("Mongo target outside fixed whitelist")
            matches = list(self.db.list_collections(filter={"name": name}))
            current = self._snapshot_collection(matches[0]) if matches else None
            if current != item:
                raise Stop("Mongo object changed immediately before deletion")
            if item["action"] == "drop":
                self.db.drop_collection(name)
            else:
                for raw in item["rows"]:
                    row = BSON(base64.b64decode(raw)).decode()
                    # Full document equality avoids deleting a concurrently changed message.
                    result = self.db[name].delete_one(
                        {"$expr": {"$eq": ["$$ROOT", {"$literal": row}]}}
                    )
                    if result.deleted_count != 1:
                        raise Stop("shared Mongo message changed; inspect partial application")

    def verify_deleted(self, snapshot):
        compare_remaining(snapshot, self.snapshot())

    def close(self):
        self.client.close()


def quoted(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise Stop("unsupported SQL identifier")
    return "`" + name + "`"


class MySQLStore:
    def __init__(self, name, url, *, restore_url=None):
        import pymysql
        from sqlalchemy.engine import make_url

        parsed = make_url(url)
        if parsed.query:
            raise Stop("SQL URL options are unsupported; use a protected tunnel for TLS")
        if parsed.get_backend_name() != "mysql" or not parsed.database:
            raise Stop("explicit MySQL database required")
        self.name, self.database = name, parsed.database
        self.restore_url = restore_url
        self.connection = pymysql.connect(
            host=parsed.host,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=parsed.database,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10,
            read_timeout=120,
            cursorclass=pymysql.cursors.DictCursor,
        )
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT @@server_uuid AS server")
            server = cursor.fetchone()["server"]
        self.identity = digest({"server": server, "database": self.database})

    def snapshot(self):
        result = {}
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=%s ORDER BY TABLE_NAME",
                (self.database,),
            )
            names = [row["TABLE_NAME"] for row in cursor.fetchall()]
            tables = {}
            if self.name == "ai_mysql":
                # Native asset reference auditing is confined to the small AI store.
                # Never load the multi-GB QS store into memory.
                for name in names:
                    cursor.execute("SELECT * FROM " + quoted(name))
                    tables[name] = list(cursor.fetchall())
            prompt_keys = (
                unreferenced(tables.get("prompt_assets", []), tables)
                if self.name == "ai_mysql"
                else set()
            )
            for name in names:
                cursor.execute("SHOW CREATE TABLE " + quoted(name))
                ddl = cursor.fetchone().get("Create Table")
                if ddl is None:
                    raise Stop("SQL views require separate review")
                cursor.execute(
                    "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, "
                    "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                    "FROM information_schema.KEY_COLUMN_USAGE "
                    "WHERE REFERENCED_TABLE_NAME IS NOT NULL AND "
                    "((TABLE_SCHEMA=%s AND TABLE_NAME=%s) OR "
                    "(REFERENCED_TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME=%s)) "
                    "ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME",
                    (self.database, name, self.database, name),
                )
                references = list(cursor.fetchall())
                cursor.execute(
                    "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                    "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
                    "ORDER BY ORDINAL_POSITION",
                    (self.database, name),
                )
                keys = [row["COLUMN_NAME"] for row in cursor.fetchall()]
                if not keys:
                    raise Stop("SQL inventory requires a stable primary key")
                action = (
                    "delete_rows"
                    if (self.name == "qs_mysql" and name in SHARED_TABLES)
                    or (self.name == "ai_mysql" and name == "prompt_assets" and not references)
                    else "preserve"
                )
                selected, protected = [], ProtectedRows()
                retained = []
                statement = None
                for row in self.rows(name, keys):
                    columns = list(row)
                    if statement is None:
                        statement = (
                            "INSERT INTO "
                            + quoted(name)
                            + " ("
                            + ",".join(quoted(col) for col in columns)
                            + ") VALUES ("
                            + ",".join(["%s"] * len(columns))
                            + ")"
                        )
                    encoded = cursor.mogrify(statement, tuple(row.values()))
                    prompt = self.name == "ai_mysql" and name == "prompt_assets"
                    identity = (
                        [row.get("template_id"), row.get("version")] if prompt else row.get("id")
                    )
                    target = action == "delete_rows" and (
                        tuple(identity) in prompt_keys
                        if prompt
                        else old_event(row, payload=name != "domain_event_outbox")
                    )
                    if prompt and candidate(row) and not target:
                        retained.append(
                            {
                                "id": identity,
                                "reason": "foreign_key_reference_or_incomplete_identity",
                                "reference_tables": reference_tables(row, tables),
                            }
                        )
                    if target:
                        if references or (not prompt and not isinstance(identity, int)):
                            raise Stop(
                                "selected SQL records have references or unsupported identity"
                            )
                        selected.append({"id": identity, "insert": encoded})
                    else:
                        protected.append(encoded)
                selected.sort(key=lambda row: row["id"])
                result[name] = record(
                    action,
                    selected,
                    protected,
                    {
                        "ddl": ddl,
                        "references": references,
                        "technical_inventory_only": name in TECHNICAL_TABLES,
                    },
                )
                result[name]["retained_candidates"] = retained
        return result

    def rows(self, table, keys):
        from pymysql.cursors import SSDictCursor

        with self.connection.cursor(SSDictCursor) as stream:
            stream.execute(
                "SELECT * FROM " + quoted(table) + " ORDER BY " + ",".join(map(quoted, keys))
            )
            yield from stream

    def restore(self, snapshot, database):
        if not re.fullmatch(r"m5_restore_[a-f0-9]{32}", database):
            raise Stop("restore destination must be an isolated generated database")
        with self.connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE " + quoted(database) + " CHARACTER SET utf8mb4")
            try:
                cursor.execute("USE " + quoted(database))
                for item in snapshot.values():
                    if item["action"] == "preserve":
                        continue
                    cursor.execute(item["metadata"]["ddl"])
                    for row in item["rows"]:
                        cursor.execute(row["insert"])
            except BaseException:
                cursor.execute("DROP DATABASE " + quoted(database))
                raise
            finally:
                cursor.execute("USE " + quoted(self.database))

    def verify_restore(self, snapshot, *, keep=False):
        if self.restore_url:
            target = MySQLStore(self.name, self.restore_url)
            try:
                return target.verify_restore(snapshot, keep=keep)
            finally:
                target.close()
        name = "m5_restore_" + uuid4().hex
        self.restore(snapshot, name)
        original = self.database
        verified = False
        try:
            self.database = name
            with self.connection.cursor() as cursor:
                cursor.execute("USE " + quoted(name))
            actual = self.snapshot()
            for key, item in snapshot.items():
                if item["action"] == "preserve":
                    continue
                for field in ("count", "sha256", "metadata"):
                    if actual[key][field] != item[field]:
                        raise Stop("SQL restoration verification failed")
            verified = True
        finally:
            self.database = original
            with self.connection.cursor() as cursor:
                cursor.execute("USE " + quoted(original))
                if not keep or not verified:
                    cursor.execute("DROP DATABASE " + quoted(name))
        return {"database": name, "target": self.identity, "kept": keep}

    def apply(self, snapshot):
        self.connection.begin()
        try:
            with self.connection.cursor() as cursor:
                # Lock all AI reference tables; keep the full maintenance write pause too.
                lock_names = snapshot if self.name == "ai_mysql" else SHARED_TABLES
                for name in sorted(lock_names):
                    if name in snapshot:
                        from pymysql.cursors import SSCursor

                        with self.connection.cursor(SSCursor) as locked:
                            locked.execute("SELECT * FROM " + quoted(name) + " FOR UPDATE")
                            for _ in locked:
                                pass
                if self.snapshot() != snapshot:
                    raise Stop("SQL snapshot changed before locking")
                for name, item in snapshot.items():
                    if item["action"] == "preserve":
                        continue
                    prompt = self.name == "ai_mysql" and name == "prompt_assets"
                    if not prompt and (self.name != "qs_mysql" or name not in SHARED_TABLES):
                        raise Stop("SQL target outside fixed whitelist")
                    for row in item["rows"]:
                        if prompt:
                            cursor.execute(
                                "DELETE FROM prompt_assets WHERE template_id=%s AND version=%s",
                                tuple(row["id"]),
                            )
                        else:
                            cursor.execute(
                                "DELETE FROM " + quoted(name) + " WHERE id=%s", (row["id"],)
                            )
                        if cursor.rowcount != 1:
                            raise Stop("SQL deletion count mismatch")
            self.verify_deleted(snapshot)
            self.connection.commit()
        except BaseException:
            self.connection.rollback()
            raise

    def verify_deleted(self, snapshot):
        compare_remaining(snapshot, self.snapshot())

    def close(self):
        self.connection.close()
