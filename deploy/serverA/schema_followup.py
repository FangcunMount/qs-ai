"""Versioned 0040 runtime handoff and current-facts rollback; never rebind v1 journals."""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4


def module_at(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


legacy = module_at("followup_legacy_utilities", Path(__file__).with_name("schema_maintenance.py"))
MaintenanceError = legacy.MaintenanceError
OLD_HEAD, NEW_HEAD = legacy.OLD_HEAD, legacy.NEW_HEAD
FORMAT = "qs-ai-host-schema-maintenance/v2"
POINTER_FORMAT = "qs-ai-schema-active-context/v2"
OPERATION_FORMAT = "qs-ai-schema-operation/v2"
REHEARSAL_COVERAGE = {
    "isolated_full_snapshot_storage_restore": "covered",
    "isolated_full_current_facts_inverse_manifest": "covered",
    "isolated_insert_update_delete_storage_fixture": "covered",
    "isolated_new_image_prompt_fixture_typed_read": "covered",
    "isolated_0038_image_prompt_fixture_typed_read": "covered",
    "production_business_prompt_assets": "not_covered",
    "production_business_asset_publication": "not_covered",
    "production_business_prompt_drafts": "not_covered",
    "production_business_semantic_drafts": "not_covered",
    "production_business_sessions": "not_covered",
    "production_business_evaluation_runs": "not_covered",
    "production_business_execution": "not_covered",
    "production_business_quotas": "not_covered",
    "production_business_messaging": "not_covered",
}
SHA = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
IDENTITY = re.compile(r"[a-z][a-z0-9_]{0,31}")
RELEASE = re.compile(r"[0-9a-f]{40}-[0-9]+-[0-9]+")


def private(path):
    return legacy.private_json(path)


def checksum(path):
    return legacy.checksum(path)


def canonical(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def timestamp(value):
    if not isinstance(value, str):
        raise MaintenanceError("Retention time is missing or invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise MaintenanceError("Retention time is missing or invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MaintenanceError("Retention time requires an explicit time zone")
    return parsed.astimezone(UTC)


def retention_report(journals, *, now=None):
    """A derived view only. Missing facts never fall back to host preparation time."""
    now = (now or datetime.now(UTC)).astimezone(UTC)
    seen, archives, deadlines = {}, [], []
    known = True
    for journal in journals:
        for record in journal.get("retained_archives", []):
            schema, head = record.get("schema"), record.get("head")
            if not isinstance(schema, str) or not re.fullmatch(
                r"ai_(backup|failed)_[a-z0-9_]+", schema
            ):
                raise MaintenanceError("Unknown retained archive identity")
            if head not in (OLD_HEAD, NEW_HEAD):
                raise MaintenanceError("Unknown retained archive layout")
            identity = {key: record.get(key) for key in ("head", "manifest", "retained_since")}
            if schema in seen:
                if seen[schema] != identity:
                    raise MaintenanceError("Conflicting retained archive evidence")
                continue
            seen[schema] = identity
            since = record.get("retained_since")
            deleted = record.get("deleted_at") is not None
            if since is None:
                if not deleted:
                    known = False
                start, due = None, None
            else:
                start = timestamp(since)
                due = start + timedelta(days=30)
                if not deleted:
                    deadlines.append(due)
            state = (
                "deleted"
                if deleted
                else "drop_pending"
                if record.get("drop_pending")
                else "empty_after_restore"
                if record.get("empty_after_restore")
                else "retained"
            )
            archives.append(
                {
                    "schema": schema,
                    "head": head,
                    "retained_since": start.isoformat() if start else None,
                    "cleanup_not_before": due.isoformat() if due else None,
                    "status": state,
                    "retention_elapsed": due is not None and now >= due,
                }
            )
    return {
        "archives": sorted(archives, key=lambda r: r["schema"]),
        "retain_until": max(deadlines).isoformat() if deadlines and known else None,
        "retention_known": known and bool(archives),
    }


def paths(root):
    directory = root / "schema-maintenance"
    return directory / "active-v2.json", directory / "operation-v2.json"


def operation(root):
    path = paths(root)[1]
    if not path.exists():
        return None
    value = private(path)
    if value.get("format") != OPERATION_FORMAT or not IDENTITY.fullmatch(
        value.get("context_id", "")
    ):
        raise MaintenanceError("Unknown active maintenance operation")
    if value.get("phase") not in ("pending", "closed"):
        raise MaintenanceError("Unknown active maintenance operation phase")
    if (
        not re.fullmatch(r"[0-9a-f]{32}", value.get("operation_id", ""))
        or value.get("kind") not in ("release", "runtime_restore", "rollback")
        or not REVISION.fullmatch(value.get("executor_revision", ""))
        or not isinstance(value.get("allowed_releases"), list)
        or len(value["allowed_releases"]) != 2
        or any(not RELEASE.fullmatch(v) for v in value["allowed_releases"])
    ):
        raise MaintenanceError("Active operation identity is incomplete")
    timestamp(value.get("stopped_at"))
    return value


def assert_idle(root, owner=None):
    value = operation(root)
    if value and value["phase"] == "pending":
        if owner is None or any(
            value.get(k) != owner.get(k) for k in ("context_id", "operation_id")
        ):
            raise MaintenanceError("An unfinished operation owns the original maintenance window")
    return value


def active_context(root):
    pointer = paths(root)[0]
    if not pointer.exists():
        return None
    value = private(pointer)
    if (
        value.get("format") != POINTER_FORMAT
        or not IDENTITY.fullmatch(value.get("context_id", ""))
        or not REVISION.fullmatch(value.get("executor_revision", ""))
        or not SHA.fullmatch(value.get("helper_sha256", ""))
        or not RELEASE.fullmatch(value.get("release", ""))
        or not SHA.fullmatch(value.get("runtime_binding_sha256", ""))
    ):
        raise MaintenanceError("Unknown active maintenance context")
    return value


def release_binding(deploy, release, head):
    if head not in (OLD_HEAD, NEW_HEAD) or not RELEASE.fullmatch(release.name):
        raise MaintenanceError("Runtime binding requires an exact known release and head")
    return {
        "format": "qs-ai-runtime-binding/v2",
        "release": release.name,
        "revision": release.name[:40],
        "image": deploy.release_image_id(release, verify_identity=True),
        "source_head": head,
        "binding": legacy.database_binding(release),
        "files": {
            name: checksum(release / name)
            for name in ("manifest.json", "runtime.json", "compose.yaml")
        },
    }


def append_binding(deploy, context, release, head):
    """Caller holds both existing locks and has verified the actual running image."""
    binding = release_binding(deploy, release, head)
    directory = deploy.ROOT / "schema-maintenance" / context["id"] / "runtime-bindings"
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / (release.name + ".json")
    if path.exists():
        if private(path) != binding:
            raise MaintenanceError("Immutable runtime binding changed")
    else:
        legacy.durable(path, binding)
    legacy.durable(
        paths(deploy.ROOT)[0],
        {
            "format": POINTER_FORMAT,
            "context_id": context["id"],
            "executor_revision": context["executor"]["revision"],
            "helper_sha256": context["executor"]["helper_sha256"],
            "release": release.name,
            "runtime_binding_sha256": canonical(binding),
        },
    )
    return binding


def executor_binding(code):
    revision = code.name.removeprefix("code-")
    if not REVISION.fullmatch(revision):
        raise MaintenanceError("Executor requires a fixed full code revision")
    package = code / "src/qs_ai/maintenance/schema_refactor"
    sidecar = package / "validation_contract.py"
    if not sidecar.is_file():
        raise MaintenanceError("Executor is missing the independent validation contract")
    migrations = {
        path.name: checksum(path) for path in sorted((code / "migrations/versions").glob("*.py"))
    }
    return {
        "revision": revision,
        "helper_sha256": checksum(code / "deploy/serverA/schema_followup.py"),
        "deployment_helper_sha256": checksum(code / "deploy/serverA/deploy.py"),
        "source_sha256": legacy.source_checksum(code),
        "validation_sha256": checksum(sidecar),
        "v0038_sha256": checksum(package / "v0038.py"),
        "v0040_sha256": checksum(package / "v0040.json"),
        "migrations_sha256": canonical(migrations),
    }


VALIDATION_READER = r"""
import asyncio, json, os
from pathlib import Path
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.validation import require_schema


async def main():
    engine = create_async_engine(os.environ["QS_AI_DATABASE_URL"])
    try:
        async with engine.connect() as conn:
            inherited = control.read(Path("/maintenance/binding-forward.json"))
            await conn.run_sync(lambda c: control._binding(c, inherited))
            if os.environ.get("MAINTENANCE_FENCED") == "1":
                import sys

                reverse = Path("/maintenance") / (
                    sys.argv[1] if len(sys.argv) > 1 else "reverse.json"
                )
                state = control.read(reverse) if reverse.exists() else inherited
                scopes = tuple(state[k] for k in ("source", "target", "archive"))
                await conn.run_sync(lambda c: control.fenced(c, scopes, True))
            head = await conn.run_sync(
                lambda c: contracts.head(c, c.scalar(sa.text("SELECT DATABASE()")))
            )
            await conn.run_sync(lambda c: require_schema(c, head))
            columns = (
                await conn.execute(
                    sa.text(
                        (
                            "SELECT TABLE_NAME,COLUMN_NAME FROM information_schema.COLUMNS "
                            "WHERE TABLE_SCHEMA=DATABASE() AND "
                            "FIND_IN_SET('auto_increment',REPLACE(EXTRA,' ',','))>0 "
                            "ORDER BY TABLE_NAME,COLUMN_NAME"
                        )
                    )
                )
            ).all()
            print(
                json.dumps(
                    {
                        "validated": True,
                        "head": head,
                        "server_uuid": await conn.scalar(sa.text("SELECT @@server_uuid")),
                        "auto_increment_columns": [a + "." + b for a, b in columns],
                    }
                )
            )
    finally:
        await engine.dispose()


asyncio.run(main())
"""


FIXTURE_WRITE = r"""
import asyncio, hashlib, json, os, sys
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets


async def main():
    schema = os.environ["QS_AI_DATABASE_URL"].rsplit("/", 1)[-1]
    if not schema.startswith("ai_refactor_followup_"):
        raise ValueError("Fixture scope rejected")
    name = "maintenance-fixture-" + sys.argv[1]
    fingerprint = "sha256:" + hashlib.sha256(name.encode()).hexdigest()
    data = {
        "Ref": {
            "TemplateID": name,
            "Version": "v1",
            "Fingerprint": fingerprint,
            "GitBlobSHA": "a" * 40,
        },
        "SystemMessage": "isolated maintenance fixture",
        "TaskTemplate": "fixture only",
        "DataPreamble": "fixture",
        "AllowedPlaceholders": [],
    }
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    asset = PromptAsset(name, "v1", fingerprint, hashlib.sha256(body.encode()).hexdigest(), body)
    database = Database(os.environ["QS_AI_DATABASE_URL"])
    engine = create_async_engine(os.environ["QS_AI_DATABASE_URL"])
    try:
        async with engine.connect() as conn:
            original = await conn.scalar(
                sa.text(
                    (
                        "SELECT asset_row_id FROM governance_asset_versions "
                        "WHERE asset_kind='prompt' ORDER BY asset_row_id LIMIT 1"
                    )
                )
            )
            if original is None:
                raise ValueError("A snapshot Prompt fact is required")
        if not await MySQLPromptAssets(Transactions(database)).put(
            asset, "followup-fixture", "maintenance"
        ):
            raise ValueError("Fixture identity already exists")
        async with engine.begin() as conn:
            deleted = await conn.execute(
                sa.text(
                    (
                        "DELETE FROM governance_asset_versions "
                        "WHERE asset_kind='prompt' AND asset_row_id=:id"
                    )
                ),
                {"id": original},
            )
            updated = await conn.execute(
                sa.text(
                    (
                        "UPDATE messaging_observations SET recorded_count=recorded_count+7 "
                        "ORDER BY kind LIMIT 1"
                    )
                )
            )
            if deleted.rowcount != 1 or updated.rowcount != 1:
                raise ValueError("Fixture mutation incomplete")
        print(
            json.dumps(
                {
                    "fixture": "passed",
                    "template_id": name,
                    "version": "v1",
                    "inserted": 1,
                    "updated": 1,
                    "deleted": 1,
                    "prompt_sha256": hashlib.sha256(
                        json.dumps(
                            {
                                "template_id": name,
                                "version": "v1",
                                "fingerprint": fingerprint,
                                "package_sha256": asset.package_sha256,
                                "package_json": body,
                            },
                            sort_keys=True,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                }
            )
        )
    finally:
        await database.close()
        await engine.dispose()


asyncio.run(main())
"""


FIXTURE_READ = r"""
import asyncio, hashlib, json, os, sys
from dataclasses import asdict
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions, MySQLProbe
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets


async def main():
    database = Database(os.environ["QS_AI_DATABASE_URL"])
    try:
        if not (await MySQLProbe(database).check()).ready:
            raise ValueError("Fixed image head rejected")
        asset = await MySQLPromptAssets(Transactions(database)).get(sys.argv[1], sys.argv[2])
        if asset is None:
            raise ValueError("Current-facts fixture missing")
        print(
            json.dumps(
                {
                    "fixture_domain": "passed",
                    "prompt_sha256": hashlib.sha256(
                        json.dumps(
                            asdict(asset), sort_keys=True, ensure_ascii=False, separators=(",", ":")
                        ).encode()
                    ).hexdigest(),
                }
            )
        )
    finally:
        await database.close()


asyncio.run(main())
"""


class FollowupTools(legacy.DockerTools):
    def container(self, image, command, *, source=True, **kwargs):
        return super().container(image, command, source=source, **kwargs)

    def validate(self, image, *, fenced=False, schema=None):
        return self.container(
            image,
            ["-c", VALIDATION_READER, getattr(self, "validation_journal", "reverse.json")],
            schema=schema,
            timeout=60,
            fenced=fenced,
        )

    def refactor(
        self, command, journal, state, *, schema=None, stopped_at=None, inherit=None, fast=False
    ):
        if fast:
            raise MaintenanceError("Followup only permits full current-facts conversion")
        args = [
            "-m",
            "qs_ai.maintenance.schema_refactor",
            command,
            "--journal",
            "/maintenance/" + journal.relative_to(self.directory).as_posix(),
        ]
        if command == "plan":
            for key in ("source", "target", "archive", "old_image", "new_image"):
                args += ["--" + key.replace("_", "-"), state[key]]
            if inherit:
                args += [
                    "--inherit-journal",
                    "/maintenance/" + inherit.relative_to(self.directory).as_posix(),
                ]
        if command in ("copy", "verify", "switch", "rollback"):
            args += ["--writers-stopped"]
        if command in ("copy", "rollback"):
            args += ["--stopped-at", stopped_at]
        return self.container(self.image, args, schema=schema, timeout=legacy.WINDOW_SECONDS)


class Followup(legacy.Maintenance):
    def __init__(self, deploy, identifier, credentials, *, code=None, tools=None):
        if not IDENTITY.fullmatch(identifier):
            raise MaintenanceError("Invalid followup identifier")
        legacy.validate_credentials(credentials)
        self.deploy, self.identifier = deploy, identifier
        self.code = code or Path(__file__).resolve().parents[2]
        self.directory = deploy.ROOT / "schema-maintenance" / identifier
        self.path = self.directory / "host.json"
        self.state = private(self.path) if self.path.exists() else None
        self._cleanup_attempts = {}
        self.credentials = credentials
        self.tools = tools
        self._tools_supplied = tools is not None
        if self.state:
            active = operation(deploy.ROOT)
            stop = active["stopped_at"] if active and active["phase"] == "pending" else None
            with self.host_budget(stop, short_probe=True):
                self.bound(bootstrap_resume=self.state.get("phase") == "bootstrap_pending")

    @property
    def executor_revision(self):
        return self.state["executor"]["revision"]

    @property
    def executor_image(self):
        return self.state["executor_release"]["image"]

    @property
    def executor_release(self):
        return self.deploy.release_path(self.state["executor_release"]["release"])

    @property
    def current_binding(self):
        pointer = active_context(self.deploy.ROOT)
        if pointer is None or pointer["context_id"] != self.identifier:
            raise MaintenanceError("Followup is not the active maintenance context")
        path = self.directory / "runtime-bindings" / (pointer["release"] + ".json")
        binding = private(path)
        if canonical(binding) != pointer["runtime_binding_sha256"]:
            raise MaintenanceError("Active immutable runtime binding changed")
        return binding

    def assert_idle(self, owner=None):
        return assert_idle(self.deploy.ROOT, owner)

    def save(self):
        legacy.durable(self.path, self.state)

    def _base(self, identifier, revision):
        if not IDENTITY.fullmatch(identifier) or not REVISION.fullmatch(revision):
            raise MaintenanceError("Invalid immutable base identity")
        if identifier == self.identifier:
            raise MaintenanceError("Followup requires a distinct base identity")
        directory = self.deploy.ROOT / "schema-maintenance" / identifier
        host_path, forward_path = directory / "host.json", directory / "forward.json"
        host, forward = private(host_path), private(forward_path)
        code = directory / ("code-" + revision)
        if (
            host.get("format") != legacy.FORMAT
            or host.get("id") != identifier
            or host.get("code_revision") != code.name
            or host.get("helper_sha256") != checksum(code / "deploy/serverA/schema_maintenance.py")
            or host.get("deployment_helper_sha256") != checksum(code / "deploy/serverA/deploy.py")
            or host.get("source_sha256") != legacy.source_checksum(code)
            or forward.get("format") != "qs-ai-schema-refactor/v1"
            or forward.get("phase") not in ("switched", "cleaned")
            or forward.get("source") != "ai"
            or forward.get("source_head") != OLD_HEAD
            or forward.get("target_head") != NEW_HEAD
            or forward.get("server_uuid") != host.get("server_uuid")
            or forward.get("old_image") != host.get("old_image")
            or forward.get("new_image") != host.get("new_image")
            or not forward.get("source_column_collations")
            or not forward.get("legacy_prompt_collations")
            or host.get("phase") != "switched"
        ):
            raise MaintenanceError("Immutable base provenance could not be verified")
        for path in directory.glob("*.json"):
            if path.name not in ("host.json", "forward.json") and path.name.endswith(
                "reverse.json"
            ):
                if private(path).get("phase", "").endswith("pending"):
                    raise MaintenanceError("The base context has an unfinished operation")
        for role in ("old", "new"):
            release = self.deploy.release_path(host[role + "_release"])
            if any(
                checksum(release / name) != digest for name, digest in host[role + "_files"].items()
            ):
                raise MaintenanceError("Base release materials changed")
        if (
            self.deploy.release_image_id(
                self.deploy.release_path(host["old_release"]), verify_identity=True
            )
            != host["old_image"]
        ):
            raise MaintenanceError("Fixed legacy rollback image changed")
        archive = directory / "old-image.tar"
        if (
            host.get("old_image_archive_sha256")
            and checksum(archive) != host["old_image_archive_sha256"]
        ):
            raise MaintenanceError("Fixed legacy image archive changed")
        package = code / "src/qs_ai/maintenance/schema_refactor"
        return (
            host,
            forward,
            {
                "id": identifier,
                "revision": revision,
                "host_sha256": checksum(host_path),
                "forward_sha256": checksum(forward_path),
                "v0038_sha256": checksum(package / "v0038.py"),
                "v0040_sha256": checksum(package / "v0040.json"),
                "migrations_sha256": canonical(
                    {
                        p.name: checksum(p)
                        for p in sorted((code / "migrations/versions").glob("*.py"))
                    }
                ),
            },
        )

    def bound(self, *, bootstrap_resume=False):
        if self.state.get("format") != FORMAT or self.state.get("id") != self.identifier:
            raise MaintenanceError("Unknown followup context")
        if self.state["executor"] != executor_binding(self.code):
            raise MaintenanceError("Versioned followup executor changed")
        host, forward, base = self._base(self.state["base"]["id"], self.state["base"]["revision"])
        if base != self.state["base"]:
            raise MaintenanceError("Immutable base journals changed")
        self.base_host, self.base_forward = host, forward
        self.old = self.deploy.release_path(host["old_release"])
        self.old_image = host["old_image"]
        self.binding = self.state["database_binding"]
        binding = self.state["bootstrap_current"] if bootstrap_resume else self.current_binding
        active = active_context(self.deploy.ROOT)
        if not bootstrap_resume and (
            active["executor_revision"] != self.executor_revision
            or active["helper_sha256"] != self.state["executor"]["helper_sha256"]
        ):
            raise MaintenanceError("Active executor binding differs from the context")
        current = private(self.deploy.ROOT / "state.json")["current"]
        intent = operation(self.deploy.ROOT)
        allowed = {binding["release"]}
        if intent and intent["phase"] == "pending" and intent["context_id"] == self.identifier:
            allowed.update(intent.get("allowed_releases", []))
        if current not in allowed:
            raise MaintenanceError("Current release is not a fixed followup runtime binding")
        for frozen in (binding, self.state["executor_release"]):
            release = self.deploy.release_path(frozen["release"])
            actual = release_binding(self.deploy, release, frozen["source_head"])
            if actual != frozen:
                raise MaintenanceError("Fixed runtime or executor image materials changed")
        if binding["binding"] != self.binding or legacy.database_binding(self.old) != self.binding:
            raise MaintenanceError("Followup database connection identity changed")
        if self.tools is None:
            self.tools = FollowupTools(
                self.deploy, self.directory, self.binding, self.credentials, self.code
            )
        self.tools.image = self.executor_image

    def bootstrap(self, base_id, base_revision, release_name):
        self.assert_idle()
        if self.state:
            if (
                self.state["base"]["id"] != base_id
                or self.state["base"]["revision"] != base_revision
                or self.state["executor_release"]["release"] != release_name
            ):
                raise MaintenanceError("Followup identity cannot be rebound")
            if self.state["phase"] == "bootstrap_pending":
                self.bound(bootstrap_resume=True)
                current = self.deploy.release_path(self.state["bootstrap_current"]["release"])
                self._validate(expected=NEW_HEAD)
                append_binding(self.deploy, self.state, current, NEW_HEAD)
                self.state["phase"] = "ready"
                self.save()
            self.bound()
            return self.status()
        self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        if (
            self.directory.is_symlink()
            or self.directory.stat().st_mode & 0o077
            or self.directory.stat().st_uid != os.getuid()
        ):
            raise MaintenanceError("Followup directory must be private and owned")
        host, forward, base = self._base(base_id, base_revision)
        self.base_host, self.base_forward = host, forward
        executor = executor_binding(self.code)
        if any(
            executor[k] != base[k] for k in ("v0038_sha256", "v0040_sha256", "migrations_sha256")
        ):
            raise MaintenanceError("Followup changed the frozen storage contract")
        if executor["v0040_sha256"] != forward["contract_sha256"]:
            raise MaintenanceError("Base conversion contract differs from the followup")
        release = self.deploy.release_path(release_name)
        if not release.name.startswith(executor["revision"] + "-"):
            raise MaintenanceError("Executor image must match its fixed tested revision")
        prepared = private(release / "prepared.json")
        candidate = release_binding(self.deploy, release, NEW_HEAD)
        if (
            prepared.get("prepared") is not True
            or prepared.get("image_id") != candidate["image"]
            or prepared.get("expected_heads") != [NEW_HEAD]
        ):
            raise MaintenanceError("Followup requires the exact prepared 0040 executor image")
        current = self.deploy.release_path(private(self.deploy.ROOT / "state.json")["current"])
        current_binding = release_binding(self.deploy, current, NEW_HEAD)
        if current_binding["binding"] != host["binding"]:
            raise MaintenanceError("Current runtime and base database bindings differ")
        if self.tools is None:
            self.tools = FollowupTools(
                self.deploy, self.directory, host["binding"], self.credentials, self.code
            )
        self.tools.image = candidate["image"]
        self._inherit("binding-forward.json")
        proof = self.tools.validate(candidate["image"])
        if (
            proof.get("validated") is not True
            or proof.get("head") != NEW_HEAD
            or proof.get("server_uuid") != host["server_uuid"]
        ):
            raise MaintenanceError("Current complete 0040 structure could not be verified")
        existing = active_context(self.deploy.ROOT)
        if existing and existing["context_id"] != self.identifier:
            other = private(
                self.deploy.ROOT / "schema-maintenance" / existing["context_id"] / "host.json"
            )
            if other.get("phase") not in ("ready", "rolled_back", "closed"):
                raise MaintenanceError("A prior context has unresolved facts")
        self.state = {
            "format": FORMAT,
            "id": self.identifier,
            "phase": "bootstrap_pending",
            "bootstrap_current": current_binding,
            "executor": executor,
            "executor_release": candidate,
            "base": base,
            "database_binding": host["binding"],
            "server_uuid": host["server_uuid"],
            "source_head": NEW_HEAD,
            "runtime_may_have_written": True,
            "created_at": datetime.now(UTC).isoformat(),
        }
        self.save()
        append_binding(self.deploy, self.state, current, NEW_HEAD)
        self.state["phase"] = "ready"
        self.save()
        self.bound()
        return self.status()

    def _validate(self, *, fenced=False, schema=None, expected=None):
        active = operation(self.deploy.ROOT)
        self.tools.validation_journal = (
            active.get("reverse_journal", "reverse.json")
            if active and active["phase"] == "pending"
            else self._reverse_name(self.state.get("bootstrap_current"))
            if self.state.get("phase") == "bootstrap_pending"
            else self._reverse_name()
        )
        proof = self.tools.validate(self.executor_image, fenced=fenced, schema=schema)
        if (
            proof.get("validated") is not True
            or proof.get("server_uuid") != self.state["server_uuid"]
            or proof.get("head") not in (OLD_HEAD, NEW_HEAD)
            or (expected and proof["head"] != expected)
        ):
            raise MaintenanceError("Complete current schema or server identity differs")
        return proof

    def _journal(self, name):
        state = private(self.directory / name)
        if (
            state.get("format") != "qs-ai-schema-refactor/v1"
            or state.get("server_uuid") != self.state["server_uuid"]
        ):
            raise MaintenanceError("Followup conversion journal identity changed")
        return state

    def _inherit(self, name="inherited-forward.json", source="ai"):
        inherited = dict(self.base_forward)
        if source != "ai":
            inherited["source"] = source
            inherited["derived_for_rehearsal"] = True
            inherited["origin_forward_sha256"] = self.state["base"]["forward_sha256"]
        path = self.directory / name
        if path.exists():
            if private(path) != inherited:
                raise MaintenanceError("Inherited stable forward journal changed")
        else:
            legacy.durable(path, inherited)
        return path

    def _reverse_name(self, binding=None):
        return "reverse-" + canonical(binding or self.current_binding)[:16] + ".json"

    def _reverse_prepare(self, name=None, source="ai", suffix=None, image=None):
        name = name or self._reverse_name()
        suffix = suffix or (self.identifier + "_" + name[8:24])
        path = self.directory / name
        spec = {
            "source": source,
            "target": "ai_rollback_" + suffix,
            "archive": "ai_failed_" + suffix,
            "old_image": self.old_image,
            "new_image": image or self.current_binding["image"],
        }
        if not path.exists():
            inherited = self._inherit("inherited-" + name, source)
            self.tools.refactor("plan", path, spec, inherit=inherited)
        state = self._journal(name)
        if (
            any(state.get(k) != v for k, v in spec.items())
            or state.get("source_head") != NEW_HEAD
            or state.get("target_head") != OLD_HEAD
        ):
            raise MaintenanceError("Reverse target or fixed image binding changed")
        if state["phase"] in ("planned", "prepare_pending"):
            self.tools.refactor("prepare", path, state)
        return self._journal(name)

    def prepare(self):
        self.bound()
        self.assert_idle()
        self._validate(expected=NEW_HEAD)
        state = self._reverse_prepare()
        return {"phase": "prepared", "target": state["target"], "target_head": OLD_HEAD}

    def _begin(self, kind, allowed, **binding):
        previous = operation(self.deploy.ROOT)
        if previous and previous["phase"] == "pending":
            if (
                previous["context_id"] != self.identifier
                or previous["kind"] != kind
                or previous["allowed_releases"] != allowed
                or any(previous.get(k) != v for k, v in binding.items())
            ):
                raise MaintenanceError(
                    "An unfinished operation cannot be rebound or given a new clock"
                )
            saved = self.state.get("operation")
            if saved and any(
                saved.get(k) != previous.get(k)
                for k in ("context_id", "operation_id", "kind", "stopped_at", "executor_revision")
            ):
                raise MaintenanceError("The original operation clock or identity changed")
            return previous
        value = {
            "format": OPERATION_FORMAT,
            "context_id": self.identifier,
            "operation_id": uuid4().hex,
            "kind": kind,
            "phase": "pending",
            "server_uuid": self.state["server_uuid"],
            "stopped_at": datetime.now(UTC).isoformat(),
            "allowed_releases": allowed,
            "executor_revision": self.executor_revision,
            **binding,
        }
        legacy.durable(paths(self.deploy.ROOT)[1], value)
        self.state["operation"] = dict(value)
        self.state["phase"] = kind + "_pending"
        self.save()
        return value

    def _close(self, intent, proof, release, phase):
        self.assert_idle(intent)
        if proof.get("server_uuid") != self.state["server_uuid"] or proof.get("head") not in (
            OLD_HEAD,
            NEW_HEAD,
        ):
            raise MaintenanceError("Operation cannot close without complete actual structure proof")
        if private(self.deploy.ROOT / "state.json").get("current") != release.name:
            raise MaintenanceError("Operation cannot close before durable runtime publication")
        binding = append_binding(self.deploy, self.state, release, proof["head"])
        with self.host_budget(intent["stopped_at"]):
            self.runtime_proof()
        value = {
            **intent,
            "phase": "closed",
            "closed_at": datetime.now(UTC).isoformat(),
            "restored_release": binding["release"],
            "actual_head": proof["head"],
        }
        self.state["phase"] = phase
        self.state["operation"] = value
        self.save()
        legacy.durable(paths(self.deploy.ROOT)[1], value)

    def release(self, release_name, *, restore=False):
        self.bound()
        rehearsal = self.state.get("rehearsal", {})
        if (
            not rehearsal.get("passed")
            or rehearsal.get("executor_image") != self.executor_image
            or rehearsal.get("legacy_image") != self.old_image
        ):
            raise MaintenanceError("Runtime handoff requires this context fixed-image rehearsal")
        target = self.deploy.release_path(release_name)
        kind = "runtime_restore" if restore else "release"
        pending = operation(self.deploy.ROOT)
        if pending and pending["phase"] == "pending":
            if (
                pending["context_id"] != self.identifier
                or pending["kind"] != kind
                or pending["allowed_releases"][-1] != target.name
            ):
                raise MaintenanceError("Runtime handoff retry cannot select another release")
            source = self.deploy.release_path(pending["allowed_releases"][0])
        else:
            source = self.deploy.release_path(self.current_binding["release"])
        if self.deploy.expected_heads(target) != [NEW_HEAD] or self.deploy.expected_heads(
            source
        ) != [NEW_HEAD]:
            raise MaintenanceError("Followup runtime release must preserve the exact 0040 head")
        self.tools.deadline = time.monotonic() + 60
        with self.host_budget(short_probe=True):
            self._validate(expected=NEW_HEAD)
        intent = self._begin(
            kind,
            [source.name, target.name],
            source_binding_sha256=canonical(release_binding(self.deploy, source, NEW_HEAD)),
            target_binding_sha256=canonical(release_binding(self.deploy, target, NEW_HEAD)),
        )
        stop = intent["stopped_at"]
        self.remaining(stop, legacy.WINDOW_SECONDS)
        self.tools.deadline = time.monotonic() + self.remaining(stop, legacy.WINDOW_SECONDS)
        self.deploy._SCHEMA_FOLLOWUP_OPERATION = intent
        try:
            with self.host_budget(stop):
                current = private(self.deploy.ROOT / "state.json")
                if current["current"] != target.name:
                    if restore:
                        self.deploy.restore({"current": source.name, "previous": target.name})
                    else:
                        self.deploy.apply(target, current)
                else:
                    self.deploy.probe(target, True)
                    self.deploy.verify(target)
                proof = self._validate(expected=NEW_HEAD)
            self._close(intent, proof, target, "ready")
        except Exception:
            self.state["last_failure"] = {
                "operation_id": intent["operation_id"],
                "reason": "runtime_release_failed",
            }
            self.save()
            try:
                with self.host_budget(stop):
                    self.deploy.stop_release(target)
                    self.deploy.probe(source, True)
                    self.deploy.verify(source)
                    legacy.durable(
                        self.deploy.ROOT / "state.json", {"current": source.name, "previous": None}
                    )
                    proof = self._validate(expected=NEW_HEAD)
                self._close(intent, proof, source, "ready")
            except Exception:
                self.stop_failed_release(target)
                self.stop_failed_release(source)
                raise MaintenanceError(
                    "Runtime handoff is unresolved; preserve the original operation "
                    "and writers stopped"
                ) from None
            raise MaintenanceError(
                "Runtime handoff failed; the previous 0040 runtime was verified and restored"
            ) from None
        finally:
            del self.deploy._SCHEMA_FOLLOWUP_OPERATION
        return {
            **self.status(),
            "phase": "runtime_restored" if restore else "released",
            "runtime_verified": True,
        }

    def rollback(self):
        self.bound()
        binding = self.current_binding
        pending = operation(self.deploy.ROOT)
        if self.state["phase"] == "rolled_back" and (
            pending is None or pending["phase"] == "closed"
        ):
            return {**self.status(), "phase": "rolled_back"}
        source = self.deploy.release_path(
            pending["allowed_releases"][0]
            if pending and pending["phase"] == "pending"
            else binding["release"]
        )
        source_binding = release_binding(self.deploy, source, NEW_HEAD)
        name = (
            pending["reverse_journal"]
            if pending and pending["phase"] == "pending"
            else self._reverse_name(source_binding)
        )
        intent = self._begin(
            "rollback",
            [source.name, self.old.name],
            reverse_journal=name,
            source_binding_sha256=canonical(source_binding),
            source_image=source_binding["image"],
        )
        stop = intent["stopped_at"]
        path = self.directory / name
        state = self._journal(name) if path.exists() else None
        exchanged = state and state["phase"] in ("switch_pending", "switched")
        try:
            remaining = self.remaining(stop, legacy.COPY_SECONDS)
        except MaintenanceError:
            if not exchanged:
                raise
            remaining = 60
        self.tools.deadline = time.monotonic() + remaining
        try:
            with self.host_budget(stop, recovery_probe=bool(exchanged)):
                proof = self._validate()
                if proof["head"] == OLD_HEAD:
                    if not exchanged:
                        raise MaintenanceError(
                            "Unexpected legacy layout before recorded reverse exchange"
                        )
                    if state["phase"] == "switch_pending":
                        self.tools.refactor("switch", path, state, stopped_at=stop)
                    if self._journal(name)["phase"] != "switched":
                        raise MaintenanceError("Recorded reverse exchange is not complete")
                else:
                    self.deploy.stop_release(source)
                    self.deploy.stop_release(self.old)
                    self._validate(fenced=True, expected=NEW_HEAD)
                    state = self._reverse_prepare(name, image=intent["source_image"])
                    for command in ("copy", "verify", "switch"):
                        state = self._journal(name)
                        phases = {
                            "copy": ("prepared", "copy_pending", "copied"),
                            "verify": ("copied", "verified"),
                            "switch": ("verified", "switch_pending", "switched"),
                        }
                        if state["phase"] in phases[command]:
                            self.tools.refactor(command, path, state, stopped_at=stop)
                    if self._journal(name)["phase"] != "switched":
                        raise MaintenanceError(
                            "Full current-facts reverse exchange is not confirmed"
                        )
            self.remaining(stop, legacy.WINDOW_SECONDS)
            self.tools.deadline = time.monotonic() + self.remaining(stop, legacy.WINDOW_SECONDS)
            with self.host_budget(stop):
                self._validate(fenced=True, expected=OLD_HEAD)
                self.deploy.messaging_transition(source, self.old)
                self.deploy.messaging_key_preflight(self.old)
                self.deploy.probe(self.old, True)
                self.state["old_runtime_may_have_written"] = True
                self.state["phase"] = "rollback_starting"
                self.save()
                self.deploy.verify(self.old)
                legacy.durable(
                    self.deploy.ROOT / "state.json",
                    {"current": self.old.name, "previous": source.name},
                )
                proof = self._validate(expected=OLD_HEAD)
            self._close(intent, proof, self.old, "rolled_back")
        except Exception:
            self.stop_failed_release(source)
            self.stop_failed_release(self.old)
            raise MaintenanceError(
                "Reverse rollback is unresolved; preserve current facts, original clock "
                "and writers stopped"
            ) from None
        return {"phase": "rolled_back", "release": self.old.name}

    def rehearse(self):
        self.bound()
        self.assert_idle()
        self._validate(expected=NEW_HEAD)
        if self.state.get("rehearsal"):
            if not self.state["rehearsal"].get("passed"):
                raise MaintenanceError(
                    "Interrupted isolated rehearsal must be inspected before a new attempt"
                )
            return self._rehearsal_receipt()
        started = time.monotonic()
        token = uuid4().hex[:12]
        source = "ai_refactor_followup_" + token
        path = self.directory / "rehearsal.jsonl.gz"
        self.state["rehearsal"] = {"passed": False, "source": source, "token": token}
        self.save()
        backup = self.tools.backup("backup", self.executor_image, "ai", path, source=True)
        if (
            backup.get("source_server_uuid") != self.state["server_uuid"]
            or backup.get("head") != NEW_HEAD
            or backup.get("tables") != 44
            or type(backup.get("rows")) is not int
            or not 0 <= backup["rows"] <= 2**64
        ):
            raise MaintenanceError("Rehearsal snapshot belongs to another source")
        restored = self.tools.backup(
            "restore", self.executor_image, "ai", path, target=source, expected=backup, source=True
        )
        if restored.get("verified") is not True:
            raise MaintenanceError("Rehearsal snapshot restore was not verified")
        fixture = self.tools.container(
            self.executor_image,
            ["-c", FIXTURE_WRITE, token],
            schema=source,
            source=False,
            timeout=60,
        )
        if fixture.get("fixture") != "passed" or any(
            fixture.get(k) != 1 for k in ("inserted", "updated", "deleted")
        ):
            raise MaintenanceError("Isolated current-facts fixture failed")
        before = self.tools.container(
            self.executor_image,
            ["-c", FIXTURE_READ, fixture["template_id"], fixture["version"]],
            schema=source,
            source=False,
            timeout=60,
        )
        state = self._reverse_prepare("rehearsal-reverse.json", source, "rehearsal_" + token)
        stop = datetime.now(UTC).isoformat()
        self.tools.deadline = time.monotonic() + legacy.COPY_SECONDS
        for command in ("copy", "verify"):
            self.tools.refactor(
                command, self.directory / "rehearsal-reverse.json", state, stopped_at=stop
            )
            state = self._journal("rehearsal-reverse.json")
        self._validate(schema=state["target"], expected=OLD_HEAD)
        after = self.tools.container(
            self.old_image,
            ["-c", FIXTURE_READ, fixture["template_id"], fixture["version"]],
            schema=state["target"],
            source=False,
            timeout=60,
        )
        if (
            before.get("fixture_domain") != "passed"
            or after.get("fixture_domain") != "passed"
            or before.get("prompt_sha256") != fixture["prompt_sha256"]
            or before.get("prompt_sha256") != after.get("prompt_sha256")
        ):
            raise MaintenanceError("Fixed old and new image typed fixture reads differ")
        self.state["rehearsal"] = {
            "passed": True,
            "source": source,
            "target": state["target"],
            "backup": backup,
            "executor_image": self.executor_image,
            "source_image": self.current_binding["image"],
            "legacy_image": self.old_image,
            "fixture": {k: fixture[k] for k in ("inserted", "updated", "deleted", "prompt_sha256")},
            "elapsed_seconds": time.monotonic() - started,
            "coverage": dict(REHEARSAL_COVERAGE),
        }
        self.save()
        return self._rehearsal_receipt()

    def _rehearsal_receipt(self):
        """Snapshot counts describe storage; isolated fixtures never accept live business flows."""
        rehearsal = self.state["rehearsal"]
        snapshot = rehearsal["backup"]
        return {
            "phase": "rehearsed",
            "passed": True,
            "elapsed_seconds": rehearsal["elapsed_seconds"],
            "fixture": dict(rehearsal["fixture"]),
            "coverage": dict(rehearsal["coverage"]),
            "rehearsal": {
                "source_head": snapshot["head"],
                "source_server_uuid": snapshot["source_server_uuid"],
                **{
                    key: snapshot[key]
                    for key in (
                        "tables",
                        "rows",
                        "backup_sha256",
                        "manifest_sha256",
                        "physical_manifest_sha256",
                    )
                    if key in snapshot
                },
            },
        }

    def _retention_journals(self):
        result = [json.loads(json.dumps(self.base_forward))]
        for path in sorted(self.directory.glob("reverse-*.json")):
            result.append(self._journal(path.name))
        delegations = self.directory / "cleanup"
        if delegations.exists():
            for path in sorted(delegations.glob("*.json")):
                delegated = private(path)
                if delegated.get("origin_sha256") not in (
                    self.state["base"]["forward_sha256"],
                    *self.state.get("reverse_cleanup_origins", {}).values(),
                ):
                    raise MaintenanceError("Cleanup delegation origin changed")
                schemas = {r["schema"] for r in delegated.get("retained_archives", [])}
                for original in result:
                    original["retained_archives"] = [
                        r
                        for r in original.get("retained_archives", [])
                        if r["schema"] not in schemas
                    ]
                result.append(delegated)
        return result

    def cleanup(self):
        self.bound()
        self.assert_idle()
        report = retention_report(self._retention_journals())
        if not report["retention_known"] or any(
            not r["retention_elapsed"] for r in report["archives"] if r["status"] != "deleted"
        ):
            raise MaintenanceError(
                "Recorded archives have not all reached their own retention deadline"
            )
        directory = self.directory / "cleanup"
        directory.mkdir(mode=0o700, exist_ok=True)
        origins = [("base", self.base_forward, self.state["base"]["forward_sha256"])]
        for path in sorted(self.directory.glob("reverse-*.json")):
            digest = checksum(path)
            expected = self.state.setdefault("reverse_cleanup_origins", {}).setdefault(
                path.name, digest
            )
            if digest != expected:
                raise MaintenanceError("Reverse cleanup origin changed")
            self.save()
            origins.append((path.stem, self._journal(path.name), digest))
        for name, original, digest in origins:
            if not original.get("retained_archives"):
                continue
            path = directory / (name + ".json")
            if not path.exists():
                legacy.durable(path, {**original, "origin_sha256": digest})
            delegated = private(path)
            if delegated.get("origin_sha256") != digest:
                raise MaintenanceError("Cleanup journal cannot be rebound")
            self.tools.refactor("cleanup", path, delegated)
        return {"phase": "cleaned", **retention_report(self._retention_journals())}

    def status(self):
        if self.state is None:
            return {"phase": "unplanned", "retain_until": None, "archives": []}
        self.tools.deadline = time.monotonic() + 60
        with self.host_budget(short_probe=True):
            self.bound()
            proof = self._validate()
            pending = operation(self.deploy.ROOT)
            try:
                runtime = self.runtime_proof()
            except MaintenanceError:
                if pending is None or pending["phase"] != "pending":
                    raise
                runtime = {"verified": False, "healthy": False, "mtls": False}
        value = operation(self.deploy.ROOT)
        binding = self.current_binding
        return {
            "phase": self.state["phase"],
            "executor_revision": self.executor_revision,
            "current_revision": binding["revision"],
            "release": binding["release"],
            "image": binding["image"],
            "server_uuid": self.state["server_uuid"],
            "source_head": proof["head"],
            "tables": 44 if proof["head"] == NEW_HEAD else 54,
            "base_id": self.state["base"]["id"],
            "base_host_sha256": self.state["base"]["host_sha256"],
            "base_forward_sha256": self.state["base"]["forward_sha256"],
            "baseline_unchanged": True,
            "auto_increment_columns": proof.get("auto_increment_columns", []),
            "pending_operation": value is not None and value["phase"] == "pending",
            "runtime": runtime,
            "runtime_verified": runtime["verified"],
            **retention_report(self._retention_journals()),
        }

    def runtime_proof(self):
        binding = self.current_binding
        release = self.deploy.release_path(binding["release"])
        services = self.deploy.run(
            "read-only runtime services", self.deploy.compose(release, "config", "--services")
        ).split()
        if not services:
            raise MaintenanceError("A running service is required for runtime evidence")
        for service in services:
            container = self.deploy.run(
                "read-only runtime container", self.deploy.compose(release, "ps", "-q", service)
            ).strip()
            if not container:
                raise MaintenanceError("Fixed runtime is not running")
            rows = json.loads(
                self.deploy.run(
                    "read-only runtime identity", ["sudo", "-n", "docker", "inspect", container]
                )
            )
            if (
                len(rows) != 1
                or rows[0].get("Image") != binding["image"]
                or rows[0].get("State", {}).get("Running") is not True
                or rows[0].get("State", {}).get("Health", {}).get("Status") != "healthy"
            ):
                raise MaintenanceError("Running image or health differs from the fixed runtime")
        if "qs-ai" in services:
            self.deploy.run(
                "read-only runtime mTLS",
                self.deploy.compose(
                    release,
                    "exec",
                    "-T",
                    "qs-ai",
                    "/app/.venv/bin/python",
                    "-m",
                    "qs_ai.bootstrap.grpc_probe",
                ),
            )
        return {
            "verified": True,
            "image": binding["image"],
            "release": binding["release"],
            "healthy": True,
            "mtls": "qs-ai" in services,
        }


def deployment_guard(root, token=None):
    assert_idle(root, token)
    pointer = active_context(root)
    if pointer:
        current = private(root / "state.json")["current"]
        allowed = {pointer["release"]}
        intent = operation(root)
        if token and intent:
            allowed.update(intent.get("allowed_releases", []))
        if current not in allowed:
            raise MaintenanceError("Runtime publication differs from the active followup binding")


def publish_runtime(deploy, release, proof):
    pointer = active_context(deploy.ROOT)
    if pointer is None:
        return
    assert_idle(deploy.ROOT, getattr(deploy, "_SCHEMA_FOLLOWUP_OPERATION", None))
    state = private(deploy.ROOT / "schema-maintenance" / pointer["context_id"] / "host.json")
    if (
        state.get("format") != FORMAT
        or state.get("executor", {}).get("helper_sha256") != pointer["helper_sha256"]
    ):
        raise MaintenanceError("Active followup executor binding changed")
    heads = proof.get("current")
    if heads not in ([OLD_HEAD], [NEW_HEAD]) or heads != deploy.expected_heads(release):
        raise MaintenanceError("Runtime publication lacks an exact known schema proof")
    if legacy.database_binding(release) != state["database_binding"]:
        raise MaintenanceError("Runtime publication changed the database binding")
    append_binding(deploy, state, release, heads[0])


def protected_images(root):
    """Fixed executor and legacy image remain available across later ordinary releases."""
    pointer = active_context(root)
    if pointer is None:
        return []
    directory = root / "schema-maintenance" / pointer["context_id"]
    context = private(directory / "host.json")
    base_path = root / "schema-maintenance" / context["base"]["id"] / "host.json"
    if (
        context.get("format") != FORMAT
        or context["executor"]["helper_sha256"] != pointer["helper_sha256"]
        or checksum(base_path) != context["base"]["host_sha256"]
    ):
        raise MaintenanceError("Protected image provenance changed")
    images = {context["executor_release"]["image"], private(base_path)["old_image"]}
    if any(not re.fullmatch(r"sha256:[0-9a-f]{64}", image) for image in images):
        raise MaintenanceError("Protected image identity is incomplete")
    return sorted(images)


def ordinary_release(deploy, release_name):
    """Future same-head releases use this fixed executor and its durable handoff intent."""
    from urllib.parse import unquote, urlsplit

    pointer = active_context(deploy.ROOT)
    if pointer is None:
        raise MaintenanceError("No active followup context")
    target = deploy.release_path(release_name)
    raw = private(target / "runtime.json")["services"]["qs-ai"]["environment"]["QS_AI_DATABASE_URL"]
    parsed = urlsplit(raw.replace("$$", "$"))
    credentials = {
        "username": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }
    legacy.validate_credentials(credentials)
    code = (
        deploy.ROOT
        / "schema-maintenance"
        / pointer["context_id"]
        / ("code-" + pointer["executor_revision"])
    )
    followup = Followup(deploy, pointer["context_id"], credentials, code=code)
    return followup.release(release_name)


def ordinary_restore(deploy, state):
    """An application rollback retains the same 0040 layout and original handoff clock."""
    from urllib.parse import unquote, urlsplit

    pointer = active_context(deploy.ROOT)
    if pointer is None:
        raise MaintenanceError("No active followup context")
    pending = operation(deploy.ROOT)
    target_name = (
        pending["allowed_releases"][1]
        if pending and pending["phase"] == "pending"
        else state.get("previous")
    )
    if not target_name:
        raise MaintenanceError("No previous successful release")
    target = deploy.release_path(target_name)
    raw = private(target / "runtime.json")["services"]["qs-ai"]["environment"]["QS_AI_DATABASE_URL"]
    parsed = urlsplit(raw.replace("$$", "$"))
    credentials = legacy.validate_credentials(
        {"username": unquote(parsed.username or ""), "password": unquote(parsed.password or "")}
    )
    code = (
        deploy.ROOT
        / "schema-maintenance"
        / pointer["context_id"]
        / ("code-" + pointer["executor_revision"])
    )
    followup = Followup(deploy, pointer["context_id"], credentials, code=code)
    return followup.release(target_name, restore=True)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "bootstrap",
            "status",
            "prepare",
            "rehearse",
            "release",
            "rollback",
            "cleanup",
            "retirement-plan",
            "retirement-backup",
            "retirement-verify",
            "retirement-restore",
        ),
    )
    parser.add_argument("--id", required=True)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--base-id")
    parser.add_argument("--base-revision")
    parser.add_argument("--release")
    args = parser.parse_args()
    try:
        credentials = legacy.validate_credentials(private(args.credentials))
        deploy = legacy.deployment()
        with legacy.locks(deploy):
            followup = Followup(deploy, args.id, credentials)
            if args.command == "bootstrap":
                result = followup.bootstrap(
                    args.base_id or "", args.base_revision or "", args.release or ""
                )
            elif args.command == "release":
                result = followup.release(args.release or "")
            elif args.command.startswith("retirement-"):
                followup.bound()
                followup.assert_idle()
                retirement = module_at(
                    "prompt_retirement_followup",
                    Path(__file__).with_name("prompt_retirement_followup.py"),
                )
                result = {
                    "retirement": retirement.run(
                        followup, args.command.removeprefix("retirement-"), args
                    )
                }
            else:
                result = getattr(followup, args.command)()
        print(
            json.dumps(
                {"followup": "ok", "id": args.id, "operation": args.command, **result},
                sort_keys=True,
            )
        )
    except Exception:
        print(
            json.dumps(
                {
                    "followup": "failed",
                    "id": args.id,
                    "operation": args.command,
                    "reason": (
                        "Followup failed; inspect private facts "
                        "and retain any pending original operation"
                    ),
                },
                sort_keys=True,
            )
        )
        raise SystemExit(1) from None
    finally:
        args.credentials.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
