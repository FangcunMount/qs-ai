"""Private, locked serverA schema maintenance. Standard library; never ordinary apply/restore."""

import argparse
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import stat
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

OLD_HEAD = "0038_messaging_observations"
NEW_HEAD = "0040_module_table_names"
FORMAT = "qs-ai-host-schema-maintenance/v1"
COPY_SECONDS = 25 * 60
WINDOW_SECONDS = 30 * 60
CLEANUP_SECONDS = 240
CAPABILITIES = {
    "process",
    "source_read",
    "source_exchange",
    "source_trigger_visibility",
    "source_routine_visibility",
    "source_event_visibility",
    "target_create_restore",
    "archive_create",
    "rollback_create",
    "failed_create",
    "random_stage_create",
    "random_old_create",
    "source_objects_absent",
    "target_absent",
}


class MaintenanceError(RuntimeError):
    """Only fixed, credential-free diagnostic messages may reach the caller."""

    def __init__(self, reason, report=None):
        super().__init__(reason)
        self.report = report


def deployment():
    spec = importlib.util.spec_from_file_location(
        "schema_maintenance_deploy", Path(__file__).with_name("deploy.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def private_json(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
        raise MaintenanceError("Expected an owned private regular JSON file")
    return json.loads(path.read_text())


def durable(path, value):
    """Publish complete private facts, including the containing directory entry."""
    descriptor, candidate = tempfile.mkstemp(prefix=".maintenance-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(candidate, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(candidate).unlink(missing_ok=True)


def checksum(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def source_checksum(code):
    """Bind the read-only source mount, excluding interpreter-generated caches."""
    source = code / "src"
    if not (source / "qs_ai/maintenance/schema_refactor/backup.py").is_file():
        raise MaintenanceError("The selected maintenance source archive is incomplete")
    files = {}
    for path in sorted(source.rglob("*")):
        if "__pycache__" in path.parts or path.suffix in (".pyc", ".pyo"):
            continue
        if path.is_symlink():
            raise MaintenanceError("The maintenance source archive contains a symbolic link")
        if path.is_file():
            files[path.relative_to(source).as_posix()] = checksum(path)
    return hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()


def database_binding(release):
    try:
        runtime = json.loads((release / "runtime.json").read_text())
        env = runtime["services"]["qs-ai"]["environment"]
        url = urlsplit(env["QS_AI_DATABASE_URL"].replace("$$", "$"))
        if url.scheme != "mysql+asyncmy" or url.query or url.fragment or not url.hostname:
            raise ValueError
        schema = url.path.removeprefix("/")
        if schema != "ai":
            raise ValueError
        return {"host": url.hostname, "port": url.port or 3306, "schema": schema}
    except (KeyError, ValueError, TypeError, OSError):
        raise MaintenanceError("Frozen runtime must bind the ai MySQL source") from None


def maintenance_url(binding, credentials, schema=None):
    host = binding["host"]
    host = "[" + host + "]" if ":" in host else host
    return (
        "mysql+asyncmy://"
        + quote(credentials["username"], safe="")
        + ":"
        + quote(credentials["password"], safe="")
        + "@"
        + host
        + ":"
        + str(binding["port"])
        + "/"
        + (schema or binding["schema"])
    )


def validate_credentials(value):
    if set(value) != {"username", "password"} or any(
        not isinstance(item, str) or not item or any(char in item for char in "\r\n\x00")
        for item in value.values()
    ):
        raise MaintenanceError("Credentials JSON must contain only username and password")
    return value


def safe_preflight(value):
    """Only typed permission/identity metadata may cross the SSH/Actions boundary."""
    result = {}
    for key, pattern in {
        "head": r"[0-9]{4}_[a-z0-9_]+",
        "source_schema": r"[a-z][a-z0-9_]{0,63}",
        "source_server_uuid": r"[0-9a-f-]{36}",
        "target": r"[a-z][a-z0-9_]{0,63}",
        "mysql_version": r"[0-9][a-zA-Z0-9._-]{0,59}",
        "source_collation": r"[a-z][a-z0-9_]{0,63}",
    }.items():
        if isinstance(value.get(key), str) and re.fullmatch(pattern, value[key]):
            result[key] = value[key]
    for key, allowed in {
        "preflight": {"passed", "missing_capabilities"},
        "cross_schema_fk_visibility": {"all_schemas", "schema_scoped_requires_admin_evidence"},
        "source_schema_contract": {"verified"},
        "permission_check": {"grant_metadata_only_no_database_created"},
    }.items():
        if value.get(key) in allowed:
            result[key] = value[key]
    for key in ("ok", "partial_revokes", "target_exists"):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    for key in ("tables", "rows", "estimated_rows", "allocated_bytes"):
        if type(value.get(key)) is int and 0 <= value[key] <= 2**64:
            result[key] = value[key]
    if isinstance(value.get("capabilities"), dict):
        result["capabilities"] = {
            key: item
            for key, item in value["capabilities"].items()
            if key in CAPABILITIES and type(item) is bool
        }
        result["missing_capabilities"] = [
            key for key in value.get("missing_capabilities", []) if key in result["capabilities"]
        ]
    return result


@contextlib.contextmanager
def locks(deploy):
    # Reuse exactly the existing inodes and retain both until all verification/state writes finish.
    with deploy.global_deploy_lock(), (deploy.ROOT / "deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


DOMAIN_READER = r"""
import asyncio, hashlib, json, os
from dataclasses import asdict
import sqlalchemy as sa
from qs_ai.infrastructure.persistence.mysql.database import Database, Transactions, MySQLProbe
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets, prompt_assets
async def main():
    database = Database(os.environ['QS_AI_DATABASE_URL'])
    transactions = Transactions(database)
    try:
        health = await MySQLProbe(database).check()
        if not health.ready: raise ValueError('Fixed image does not recognize the database')
        async with transactions.open() as db:
            row = (await db.execute(sa.select(prompt_assets.c.template_id, prompt_assets.c.version)
                .order_by(prompt_assets.c.template_id, prompt_assets.c.version).limit(1))).first()
        if row is None: raise ValueError('A real Prompt fact is required for fixed image evidence')
        asset = await MySQLPromptAssets(transactions).get(row[0], row[1])
        if asset is None: raise ValueError('Fixed image Prompt reader could not read the fact')
        digest = hashlib.sha256(json.dumps(asdict(asset),sort_keys=True,ensure_ascii=False,
            separators=(',',':')).encode()).hexdigest()
        print(json.dumps({'fixed_image_domain':'passed','prompt_sha256':digest}))
    finally:
        await database.close()
asyncio.run(main())
"""

LAYOUT_READER = r"""
import asyncio, json, os
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
async def main():
    engine = create_async_engine(os.environ['QS_AI_DATABASE_URL'],
        connect_args={'connect_timeout':3})
    try:
        async with asyncio.timeout(45):
            async with engine.connect() as conn:
                result = await conn.execute(sa.text('SELECT version_num FROM alembic_version'))
                heads = list(result.scalars())
                server = await conn.scalar(sa.text('SELECT @@server_uuid'))
                tables = await conn.scalar(sa.text('SELECT COUNT(*) FROM information_schema.TABLES '
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'"))
                print(json.dumps({'server_uuid':server,'heads':heads,'tables':tables}))
    finally:
        await engine.dispose()
asyncio.run(main())
"""

VALIDATION_READER = r"""
import asyncio, json, os
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine
from qs_ai.maintenance.schema_refactor import contracts, control
from qs_ai.maintenance.schema_refactor.validation import require_schema
async def main():
    engine = create_async_engine(os.environ['QS_AI_DATABASE_URL'],
        connect_args={'connect_timeout':3})
    try:
        async with asyncio.timeout(60):
            async with engine.connect() as conn:
                def validate(c):
                    state = control.read(Path('/maintenance/forward.json'))
                    control._binding(c,state)
                    if os.environ.get('MAINTENANCE_FENCED') == '1':
                        control.fenced(c,(state['source'],state['target'],state['archive']),True)
                    head = contracts.head(c,state['source'])
                    require_schema(c,head,state['source'])
                    return {'validated':True,'head':head,'server_uuid':state['server_uuid']}
                result = await conn.run_sync(validate)
                print(json.dumps(result))
    finally:
        await engine.dispose()
asyncio.run(main())
"""


class DockerTools:
    def __init__(self, deploy, directory, binding, credentials, code):
        self.deploy, self.directory, self.binding = deploy, directory, binding
        self.credentials, self.code = credentials, code
        self.image = None
        self.deadline = None

    def container(self, image, command, *, schema=None, source=False, timeout=1200, fenced=False):
        self.deploy.image_id(image)
        if self.deadline is not None:
            timeout = min(timeout, self.deadline - time.monotonic())
            if timeout <= 0:
                raise MaintenanceError("Original maintenance operation budget exceeded")
        name = "qs-ai-maintenance-" + uuid4().hex
        descriptor, env_name = tempfile.mkstemp(prefix=".credentials-", dir=self.directory)
        env_path = Path(env_name)
        try:
            with os.fdopen(descriptor, "w") as stream:
                stream.write(
                    "QS_AI_DATABASE_URL="
                    + maintenance_url(self.binding, self.credentials, schema)
                    + "\n"
                )
                stream.write("PYTHONDONTWRITEBYTECODE=1\n")
                if fenced:
                    stream.write("MAINTENANCE_FENCED=1\n")
                if source:
                    stream.write("PYTHONPATH=/app/src\n")
            args = [
                "sudo",
                "-n",
                "docker",
                "run",
                "--rm",
                "--name",
                name,
                "--read-only",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges:true",
                "--network",
                "infra-network",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--tmpfs",
                "/tmp:rw,noexec,nosuid,size=64m",
                "--env-file",
                str(env_path),
                "--mount",
                f"type=bind,src={self.directory},dst=/maintenance",
            ]
            if source:
                args += ["--mount", f"type=bind,src={self.code / 'src'},dst=/app/src,readonly"]
            args += [image, "/app/.venv/bin/python", *command]
            result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
            if result.returncode:
                report = None
                with contextlib.suppress(ValueError, IndexError, AttributeError):
                    value = json.loads(result.stdout.strip().splitlines()[-1])
                    if value.get("preflight") == "missing_capabilities":
                        report = safe_preflight(value)
                raise MaintenanceError(
                    "Maintenance container failed; writers must remain stopped "
                    "if maintenance began",
                    report=report,
                )
            try:
                return json.loads(result.stdout.strip().splitlines()[-1])
            except (ValueError, IndexError):
                raise MaintenanceError(
                    "Maintenance container returned no verified JSON receipt"
                ) from None
        except subprocess.TimeoutExpired:
            # Docker client cancellation alone would leave its server-side operation running.
            with contextlib.suppress(Exception):
                subprocess.run(
                    ["sudo", "-n", "docker", "stop", "-t", "10", name],
                    capture_output=True,
                    timeout=15,
                )
            raise MaintenanceError(
                "Maintenance container timed out; saved journal must be probed"
            ) from None
        finally:
            env_path.unlink(missing_ok=True)

    def backup(
        self,
        command,
        image,
        schema,
        path=None,
        target=None,
        expected=None,
        source=False,
        timeout=1200,
    ):
        args = ["-m", "qs_ai.maintenance.schema_refactor.backup", command, "--schema", schema]
        if path:
            args += ["--path", "/maintenance/" + path.name]
        if target:
            args += ["--target", target]
        if expected:
            args += [
                "--expected-server-uuid",
                expected["source_server_uuid"],
                "--expected-head",
                expected["head"],
            ]
            if command in ("inspect", "restore"):
                args += ["--expected-sha256", expected["backup_sha256"]]
        return self.container(image, args, source=source, timeout=timeout)

    def refactor(
        self, command, journal, state, *, schema=None, stopped_at=None, inherit=None, fast=False
    ):
        args = [
            "-m",
            "qs_ai.maintenance.schema_refactor",
            command,
            "--journal",
            "/maintenance/" + journal.name,
        ]
        if command == "plan":
            for key in ("source", "target", "archive", "old_image", "new_image"):
                args += ["--" + key.replace("_", "-"), state[key]]
            if inherit:
                args += ["--inherit-journal", "/maintenance/" + inherit.name]
        if command in ("copy", "verify", "switch", "rollback"):
            args += ["--writers-stopped"]
        if command in ("copy", "rollback"):
            args += ["--stopped-at", stopped_at]
        if fast:
            args += ["--runtime-never-started"]
        return self.container(self.image, args, schema=schema, timeout=WINDOW_SECONDS + 30)

    def domain(self, image, schema):
        value = self.container(image, ["-c", DOMAIN_READER], schema=schema, timeout=60)
        if value.get("fixed_image_domain") != "passed" or not re.fullmatch(
            r"[0-9a-f]{64}", value.get("prompt_sha256", "")
        ):
            raise MaintenanceError("Fixed image domain reader proof missing")
        return value

    def layout(self, image, schema="ai"):
        return self.container(image, ["-c", LAYOUT_READER], schema=schema, timeout=60)

    def validate(self, image, *, fenced=False):
        return self.container(image, ["-c", VALIDATION_READER], timeout=70, fenced=fenced)


class Maintenance:
    def __init__(self, deploy, identifier, credentials, code=None, tools=None):
        if not re.fullmatch(r"[a-z0-9][a-z0-9_]{0,31}", identifier):
            raise MaintenanceError("Invalid maintenance identifier")
        validate_credentials(credentials)
        self.deploy, self.identifier = deploy, identifier
        self.directory = deploy.ROOT / "schema-maintenance" / identifier
        self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        if (
            self.directory.is_symlink()
            or self.directory.stat().st_mode & 0o077
            or self.directory.stat().st_uid != os.getuid()
        ):
            raise MaintenanceError("Maintenance directory must be owned and private")
        self.path = self.directory / "host.json"
        self.code = code or Path(__file__).resolve().parents[2]
        self.state = private_json(self.path) if self.path.exists() else None
        current = private_json(deploy.ROOT / "state.json")
        if self.state:
            if self.state.get("format") != FORMAT or self.state.get("id") != identifier:
                raise MaintenanceError("Unknown host journal")
            allowed = {self.state["old_release"], self.state.get("new_release")}
            if current.get("current") not in allowed:
                raise MaintenanceError("Current release differs from the maintenance journal")
            old = deploy.release_path(self.state["old_release"])
        else:
            old = deploy.release_path(current["current"])
        self.old = old
        self.binding = database_binding(old)
        self._cleanup_attempts = {}
        stop = (
            self.state.get("rollback_stopped_at", self.state.get("stopped_at"))
            if self.state
            else None
        )
        # Identity inspection runs before the command entry point. It must not
        # inherit ordinary deployment's twenty-minute subprocess timeout.
        with self.host_budget(stop, short_probe=True):
            self.old_image = deploy.release_image_id(old, verify_identity=True)
            if self.state:
                self.bound()
        self.tools = tools or DockerTools(
            deploy,
            self.directory,
            self.binding,
            credentials,
            self.code,
        )
        if self.state:
            self.tools.image = self.state.get("new_image")

    def save(self):
        durable(self.path, self.state)

    def bound(self):
        if self.state.get("code_revision") != self.code.name:
            raise MaintenanceError("Maintenance code revision differs from the prepared journal")
        if self.state.get("helper_sha256") != checksum(Path(__file__)):
            raise MaintenanceError("Maintenance helper differs from the prepared journal")
        if self.state.get("deployment_helper_sha256") != checksum(
            Path(__file__).with_name("deploy.py")
        ):
            raise MaintenanceError("Deployment helper differs from the prepared journal")
        if self.state.get("source_sha256") != source_checksum(self.code):
            raise MaintenanceError("Maintenance source differs from the prepared journal")
        if self.state["binding"] != self.binding or self.state["old_image"] != self.old_image:
            raise MaintenanceError("Old release, image or source identity drifted")
        for key in ("old", "new"):
            if key + "_release" not in self.state:
                continue
            release = self.deploy.release_path(self.state[key + "_release"])
            for file in ("manifest.json", "runtime.json", "compose.yaml"):
                if checksum(release / file) != self.state[key + "_files"][file]:
                    raise MaintenanceError("Frozen release files changed")
            if (
                self.deploy.release_image_id(release, verify_identity=True)
                != self.state[key + "_image"]
            ):
                raise MaintenanceError("Fixed release image changed")

    def evidence(self, receipt, *, head, schema="ai"):
        if (
            receipt.get("head") != head
            or receipt.get("source_schema") != schema
            or not receipt.get("source_server_uuid")
        ):
            raise MaintenanceError("Backup source identity or schema head mismatch")
        if self.state and receipt["source_server_uuid"] != self.state["server_uuid"]:
            raise MaintenanceError("Maintenance connection reached a different MySQL server")
        return receipt

    def preflight(self, *, allow_owned_clone=False, head=OLD_HEAD):
        target = "ai_refactor_rehearsal_" + self.identifier
        try:
            result = self.tools.backup(
                "preflight", self.old_image, "ai", target=target, source=True
            )
        except MaintenanceError as error:
            result = error.report
            if not allow_owned_clone or not result:
                raise
        owned = (
            self.state
            and self.state.get("rehearsal", {}).get("passed")
            and self.state["rehearsal"]["restored"].get("target") == target
        )
        existing_owned = (
            allow_owned_clone
            and owned
            and result.get("target") == target
            and result.get("target_exists") is True
            and result.get("missing_capabilities") == ["target_absent"]
        )
        capabilities = result.get("capabilities", {})
        valid = set(capabilities) == CAPABILITIES and all(
            type(capabilities[key]) is bool
            and (capabilities[key] or (key == "target_absent" and existing_owned))
            for key in CAPABILITIES
        )
        if not valid or (
            (
                result.get("preflight") != "passed"
                or result.get("ok") is not True
                or result.get("missing_capabilities")
            )
            and not existing_owned
        ):
            raise MaintenanceError(
                "Maintenance account lacks the required verified capabilities",
                report=safe_preflight(result),
            )
        return self.evidence(result, head=head)

    def permissions(self, *, head=OLD_HEAD, allow_owned_clone=False):
        proof = self.preflight(allow_owned_clone=allow_owned_clone, head=head)
        if proof.get("cross_schema_fk_visibility") != "all_schemas":
            raise MaintenanceError(
                "Preparation requires complete cross-schema foreign-key visibility",
                report=safe_preflight(proof),
            )
        return proof

    def prepare(self, release_name):
        if not release_name:
            raise MaintenanceError("Prepare requires the exact staged immutable release")
        revision = self.code.name.removeprefix("code-")
        if not re.fullmatch(r"[0-9a-f]{40}", revision) or not release_name.startswith(
            revision + "-"
        ):
            raise MaintenanceError(
                "Staged release must match the selected immutable maintenance code revision"
            )
        if self.state:
            if self.state.get("new_release") != release_name:
                raise MaintenanceError("Maintenance ID cannot be rebound to a different release")
            self.bound()
            if self.state["phase"] not in ("preparing", "prepared") or "stopped_at" in self.state:
                raise MaintenanceError("Preparation cannot reset a started maintenance operation")
        proof = self.permissions(allow_owned_clone=bool(self.state and self.state.get("rehearsal")))
        release = self.deploy.release_path(release_name)
        self.deploy.messaging_transition(self.old, release)
        self.deploy.messaging_key_preflight(self.old)
        prepared = self.deploy.stage_prepare(release)
        if prepared.get("expected_heads") != [NEW_HEAD] or prepared.get("prepared") is not True:
            raise MaintenanceError("Staged image does not provide the exact new schema contract")
        if database_binding(release) != self.binding:
            raise MaintenanceError("Staged release points at a different database")
        image = self.deploy.release_image_id(release, verify_identity=True)
        if prepared.get("image_id") != image or any(
            prepared.get(key + "_sha256") != checksum(release / file)
            for key, file in (
                ("manifest", "manifest.json"),
                ("runtime", "runtime.json"),
                ("compose", "compose.yaml"),
            )
        ):
            raise MaintenanceError(
                "Prepared image metadata receipt differs from the immutable release"
            )
        if self.state is None:
            self.state = {
                "format": FORMAT,
                "id": self.identifier,
                "phase": "preparing",
                "old_release": self.old.name,
                "new_release": release.name,
                "old_image": self.old_image,
                "new_image": image,
                "binding": self.binding,
                "server_uuid": proof["source_server_uuid"],
                "created_at": datetime.now(UTC).isoformat(),
                "code_revision": self.code.name,
                "helper_sha256": checksum(Path(__file__)),
                "deployment_helper_sha256": checksum(Path(__file__).with_name("deploy.py")),
                "source_sha256": source_checksum(self.code),
                "prepared_receipt": prepared,
                "retain_until": (datetime.now(UTC) + timedelta(days=30)).isoformat(),
            }
            for key, directory in (("old", self.old), ("new", release)):
                self.state[key + "_files"] = {
                    file: checksum(directory / file)
                    for file in ("manifest.json", "runtime.json", "compose.yaml")
                }
            self.save()
        self.bound()
        self.tools.image = image
        self.pin_old()
        self.plan(
            "forward.json", "ai", "ai_refactor_" + self.identifier, "ai_backup_" + self.identifier
        )
        self.state["phase"] = "prepared"
        self.save()
        return {"phase": "prepared", "release": release.name}

    def pin_old(self):
        archive = self.directory / "old-image.tar"
        if not archive.exists():
            self.deploy.save_loaded_archive(archive, self.old_image)
        identity = self.deploy.archive_identity(
            archive, None, json.loads((self.old / "manifest.json").read_text())["revision"]
        )
        inspected = json.loads(
            self.deploy.run(
                "old image retention identity",
                ["sudo", "-n", "docker", "image", "inspect", self.old_image],
            )
        )[0]
        self.deploy.match_image(
            inspected, identity, json.loads((self.old / "manifest.json").read_text())["revision"]
        )
        pin = "qs-ai-schema-pin-" + self.identifier
        found = subprocess.run(
            ["sudo", "-n", "docker", "inspect", pin], capture_output=True, text=True, timeout=30
        )
        if found.returncode == 0:
            row = json.loads(found.stdout)[0]
            if row["Image"] != self.old_image or row["State"]["Status"] != "created":
                raise MaintenanceError("Rollback image pin is not the never-started fixed image")
        else:
            self.deploy.run(
                "pin old image",
                [
                    "sudo",
                    "-n",
                    "docker",
                    "create",
                    "--name",
                    pin,
                    "--network",
                    "none",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    self.old_image,
                    "/bin/true",
                ],
            )
        self.state["old_image_archive_sha256"] = checksum(archive)
        self.state["old_image_pin"] = pin
        self.save()

    def journal(self, name):
        state = private_json(self.directory / name)
        if (
            state.get("format") != "qs-ai-schema-refactor/v1"
            or state.get("server_uuid") != self.state["server_uuid"]
            or state.get("old_image") != self.old_image
            or state.get("new_image") != self.state["new_image"]
        ):
            raise MaintenanceError("Conversion journal identity mismatch")
        return state

    def plan(self, name, source, target, archive, inherit=None):
        path = self.directory / name
        if not path.exists():
            state = dict(
                source=source,
                target=target,
                archive=archive,
                old_image=self.old_image,
                new_image=self.state["new_image"],
            )
            self.tools.refactor(
                "plan",
                path,
                state,
                schema=source,
                inherit=self.directory / inherit if inherit else None,
            )
        state = self.journal(name)
        if (state["source"], state["target"], state["archive"]) != (source, target, archive):
            raise MaintenanceError("Conversion journal destinations mismatch")
        if state["phase"] in ("planned", "prepare_pending"):
            self.tools.refactor("prepare", path, state, schema=source)
        return self.journal(name)

    def exchange(self, name, stopped_at):
        path = self.directory / name
        state = self.journal(name)
        if state.get("stopped_at", stopped_at) != stopped_at:
            raise MaintenanceError("The original writer stop clock cannot be reset")
        if state["phase"] in ("prepared", "copy_pending", "copied"):
            self.tools.refactor("copy", path, state, schema=state["source"], stopped_at=stopped_at)
            state = self.journal(name)
        if state["phase"] == "copied":
            self.tools.refactor("verify", path, state, schema=state["source"])
            state = self.journal(name)
        if state["phase"] in ("verified", "switch_pending", "switched"):
            self.tools.refactor("switch", path, state, schema=state["source"])
        if self.journal(name)["phase"] != "switched":
            raise MaintenanceError("Atomic exchange is not confirmed; keep writers stopped")

    def rehearse(self):
        if not self.state or self.state["phase"] != "prepared":
            raise MaintenanceError("Rehearsal requires prepared formal destinations")
        self.bound()
        started = time.monotonic()
        backup = self.directory / "rehearsal.jsonl.gz"
        receipt = self.evidence(
            self.tools.backup("backup", self.tools.image, "ai", backup), head=OLD_HEAD
        )
        self.state["rehearsal_backup"] = receipt
        self.save()
        clone = "ai_refactor_rehearsal_" + self.identifier
        restored = self.tools.backup(
            "restore", self.tools.image, "ai", backup, target=clone, expected=receipt
        )
        if (
            restored.get("verified") is not True
            or restored.get("target") != clone
            or restored.get("server_uuid") != self.state["server_uuid"]
            or any(
                restored.get(k) != receipt[k]
                for k in ("backup_sha256", "manifest_sha256", "physical_manifest_sha256")
            )
        ):
            raise MaintenanceError("Independent backup restore proof differs")
        stop = datetime.now(UTC).isoformat()
        old = self.tools.domain(self.old_image, clone)
        self.plan(
            "rehearsal-forward.json",
            clone,
            "ai_refactor_probe_" + self.identifier,
            "ai_backup_probe_" + self.identifier,
        )
        self.exchange("rehearsal-forward.json", stop)
        new = self.tools.domain(self.state["new_image"], clone)
        self.plan(
            "rehearsal-reverse.json",
            clone,
            "ai_rollback_rehearsal_" + self.identifier,
            "ai_failed_rehearsal_" + self.identifier,
            "rehearsal-forward.json",
        )
        self.exchange("rehearsal-reverse.json", stop)
        old_after = self.tools.domain(self.old_image, clone)
        for name in ("rehearsal-reverse.json", "rehearsal-forward.json"):
            state = self.journal(name)
            self.tools.refactor(
                "rollback", self.directory / name, state, schema=clone, stopped_at=stop, fast=True
            )
            if self.journal(name)["phase"] != "rolled_back":
                raise MaintenanceError("Rehearsal fast rollback outcome is unknown")
        final = self.tools.domain(self.old_image, clone)
        if (
            old != new
            or old != old_after
            or old != final
            or time.monotonic() - started >= COPY_SECONDS
        ):
            raise MaintenanceError(
                "Rehearsal exceeded the exchange budget or fixed image readers differ"
            )
        self.state["rehearsal"] = {
            "passed": True,
            "server_uuid": self.state["server_uuid"],
            "old_image": self.old_image,
            "new_image": self.state["new_image"],
            "restored": restored,
            "elapsed_seconds": time.monotonic() - started,
            "domain": old,
        }
        self.save()
        return {"phase": "rehearsed", "elapsed_seconds": self.state["rehearsal"]["elapsed_seconds"]}

    def clock(self, key):
        if key not in self.state:
            self.state[key] = datetime.now(UTC).isoformat()
            self.save()
        return self.state[key]

    def remaining(self, stopped_at, seconds):
        elapsed = (datetime.now(UTC) - datetime.fromisoformat(stopped_at)).total_seconds()
        if elapsed < 0 or elapsed >= seconds:
            raise MaintenanceError(
                "Original maintenance stop window exceeded; keep writers stopped"
            )
        return seconds - elapsed

    @contextlib.contextmanager
    def host_budget(
        self, stopped_at=None, *, cleanup=False, recovery_probe=False, short_probe=False
    ):
        """Compose subprocesses share the original remaining stop window."""
        original = getattr(self.deploy, "run", None)
        cleanup_deadline = time.monotonic() + CLEANUP_SECONDS
        probe_deadline = time.monotonic() + 60

        def budget():
            if cleanup:
                remaining = cleanup_deadline - time.monotonic()
                if remaining <= 0:
                    raise MaintenanceError(
                        "Writer cleanup could not be confirmed; inspect saved journals"
                    )
                return remaining
            probe_remaining = probe_deadline - time.monotonic()
            if short_probe and stopped_at is None:
                remaining = probe_remaining
            else:
                try:
                    remaining = self.remaining(stopped_at, WINDOW_SECONDS)
                    if short_probe:
                        remaining = min(remaining, probe_remaining)
                except MaintenanceError:
                    if not (recovery_probe or short_probe):
                        raise
                    remaining = probe_remaining
            if remaining <= 0:
                raise MaintenanceError("Read-only recovery inspection budget exceeded")
            return remaining

        def bounded_run(_phase, args):
            args = list(args)
            name = None
            if "compose" in args and "run" in args[args.index("compose") + 1 :]:
                # A killed Docker CLI does not kill its daemon-owned probe container.
                index = args.index("run", args.index("compose") + 1)
                name = "qs-ai-maintenance-probe-" + uuid4().hex
                args[index + 1 : index + 1] = ["--name", name]
            try:
                result = subprocess.run(
                    args,
                    capture_output=True,
                    text=True,
                    timeout=min(1200, budget()),
                )
            except subprocess.TimeoutExpired:
                if name:
                    with contextlib.suppress(Exception):
                        subprocess.run(
                            ["sudo", "-n", "docker", "stop", "-t", "10", name],
                            capture_output=True,
                            timeout=15,
                        )
                raise MaintenanceError(
                    "Frozen release command exceeded the original stop window"
                ) from None
            if result.returncode:
                raise MaintenanceError("Frozen release command failed; details withheld")
            return result.stdout

        self.deploy.run = bounded_run
        try:
            yield
        finally:
            if original is None:
                del self.deploy.run
            else:
                self.deploy.run = original

    def stop_failed_release(self, release):
        # Cleanup may still be needed after the stop window expired. Give it a
        # separate bounded drain, without resetting any cutover/rollback clock.
        if release.name in self._cleanup_attempts:
            if self._cleanup_attempts[release.name]:
                return
            raise MaintenanceError("Writer cleanup could not be confirmed; inspect saved journals")
        self._cleanup_attempts[release.name] = False
        with self.host_budget(cleanup=True):
            self.deploy.stop_release(release)
        self._cleanup_attempts[release.name] = True

    def operation_budget(self, stopped_at, journal_name):
        state = self.journal(journal_name)
        if state["phase"] in ("switch_pending", "switched"):
            # The CLI itself prohibits an overdue new exchange. A short bounded probe
            # may confirm an earlier exchange, but cannot reset the original clock.
            try:
                budget = self.remaining(stopped_at, COPY_SECONDS)
            except MaintenanceError:
                budget = 60
        else:
            budget = self.remaining(stopped_at, COPY_SECONDS)
        self.tools.deadline = time.monotonic() + budget

    def record_ready(self, release, previous, database):
        if database.get("current") != self.deploy.expected_heads(release):
            raise MaintenanceError("Running image schema validation differs")
        durable(
            release / "verification.json",
            {
                "phase": "ready",
                "database": database,
                "image_id": self.deploy.release_image_id(release),
                "schema_maintenance_id": self.identifier,
            },
        )
        if previous is not None:
            durable(
                self.deploy.ROOT / "state.json",
                {"current": release.name, "previous": previous.name},
            )

    def start_verified(self, release, previous, stop, phase):
        self.remaining(stop, WINDOW_SECONDS)
        from_release = (
            self.old
            if release.name == self.state["new_release"]
            else self.deploy.release_path(self.state["new_release"])
        )
        with self.host_budget(stop):
            self.deploy.messaging_transition(from_release, release)
            self.deploy.messaging_key_preflight(release)
        # Persist BEFORE verify: compose up can admit writers even if readiness later fails.
        key = (
            "runtime_started"
            if release.name == self.state["new_release"]
            else "old_runtime_started"
        )
        self.state[key] = True
        self.state["phase"] = phase + "_starting"
        self.save()
        try:
            with self.host_budget(stop):
                database = self.deploy.verify(release)
                self.remaining(stop, WINDOW_SECONDS)
                self.record_ready(release, previous, database)
        except Exception:
            self.stop_failed_release(release)
            raise
        self.state["phase"] = phase
        self.save()

    def switch(self):
        if self.state and "stopped_at" in self.state:
            if self.journal("forward.json")["phase"] in ("switch_pending", "switched"):
                self.operation_budget(self.state["stopped_at"], "forward.json")
            else:
                try:
                    self.tools.deadline = time.monotonic() + self.remaining(
                        self.state["stopped_at"], WINDOW_SECONDS
                    )
                except MaintenanceError:
                    self.tools.deadline = time.monotonic() + 60
            with self.host_budget(self.state["stopped_at"], recovery_probe=True):
                return self._switch()
        return self._switch()

    def _switch(self):
        if not self.state or not self.state.get("rehearsal", {}).get("passed"):
            raise MaintenanceError("Switch requires a successful independent restore and rehearsal")
        self.bound()
        rehearsal = self.state["rehearsal"]
        if any(rehearsal[k] != self.state[k] for k in ("server_uuid", "old_image", "new_image")):
            raise MaintenanceError("Rehearsal belongs to different fixed images or server")
        if self.state.get("runtime_started"):
            raise MaintenanceError(
                "Runtime may have written; use full rollback, never replay forward exchange"
            )
        if self.state.get("forward_aborted"):
            raise MaintenanceError(
                "Old runtime resumed; this maintenance ID cannot reset its source or stop clock"
            )
        self.journal("forward.json")
        self.permissions(allow_owned_clone=True, head=self.actual())
        receipt = self.evidence(
            self.tools.backup(
                "inspect",
                self.tools.image,
                "ai",
                self.directory / "rehearsal.jsonl.gz",
                expected=self.state["rehearsal_backup"],
            ),
            head=OLD_HEAD,
        )
        if receipt.get("verified") is not True or any(
            receipt.get(key) != self.state["rehearsal_backup"][key]
            for key in ("backup_sha256", "manifest_sha256", "physical_manifest_sha256")
        ):
            raise MaintenanceError("Independent backup evidence changed before stopping writers")
        stopped_at = self.clock("stopped_at")
        try:
            with self.host_budget(stopped_at):
                self.deploy.stop_release(self.old)
            if "stopped_backup" not in self.state:
                budget = self.remaining(stopped_at, COPY_SECONDS)
                receipt = self.evidence(
                    self.tools.backup(
                        "backup",
                        self.tools.image,
                        "ai",
                        self.directory / "stopped.jsonl.gz",
                        timeout=budget,
                    ),
                    head=OLD_HEAD,
                )
                self.state["stopped_backup"] = receipt
                self.save()
            self.operation_budget(stopped_at, "forward.json")
            self.exchange("forward.json", stopped_at)
            accepted = hashlib.sha256(
                json.dumps(
                    self.journal("forward.json")["source_manifest"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            if accepted != self.state["stopped_backup"]["manifest_sha256"]:
                raise MaintenanceError("Stopped backup differs from accepted conversion facts")
            release = self.deploy.release_path(self.state["new_release"])
            with self.host_budget(stopped_at):
                self.deploy.probe(release, True)
            self.start_verified(release, self.old, stopped_at, "switched")
        except Exception:
            if self.state.get("runtime_started"):
                self.stop_failed_release(self.deploy.release_path(self.state["new_release"]))
                raise MaintenanceError(
                    "New runtime may have written; keep writers stopped "
                    "and perform full inverse rollback"
                ) from None
            try:
                self.recover_old(stopped_at)
            except Exception:
                raise MaintenanceError(
                    "Switch could not be safely recovered inside the original window; "
                    "preserve journals and keep writers stopped"
                ) from None
            raise MaintenanceError(
                "Switch failed; old runtime safely restored inside the original stop window"
            ) from None
        return {"phase": "switched", "release": release.name}

    def actual(self, *, fenced=False):
        result = self.tools.validate(self.state["new_image"], fenced=fenced)
        if (
            result.get("validated") is not True
            or result.get("server_uuid") != self.state["server_uuid"]
            or result.get("head") not in (OLD_HEAD, NEW_HEAD)
        ):
            raise MaintenanceError("Complete live schema or server identity cannot be verified")
        return result["head"]

    def recover_old(self, stopped_at):
        self.remaining(stopped_at, WINDOW_SECONDS)
        self.tools.deadline = time.monotonic() + self.remaining(stopped_at, WINDOW_SECONDS)
        head = self.actual(fenced=True)
        if head == NEW_HEAD:
            self.remaining(stopped_at, COPY_SECONDS)
            self.tools.deadline = time.monotonic() + self.remaining(stopped_at, COPY_SECONDS)
            forward = self.journal("forward.json")
            self.tools.refactor(
                "rollback",
                self.directory / "forward.json",
                forward,
                stopped_at=stopped_at,
                fast=True,
            )
            if (
                self.journal("forward.json")["phase"] != "rolled_back"
                or self.actual(fenced=True) != OLD_HEAD
            ):
                raise MaintenanceError("Fast rollback is not confirmed")
        with self.host_budget(stopped_at):
            self.deploy.probe(self.old, True)
        self.state["forward_aborted"] = True
        self.save()
        self.start_verified(self.old, None, stopped_at, "recovered_old")

    def rollback(self):
        if self.state and (
            "rollback_stopped_at" in self.state
            or ("stopped_at" in self.state and not self.state.get("runtime_started"))
        ):
            stop = self.state.get("rollback_stopped_at", self.state["stopped_at"])
            if (self.directory / "reverse.json").exists():
                self.operation_budget(stop, "reverse.json")
            else:
                try:
                    self.tools.deadline = time.monotonic() + self.remaining(stop, WINDOW_SECONDS)
                except MaintenanceError:
                    self.tools.deadline = time.monotonic() + 60
            with self.host_budget(stop, recovery_probe=True):
                return self._rollback()
        return self._rollback()

    def _rollback(self):
        if not self.state or "stopped_at" not in self.state:
            raise MaintenanceError("Rollback requires a recorded stopped maintenance operation")
        self.bound()
        release = self.deploy.release_path(self.state["new_release"])
        self.permissions(allow_owned_clone=True, head=self.actual())
        self.deploy.messaging_transition(release, self.old)
        self.deploy.messaging_key_preflight(self.old)
        stop = (
            self.clock("rollback_stopped_at")
            if self.state.get("runtime_started")
            else self.state["stopped_at"]
        )
        with self.host_budget(stop):
            self.deploy.stop_release(release)
            self.deploy.stop_release(self.old)
        if not self.state.get("runtime_started"):
            self.recover_old(stop)
            return {"phase": "recovered_old", "release": self.old.name}
        else:
            if (self.directory / "reverse.json").exists():
                self.operation_budget(stop, "reverse.json")
            else:
                self.tools.deadline = time.monotonic() + self.remaining(stop, COPY_SECONDS)
            self.plan(
                "reverse.json",
                "ai",
                "ai_rollback_" + self.identifier,
                "ai_failed_" + self.identifier,
                "forward.json",
            )
            self.exchange("reverse.json", stop)
        # Never probe the new image against the restored old schema, and never ordinary restore.
        with self.host_budget(stop):
            self.deploy.probe(self.old, True)
        self.start_verified(self.old, release, stop, "rolled_back")
        return {"phase": "rolled_back", "release": self.old.name}

    def status(self):
        if not self.state:
            return {"phase": "unplanned"}
        stop = self.state.get("rollback_stopped_at", self.state.get("stopped_at"))
        budget = 60
        if stop:
            with contextlib.suppress(MaintenanceError):
                budget = min(budget, self.remaining(stop, WINDOW_SECONDS))
        self.tools.deadline = time.monotonic() + budget
        with self.host_budget(stop, short_probe=True):
            return self._status()

    def _status(self):
        self.bound()
        journals = {
            name: self.journal(name)["phase"]
            for name in ("forward.json", "reverse.json")
            if (self.directory / name).exists()
        }
        pending = any(value.endswith("pending") for value in journals.values())
        layout = self.tools.layout(self.state["new_image"])
        if (
            layout.get("server_uuid") != self.state["server_uuid"]
            or layout.get("heads") not in ([OLD_HEAD], [NEW_HEAD])
            or layout.get("tables") != (54 if layout["heads"] == [OLD_HEAD] else 44)
        ):
            raise MaintenanceError(
                "Unknown current schema layout; preserve journals and writers stopped"
            )
        self.actual()
        return {
            "phase": self.state["phase"],
            "journals": journals,
            "runtime_started": self.state.get("runtime_started", False),
            "stopped_at": self.state.get("stopped_at"),
            "rollback_stopped_at": self.state.get("rollback_stopped_at"),
            "retain_until": self.state["retain_until"],
            "source_head": layout["heads"][0],
            "server_uuid": layout["server_uuid"],
            "tables": layout["tables"],
            "exchange_requires_journal_probe": pending,
        }


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("preflight", "prepare", "rehearse", "switch", "rollback", "status")
    )
    parser.add_argument("--id", required=True)
    parser.add_argument("--release")
    parser.add_argument("--credentials", required=True, type=Path)
    args = parser.parse_args()
    accepted_credentials = False
    try:
        credentials = validate_credentials(private_json(args.credentials))
        accepted_credentials = True
        deploy = deployment()
        with locks(deploy):
            maintenance = Maintenance(deploy, args.id, credentials)
            if args.command == "prepare":
                result = maintenance.prepare(args.release)
            else:
                result = getattr(maintenance, args.command)()
            if args.command == "preflight":
                result = safe_preflight(result)
            if maintenance.state:
                result.update(
                    {
                        key: maintenance.state[key]
                        for key in (
                            "old_release",
                            "new_release",
                            "old_image",
                            "new_image",
                            "server_uuid",
                        )
                    }
                )
        print(
            json.dumps(
                {"schema_maintenance": "ok", "id": args.id, "operation": args.command, **result},
                sort_keys=True,
            )
        )
    except Exception as error:
        # Raw subprocess output, database bodies/URLs and exception text remain private.
        reason = (
            str(error)
            if type(error) is MaintenanceError
            else "Operation failed; preserve the journal and keep writers stopped "
            "if maintenance began"
        )
        print(
            json.dumps(
                {
                    "schema_maintenance": "failed",
                    "id": args.id,
                    "operation": args.command,
                    "reason": reason,
                    **(error.report if type(error) is MaintenanceError and error.report else {}),
                }
            )
        )
        raise SystemExit(1) from None
    finally:
        # The transport also removes it. Never retain maintenance credentials between stages.
        if accepted_credentials and not args.credentials.is_symlink():
            args.credentials.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
