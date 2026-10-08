"""Host maintenance fences and durable release publication, without a production connection."""

import contextlib
import fcntl
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_deployment import load

OLD_RELEASE = "a" * 40 + "-1-1"
NEW_RELEASE = "b" * 40 + "-2-1"
OLD_IMAGE = "sha256:" + "1" * 64
NEW_IMAGE = "sha256:" + "2" * 64
SERVER = "cc33cc33-1111-2222-3333-444444444444"
CREDENTIALS = {"username": "maintenance", "password": "special @:/?$中文"}


@pytest.fixture
def host(tmp_path, monkeypatch):
    module = load("deploy/serverA/schema_maintenance.py")
    events, schemas = [], {"ai": module.OLD_HEAD}
    state_path = tmp_path / "state.json"
    module.durable(state_path, {"current": OLD_RELEASE, "previous": None})
    for name, image in ((OLD_RELEASE, OLD_IMAGE), (NEW_RELEASE, NEW_IMAGE)):
        release = tmp_path / "releases" / name
        release.mkdir(parents=True)
        (release / "manifest.json").write_text(json.dumps({"image_id": image}))
        (release / "compose.yaml").write_text("services: frozen")
        (release / "runtime.json").write_text(
            json.dumps(
                {
                    "services": {
                        "qs-ai": {
                            "environment": {
                                "QS_AI_DATABASE_URL": "mysql+asyncmy://app:p%24%24%40@mysql.internal:3306/ai"
                            }
                        }
                    }
                }
            )
        )

    @contextlib.contextmanager
    def global_lock():
        events.append("global-enter")
        try:
            yield
        finally:
            events.append("global-exit")

    def release_path(name):
        if name not in (OLD_RELEASE, NEW_RELEASE):
            raise ValueError("unexpected release")
        return tmp_path / "releases" / name

    def prepare(path):
        events.append("stage-image-only")
        return {
            "prepared": True,
            "expected_heads": [module.NEW_HEAD],
            "image_id": NEW_IMAGE,
            **{
                key + "_sha256": module.checksum(path / file)
                for key, file in (
                    ("manifest", "manifest.json"),
                    ("runtime", "runtime.json"),
                    ("compose", "compose.yaml"),
                )
            },
        }

    def stop(path):
        events.append("stop:" + path.name)

    def probe(path, require):
        events.append("probe:" + path.name)
        assert require
        expected = module.OLD_HEAD if path.name == OLD_RELEASE else module.NEW_HEAD
        if schemas["ai"] != expected:
            raise module.MaintenanceError("strict fixed image schema mismatch")
        return {"current": [expected], "expected": [expected]}

    def verify(path):
        events.append("start:" + path.name)
        journal = module.private_json(tmp_path / "schema-maintenance" / "test_1" / "host.json")
        assert (
            journal["runtime_started" if path.name == NEW_RELEASE else "old_runtime_started"]
            is True
        )
        return probe(path, True)

    deploy = SimpleNamespace(
        ROOT=tmp_path,
        global_deploy_lock=global_lock,
        release_path=release_path,
        release_image_id=lambda p, **_: OLD_IMAGE if p.name == OLD_RELEASE else NEW_IMAGE,
        stage_prepare=prepare,
        stop_release=stop,
        probe=probe,
        verify=verify,
        expected_heads=lambda p: [module.OLD_HEAD if p.name == OLD_RELEASE else module.NEW_HEAD],
        messaging_transition=lambda a, b: events.append("messaging:" + a.name + ":" + b.name),
        messaging_key_preflight=lambda p: events.append("keys:" + p.name),
    )

    class Tools:
        image = None
        domain_digest = "d" * 64
        facts = 1
        fail = None
        exchange_count = 0

        def backup(self, operation, image, schema, path=None, target=None, **kwargs):
            events.append("backup:" + operation)
            if self.fail == "backup:" + operation:
                raise module.MaintenanceError("Backup evidence rejected")
            receipt = dict(
                source_schema=schema,
                source_server_uuid=SERVER,
                head=module.OLD_HEAD if operation == "inspect" else schemas[schema],
                backup_sha256="f" * 64,
                manifest_sha256=hashlib.sha256(
                    json.dumps(
                        {"facts": self.facts}, sort_keys=True, separators=(",", ":")
                    ).encode()
                ).hexdigest(),
                physical_manifest_sha256="c" * 64,
                tables=54,
                rows=1,
            )
            if operation == "preflight":
                exists = target in schemas
                receipt.update(
                    preflight="missing_capabilities" if exists else "passed",
                    ok=not exists,
                    missing_capabilities=["target_absent"] if exists else [],
                    capabilities={
                        key: (not exists if key == "target_absent" else True)
                        for key in module.CAPABILITIES
                    },
                    cross_schema_fk_visibility="all_schemas",
                    target=target,
                    target_exists=exists,
                )
            if operation == "inspect":
                receipt.update(verified=True)
            if operation == "restore":
                schemas[target] = schemas[schema]
                receipt.update(target=target, server_uuid=SERVER, verified=True)
            return receipt

        def refactor(
            self, operation, path, state, *, schema=None, stopped_at=None, inherit=None, fast=False
        ):
            events.append("conversion:" + operation + (":fast" if fast else ""))
            if operation == "plan":
                assert "old_image" in state and "new_image" in state
                state = {
                    **state,
                    "format": "qs-ai-schema-refactor/v1",
                    "phase": "planned",
                    "server_uuid": SERVER,
                    "source_head": schemas[state["source"]],
                    "target_head": module.OLD_HEAD if inherit else module.NEW_HEAD,
                }
                if inherit:
                    assert module.private_json(inherit)["phase"] == "switched"
            else:
                state = module.private_json(path)
            if operation == "prepare":
                state["phase"] = "prepared"
            elif operation == "copy":
                state.update(
                    phase="copied", stopped_at=stopped_at, source_manifest={"facts": self.facts}
                )
            elif operation == "verify":
                state["phase"] = "verified"
            elif operation == "switch":
                if (
                    state["phase"] != "switched"
                    and schemas[state["source"]] != state["target_head"]
                ):
                    self.exchange_count += 1
                    schemas[state["source"]] = state["target_head"]
                state["phase"] = "switch_pending" if self.fail == "disconnect" else "switched"
            elif operation == "rollback":
                assert fast
                schemas[state["source"]] = state["source_head"]
                state["phase"] = "rolled_back"
            module.durable(path, state)
            if self.fail == operation or (self.fail == "disconnect" and operation == "switch"):
                raise module.MaintenanceError("Uncertain operation; probe saved journal")
            return {"schema_refactor": "ok", "phase": state["phase"]}

        def domain(self, image, schema):
            events.append("domain:" + image)
            return {"fixed_image_domain": "passed", "prompt_sha256": self.domain_digest}

        def layout(self, image, schema="ai"):
            events.append("layout-only")
            return {
                "server_uuid": SERVER,
                "heads": [schemas[schema]],
                "tables": 54 if schemas[schema] == module.OLD_HEAD else 44,
            }

        def validate(self, image, *, fenced=False):
            events.append("validate:fenced" if fenced else "validate:readonly")
            if self.fail == "disconnect" and schemas["ai"] == module.NEW_HEAD:
                raise module.MaintenanceError("Server connection outcome remains unknown")
            return {"validated": True, "head": schemas["ai"], "server_uuid": SERVER}

    tools = Tools()
    code = tmp_path / ("code-" + "b" * 40)
    backup_source = code / "src/qs_ai/maintenance/schema_refactor/backup.py"
    backup_source.parent.mkdir(parents=True)
    backup_source.write_text("# synthetic private archive\n")
    maintenance = module.Maintenance(deploy, "test_1", CREDENTIALS, code=code, tools=tools)
    monkeypatch.setattr(maintenance, "pin_old", lambda: events.append("pin-old"))
    return SimpleNamespace(
        module=module,
        deploy=deploy,
        tools=tools,
        maintenance=maintenance,
        events=events,
        schemas=schemas,
        state_path=state_path,
        root=tmp_path,
    )


def prepared(host, *, rehearsal=True):
    host.maintenance.prepare(NEW_RELEASE)
    if rehearsal:
        host.maintenance.rehearse()
    return host.maintenance


@pytest.mark.parametrize("identifier", ["../ai", "UPPER", "a-b", "", "a" * 33, "ai/other"])
def test_identifier_rejects_path_and_sql_names(host, identifier):
    with pytest.raises(host.module.MaintenanceError, match="identifier"):
        host.module.Maintenance(host.deploy, identifier, CREDENTIALS)


@pytest.mark.parametrize(
    "credentials",
    [
        {},
        {**CREDENTIALS, "database": "ai"},
        {**CREDENTIALS, "password": ""},
        {**CREDENTIALS, "username": "bad\nline"},
    ],
)
def test_only_private_maintenance_credentials_are_accepted(host, credentials):
    with pytest.raises(host.module.MaintenanceError, match="username and password"):
        host.module.Maintenance(host.deploy, "test_1", credentials)


def test_credentials_require_private_regular_owned_file(host):
    path = host.root / "credentials.json"
    path.write_text(json.dumps(CREDENTIALS))
    path.chmod(0o644)
    with pytest.raises(host.module.MaintenanceError, match="private"):
        host.module.private_json(path)
    path.chmod(0o600)
    assert host.module.private_json(path) == CREDENTIALS
    link = host.root / "link.json"
    link.symlink_to(path)
    with pytest.raises(host.module.MaintenanceError, match="private"):
        host.module.private_json(link)


def test_preflight_has_no_stop_plan_or_release_state_write(host):
    before = host.state_path.read_bytes()
    assert host.maintenance.preflight()["head"] == host.module.OLD_HEAD
    assert host.events == ["backup:preflight"]
    assert host.state_path.read_bytes() == before
    assert not host.maintenance.path.exists()


def test_prepare_holds_both_deploy_lock_inodes_and_never_starts_service(host):
    before = host.state_path.read_bytes()
    with host.module.locks(host.deploy):
        with (host.root / "deploy.lock").open("a") as competing:
            with pytest.raises(BlockingIOError):
                fcntl.flock(competing, fcntl.LOCK_EX | fcntl.LOCK_NB)
        host.maintenance.prepare(NEW_RELEASE)
        assert host.events[0] == "global-enter" and "global-exit" not in host.events
    assert host.events[-1] == "global-exit"
    assert not any(x.startswith(("start:", "stop:")) for x in host.events)
    assert host.state_path.read_bytes() == before
    assert host.maintenance.state["server_uuid"] == SERVER


def test_new_release_endpoint_mismatch_stops_before_plan_and_writer_stop(host):
    path = host.deploy.release_path(NEW_RELEASE) / "runtime.json"
    path.write_text(path.read_text().replace("mysql.internal", "other.internal"))
    with pytest.raises(host.module.MaintenanceError, match="different database"):
        host.maintenance.prepare(NEW_RELEASE)
    assert "conversion:plan" not in host.events
    assert not host.maintenance.path.exists()
    assert not any(x.startswith("stop:") for x in host.events)


@pytest.mark.parametrize("part", ["runtime.json", "manifest.json", "compose.yaml"])
def test_frozen_release_drift_rejected_before_cutover(host, part):
    maintenance = prepared(host)
    path = host.deploy.release_path(NEW_RELEASE) / part
    path.write_text(path.read_text() + " ")
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="files changed"):
        maintenance.switch()
    assert host.events == []


def test_journal_server_mismatch_never_executes_copy(host):
    maintenance = prepared(host)
    path = maintenance.directory / "forward.json"
    value = host.module.private_json(path)
    value["server_uuid"] = "other"
    host.module.durable(path, value)
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="identity mismatch"):
        maintenance.switch()
    assert "conversion:copy" not in host.events
    assert host.module.private_json(host.state_path)["current"] == OLD_RELEASE


def test_rehearsal_requires_independent_restore_before_formal_source_can_stop(host):
    maintenance = prepared(host, rehearsal=False)
    host.tools.fail = "backup:restore"
    with pytest.raises(host.module.MaintenanceError, match="Backup evidence"):
        maintenance.rehearse()
    assert "rehearsal" not in maintenance.state
    assert not any(x.startswith("stop:") for x in host.events)
    with pytest.raises(host.module.MaintenanceError, match="rehearsal"):
        maintenance.switch()


def test_rehearsal_uses_both_fixed_images_full_inverse_and_two_fast_swaps(host):
    maintenance = prepared(host)
    assert maintenance.state["rehearsal"]["passed"]
    assert host.events.count("conversion:plan") == 3
    assert host.events.count("conversion:rollback:fast") == 2
    assert "domain:" + OLD_IMAGE in host.events and "domain:" + NEW_IMAGE in host.events
    assert host.schemas["ai"] == host.module.OLD_HEAD
    assert not any(x.startswith(("start:", "stop:")) for x in host.events)


def test_cutover_deadline_is_durable_and_cannot_be_reset_on_retry(host):
    maintenance = prepared(host)
    original = (datetime.now(UTC) - timedelta(minutes=26)).isoformat()
    maintenance.state["stopped_at"] = original
    maintenance.save()
    with pytest.raises(host.module.MaintenanceError, match="old runtime safely restored"):
        maintenance.switch()
    assert host.module.private_json(maintenance.path)["stopped_at"] == original
    assert host.schemas["ai"] == host.module.OLD_HEAD
    assert "start:" + OLD_RELEASE in host.events
    assert "start:" + NEW_RELEASE not in host.events
    assert maintenance.state["forward_aborted"]


def test_completed_exchange_is_not_repeated_after_lost_reply(host):
    maintenance = prepared(host)
    baseline = host.tools.exchange_count
    host.tools.fail = "disconnect"
    with pytest.raises(host.module.MaintenanceError, match="could not be safely"):
        maintenance.switch()
    original = maintenance.state["stopped_at"]
    assert maintenance.journal("forward.json")["phase"] == "switch_pending"
    assert host.module.private_json(host.state_path)["current"] == OLD_RELEASE
    host.tools.fail = None
    maintenance.switch()
    assert host.tools.exchange_count == baseline + 1
    assert maintenance.state["stopped_at"] == original
    assert host.module.private_json(host.state_path)["current"] == NEW_RELEASE


def test_start_failure_does_not_publish_new_state_or_restore_old_backup(host):
    maintenance = prepared(host)

    def failed_start(path):
        assert host.module.private_json(maintenance.path)["runtime_started"]
        host.tools.facts = 2  # The new image can write before a readiness failure.
        raise RuntimeError("synthetic secret driver details")

    host.deploy.verify = failed_start
    with pytest.raises(host.module.MaintenanceError, match="full inverse rollback"):
        maintenance.switch()
    assert maintenance.state["runtime_started"] is True
    assert host.schemas["ai"] == host.module.NEW_HEAD
    assert host.module.private_json(host.state_path)["current"] == OLD_RELEASE
    assert host.events[-1] == "stop:" + NEW_RELEASE
    assert host.events.count("conversion:rollback:fast") == 2  # rehearsal only

    def verify_old(path):
        assert path.name == OLD_RELEASE
        assert host.tools.facts == 2
        return host.deploy.probe(path, True)

    host.deploy.verify = verify_old
    maintenance.rollback()
    assert maintenance.journal("reverse.json")["source_manifest"] == {"facts": 2}
    assert host.events.count("conversion:rollback:fast") == 2
    assert host.module.private_json(host.state_path)["current"] == OLD_RELEASE


def test_validation_failure_after_exchange_remains_safe_for_explicit_fast_rollback(host):
    maintenance = prepared(host)
    original_probe = host.deploy.probe

    def broken_new_probe(path, require):
        if path.name == NEW_RELEASE:
            raise host.module.MaintenanceError("strict probe failed")
        return original_probe(path, require)

    host.deploy.probe = broken_new_probe
    with pytest.raises(host.module.MaintenanceError, match="old runtime safely restored"):
        maintenance.switch()
    assert not maintenance.state.get("runtime_started")
    host.deploy.probe = original_probe
    maintenance.rollback()
    assert maintenance.journal("forward.json")["phase"] == "rolled_back"
    assert host.schemas["ai"] == host.module.OLD_HEAD
    assert not (maintenance.directory / "reverse.json").exists()


def test_unknown_layout_status_never_changes_saved_journals_or_starts_writer(host):
    maintenance = prepared(host)
    host.schemas["ai"] = "0039_data_consolidation"
    before = maintenance.path.read_bytes()
    with pytest.raises(host.module.MaintenanceError, match="Unknown current schema"):
        maintenance.status()
    assert maintenance.path.read_bytes() == before
    assert not any(x.startswith("start:") for x in host.events)


def test_docker_maintenance_is_isolated_and_credentials_are_removed_on_failure(host, monkeypatch):
    tools = host.module.DockerTools(
        host.deploy, host.maintenance.directory, host.maintenance.binding, CREDENTIALS, host.root
    )
    captured = []

    def execute(args, **kwargs):
        captured.append(args)
        env = Path(args[args.index("--env-file") + 1])
        assert env.stat().st_mode & 0o777 == 0o600
        assert "QS_AI_DATABASE_URL=" in env.read_text()
        assert CREDENTIALS["password"] not in " ".join(args)
        return SimpleNamespace(returncode=1, stdout="password secret", stderr="driver secret")

    monkeypatch.setattr(host.module.subprocess, "run", execute)
    host.deploy.image_id = lambda image: image
    with pytest.raises(host.module.MaintenanceError, match="container failed"):
        tools.container(OLD_IMAGE, ["-m", "safe.module"], source=True)
    args = captured[0]
    for option in ("--rm", "--read-only", "--cap-drop", "--security-opt"):
        assert option in args
    assert args[args.index("--network") + 1] == "infra-network"
    assert args[args.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    assert not list(host.maintenance.directory.glob(".credentials-*"))


def test_timeout_stops_only_its_owned_named_container(host, monkeypatch):
    tools = host.module.DockerTools(
        host.deploy, host.maintenance.directory, host.maintenance.binding, CREDENTIALS, host.root
    )
    host.deploy.image_id = lambda image: image
    captured = []

    def execute(args, **kwargs):
        captured.append(args)
        if "run" in args:
            raise subprocess.TimeoutExpired(args, 1)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(host.module.subprocess, "run", execute)
    with pytest.raises(host.module.MaintenanceError, match="timed out"):
        tools.container(NEW_IMAGE, ["-m", "safe.module"], timeout=1)
    assert captured[1][-1] == captured[0][captured[0].index("--name") + 1]
    assert "stop" in captured[1] and "qs-ai" not in captured[1]
    assert not list(host.maintenance.directory.glob(".credentials-*"))


def test_only_completed_verified_clone_can_be_existing_during_internal_permission_check(host):
    maintenance = prepared(host)
    with pytest.raises(host.module.MaintenanceError, match="capabilities"):
        maintenance.preflight()
    proof = maintenance.permissions(allow_owned_clone=True)
    assert proof["preflight"] == "missing_capabilities" and proof["ok"] is False
    assert proof["missing_capabilities"] == ["target_absent"]
    maintenance.state["rehearsal"]["restored"]["target"] = "ai_refactor_other"
    with pytest.raises(host.module.MaintenanceError, match="capabilities"):
        maintenance.permissions(allow_owned_clone=True)


@pytest.mark.parametrize("operation", ["prepare", "switch"])
def test_incomplete_cross_schema_visibility_blocks_before_stopping_service(host, operation):
    maintenance = prepared(host) if operation == "switch" else host.maintenance
    original = host.tools.backup

    def scoped(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[0] == "preflight":
            result["cross_schema_fk_visibility"] = "schema_scoped_requires_admin_evidence"
        return result

    host.tools.backup = scoped
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="foreign-key visibility"):
        maintenance.prepare(NEW_RELEASE) if operation == "prepare" else maintenance.switch()
    assert not any(event.startswith(("stop:", "start:")) for event in host.events)
    assert "stopped_at" not in (maintenance.state or {})


def test_restore_hash_or_server_mismatch_never_qualifies_rehearsal(host):
    maintenance = prepared(host, rehearsal=False)
    original = host.tools.backup

    def wrong(*args, **kwargs):
        result = original(*args, **kwargs)
        if args[0] == "restore":
            result["physical_manifest_sha256"] = "0" * 64
        return result

    host.tools.backup = wrong
    with pytest.raises(host.module.MaintenanceError, match="restore proof differs"):
        maintenance.rehearse()
    assert "rehearsal" not in maintenance.state
    assert not any(event.startswith("stop:") for event in host.events)


def test_expired_pending_status_is_read_only_and_retains_private_journals(host):
    maintenance = prepared(host)
    maintenance.state["stopped_at"] = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    maintenance.save()
    path = maintenance.directory / "forward.json"
    state = host.module.private_json(path)
    state["phase"] = "switch_pending"
    host.module.durable(path, state)
    before = {file.name: file.read_bytes() for file in maintenance.directory.glob("*.json")}
    host.events.clear()
    result = maintenance.status()
    assert result["exchange_requires_journal_probe"] is True
    assert host.events == ["layout-only", "validate:readonly"]
    assert before == {file.name: file.read_bytes() for file in maintenance.directory.glob("*.json")}


def test_copy_failure_can_resume_verified_old_source_without_calling_rollback(host):
    maintenance = prepared(host)
    host.tools.fail = "copy"
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="old runtime safely restored"):
        maintenance.switch()
    assert host.schemas["ai"] == host.module.OLD_HEAD
    assert "validate:fenced" in host.events
    assert "start:" + OLD_RELEASE in host.events and "start:" + NEW_RELEASE not in host.events
    assert "conversion:rollback:fast" not in host.events
    assert host.module.private_json(host.state_path)["previous"] is None


def test_recovery_never_extends_original_thirty_minute_window(host):
    maintenance = prepared(host)
    original = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    maintenance.state["stopped_at"] = original
    maintenance.save()
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="could not be safely"):
        maintenance.switch()
    assert maintenance.state["stopped_at"] == original
    assert not any(event.startswith("start:") for event in host.events)
    assert host.module.private_json(host.state_path)["current"] == OLD_RELEASE


def test_explicit_rollback_before_exchange_validates_old_source_and_uses_original_clock(host):
    maintenance = prepared(host)
    original = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
    maintenance.state["stopped_at"] = original
    maintenance.save()
    host.events.clear()
    result = maintenance.rollback()
    assert result["phase"] == "recovered_old"
    assert "conversion:rollback:fast" not in host.events
    assert "conversion:plan" not in host.events
    assert maintenance.state["stopped_at"] == original
    assert "rollback_stopped_at" not in maintenance.state
    assert "validate:fenced" in host.events


def test_compose_probe_timeout_cleans_only_its_named_container(host, monkeypatch):
    maintenance = prepared(host)
    stopped = (datetime.now(UTC) - timedelta(minutes=29)).isoformat()
    calls = []

    def execute(args, **kwargs):
        calls.append((args, kwargs["timeout"]))
        if "compose" in args:
            assert 0 < kwargs["timeout"] <= 60
            raise subprocess.TimeoutExpired(args, kwargs["timeout"])
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(host.module.subprocess, "run", execute)
    with maintenance.host_budget(stopped):
        with pytest.raises(host.module.MaintenanceError, match="original stop window"):
            host.deploy.run(
                "probe", ["sudo", "-n", "docker", "compose", "run", "--rm", "qs-ai", "python"]
            )
    first = calls[0][0]
    name = first[first.index("--name") + 1]
    assert calls[1][0][-1] == name and name.startswith("qs-ai-maintenance-probe-")
    assert calls[1][1] == 15
    assert not hasattr(host.deploy, "run")


def test_verify_subprocesses_share_decreasing_original_remaining_time(host, monkeypatch):
    maintenance = prepared(host)
    real_datetime = datetime
    now = [datetime.now(UTC)]
    original = (now[0] - timedelta(minutes=20)).isoformat()

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0] if tz else now[0].replace(tzinfo=None)

    monkeypatch.setattr(host.module, "datetime", Clock)
    timeouts = []

    def execute(args, **kwargs):
        timeouts.append(kwargs["timeout"])
        now[0] += timedelta(seconds=60)
        return SimpleNamespace(returncode=0, stdout="safe")

    monkeypatch.setattr(host.module.subprocess, "run", execute)

    def verify(path):
        assert host.module.private_json(maintenance.path)["runtime_started"]
        host.deploy.run("first", ["synthetic"])
        host.deploy.run("second", ["synthetic"])
        return {"current": [host.module.NEW_HEAD]}

    host.deploy.verify = verify
    maintenance.state["stopped_at"] = original
    maintenance.save()
    maintenance.start_verified(
        host.deploy.release_path(NEW_RELEASE), maintenance.old, original, "switched"
    )
    assert timeouts == [600, 540]
    assert real_datetime.fromisoformat(
        maintenance.state["stopped_at"]
    ) == real_datetime.fromisoformat(original)
    assert not hasattr(host.deploy, "run")


def test_failure_writer_cleanup_is_bounded_even_after_original_window_expired(host, monkeypatch):
    maintenance = prepared(host)
    now = [100.0]
    monkeypatch.setattr(host.module.time, "monotonic", lambda: now[0])
    calls = []

    def execute(args, **kwargs):
        calls.append(kwargs["timeout"])
        now[0] += 10
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(host.module.subprocess, "run", execute)

    def stop(path):
        host.deploy.run("stop", ["synthetic"])
        host.deploy.run("confirm", ["synthetic"])

    host.deploy.stop_release = stop
    maintenance.state["stopped_at"] = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    original = maintenance.state["stopped_at"]
    maintenance.stop_failed_release(host.deploy.release_path(NEW_RELEASE))
    assert calls == [240, 230]
    assert maintenance.state["stopped_at"] == original


@pytest.mark.parametrize("elapsed,expected", [(None, 60), (1770, 30), (1860, 60)])
def test_constructor_identity_checks_share_short_original_budget(
    host, monkeypatch, elapsed, expected
):
    maintenance = prepared(host, rehearsal=False)
    now = [datetime.now(UTC)]
    monotonic = [100.0]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now[0]

    if elapsed is not None:
        maintenance.state["stopped_at"] = (now[0] - timedelta(seconds=elapsed)).isoformat()
        maintenance.save()
    before = maintenance.path.read_bytes()
    monkeypatch.setattr(host.module, "datetime", Clock)
    monkeypatch.setattr(host.module.time, "monotonic", lambda: monotonic[0])
    timeouts = []

    def execute(args, **kwargs):
        timeouts.append(kwargs["timeout"])
        monotonic[0] += 5
        now[0] += timedelta(seconds=5)
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(host.module.subprocess, "run", execute)

    def identity(path, **kwargs):
        host.deploy.run("fixed identity", ["synthetic"])
        return OLD_IMAGE if path.name == OLD_RELEASE else NEW_IMAGE

    host.deploy.release_image_id = identity
    host.events.clear()
    host.module.Maintenance(
        host.deploy, "test_1", CREDENTIALS, code=maintenance.code, tools=host.tools
    )
    assert timeouts == [expected, expected - 5, expected - 10]
    assert maintenance.path.read_bytes() == before
    assert not host.events and not hasattr(host.deploy, "run")


def test_source_archive_drift_blocks_resume_before_stopping_writers(host):
    maintenance = prepared(host)
    source = maintenance.code / "src/qs_ai/maintenance/schema_refactor/backup.py"
    source.write_text("# changed source after preparation\n")
    before = maintenance.path.read_bytes()
    host.events.clear()
    with pytest.raises(host.module.MaintenanceError, match="source differs"):
        host.module.Maintenance(
            host.deploy, "test_1", CREDENTIALS, code=maintenance.code, tools=host.tools
        )
    assert not host.events and maintenance.path.read_bytes() == before


def test_expired_recovery_inspections_share_one_short_budget(host, monkeypatch):
    maintenance = prepared(host)
    now = [100.0]
    monkeypatch.setattr(host.module.time, "monotonic", lambda: now[0])
    timeouts = []

    def execute(args, **kwargs):
        timeouts.append(kwargs["timeout"])
        now[0] += 40
        return SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(host.module.subprocess, "run", execute)
    original = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    with maintenance.host_budget(original, recovery_probe=True):
        host.deploy.run("identity one", ["synthetic"])
        host.deploy.run("identity two", ["synthetic"])
        with pytest.raises(host.module.MaintenanceError, match="inspection budget"):
            host.deploy.run("identity three", ["synthetic"])
    assert timeouts == [60, 20]


def test_failed_writer_cleanup_is_never_retried_with_a_new_drain_budget(host):
    maintenance = prepared(host)
    calls = []

    def stop(path):
        calls.append(path.name)
        raise host.module.MaintenanceError("Synthetic cleanup timeout")

    host.deploy.stop_release = stop
    release = host.deploy.release_path(NEW_RELEASE)
    with pytest.raises(host.module.MaintenanceError, match="Synthetic"):
        maintenance.stop_failed_release(release)
    with pytest.raises(host.module.MaintenanceError, match="could not be confirmed"):
        maintenance.stop_failed_release(release)
    assert calls == [NEW_RELEASE]


def test_safe_failed_preflight_report_never_exposes_password_grants_or_driver_objects(host):
    value = dict(
        preflight="missing_capabilities",
        ok=False,
        head=host.module.OLD_HEAD,
        source_schema="ai",
        source_server_uuid=SERVER,
        partial_revokes=False,
        capabilities={"process": True, "archive_create": False, "bad_secret": "secret"},
        missing_capabilities=["archive_create", "bad_secret"],
        password="secret",
        grants=["secret"],
        cross_schema_fk_visibility="schema_scoped_requires_admin_evidence",
        errors=[{"driver": "secret"}],
    )
    result = host.module.safe_preflight(value)
    assert result["missing_capabilities"] == ["archive_create"]
    assert result["capabilities"] == {"process": True, "archive_create": False}
    assert "secret" not in json.dumps(result)
    transport = load("scripts/cd/schema_maintenance.py")
    receipt = {"schema_maintenance": "failed", "id": "test_1", "operation": "preflight", **result}
    assert (
        transport.safe_receipt(json.dumps(receipt), {"id": "test_1", "operation": "preflight"})
        == receipt
    )
