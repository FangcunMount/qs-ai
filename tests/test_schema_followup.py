"""Immutable v1 provenance, v2 runtime publication and current-facts recovery contracts."""

import contextlib
import copy
import json
import shutil
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from tests.test_deployment import ROOT, load

BASE_REV = "a" * 40
EXEC_REV = "b" * 40
LEGACY = "c" * 40 + "-1-1"
CURRENT = BASE_REV + "-2-1"
CANDIDATE = EXEC_REV + "-3-1"
IMAGES = {
    LEGACY: "sha256:" + "1" * 64,
    CURRENT: "sha256:" + "2" * 64,
    CANDIDATE: "sha256:" + "3" * 64,
}
SERVER = "cc33cc33-1111-2222-3333-444444444444"
CREDS = {"username": "maintenance", "password": "fixture-only"}


@pytest.fixture
def context(tmp_path, monkeypatch):
    module = load("deploy/serverA/schema_followup.py")
    root = tmp_path / "host"
    root.mkdir()
    events, schemas, facts = [], {"ai": module.NEW_HEAD}, {"ai": {"row": "new fact"}}
    for name, image in IMAGES.items():
        release = root / "releases" / name
        release.mkdir(parents=True)
        module.legacy.durable(release / "manifest.json", {"revision": name[:40], "image_id": image})
        module.legacy.durable(
            release / "runtime.json",
            {
                "services": {
                    "qs-ai": {
                        "environment": {
                            "QS_AI_DATABASE_URL": "mysql+asyncmy://app:fake@mysql.fixture:3306/ai",
                        }
                    }
                }
            },
        )
        (release / "compose.yaml").write_text("services: fixture")
        module.legacy.durable(
            release / "prepared.json",
            {
                "prepared": True,
                "image_id": image,
                "expected_heads": [module.NEW_HEAD],
            },
        )
    code = root / "schema-maintenance" / "followup_1" / ("code-" + EXEC_REV)
    base_code = root / "schema-maintenance" / "base_1" / ("code-" + BASE_REV)
    for directory in (code, base_code):
        package = directory / "src/qs_ai/maintenance/schema_refactor"
        package.mkdir(parents=True)
        for name in ("v0038.py", "v0040.json", "backup.py"):
            (package / name).write_text("frozen " + name)
        (directory / "migrations/versions").mkdir(parents=True)
        (directory / "migrations/versions/0001.py").write_text("frozen historical migration")
        (directory / "deploy/serverA").mkdir(parents=True)
        for name in ("deploy.py", "schema_maintenance.py", "schema_followup.py"):
            shutil.copyfile(ROOT / "deploy/serverA" / name, directory / "deploy/serverA" / name)
        directory.parent.chmod(0o700)
    (code / "src/qs_ai/maintenance/schema_refactor/validation_contract.py").write_text(
        "new validator"
    )
    monkeypatch.setattr(module.Followup, "host_budget", lambda *_a, **_kw: contextlib.nullcontext())

    def release_path(name):
        assert name in IMAGES
        return root / "releases" / name

    def image(path, **_kw):
        return IMAGES[path.name]

    def run(phase, args):
        events.append(phase)
        if phase == "read-only runtime services":
            return "qs-ai\n"
        if phase == "read-only runtime container":
            return "container-fixture\n"
        if phase == "read-only runtime identity":
            current = module.private(root / "state.json")["current"]
            return json.dumps(
                [
                    {
                        "Image": IMAGES[current],
                        "State": {
                            "Running": True,
                            "Health": {"Status": "healthy"},
                        },
                    }
                ]
            )
        return ""

    def heads(path):
        return [module.OLD_HEAD if path.name == LEGACY else module.NEW_HEAD]

    def probe(path, require=True):
        events.append("probe:" + path.name)
        assert require
        if heads(path) != [schemas["ai"]]:
            raise module.MaintenanceError("fixture exact-head mismatch")
        return {"current": heads(path), "expected": heads(path)}

    def verify(path):
        events.append("verify:" + path.name)
        return probe(path)

    def apply(path, state):
        registry = module.operation(root)
        assert registry["phase"] == "pending"
        assert state["current"] == registry["allowed_releases"][0]
        events.append("apply:" + path.name)
        module.legacy.durable(
            root / "state.json", {"current": path.name, "previous": state["current"]}
        )
        module.publish_runtime(deploy, path, probe(path))

    def restore(state):
        registry = module.operation(root)
        assert registry["phase"] == "pending"
        assert registry["kind"] == "runtime_restore"
        target = release_path(state["previous"])
        events.append("restore:" + target.name)
        probe(target)
        module.legacy.durable(
            root / "state.json", {"current": target.name, "previous": state["current"]}
        )
        module.publish_runtime(deploy, target, probe(target))

    deploy = SimpleNamespace(
        ROOT=root,
        release_path=release_path,
        release_image_id=image,
        expected_heads=heads,
        run=run,
        compose=lambda p, *args: [p.name, *args],
        probe=probe,
        verify=verify,
        apply=apply,
        restore=restore,
        stop_release=lambda p: events.append("stop:" + p.name),
        messaging_transition=lambda a, b: events.append("messaging:" + b.name),
        messaging_key_preflight=lambda p: events.append("keys:" + p.name),
    )
    module.legacy.durable(root / "state.json", {"current": CURRENT, "previous": LEGACY})
    base_dir = base_code.parent
    base_host = {
        "format": module.legacy.FORMAT,
        "id": "base_1",
        "phase": "switched",
        "code_revision": base_code.name,
        "helper_sha256": module.checksum(base_code / "deploy/serverA/schema_maintenance.py"),
        "deployment_helper_sha256": module.checksum(base_code / "deploy/serverA/deploy.py"),
        "source_sha256": module.legacy.source_checksum(base_code),
        "server_uuid": SERVER,
        "old_release": LEGACY,
        "new_release": CURRENT,
        "old_image": IMAGES[LEGACY],
        "new_image": IMAGES[CURRENT],
        "binding": module.legacy.database_binding(release_path(CURRENT)),
        "retain_until": "2026-11-07T10:00:00+00:00",
        **{
            role + "_files": {
                name: module.checksum(release_path(release) / name)
                for name in ("manifest.json", "runtime.json", "compose.yaml")
            }
            for role, release in (("old", LEGACY), ("new", CURRENT))
        },
    }
    forward = {
        "format": "qs-ai-schema-refactor/v1",
        "phase": "switched",
        "source": "ai",
        "target": "ai_refactor_base_1",
        "archive": "ai_backup_base_1",
        "source_head": module.OLD_HEAD,
        "target_head": module.NEW_HEAD,
        "old_image": IMAGES[LEGACY],
        "new_image": IMAGES[CURRENT],
        "server_uuid": SERVER,
        "contract_sha256": module.checksum(
            base_code / "src/qs_ai/maintenance/schema_refactor/v0040.json"
        ),
        "source_column_collations": {"prompt_assets": {"template_id": "utf8mb4_bin"}},
        "legacy_prompt_collations": {"template_id": "utf8mb4_bin"},
        "owned_schemas": ["ai_refactor_base_1", "ai_backup_base_1"],
        "retained_archives": [
            {
                "schema": "ai_backup_base_1",
                "head": module.OLD_HEAD,
                "manifest": {"old": "facts"},
                "retained_since": "2026-10-08T10:15:00+00:00",
            }
        ],
    }
    module.legacy.durable(base_dir / "host.json", base_host)
    module.legacy.durable(base_dir / "forward.json", forward)
    frozen = {p: p.read_bytes() for p in base_dir.rglob("*") if p.is_file()}

    class Tools:
        image = None
        deadline = None
        fail = None

        def validate(self, _image, *, fenced=False, schema=None):
            events.append("validate:" + (schema or "ai") + (":fenced" if fenced else ""))
            return {
                "validated": True,
                "head": schemas[schema or "ai"],
                "server_uuid": SERVER,
                "auto_increment_columns": [
                    "governance_asset_versions.asset_row_id",
                    "governance_draft_heads.draft_row_id",
                    "quota_evaluation_admission_locks.organization_id",
                    "quota_participant_admission_locks.organization_id",
                ],
            }

        def refactor(self, command, path, state, *, stopped_at=None, inherit=None, **kwargs):
            events.append("conversion:" + command)
            assert not kwargs.get("fast")
            if command == "plan":
                inherited = module.private(inherit)
                assert inherited["source"] == state["source"]
                value = {
                    **state,
                    "format": "qs-ai-schema-refactor/v1",
                    "phase": "planned",
                    "source_head": module.NEW_HEAD,
                    "target_head": module.OLD_HEAD,
                    "server_uuid": SERVER,
                    "owned_schemas": [state["target"], state["archive"]],
                }
            else:
                value = module.private(path)
            if command == "prepare":
                schemas[value["target"]] = module.OLD_HEAD
                value["phase"] = "prepared"
            if command == "copy":
                value.update(phase="copied", stopped_at=stopped_at)
                facts[value["target"]] = copy.deepcopy(facts[value["source"]])
            if command == "verify":
                assert facts[value["target"]] == facts[value["source"]]
                value["phase"] = "verified"
            if command == "switch":
                schemas[value["source"]] = module.OLD_HEAD
                facts[value["source"]] = copy.deepcopy(facts[value["target"]])
                value["phase"] = "switched"
                value["retained_archives"] = [
                    {
                        "schema": value["archive"],
                        "head": module.NEW_HEAD,
                        "manifest": facts[value["source"]],
                        "retained_since": stopped_at,
                    }
                ]
            if command == "cleanup":
                for record in value["retained_archives"]:
                    record["deleted_at"] = datetime.now(UTC).isoformat()
                value["phase"] = "cleaned"
            module.legacy.durable(path, value)
            if self.fail == command:
                raise module.MaintenanceError("fixture interrupted")
            return {"phase": value["phase"]}

        def backup(self, command, _image, _schema, path, *, target=None, **kwargs):
            events.append("backup:" + command)
            if command == "restore":
                schemas[target] = module.NEW_HEAD
                facts[target] = copy.deepcopy(facts["ai"])
            return {
                "source_server_uuid": SERVER,
                "head": module.NEW_HEAD,
                "verified": True,
                "backup_sha256": "d" * 64,
                "manifest_sha256": "e" * 64,
            }

        def container(self, _image, command, *, schema=None, **kwargs):
            if command[1] == module.FIXTURE_WRITE:
                facts[schema] = {"inserted": 1, "updated": 1, "deleted": 1}
                return {
                    "fixture": "passed",
                    "template_id": "fixture",
                    "version": "v1",
                    "inserted": 1,
                    "updated": 1,
                    "deleted": 1,
                    "prompt_sha256": "f" * 64,
                }
            assert command[1] == module.FIXTURE_READ
            return {"fixture_domain": "passed", "prompt_sha256": "f" * 64}

    tools = Tools()
    followup = module.Followup(deploy, "followup_1", CREDS, code=code, tools=tools)
    return SimpleNamespace(
        module=module,
        followup=followup,
        deploy=deploy,
        tools=tools,
        root=root,
        code=code,
        base_dir=base_dir,
        frozen=frozen,
        events=events,
        schemas=schemas,
        facts=facts,
    )


def bootstrap(context):
    return context.followup.bootstrap("base_1", BASE_REV, CANDIDATE)


def rehearsed(context):
    bootstrap(context)
    context.followup.rehearse()
    return context.followup


def assert_base_unchanged(context):
    assert all(path.read_bytes() == original for path, original in context.frozen.items())


def test_bootstrap_current_44_keeps_base_immutable_and_binds_executor(context):
    result = bootstrap(context)
    assert result["tables"] == 44
    assert result["runtime_verified"] is True
    assert len(result["auto_increment_columns"]) == 4
    assert result["retain_until"] == "2026-11-07T10:15:00+00:00"
    assert context.followup.executor_image == IMAGES[CANDIDATE]
    assert context.followup.current_binding["image"] == IMAGES[CURRENT]
    assert not any(event.startswith(("stop:", "conversion:", "apply:")) for event in context.events)
    assert_base_unchanged(context)


def test_bootstrap_retries_same_identity_and_refuses_rebind(context):
    bootstrap(context)
    before = context.followup.path.read_bytes()
    bootstrap(context)
    assert context.followup.path.read_bytes() == before
    with pytest.raises(context.module.MaintenanceError, match="rebound"):
        context.followup.bootstrap("base_1", BASE_REV, CURRENT)
    assert_base_unchanged(context)


def test_bootstrap_crash_before_active_pointer_resumes_without_rebinding(context, monkeypatch):
    append = context.module.append_binding
    monkeypatch.setattr(
        context.module,
        "append_binding",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("interrupted publication")),
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        bootstrap(context)
    assert context.followup.state["phase"] == "bootstrap_pending"
    assert context.module.active_context(context.root) is None
    monkeypatch.setattr(context.module, "append_binding", append)
    assert bootstrap(context)["phase"] == "ready"
    assert context.followup.current_binding["release"] == CURRENT
    assert_base_unchanged(context)


def test_prepare_only_creates_reverse_destination_without_copy(context):
    bootstrap(context)
    result = context.followup.prepare()
    assert result["target_head"] == context.module.OLD_HEAD
    assert "conversion:copy" not in context.events
    assert not any(e.startswith("stop:") for e in context.events)
    assert context.facts["ai"] == {"row": "new fact"}
    assert_base_unchanged(context)


def test_rehearsal_mutates_only_snapshot_and_reads_fixed_old_and_new(context):
    bootstrap(context)
    original = copy.deepcopy(context.facts["ai"])
    result = context.followup.rehearse()
    assert result["passed"] is True
    assert result["fixture"] == {
        "inserted": 1,
        "updated": 1,
        "deleted": 1,
        "prompt_sha256": "f" * 64,
    }
    assert context.facts["ai"] == original
    assert context.schemas["ai"] == context.module.NEW_HEAD
    assert "conversion:switch" not in context.events
    assert not any(e.startswith("stop:") for e in context.events)
    assert_base_unchanged(context)


def test_release_requires_fixed_rehearsal_before_any_stop(context):
    bootstrap(context)
    with pytest.raises(context.module.MaintenanceError, match="rehearsal"):
        context.followup.release(CANDIDATE)
    assert context.module.operation(context.root) is None


def test_release_updates_immutable_runtime_binding_and_closes_actual_facts(context):
    followup = rehearsed(context)
    original = (followup.directory / "runtime-bindings" / (CURRENT + ".json")).read_bytes()
    result = followup.release(CANDIDATE)
    assert result["phase"] == "released" and result["runtime_verified"] is True
    assert context.module.operation(context.root)["phase"] == "closed"
    assert followup.current_binding["release"] == CANDIDATE
    assert (followup.directory / "runtime-bindings" / (CURRENT + ".json")).read_bytes() == original
    assert_base_unchanged(context)


def test_current_facts_full_rollback_does_not_read_base_backup(context):
    followup = rehearsed(context)
    followup.release(CANDIDATE)
    context.facts["ai"] = {"new": "written after new runtime", "update": 7}
    result = followup.rollback()
    assert result["phase"] == "rolled_back"
    assert context.facts["ai"] == {"new": "written after new runtime", "update": 7}
    assert context.schemas["ai"] == context.module.OLD_HEAD
    assert context.module.operation(context.root)["phase"] == "closed"
    assert "validate:ai:fenced" in context.events
    assert "probe:" + LEGACY in context.events
    assert "backup:restore" not in context.events[context.events.index("apply:" + CANDIDATE) :]
    assert_base_unchanged(context)


def test_pending_operation_blocks_new_id_and_ordinary_deploy(context):
    bootstrap(context)
    intent = context.followup._begin("rollback", [CURRENT, LEGACY])
    with pytest.raises(context.module.MaintenanceError, match="unfinished"):
        context.module.assert_idle(context.root)
    other = context.module.Followup(
        context.deploy, "followup_2", CREDS, code=context.code, tools=context.tools
    )
    with pytest.raises(context.module.MaintenanceError, match="unfinished"):
        other.bootstrap("base_1", BASE_REV, CANDIDATE)
    assert (
        context.module.assert_idle(context.root, intent)["operation_id"] == intent["operation_id"]
    )
    assert_base_unchanged(context)


def test_retry_preserves_original_clock_and_rejects_registry_clock_drift(context):
    bootstrap(context)
    original = context.followup._begin("rollback", [CURRENT, LEGACY])
    assert context.followup._begin("rollback", [CURRENT, LEGACY]) == original
    changed = {**original, "stopped_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat()}
    context.module.legacy.durable(context.module.paths(context.root)[1], changed)
    with pytest.raises(context.module.MaintenanceError, match="clock"):
        context.followup._begin("rollback", [CURRENT, LEGACY])


def test_overdue_rollback_does_not_reset_or_copy(context):
    bootstrap(context)
    binding = context.followup.current_binding
    intent = context.followup._begin(
        "rollback",
        [CURRENT, LEGACY],
        reverse_journal=context.followup._reverse_name(binding),
        source_image=binding["image"],
        source_binding_sha256=context.module.canonical(binding),
    )
    intent["stopped_at"] = (datetime.now(UTC) - timedelta(minutes=31)).isoformat()
    context.followup.state["operation"] = intent
    context.followup.save()
    context.module.legacy.durable(context.module.paths(context.root)[1], intent)
    with pytest.raises(context.module.MaintenanceError, match="window"):
        context.followup.rollback()
    assert context.module.operation(context.root)["stopped_at"] == intent["stopped_at"]
    assert "conversion:copy" not in context.events


def test_disconnected_exchange_resume_uses_original_facts_clock(context):
    bootstrap(context)
    context.tools.fail = "switch"
    with pytest.raises(context.module.MaintenanceError, match="unresolved"):
        context.followup.rollback()
    pending = context.module.operation(context.root)
    assert context.schemas["ai"] == context.module.OLD_HEAD
    context.tools.fail = None
    context.followup.rollback()
    assert context.module.operation(context.root)["stopped_at"] == pending["stopped_at"]
    assert context.events.count("conversion:copy") == 1
    assert_base_unchanged(context)


def test_switch_pending_actual_old_finishes_journal_before_legacy_start(context):
    bootstrap(context)
    refactor = context.tools.refactor

    def interrupted(command, path, state, **kwargs):
        value = refactor(command, path, state, **kwargs)
        if command == "switch":
            journal = context.module.private(path)
            journal["phase"] = "switch_pending"
            context.module.legacy.durable(path, journal)
            raise context.module.MaintenanceError("lost response after atomic exchange")
        return value

    context.tools.refactor = interrupted
    with pytest.raises(context.module.MaintenanceError, match="unresolved"):
        context.followup.rollback()
    original = context.module.operation(context.root)
    assert context.schemas["ai"] == context.module.OLD_HEAD
    context.tools.refactor = refactor
    context.events.clear()
    context.followup.rollback()
    assert context.module.operation(context.root)["stopped_at"] == original["stopped_at"]
    assert "conversion:copy" not in context.events
    assert context.events.index("conversion:switch") < context.events.index("probe:" + LEGACY)
    assert_base_unchanged(context)


def test_status_is_read_only_and_checks_running_image_health_mtls(context):
    bootstrap(context)
    before = {p: p.read_bytes() for p in context.followup.directory.rglob("*.json")}
    context.events.clear()
    result = context.followup.status()
    assert result["runtime"]["healthy"] and result["runtime"]["mtls"]
    assert "read-only runtime mTLS" in context.events
    assert not any(
        e.startswith(("stop:", "apply:", "conversion:", "verify:")) for e in context.events
    )
    assert all(p.read_bytes() == data for p, data in before.items())
    assert_base_unchanged(context)


def test_prepare_release_rollback_uses_fresh_journal_without_rewriting_prepared(context):
    followup = rehearsed(context)
    prepared = followup.prepare()
    old_name = followup._reverse_name()
    old_bytes = (followup.directory / old_name).read_bytes()
    followup.release(CANDIDATE)
    new_name = followup._reverse_name()
    assert new_name != old_name
    context.facts["ai"] = {"new": "after candidate release"}
    followup.rollback()
    new = context.module.private(followup.directory / new_name)
    assert new["new_image"] == IMAGES[CANDIDATE]
    assert new["target"] != prepared["target"]
    assert context.facts["ai"] == {"new": "after candidate release"}
    assert (followup.directory / old_name).read_bytes() == old_bytes
    assert followup.status()["retain_until"] is not None
    assert_base_unchanged(context)


def test_ordinary_runtime_restore_records_intent_and_keeps_0040_facts(context):
    followup = rehearsed(context)
    followup.release(CANDIDATE)
    context.facts["ai"] = {"new": "must survive application rollback"}
    result = followup.release(CURRENT, restore=True)
    assert result["phase"] == "runtime_restored"
    assert context.module.operation(context.root)["kind"] == "runtime_restore"
    assert context.module.operation(context.root)["phase"] == "closed"
    assert "restore:" + CURRENT in context.events
    assert context.facts["ai"] == {"new": "must survive application rollback"}
    assert context.schemas["ai"] == context.module.NEW_HEAD
    assert followup.current_binding["release"] == CURRENT
    assert_base_unchanged(context)


def test_runtime_restore_failure_restores_last_working_0040_and_preserves_failure(context):
    followup = rehearsed(context)
    followup.release(CANDIDATE)

    def fail(_state):
        raise context.module.MaintenanceError("fixture failure")

    context.deploy.restore = fail
    with pytest.raises(context.module.MaintenanceError, match="verified and restored"):
        followup.release(CURRENT, restore=True)
    assert context.module.operation(context.root)["phase"] == "closed"
    assert followup.state["last_failure"]["reason"] == "runtime_release_failed"
    assert followup.current_binding["release"] == CANDIDATE
    assert context.schemas["ai"] == context.module.NEW_HEAD
    assert_base_unchanged(context)


def test_runtime_restore_unresolved_retry_preserves_original_clock(context):
    followup = rehearsed(context)
    followup.release(CANDIDATE)
    original_restore, original_verify = context.deploy.restore, context.deploy.verify
    context.deploy.restore = lambda _state: (_ for _ in ()).throw(RuntimeError("interrupted"))
    context.deploy.verify = lambda _release: (_ for _ in ()).throw(RuntimeError("unavailable"))
    with pytest.raises(context.module.MaintenanceError, match="unresolved"):
        followup.release(CURRENT, restore=True)
    pending = context.module.operation(context.root)
    assert pending["kind"] == "runtime_restore" and pending["phase"] == "pending"
    context.deploy.restore, context.deploy.verify = original_restore, original_verify
    followup.release(CURRENT, restore=True)
    assert context.module.operation(context.root)["stopped_at"] == pending["stopped_at"]
    assert context.module.operation(context.root)["operation_id"] == pending["operation_id"]
    assert_base_unchanged(context)


def test_ordinary_runtime_restore_rejects_0038_before_intent_or_stop(context):
    followup = rehearsed(context)
    with pytest.raises(context.module.MaintenanceError, match="exact 0040"):
        followup.release(LEGACY, restore=True)
    assert context.module.operation(context.root) is None
    assert not any(event.startswith("stop:") for event in context.events)


def test_ordinary_main_rollback_uses_followup_restore_wrapper(context, monkeypatch):
    remote = load("deploy/serverA/deploy.py")
    called = []
    monkeypatch.setattr(remote, "ROOT", context.root)
    monkeypatch.setattr(remote, "global_deploy_lock", contextlib.nullcontext)
    monkeypatch.setattr(remote.sys, "argv", ["deploy.py", "rollback"])
    monkeypatch.setattr(
        remote,
        "schema_followup_module",
        lambda: SimpleNamespace(ordinary_restore=lambda _deploy, state: called.append(state)),
    )
    monkeypatch.setattr(remote, "retain_successful_image", lambda _release: None)
    monkeypatch.setattr(
        remote, "restore", lambda _state: pytest.fail("unwrapped ordinary restore was invoked")
    )
    remote.main()
    assert called == [{"current": CURRENT, "previous": LEGACY}]


def test_fixed_executor_and_legacy_images_remain_protected_after_runtime_release(context):
    followup = rehearsed(context)
    followup.release(CANDIDATE)
    assert context.module.protected_images(context.root) == sorted(
        [IMAGES[CANDIDATE], IMAGES[LEGACY]]
    )
    assert_base_unchanged(context)


def test_cleanup_delegation_retries_drop_pending_without_mutating_base(context):
    bootstrap(context)
    original = copy.deepcopy(context.followup.base_forward)
    delegation = copy.deepcopy(original)
    delegation["retained_archives"][0]["drop_pending"] = True
    delegation["phase"] = "cleanup_pending"
    delegation["origin_sha256"] = context.followup.state["base"]["forward_sha256"]
    directory = context.followup.directory / "cleanup"
    directory.mkdir()
    context.module.legacy.durable(directory / "base.json", delegation)
    assert context.followup.status()["archives"][0]["status"] == "drop_pending"
    assert context.followup.base_forward == original
    future = datetime(2026, 11, 8, tzinfo=UTC)
    report = context.module.retention_report
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(context.module, "retention_report", lambda value: report(value, now=future))
        result = context.followup.cleanup()
    assert result["phase"] == "cleaned"
    assert "conversion:cleanup" in context.events
    assert result["archives"][0]["status"] == "deleted"
    assert context.followup.base_forward == original
    assert_base_unchanged(context)


@pytest.mark.parametrize("kind", ["executor", "base", "runtime"])
def test_identity_drift_is_rejected(context, kind):
    bootstrap(context)
    if kind == "executor":
        path = context.code / "src/qs_ai/maintenance/schema_refactor/validation_contract.py"
    elif kind == "base":
        path = context.base_dir / "forward.json"
    else:
        path = context.deploy.release_path(CURRENT) / "runtime.json"
    path.write_text(path.read_text() + "\n")
    with pytest.raises(context.module.MaintenanceError, match="changed"):
        context.followup.bound()


def journal(module, records):
    return {
        "retained_archives": [
            {
                "schema": name,
                "head": module.OLD_HEAD,
                "manifest": {"hash": 1},
                "retained_since": since,
                **extra,
            }
            for name, since, extra in records
        ]
    }


def test_retention_uses_individual_exchange_times_and_exact_threshold():
    module = load("deploy/serverA/schema_followup.py")
    value = journal(
        module,
        [
            ("ai_backup_first", "2026-10-08T18:15:00+08:00", {}),
            ("ai_failed_later", "2026-10-09T10:15:00+00:00", {}),
        ],
    )
    original = copy.deepcopy(value)
    before = module.retention_report([value], now=datetime(2026, 11, 7, 10, 15, tzinfo=UTC))
    assert before["retain_until"] == "2026-11-08T10:15:00+00:00"
    assert before["archives"][0]["retention_elapsed"] is True
    assert before["archives"][1]["retention_elapsed"] is False
    assert value == original


def test_missing_exchange_time_is_unknown_but_deleted_does_not_mask_live_date():
    module = load("deploy/serverA/schema_followup.py")
    missing = journal(module, [("ai_backup_missing", None, {})])
    assert module.retention_report([missing])["retain_until"] is None
    assert module.retention_report([missing])["retention_known"] is False
    deleted = journal(
        module, [("ai_backup_gone", None, {"deleted_at": "2026-11-08T00:00:00+00:00"})]
    )
    live = journal(module, [("ai_backup_live", "2026-10-08T10:15:00+00:00", {})])
    result = module.retention_report([deleted, live])
    assert result["retain_until"] == "2026-11-07T10:15:00+00:00"
    assert result["retention_known"] is True
    assert module.retention_report([{"retained_archives": []}])["retention_known"] is False


@pytest.mark.parametrize("bad", ["2026-10-08T10:00:00", "unknown"])
def test_retention_rejects_unbound_time(bad):
    module = load("deploy/serverA/schema_followup.py")
    with pytest.raises(module.MaintenanceError):
        module.retention_report([journal(module, [("ai_backup_bad", bad, {})])])


def test_retention_rejects_contradictory_archive_evidence():
    module = load("deploy/serverA/schema_followup.py")
    first = journal(module, [("ai_backup_same", "2026-10-08T10:00:00+00:00", {})])
    second = journal(module, [("ai_backup_same", "2026-10-09T10:00:00+00:00", {})])
    with pytest.raises(module.MaintenanceError, match="Conflicting"):
        module.retention_report([first, second])
