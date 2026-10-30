"""Import a verified existing AI runtime as a rollback checkpoint; never start it.

Run as the deploy user with deploy.py beside this file. Configuration stays on the
host, privately. Prepare and adopt are separate, bound to exact reviewed hashes.
"""

import copy
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "handoff_deploy", Path(__file__).with_name("deploy.py")
)
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)

NATIVE_ROOT = Path("/opt/fangcun-readable-names/releases/readable-20261028-v2")
FILES = ("manifest.json", "runtime.json", "compose.yaml", "image.env", "old-state.json")
PHASE = "initialization"


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def state_bytes():
    return (deploy.ROOT / "state.json").read_bytes()


def source(env):
    pairs = dict(item.split("=", 1) for item in env)
    value = pairs.get("QS_AI_RELEASE_SHA", "")
    if not re.fullmatch("[a-f0-9]{40}", value):
        raise deploy.DeploymentError("Runtime source identity unavailable")
    return value


def observe():
    """Read Docker/configuration locally; return no credential-bearing values publicly."""
    global PHASE
    PHASE = "runtime_identity"
    row = json.loads(deploy.run("handoff runtime", ["sudo", "-n", "docker", "inspect", "qs-ai"]))[0]
    labels = row["Config"].get("Labels") or {}
    if (
        row["Name"] != "/qs-ai"
        or row["State"]["Running"] is not True
        or row["State"].get("Health", {}).get("Status") != "healthy"
        or labels.get("com.docker.compose.project") != "qs-ai"
        or labels.get("com.docker.compose.service") != "qs-ai"
    ):
        raise deploy.DeploymentError("Expected one healthy native AI runtime")
    PHASE = "inventory"
    ids = deploy.run(
        "handoff project inventory",
        [
            "sudo",
            "-n",
            "docker",
            "ps",
            "-q",
            "--no-trunc",
            "--filter",
            "label=com.docker.compose.project=qs-ai",
        ],
    ).split()
    if ids != [row["Id"]]:
        raise deploy.DeploymentError("Ambiguous AI runtime inventory")
    PHASE = "image_source"
    image = deploy.image_id(row["Image"])
    inspected = json.loads(
        deploy.run("handoff image", ["sudo", "-n", "docker", "image", "inspect", image])
    )[0]
    revision = source(inspected["Config"]["Env"])
    if (inspected["Os"], inspected["Architecture"], source(row["Config"]["Env"])) != (
        "linux",
        "amd64",
        revision,
    ):
        raise deploy.DeploymentError("Runtime source/platform identity differs")
    PHASE = "compose_source"
    files = labels.get("com.docker.compose.project.config_files", "").split(",")
    if not files or len(files) > 4:
        raise deploy.DeploymentError("Native Compose source unavailable")
    paths = [Path(value) for value in files]
    directory = paths[0].parent
    if any(
        not p.is_absolute()
        or ".." in p.parts
        or not p.is_relative_to(NATIVE_ROOT)
        or p.parent != directory
        for p in paths
    ):
        raise deploy.DeploymentError("Expected reviewed native Compose paths")
    environment = directory / "runtime.env"
    # The accepted native profile can be root-private. Use the already-authorized
    # Docker Compose renderer below; never widen filesystem/Python permissions.
    # Raw-byte fingerprints remain explicitly unavailable when unreadable. The
    # complete rendered-config digest and actual-runtime comparison are mandatory.
    frozen = {}
    for path in [*paths, environment]:
        try:
            frozen[str(path)] = digest(path.read_bytes())
        except PermissionError:
            frozen[str(path)] = None
    args = ["sudo", "-n", "docker", "compose", "-p", "qs-ai", "--env-file", str(environment)]
    for path in paths:
        args += ["-f", str(path)]
    config = json.loads(deploy.run("handoff frozen Compose", [*args, "config", "--format", "json"]))
    if set(config["services"]) != {"qs-ai"}:
        raise deploy.DeploymentError("Expected single-service Compose configuration")
    PHASE = "runtime_binding"
    service = config["services"]["qs-ai"]
    effective_uid = deploy.run(
        "handoff effective uid", ["sudo", "-n", "docker", "exec", "qs-ai", "id", "-u"]
    ).strip()
    actual_env = dict(item.split("=", 1) for item in row["Config"]["Env"])
    frozen_env = dict(item.split("=", 1) for item in inspected["Config"]["Env"])
    for key, value in service.get("environment", {}).items():
        if value is None:
            frozen_env.pop(key, None)  # Compose null explicitly unsets inherited environment.
        else:
            frozen_env[key] = str(value).replace("$$", "$")
    expected_command = service.get("command")
    if expected_command is None:
        expected_command = inspected["Config"].get("Cmd")
    expected_entrypoint = service.get("entrypoint")
    if expected_entrypoint is None:
        expected_entrypoint = inspected["Config"].get("Entrypoint")
    actual_mounts = {
        (m["Source"], m["Destination"], m["RW"]) for m in row["Mounts"] if m["Type"] != "tmpfs"
    }
    desired_mounts = {
        (m["source"], m["target"], not m.get("read_only", False))
        for m in service.get("volumes", [])
    }
    if (
        service["image"] != image
        or actual_env != frozen_env
        or actual_mounts != desired_mounts
        or row["HostConfig"]["Memory"] != 768 * 1024 * 1024
        or row["HostConfig"]["NanoCpus"] != 750000000
        or effective_uid != "10001"
        or row["HostConfig"]["ReadonlyRootfs"] is not True
        or int(service.get("mem_limit", 0)) != row["HostConfig"]["Memory"]
        or float(service.get("cpus", 0)) != 0.75
        or str(service.get("user") or inspected["Config"].get("User")) != row["Config"]["User"]
        or service.get("read_only") is not True
        or expected_command != row["Config"].get("Cmd")
        or expected_entrypoint != row["Config"].get("Entrypoint")
    ):
        raise deploy.DeploymentError("Frozen Compose differs from actual runtime")
    facts = {
        "container_id": row["Id"],
        "image_id": image,
        "revision": revision,
        "state_sha256": digest(state_bytes()),
        "config_sha256": deploy.json_digest(config),
        "native_files_sha256": frozen,
        "native_files_raw_hashes_available": all(value is not None for value in frozen.values()),
        "effective_uid": effective_uid,
    }
    return facts, config


def materials(release):
    return {name: digest((release / name).read_bytes()) for name in FILES}


def compatible(release):
    deploy.require_final_messaging(release)
    deploy._offline_release_preflight(release, True)
    heads = deploy.expected_heads(release)
    result = deploy.probe(release, True)  # Existing read-only schema checker, no Alembic.
    if result.get("current") != heads or result.get("expected") != heads:
        raise deploy.DeploymentError("Existing runtime schema is not an exact rollback head")
    deploy.run(
        "handoff live mTLS probe",
        deploy.compose(
            release,
            "exec",
            "-T",
            "qs-ai",
            "/app/.venv/bin/python",
            "-m",
            "qs_ai.bootstrap.grpc_probe",
        ),
    )
    return result


def prepare(name, image, state_sha, config_sha):
    release = deploy.release_path(name)
    facts, config = observe()
    if not name.startswith(facts["revision"] + "-") or (
        facts["image_id"],
        facts["state_sha256"],
        facts["config_sha256"],
    ) != (image, state_sha, config_sha):
        raise deploy.DeploymentError("Reviewed runtime/state/configuration identity differs")
    raw_state = state_bytes()
    if digest(raw_state) != state_sha:
        raise deploy.DeploymentError("Previous state changed before preparation")
    release.mkdir(mode=0o700)  # A fresh immutable checkpoint; never overwrite any old release.
    service = config["services"]["qs-ai"]
    for mount in service.get("volumes", []):
        if mount["type"] != "bind" or mount.get("read_only") is not True:
            raise deploy.DeploymentError("Expected existing read-only file mounts")
        options = mount.setdefault("bind", {})
        if options.get("create_host_path", False) is not False:
            raise deploy.DeploymentError("Automatic host path creation is not a rollback binding")
        # Compose config omits false defaults. The stored rollback contract must
        # retain this explicit restriction, including the existing MQ validator.
        options["create_host_path"] = False
    runtime = {"services": {"qs-ai": {"environment": {}, "volumes": []}}}
    target = runtime["services"]["qs-ai"]
    for key in ("QS_AI_MESSAGING", "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"):
        target["environment"][key] = service["environment"].pop(key)
    for mount in copy.deepcopy(service.get("volumes", [])):
        if mount["target"].startswith(("/run/qs-ai-jose/", "/run/qs-ai-rocketmq/")):
            target["volumes"].append(mount)
            service["volumes"].remove(mount)
    service["image"] = "${QS_AI_IMAGE:?Set the immutable image ID}"
    manifest = {
        "revision": facts["revision"],
        "image_id": image,
        "schema_policy": "require-existing",
        "provenance": "observed-existing-runtime",
        "messaging_binding_sha256": deploy.json_digest(
            {"options": target["environment"]["QS_AI_MESSAGING"], "volumes": target["volumes"]}
        ),
    }
    deploy.write_json(release / "manifest.json", manifest)
    deploy.write_json(release / "runtime.json", runtime)
    deploy.write_json(release / "compose.yaml", config)  # JSON is valid Compose YAML.
    (release / "image.env").write_text(f"QS_AI_IMAGE={image}\n")
    (release / "old-state.json").write_bytes(raw_state)
    database = compatible(release)
    if observe()[0] != facts:
        raise deploy.DeploymentError("Runtime changed during checkpoint preparation")
    proof = {
        "format": "qs-ai-runtime-handoff/v1",
        "release": name,
        "facts": facts,
        "materials": materials(release),
        "database": database,
        "deployed": False,
    }
    deploy.write_json(release / "handoff-prepared.json", proof)
    return {
        "prepared": True,
        "release": name,
        "prepared_sha256": digest((release / "handoff-prepared.json").read_bytes()),
        "deployed": False,
    }


def adopt(name, prepared_sha):
    release = deploy.release_path(name)
    raw = (release / "handoff-prepared.json").read_bytes()
    if digest(raw) != prepared_sha:
        raise deploy.DeploymentError("Reviewed handoff proof differs")
    proof = json.loads(raw)
    if (
        proof["release"] != name
        or proof["format"] != "qs-ai-runtime-handoff/v1"
        or materials(release) != proof["materials"]
    ):
        raise deploy.DeploymentError("Prepared checkpoint changed")
    if observe()[0] != proof["facts"]:
        raise deploy.DeploymentError(
            "Runtime or previous state changed; prepare a fresh checkpoint"
        )
    database = compatible(release)
    if observe()[0] != proof["facts"]:
        raise deploy.DeploymentError("Runtime changed during adoption")
    deploy.write_json(
        release / "verification.json",
        {
            "phase": "ready",
            "provenance": "observed-existing-runtime",
            "database": database,
            "image_id": proof["facts"]["image_id"],
            "deployed": False,
        },
    )
    # Incompatible historical pointers remain in the exact old-state.json bytes.
    # They must not be offered as rollback candidates for the current schema.
    deploy.write_json(deploy.ROOT / "state.json", {"current": name, "previous": None})
    return {"adopted": True, "release": name, "deployed": False}


def main():
    os.umask(0o077)
    deploy.read_material_auth()
    if sys.argv[1:] == ["inspect"]:
        print(json.dumps({"read_only": True, "deployed": False, **observe()[0]}, sort_keys=True))
        return
    with deploy.global_deploy_lock(), (deploy.ROOT / "deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        deploy.schema_followup_guard()
        if deploy.schema_followup_module() is not None:
            raise deploy.DeploymentError("Active schema maintenance requires its own handoff")
        if len(sys.argv) == 6 and sys.argv[1] == "prepare":
            result = prepare(*sys.argv[2:])
        elif len(sys.argv) == 4 and sys.argv[1] == "adopt":
            result = adopt(*sys.argv[2:])
        else:
            raise deploy.DeploymentError(
                "Expected inspect, prepare <release> <image> <state hash> <config hash>, "
                "or adopt <release> <proof hash>"
            )
        print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("QS_AI_HANDOFF_FAILURE:" + PHASE + ":" + type(error).__name__, file=sys.stderr)
        raise SystemExit(1) from None
