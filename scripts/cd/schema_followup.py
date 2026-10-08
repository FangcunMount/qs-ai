"""Actions transport for v2 closure; private backup data never crosses this boundary."""

import importlib.util
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPERATIONS = {
    "bootstrap",
    "prepare",
    "rehearse",
    "release",
    "status",
    "retirement-plan",
    "retirement-backup",
    "retirement-verify",
    "retirement-restore",
}
ID = r"[a-z][a-z0-9_]{0,31}"
SHA = r"[0-9a-f]{40}"
RELEASE = SHA + r"-[0-9]+-[0-9]+"
IMAGE = r"sha256:[0-9a-f]{64}"
HASH = r"[0-9a-f]{64}"
SCHEMA = r"[a-z][a-z0-9_]{0,63}"
UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def legacy_transport():
    spec = importlib.util.spec_from_file_location(
        "qs_ai_followup_transport_common", ROOT / "scripts/cd/schema_maintenance.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def required(environment, name):
    value = environment.get(name, "")
    if not value or "\x00" in value:
        raise ValueError(f"Missing or invalid {name}")
    return value


def request(environment):
    selected = {
        "operation": required(environment, "FOLLOWUP_OPERATION"),
        "id": required(environment, "FOLLOWUP_ID"),
        "revision": required(environment, "DEPLOY_SHA"),
        "base_id": environment.get("FOLLOWUP_BASE_ID", "dbrefactor_20261008"),
        "base_revision": environment.get(
            "FOLLOWUP_BASE_REVISION", "7c4f60def8afbda46deba2c00ef8b73dd2d3d3a0"
        ),
        "release": environment.get("FOLLOWUP_RELEASE", ""),
    }
    if selected["operation"] not in OPERATIONS:
        raise ValueError("Invalid follow-up operation")
    for key, pattern in {"id": ID, "base_id": ID, "revision": SHA, "base_revision": SHA}.items():
        if not re.fullmatch(pattern, selected[key]):
            raise ValueError(f"Invalid follow-up {key}")
    if selected["id"] == selected["base_id"]:
        raise ValueError("Follow-up must use a new context identifier")
    if selected["operation"] == "bootstrap" and not selected["release"]:
        value = json.loads(Path("release-receipt.json").read_text())
        if value.get("revision") != selected["revision"] or value.get("prepared") is not True:
            raise ValueError("Prepared release receipt differs from tested revision")
        selected["release"] = value["release_id"]
    if selected["release"] and not re.fullmatch(RELEASE, selected["release"]):
        raise ValueError("Invalid immutable follow-up release")
    if selected["operation"] in {"bootstrap", "release"} and not selected["release"].startswith(
        selected["revision"] + "-"
    ):
        raise ValueError("Follow-up release must bind the tested revision")
    return selected


def run(phase, arguments):
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=7200)
    if result.returncode:
        if phase == "schema follow-up":
            try:
                value = json.loads(result.stdout.strip().splitlines()[-1])
                if isinstance(value, dict) and value.get("followup") == "failed":
                    return result.stdout
            except (ValueError, IndexError):
                pass
        raise RuntimeError(f"{phase} failed; inspect private host evidence")
    return result.stdout


def date(value):
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9T:.+Z-]{20,40}", value):
        raise ValueError
    if datetime.fromisoformat(value.replace("Z", "+00:00")).tzinfo is None:
        raise ValueError
    return value


def safe_metadata(value):
    """Positive field allowlist: driver errors, references and row bodies remain private."""
    if not isinstance(value, dict):
        raise ValueError
    result = {}
    patterns = {
        "id": ID,
        "base_id": ID,
        "executor_revision": SHA,
        "current_revision": SHA,
        "revision": SHA,
        "release": RELEASE,
        "current_release": RELEASE,
        "executor_release": RELEASE,
        "old_release": RELEASE,
        "previous_release": RELEASE,
        "image": IMAGE,
        "image_id": IMAGE,
        "executor_image": IMAGE,
        "old_image": IMAGE,
        "server_uuid": UUID,
        "source_server_uuid": UUID,
        "source_schema": SCHEMA,
        "source_head": r"(0038_messaging_observations|0040_module_table_names)",
        "head": r"(0038_messaging_observations|0040_module_table_names)",
        "target_head": r"0038_messaging_observations",
        "target": SCHEMA,
        "restore_target": SCHEMA,
    }
    for key, pattern in patterns.items():
        if key in value:
            if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
                raise ValueError
            result[key] = value[key]
    for key, item in value.items():
        if key.endswith("_sha256"):
            if not isinstance(item, str) or not re.fullmatch(HASH, item):
                raise ValueError
            result[key] = item
    for key in (
        "tables",
        "rows",
        "candidates",
        "retained",
        "auto_increment_columns",
        "inserted",
        "updated",
        "deleted",
    ):
        if key not in value:
            continue
        item = value[key]
        if key == "auto_increment_columns":
            head = value.get("source_head", value.get("head"))
            allowed = {
                "0040_module_table_names": {
                    "governance_asset_versions.asset_row_id",
                    "governance_draft_heads.draft_row_id",
                    "quota_evaluation_admission_locks.organization_id",
                    "quota_participant_admission_locks.organization_id",
                },
                "0038_messaging_observations": {
                    "evaluation_admission_locks.organization_id",
                    "participant_admission_locks.organization_id",
                },
            }.get(head)
            if (
                allowed is None
                or not isinstance(item, list)
                or set(item) != allowed
                or len(item) != len(allowed)
            ):
                raise ValueError
        elif type(item) is not int or not 0 <= item <= 2**64:
            raise ValueError
        result[key] = item
    for key in (
        "verified",
        "prepared",
        "runtime_verified",
        "read_only",
        "restore_verified",
        "restore_target_retained",
        "baseline_unchanged",
        "inverse_verified",
        "domain_verified",
        "retention_known",
        "retention_elapsed",
        "pending_operation",
        "passed",
        "healthy",
        "mtls",
    ):
        if key in value:
            if type(value[key]) is not bool:
                raise ValueError
            result[key] = value[key]
    for key in ("retain_until", "retained_since", "cleanup_not_before", "stopped_at"):
        if key in value:
            result[key] = date(value[key])
    for key in ("status", "phase"):
        if key in value:
            if value[key] not in {
                "passed",
                "verified",
                "bound",
                "prepared",
                "released",
                "ready",
                "retained",
                "not_due",
                "eligible",
                "unknown",
                "deleted",
                "pending",
                "rehearsed",
                "failed",
                "drop_pending",
                "empty_after_restore",
                "rolled_back",
                "cleaned",
                "unplanned",
                "bootstrap_pending",
                "release_pending",
                "rollback_pending",
                "rollback_starting",
                "runtime_restore_pending",
            }:
                raise ValueError
            result[key] = value[key]
    if "elapsed_seconds" in value:
        item = value["elapsed_seconds"]
        if type(item) not in (int, float) or not 0 <= item <= 10**7:
            raise ValueError
        result["elapsed_seconds"] = item
    for key in ("retention", "archives"):
        if key in value:
            if not isinstance(value[key], list) or len(value[key]) > 100:
                raise ValueError
            result[key] = []
            for archive in value[key]:
                if not isinstance(archive, dict) or not re.fullmatch(
                    SCHEMA, archive.get("schema", "")
                ):
                    raise ValueError
                result[key].append({"schema": archive["schema"], **safe_metadata(archive)})
    if "coverage" in value:
        if not isinstance(value["coverage"], dict) or len(value["coverage"]) > 60:
            raise ValueError
        if any(
            not re.fullmatch(SCHEMA, key) or item not in {"covered", "not_covered"}
            for key, item in value["coverage"].items()
        ):
            raise ValueError
        result["coverage"] = value["coverage"]
    # Nested retirement/domain proof is projected recursively, with the same policy.
    for key in ("retirement", "rehearsal", "runtime", "fixture"):
        if key in value:
            result[key] = safe_metadata(value[key])
    return result


def safe_receipt(output, selected):
    try:
        value = json.loads(output.strip().splitlines()[-1])
        if (
            not isinstance(value, dict)
            or value.get("followup") not in {"ok", "failed"}
            or value.get("id") != selected["id"]
            or value.get("operation") != selected["operation"]
        ):
            raise ValueError
        result = safe_metadata(value)
        result.update(followup=value["followup"], operation=selected["operation"])
        if value["followup"] == "ok" and selected["operation"] == "release":
            if result.get("runtime_verified") is not True or result.get("phase") != "released":
                raise ValueError
        return result
    except (ValueError, KeyError, IndexError, TypeError):
        raise RuntimeError("Follow-up receipt is invalid; output withheld") from None


def install_command(remote, code, upload, digest):
    """Install once; same-SHA retries compare the archive hash and never overwrite code."""
    marker = code + ".archive-sha256"
    stage = upload.removesuffix(".tar.gz") + ".stage"
    return (
        f"umask 077; test ! -L {remote} && test ! -L {code} && "
        f"printf '%s  %s\\n' {digest} {upload} | sha256sum -c - >/dev/null && "
        f'if test -d {code}; then test -f {marker} && test "$(cat {marker})" = {digest}; '
        f"else mkdir {stage} && tar -xzf {upload} -C {stage} && mv {stage} {code} && "
        f"printf '%s\\n' {digest} > {marker}; fi && rm -f {upload}"
    )


def main():
    import hashlib

    os.umask(0o077)
    environment = dict(os.environ)
    selected = request(environment)
    run_id, attempt = (
        required(environment, "GITHUB_RUN_ID"),
        required(environment, "GITHUB_RUN_ATTEMPT"),
    )
    if not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("Invalid Actions run identifier")
    credentials_value = {
        "username": required(environment, "MYSQL_MAINTENANCE_USERNAME"),
        "password": required(environment, "MYSQL_MAINTENANCE_PASSWORD"),
    }
    common = legacy_transport()
    with tempfile.TemporaryDirectory(
        prefix="qs-ai-followup-", dir=environment.get("RUNNER_TEMP")
    ) as temporary:
        directory = Path(temporary)
        ssh, scp, target = common.ssh_arguments(environment, directory)
        if run("server identity", [*ssh, "hostname -s"]).strip().lower() != "servera":
            raise RuntimeError("Follow-up SSH target is not serverA")
        remote = f"/opt/qs-ai/schema-maintenance/{selected['id']}"
        code = f"{remote}/code-{selected['revision']}"
        archive = directory / "code.tar.gz"
        run(
            "package versioned follow-up",
            [
                "git",
                "archive",
                "--format=tar.gz",
                "--output",
                str(archive),
                selected["revision"],
                "src",
                "migrations",
                "alembic.ini",
                "configs",
                "deploy/serverA",
                "scripts/cd/image-retention.py",
            ],
        )
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        credentials = directory / "credentials.json"
        credentials.write_text(json.dumps(credentials_value) + "\n")
        credentials.chmod(0o600)
        private = f"{remote}/credentials-{run_id}-{attempt}.json"
        upload = f"{remote}/code-{run_id}-{attempt}.tar.gz"
        run(
            "prepare private follow-up directory",
            [*ssh, f"umask 077; test ! -L {remote} && mkdir -p {remote} && chmod 700 {remote}"],
        )
        try:
            run("upload follow-up credentials", [*scp, str(credentials), f"{target}:{private}"])
            run("protect follow-up credentials", [*ssh, f"chmod 600 {private}"])
            run("upload versioned follow-up", [*scp, str(archive), f"{target}:{upload}"])
            run(
                "install immutable follow-up", [*ssh, install_command(remote, code, upload, digest)]
            )
            command = (
                f"python3 {code}/deploy/serverA/schema_followup.py {selected['operation']} "
                f"--id {selected['id']} --credentials {private}"
            )
            if selected["operation"] == "bootstrap":
                command += (
                    f" --base-id {selected['base_id']} --base-revision {selected['base_revision']}"
                )
            if selected["release"]:
                command += f" --release {selected['release']}"
            receipt = safe_receipt(run("schema follow-up", [*ssh, command]), selected)
            receipt["workflow_revision"] = selected["revision"]
            path = Path("schema-followup-receipt.json")
            path.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
            path.chmod(0o600)
            print(json.dumps(receipt, sort_keys=True))
            if receipt["followup"] != "ok":
                raise RuntimeError("Follow-up failed; inspect private context evidence")
            if selected["operation"] == "release":
                common.deployment_module().record_deployment(ssh)
        finally:
            run("remove temporary follow-up credentials", [*ssh, f"rm -f {private}"])


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        detail = str(error) if type(error) in (ValueError, RuntimeError) else type(error).__name__
        print(json.dumps({"followup": "failed", "reason": detail}))
        raise SystemExit(1) from None
