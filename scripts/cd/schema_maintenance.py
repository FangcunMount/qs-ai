"""Actions transport for schema maintenance; credentials never enter command arguments."""

import importlib.util
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPERATIONS = {"preflight", "prepare", "rehearse", "switch", "rollback", "status"}


def required(environment: dict[str, str], name: str) -> str:
    value = environment.get(name, "")
    if not value or "\x00" in value:
        raise ValueError(f"Missing or invalid {name}")
    return value


def request(environment: dict[str, str]) -> dict[str, str]:
    operation = required(environment, "SCHEMA_OPERATION")
    migration_id = required(environment, "SCHEMA_MAINTENANCE_ID")
    revision = required(environment, "DEPLOY_SHA")
    release = environment.get("SCHEMA_RELEASE", "")
    if operation not in OPERATIONS:
        raise ValueError("Invalid schema operation")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", migration_id):
        raise ValueError("Invalid maintenance identifier")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Invalid maintenance revision")
    if release and not re.fullmatch(r"[0-9a-f]{40}-[0-9]+-[0-9]+", release):
        raise ValueError("Invalid immutable release identifier")
    if operation == "prepare" and not release:
        receipt = json.loads(Path("release-receipt.json").read_text())
        release = receipt["release_id"]
        if (
            not re.fullmatch(r"[0-9a-f]{40}-[0-9]+-[0-9]+", release)
            or receipt["revision"] != revision
            or not receipt.get("prepared")
        ):
            raise ValueError("Prepared release receipt does not bind this revision")
    if operation == "prepare" and not release.startswith(revision + "-"):
        raise ValueError("Prepared release must use the tested workflow revision")
    return {"operation": operation, "id": migration_id, "revision": revision, "release": release}


def run(phase: str, arguments: list[str]) -> str:
    result = subprocess.run(arguments, capture_output=True, text=True, timeout=7200)
    if result.returncode:
        if phase == "schema maintenance":
            try:
                value = json.loads(result.stdout.strip().splitlines()[-1])
                if isinstance(value, dict) and value.get("schema_maintenance") == "failed":
                    return result.stdout
            except (ValueError, IndexError):
                pass
        # SSH/driver output can contain credentials or business records. Keep it private.
        raise RuntimeError(
            f"{phase} failed (exit {result.returncode}); inspect private host journal"
        )
    return result.stdout


def ssh_arguments(environment: dict[str, str], directory: Path) -> tuple[list[str], list[str], str]:
    host, user = required(environment, "SVRA_HOST"), required(environment, "SVRA_USERNAME")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*", host):
        raise ValueError("Invalid SSH host")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]*", user):
        raise ValueError("Invalid SSH user")
    port = int(environment.get("SVRA_SSH_PORT") or "22")
    if not 1 <= port <= 65535:
        raise ValueError("Invalid SSH port")
    key, known = directory / "key", directory / "known_hosts"
    key.write_text(required(environment, "SVRA_SSH_KEY") + "\n")
    key.chmod(0o600)
    public = required(environment, "SVRA_HOST_PUBLIC_KEY").strip()
    if not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]+)?", public):
        raise ValueError("Expected verified serverA host public key")
    known.write_text(f"[{host}]:{port} {public}\n{host} {public}\n")
    known.chmod(0o600)
    options = [
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known}",
        "-o",
        "ConnectTimeout=20",
        "-i",
        str(key),
    ]
    target = f"{user}@{host}"
    return ["ssh", *options, "-p", str(port), target], ["scp", *options, "-P", str(port)], target


def safe_receipt(output: str, expected: dict[str, str]) -> dict:
    """Project typed metadata, never free-text errors, nested driver objects or bodies."""
    try:
        value = json.loads(output.strip().splitlines()[-1])
        if not isinstance(value, dict) or value.get("schema_maintenance") not in {"ok", "failed"}:
            raise ValueError
        if value.get("id") != expected["id"] or value.get("operation") != expected["operation"]:
            raise ValueError
        # The preflight host wrapper includes a typed database report. Flatten only
        # known proof fields, never caller-supplied phase/operation or raw errors.
        if isinstance(value.get("report"), dict):
            proof_fields = {
                "ok",
                "preflight",
                "source_server_uuid",
                "source_schema",
                "head",
                "mysql_version",
                "partial_revokes",
                "capabilities",
                "missing_capabilities",
                "source_schema_contract",
                "cross_schema_fk_visibility",
                "tables",
                "estimated_rows",
                "allocated_bytes",
                "source_collation",
                "target",
                "target_exists",
                "permission_check",
            }
            value = {
                **{key: item for key, item in value["report"].items() if key in proof_fields},
                **value,
            }
        result = {key: value[key] for key in ("schema_maintenance", "id", "operation")}
        phases = {
            "preflight",
            "unplanned",
            "prepared",
            "rehearsed",
            "switched",
            "rolled_back",
            "recovered_old",
            "preparing",
            "switched_starting",
            "rolled_back_starting",
            "recovered_old_starting",
        }
        if "phase" in value:
            if value["phase"] not in phases:
                raise ValueError
            result["phase"] = value["phase"]
        patterns = {
            "server_uuid": r"[0-9a-f-]{36}",
            "source_server_uuid": r"[0-9a-f-]{36}",
            "head": r"[0-9]{4}_[a-z0-9_]+",
            "source_head": r"[0-9]{4}_[a-z0-9_]+",
            "target_head": r"[0-9]{4}_[a-z0-9_]+",
            "source_schema": r"[a-z][a-z0-9_]{0,63}",
            "source": r"[a-z][a-z0-9_]{0,63}",
            "target": r"[a-z][a-z0-9_]{0,63}",
            "old_image": r"sha256:[0-9a-f]{64}",
            "new_image": r"sha256:[0-9a-f]{64}",
            "old_release": r"[0-9a-f]{40}-[0-9]+-[0-9]+",
            "new_release": r"[0-9a-f]{40}-[0-9]+-[0-9]+",
            "release": r"[0-9a-f]{40}-[0-9]+-[0-9]+",
            "mysql_version": r"[0-9][a-zA-Z0-9._-]{0,59}",
            "source_collation": r"[a-z][a-z0-9_]{0,63}",
        }
        for key, pattern in patterns.items():
            if key in value:
                if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
                    raise ValueError
                result[key] = value[key]
        for key in ("version", "tables", "rows", "estimated_rows", "allocated_bytes"):
            if key in value:
                if type(value[key]) is not int or not 0 <= value[key] <= 2**64:
                    raise ValueError
                result[key] = value[key]
        if "elapsed_seconds" in value:
            elapsed = value["elapsed_seconds"]
            if type(elapsed) not in (int, float) or not 0 <= elapsed <= 10**7:
                raise ValueError
            result["elapsed_seconds"] = elapsed
        for key in (
            "ok",
            "prepared",
            "verified",
            "runtime_started",
            "runtime_may_have_written",
            "exchange_requires_journal_probe",
            "target_exists",
            "partial_revokes",
        ):
            if key in value:
                if type(value[key]) is not bool:
                    raise ValueError
                result[key] = value[key]
        enums = {
            "preflight": {"passed", "missing_capabilities"},
            "source_schema_contract": {"verified"},
            "cross_schema_fk_visibility": {"all_schemas", "schema_scoped_requires_admin_evidence"},
            "permission_check": {"grant_metadata_only_no_database_created"},
        }
        for key, allowed in enums.items():
            if key in value:
                if value[key] not in allowed:
                    raise ValueError
                result[key] = value[key]
        if "capabilities" in value:
            capabilities = value["capabilities"]
            if not isinstance(capabilities, dict) or any(
                not re.fullmatch(r"[a-z_]{1,64}", key) or type(item) is not bool
                for key, item in capabilities.items()
            ):
                raise ValueError
            result["capabilities"] = capabilities
            missing = value.get("missing_capabilities", [])
            if not isinstance(missing, list) or any(key not in capabilities for key in missing):
                raise ValueError
            result["missing_capabilities"] = missing
        if "journals" in value:
            journals = value["journals"]
            journal_phases = {
                "planned",
                "prepare_pending",
                "prepared",
                "copy_pending",
                "copied",
                "verified",
                "switch_pending",
                "switched",
                "rollback_pending",
                "rolled_back",
                "cleaned",
            }
            if not isinstance(journals, dict) or any(
                key not in {"forward.json", "reverse.json"} or item not in journal_phases
                for key, item in journals.items()
            ):
                raise ValueError
            result["journals"] = journals
        for key in ("stopped_at", "rollback_stopped_at", "retain_until"):
            if value.get(key) is not None:
                if not isinstance(value[key], str) or not re.fullmatch(
                    r"[0-9T:.+Z-]{20,40}", value[key]
                ):
                    raise ValueError
                result[key] = value[key]
        if value["schema_maintenance"] == "ok" and expected["operation"] in {"switch", "rollback"}:
            allowed_phase = (
                {"switched"}
                if expected["operation"] == "switch"
                else {"rolled_back", "recovered_old"}
            )
            if result.get("phase") not in allowed_phase:
                raise ValueError
        return result
    except (ValueError, KeyError, IndexError, TypeError):
        raise RuntimeError(
            "Maintenance host returned an invalid receipt; output withheld"
        ) from None


def deployment_module():
    spec = importlib.util.spec_from_file_location(
        "qs_ai_cd_deployment", ROOT / "scripts/cd/deploy.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Deployment transport unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    os.umask(0o077)
    environment = dict(os.environ)
    selected = request(environment)
    run_id, attempt = (
        required(environment, "GITHUB_RUN_ID"),
        required(environment, "GITHUB_RUN_ATTEMPT"),
    )
    if not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("Invalid Actions run identifier")
    username = required(environment, "MYSQL_MAINTENANCE_USERNAME")
    password = required(environment, "MYSQL_MAINTENANCE_PASSWORD")
    with tempfile.TemporaryDirectory(
        prefix="qs-ai-schema-", dir=environment.get("RUNNER_TEMP")
    ) as temporary:
        directory = Path(temporary)
        ssh, scp, target = ssh_arguments(environment, directory)
        if run("server identity", [*ssh, "hostname -s"]).strip().lower() != "servera":
            raise RuntimeError("Maintenance SSH target is not serverA")
        # All interpolated remote path components have been restricted above.
        remote = f"/opt/qs-ai/schema-maintenance/{selected['id']}"
        code = f"{remote}/code-{selected['revision']}"
        archive = directory / "code.tar.gz"
        run(
            "package versioned maintenance",
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
        credentials = directory / "credentials.json"
        credentials.write_text(json.dumps({"username": username, "password": password}) + "\n")
        credentials.chmod(0o600)
        remote_credentials = f"{remote}/credentials-{run_id}-{attempt}.json"
        run(
            "prepare private maintenance directory",
            [*ssh, f"umask 077; mkdir -p {remote}; chmod 700 {remote}"],
        )
        try:
            run(
                "upload maintenance credentials",
                [*scp, str(credentials), f"{target}:{remote_credentials}"],
            )
            run("protect maintenance credentials", [*ssh, f"chmod 600 {remote_credentials}"])
            run(
                "upload versioned maintenance",
                [*scp, str(archive), f"{target}:{remote}/code-{run_id}-{attempt}.tar.gz"],
            )
            # Revision-specific source is immutable; retries use the same selected commit.
            run(
                "install versioned maintenance",
                [
                    *ssh,
                    f"umask 077; mkdir -p {code}; "
                    f"tar -xzf {remote}/code-{run_id}-{attempt}.tar.gz -C {code}; "
                    f"rm -f {remote}/code-{run_id}-{attempt}.tar.gz",
                ],
            )
            command = (
                f"python3 {code}/deploy/serverA/schema_maintenance.py {selected['operation']} "
                f"--id {selected['id']} --credentials {remote_credentials}"
            )
            if selected["release"]:
                command += f" --release {selected['release']}"
            output = run("schema maintenance", [*ssh, command])
            receipt = safe_receipt(output, selected)
            receipt["workflow_revision"] = selected["revision"]
            path = Path("schema-maintenance-receipt.json")
            path.write_text(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
            path.chmod(0o600)
            print(json.dumps(receipt, sort_keys=True))
            if receipt["schema_maintenance"] != "ok":
                raise RuntimeError(
                    "Maintenance failed; inspect the metadata receipt and private host journal"
                )
            if selected["operation"] in {"switch", "rollback"}:
                deployment_module().record_deployment(ssh)
        finally:
            run("remove temporary maintenance credentials", [*ssh, f"rm -f {remote_credentials}"])


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        detail = str(error) if type(error) in (ValueError, RuntimeError) else type(error).__name__
        print(json.dumps({"schema_maintenance": "failed", "reason": detail}))
        raise SystemExit(1) from None
