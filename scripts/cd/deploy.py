"""Mac mini release packaging and SSH transport. No production values in logs."""

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from sqlalchemy import URL

ROOT = Path(__file__).resolve().parents[2]


def required(environment: dict, name: str) -> str:
    value = environment.get(name, "")
    if not value or "\x00" in value:
        raise ValueError(f"Missing or invalid {name}")
    return value


def database_url(environment: dict) -> str:
    database = environment.get("MYSQL_DATABASE") or environment.get("MYSQL_DBNAME")
    if not database:
        raise ValueError("Missing MYSQL_DATABASE")
    if (
        environment.get("MYSQL_DATABASE")
        and environment.get("MYSQL_DBNAME")
        and environment["MYSQL_DATABASE"] != environment["MYSQL_DBNAME"]
    ):
        raise ValueError("MYSQL_DATABASE and MYSQL_DBNAME disagree")
    port = int(environment.get("MYSQL_PORT") or "3306")
    if not 1 <= port <= 65535:
        raise ValueError("Invalid MYSQL_PORT")
    return URL.create(
        "mysql+asyncmy",
        username=required(environment, "MYSQL_USERNAME"),
        password=required(environment, "MYSQL_PASSWORD"),
        host=required(environment, "MYSQL_HOST"),
        port=port,
        database=database,
    ).render_as_string(hide_password=False)


def runtime_config(environment: dict) -> dict:
    # Compose processes interpolation in JSON too.
    url = database_url(environment).replace("$", "$$")
    runtime = {
        "services": {name: {"environment": {"QS_AI_DATABASE_URL": url}} for name in ("api", "grpc")}
    }
    flags = {}
    for name in ("EXECUTION", "GOVERNANCE", "EVALUATION"):
        key = f"QS_AI_{name}_ENABLED"
        value = environment.get(key, "false")
        if value not in {"true", "false"}:
            raise ValueError(f"{key} must be true or false")
        flags[name] = value == "true"
    grpc_environment = runtime["services"]["grpc"]["environment"]
    grpc_environment.update(
        {
            "QS_AI_GRPC__GOVERNANCE_ENABLED": str(flags["GOVERNANCE"]).lower(),
        }
    )
    if flags["EXECUTION"] or flags["GOVERNANCE"]:
        address = required(environment, "QS_AI_QS_ADDRESS")
        if (
            not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*:[0-9]{1,5}", address)
            or not 1 <= int(address.rsplit(":", 1)[1]) <= 65535
        ):
            raise ValueError("Expected QS internal host:port")
        # Command acceptance and managed retry both recheck current QS authorization.
        grpc_environment["QS_AI_GRPC__ACCESS_ADDRESS"] = address
    if flags["EXECUTION"] or flags["EVALUATION"]:
        endpoint = required(environment, "QS_AI_MODEL_ENDPOINT")
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
        ):
            raise ValueError("Expected HTTPS model endpoint without embedded credentials")
        credential = required(environment, "QS_AI_MODEL_API_KEY")
        if not credential.strip():
            raise ValueError("Missing model credential")
        services = yaml.safe_load((ROOT / "deploy/serverA/execution.yaml").read_text())["services"]
        selected = []
        if flags["EXECUTION"]:
            selected.extend(("worker", "delivery"))
            services["worker"]["environment"]["QS_AI_GENERATION__ENABLED"] = "true"
            services["worker"]["environment"]["QS_AI_GRPC__ACCESS_ADDRESS"] = address
            services["delivery"]["environment"]["QS_AI_GRPC__RESULT_ADDRESS"] = address
        if flags["EVALUATION"]:
            selected.append("evaluation")
            services["evaluation"]["environment"]["QS_AI_EVALUATION__ENABLED"] = "true"
        for name in selected:
            values = services[name]["environment"]
            values["QS_AI_DATABASE_URL"] = url
            if name in {"worker", "evaluation"}:
                values["QS_AI_GENERATION__ENDPOINT"] = endpoint.replace("$", "$$")
                values["QS_AI_MODEL_API_KEY"] = credential.replace("$", "$$")
            runtime["services"][name] = services[name]
    return runtime


def write_registry_auth(directory: Path, environment: dict) -> None:
    """Use QS's isolated auth file; docker login may select macOS Keychain."""
    registry = required(environment, "ALIYUN_ACR_REGISTRY")
    username = required(environment, "ALIYUN_ACR_USERNAME")
    password = required(environment, "ALIYUN_ACR_PASSWORD")
    auth = base64.b64encode(f"{username}:{password}".encode()).decode()
    path = directory / "config.json"
    # Creation mode is explicit even when called outside main's private umask.
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump({"auths": {registry: {"auth": auth}}}, stream)


def run(phase: str, args: list[str], **kwargs) -> str:
    print(f"{phase}: started", flush=True)
    result = subprocess.run(args, capture_output=True, text=True, timeout=1800, **kwargs)
    if result.returncode:
        raise RuntimeError(f"{phase} failed; raw output suppressed")
    return result.stdout


def export_failure_kind(raw: bytes) -> str:
    """Allowlisted categories only; daemon stderr may contain private registry details."""
    message = raw.lower()
    for category, patterns in (
        ("no_space", (b"no space left", b"disk full", b"not enough space")),
        ("missing_content", (b"blob not found", b"missing blob")),
        ("permission_denied", (b"permission denied", b"access denied")),
        (
            "daemon_unavailable",
            (b"cannot connect", b"connection refused", b"daemon is not running"),
        ),
    ):
        if any(pattern in message for pattern in patterns):
            return category
    if b"content digest" in message and b"not found" in message:
        return "missing_content"
    return "unclassified"


def export_image(image: str, archive: Path, environment: dict, timeout: float = 1800) -> None:
    """Stream into a private compressed file; check both children before publishing."""
    if timeout <= 0:
        raise ValueError("Image export timeout must be positive")
    exporter = compressor = None
    temporary = None
    deadline = time.monotonic() + timeout
    print("image export: started", flush=True)
    try:
        with (
            tempfile.NamedTemporaryFile(
                dir=archive.parent, suffix=".tar.gz.tmp", delete=False
            ) as target,
            tempfile.TemporaryFile() as export_errors,
            tempfile.TemporaryFile() as compression_errors,
        ):
            temporary = Path(target.name)
            exporter = subprocess.Popen(
                ["docker", "save", image],
                stdout=subprocess.PIPE,
                stderr=export_errors,
                env=environment,
            )
            compressor = subprocess.Popen(
                ["gzip", "-1", "-c"],
                stdin=exporter.stdout,
                stdout=target,
                stderr=compression_errors,
                env=environment,
            )
            exporter.stdout.close()
            compressed = compressor.wait(timeout=max(0, deadline - time.monotonic()))
            exported = exporter.wait(timeout=max(0, deadline - time.monotonic()))
            if compressed or exported:
                export_errors.seek(0)
                compression_errors.seek(0)
                export_kind = export_failure_kind(export_errors.read(16384))
                compression_kind = export_failure_kind(compression_errors.read(16384))
                free_mib = shutil.disk_usage(archive.parent).free // (1024 * 1024)
                raise RuntimeError(
                    f"Image export failed: docker_exit={exported} docker_kind={export_kind} "
                    f"gzip_exit={compressed} gzip_kind={compression_kind} "
                    f"archive_disk_free_mib={free_mib}; raw output suppressed"
                )
        run("gzip integrity", ["gzip", "-t", str(temporary)])
        # A short garbage stream can be accepted as an empty archive by GNU tar.
        # Read entries without extracting layers or accumulating a large file list.
        try:
            manifest_found = False
            with tarfile.open(temporary, "r|gz") as package:
                for member in package:
                    if member.isfile():
                        content = package.extractfile(member)
                        assert content is not None
                        with content:
                            while content.read(1024 * 1024):
                                if time.monotonic() >= deadline:
                                    raise subprocess.TimeoutExpired("archive integrity", timeout)
                        manifest_found |= member.name == "manifest.json"
            if not manifest_found:
                raise RuntimeError("Image archive has no manifest")
        except (tarfile.TarError, EOFError, OSError):
            raise RuntimeError("Image archive is invalid; raw output suppressed") from None
        os.replace(temporary, archive)
    finally:
        for process in (compressor, exporter):
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait()
        if exporter is not None and exporter.stdout is not None:
            exporter.stdout.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def record_deployment(ssh: list[str]) -> None:
    # Remote state is written only after health, image and schema verification.
    # It also identifies the actual target of rollback, not the workflow source SHA.
    state = json.loads(run("deployment receipt", [*ssh, "cat /opt/qs-ai/state.json"]))
    current = state.get("current", "")
    if not isinstance(current, str) or not re.fullmatch(r"[0-9a-f]{40}-[A-Za-z0-9_.-]+", current):
        raise RuntimeError("Invalid deployed release receipt")
    revision = current[:40]
    Path("deployment-receipt.json").write_text(
        json.dumps({"revision": revision, "release": current})
    )
    if output := os.environ.get("GITHUB_OUTPUT"):
        with Path(output).open("a") as stream:
            stream.write(f"actual_revision={revision}\n")


def main() -> None:
    os.umask(0o077)
    env = dict(os.environ)
    action = env.get("DEPLOY_OPERATION", "deploy")
    if action not in ("deploy", "rollback"):
        raise ValueError("Invalid operation")
    config = runtime_config(env) if action == "deploy" else None
    revision = required(env, "DEPLOY_SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("Invalid commit")
    run_id, attempt = required(env, "GITHUB_RUN_ID"), required(env, "GITHUB_RUN_ATTEMPT")
    if not run_id.isdigit() or not attempt.isdigit():
        raise ValueError("Invalid run identifier")
    release_id = f"{revision}-{run_id}-{attempt}"
    host, user = required(env, "SVRA_HOST"), required(env, "SVRA_USERNAME")
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]*", host):
        raise ValueError("Invalid SSH host")
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]*", user):
        raise ValueError("Invalid SSH user")
    port = int(env.get("SVRA_SSH_PORT") or "22")
    if not 1 <= port <= 65535:
        raise ValueError("Invalid SSH port")
    with tempfile.TemporaryDirectory(prefix="qs-ai-", dir=env.get("RUNNER_TEMP")) as temporary:
        work = Path(temporary)
        key, hosts = work / "key", work / "known_hosts"
        key.write_text(required(env, "SVRA_SSH_KEY") + "\n")
        public_key = required(env, "SVRA_HOST_PUBLIC_KEY").strip()
        if not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\r\n]+)?", public_key):
            raise ValueError("Expected verified serverA ed25519 host public key")
        hosts.write_text(f"[{host}]:{port} {public_key}\n{host} {public_key}\n")
        common = [
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={hosts}",
            "-o",
            "ConnectTimeout=20",
            "-i",
            str(key),
        ]
        ssh = ["ssh", *common, "-p", str(port), f"{user}@{host}"]
        scp = ["scp", *common, "-P", str(port)]
        if run("server identity", [*ssh, "hostname -s"]).strip().lower() != "servera":
            raise RuntimeError("SSH target is not serverA")
        remote_script = f"/tmp/qs-ai-deploy-{run_id}-{attempt}.py"
        run(
            "upload deployment script",
            [*scp, str(ROOT / "deploy/serverA/deploy.py"), f"{user}@{host}:{remote_script}"],
        )
        remote_retention = remote_script.replace(".py", "-retention.py")
        run(
            "upload retention script",
            [
                *scp,
                str(ROOT / "scripts/cd/image-retention.py"),
                f"{user}@{host}:{remote_retention}",
            ],
        )
        try:
            if action == "rollback":
                print(run("rollback", [*ssh, f"python3 {remote_script} rollback"]), end="")
                record_deployment(ssh)
                return
            docker_config = work / "docker"
            docker_config.mkdir()
            docker_env = {**env, "DOCKER_CONFIG": str(docker_config)}
            registry = required(env, "ALIYUN_ACR_REGISTRY")
            namespace = required(env, "ALIYUN_ACR_NAMESPACE")
            digest = required(env, "IMAGE_DIGEST")
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError("Invalid image digest")
            write_registry_auth(docker_config, env)
            image = f"{registry}/{namespace}/qs-ai@{digest}"
            run(
                "image pull", ["docker", "pull", "--platform", "linux/amd64", image], env=docker_env
            )
            alias = f"qs-ai:{revision}"
            run("image tag", ["docker", "tag", image, alias], env=docker_env)
            inspected = json.loads(
                run("image inspect", ["docker", "image", "inspect", alias], env=docker_env)
            )[0]
            package = work / "package"
            package.mkdir()
            archive = package / "image.tar.gz"
            export_image(alias, archive, docker_env)
            checksum = hashlib.sha256()
            with archive.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    checksum.update(block)
            manifest = {
                "revision": revision,
                "image_id": inspected["Id"],
                "digest": digest,
                "archive_sha256": checksum.hexdigest(),
            }
            (package / "manifest.json").write_text(json.dumps(manifest))
            (package / "runtime.json").write_text(json.dumps(config))
            (package / "image.env").write_text(f"QS_AI_IMAGE={alias}\n")
            shutil.copy(ROOT / "deploy/serverA/compose.yaml", package)
            target = f"/opt/qs-ai/releases/{release_id}"
            run(
                "prepare release directory",
                [
                    *ssh,
                    f"sudo -n mkdir -p {target} && sudo -n chown -R {user} /opt/qs-ai "
                    f"&& chmod 700 /opt/qs-ai {target}",
                ],
            )
            for path in package.iterdir():
                run("upload release", [*scp, str(path), f"{user}@{host}:{target}/{path.name}"])
            # Keep script with the release for local operational recovery.
            run(
                "retain deployment script",
                [
                    *ssh,
                    f"cp {remote_script} {target}/deploy.py && "
                    f"cp {remote_retention} {target}/image-retention.py && chmod 600 {target}/*",
                ],
            )
            try:
                print(
                    run("remote deployment", [*ssh, f"python3 {remote_script} {release_id}"]),
                    end="",
                )
            except RuntimeError:
                failure = json.loads(run("failure record", [*ssh, f"cat {target}/failure.json"]))
                print(
                    json.dumps({key: failure.get(key) for key in ("status", "error_type", "phase")})
                )
                raise
            record_deployment(ssh)
        finally:
            run("remove temporary script", [*ssh, f"rm -f {remote_script} {remote_retention}"])


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Our own validation messages contain field/phase names only.
        message = (
            str(error) if isinstance(error, (ValueError, RuntimeError)) else type(error).__name__
        )
        print(f"Release failed: {message}")
        raise SystemExit(1) from None
