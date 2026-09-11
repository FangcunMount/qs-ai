"""Mac mini release packaging and SSH transport. No production values in logs."""

import base64
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

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
    return {
        "services": {name: {"environment": {"QS_AI_DATABASE_URL": url}} for name in ("api", "grpc")}
    }


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
        try:
            if action == "rollback":
                print(run("rollback", [*ssh, f"python3 {remote_script} rollback"]), end="")
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
            uncompressed = work / "image.tar"
            archive = package / "image.tar.gz"
            run("image export", ["docker", "save", "-o", str(uncompressed), alias], env=docker_env)
            with (
                uncompressed.open("rb") as source,
                gzip.open(archive, "wb", compresslevel=1) as target_stream,
            ):
                shutil.copyfileobj(source, target_stream)
            uncompressed.unlink()
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
                [*ssh, f"cp {remote_script} {target}/deploy.py && chmod 600 {target}/*"],
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
        finally:
            run("remove temporary script", [*ssh, f"rm -f {remote_script}"])


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
