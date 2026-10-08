"""Server-side release transaction; standard library only, run as deploy user."""

import contextlib
import fcntl
import gzip
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path


class DeploymentError(RuntimeError):
    pass


ROOT = Path("/opt/qs-ai")
RETENTION_ROOT = Path("/var/lib/fangcun-image-retention")


class FollowupDeployment:
    """Expose this module's live globals, including the bounded run replacement."""

    def __getattr__(self, name):
        try:
            return globals()[name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        globals()[name] = value

    def __delattr__(self, name):
        globals().pop(name, None)


def schema_followup_module():
    pointer = ROOT / "schema-maintenance" / "active-v2.json"
    pending = ROOT / "schema-maintenance" / "operation-v2.json"
    if not pointer.exists():
        if pending.exists():
            raise DeploymentError("Maintenance operation lacks its active context; preserve facts")
        return None
    try:
        import stat

        info = pointer.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid():
            raise ValueError
        value = json.loads(pointer.read_text())
        if (
            value.get("format") != "qs-ai-schema-active-context/v2"
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value["context_id"])
            or not re.fullmatch(r"[0-9a-f]{40}", value["executor_revision"])
            or not re.fullmatch(r"[0-9a-f]{64}", value["helper_sha256"])
        ):
            raise ValueError
        path = (
            ROOT
            / "schema-maintenance"
            / value["context_id"]
            / ("code-" + value["executor_revision"])
            / "deploy/serverA/schema_followup.py"
        )
        if (
            path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != value["helper_sha256"]
        ):
            raise ValueError
        spec = importlib.util.spec_from_file_location("active_schema_followup", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception:
        raise DeploymentError("Active schema followup executor cannot be verified") from None


def schema_followup_guard():
    module = schema_followup_module()
    if module is not None:
        try:
            module.deployment_guard(ROOT, globals().get("_SCHEMA_FOLLOWUP_OPERATION"))
        except Exception:
            raise DeploymentError("An unfinished schema operation blocks deployment") from None


def schema_followup_publish(release, proof):
    module = schema_followup_module()
    if module is not None:
        try:
            module.publish_runtime(FollowupDeployment(), release, proof)
        except Exception:
            raise DeploymentError(
                "Runtime publication could not bind its maintenance context"
            ) from None


# Docker inspect expands omitted legacy Config fields. Normalize only their exact
# empty defaults; every non-default and unknown field remains in the comparison.
CONFIG_DEFAULTS = {
    "Hostname": "",
    "Domainname": "",
    "Image": "",
    "AttachStdin": False,
    "AttachStdout": False,
    "AttachStderr": False,
    "Tty": False,
    "OpenStdin": False,
    "StdinOnce": False,
    "Entrypoint": None,
    "OnBuild": None,
    "Volumes": None,
}


def json_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def config_digest(value: dict) -> str:
    if not isinstance(value, dict):
        raise DeploymentError("Invalid image configuration")
    normalized = dict(value)
    for key, default in CONFIG_DEFAULTS.items():
        if (
            key in normalized
            and type(normalized[key]) is type(default)
            and normalized[key] == default
        ):
            del normalized[key]
    return json_digest(normalized)


def image_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise DeploymentError("Invalid image digest identity")
    return value


def archive_identity(path: Path, expected_ref: str | None, revision: str) -> dict:
    """Verify one standard docker-save image, including each uncompressed layer.

    Never extract archive paths, and never return Config/Env contents. A config
    digest and ordered diff_ids identify the original asset independently of the
    daemon's config-vs-index choice for inspect.Id.
    """
    deadline = time.monotonic() + 1200

    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError
        with tarfile.open(path, "r:*") as archive:
            members = {}
            for member in archive:
                if member.name in members or len(members) >= 4096:
                    raise ValueError
                members[member.name] = member
                if time.monotonic() > deadline:
                    raise ValueError

            def content(name, limit):
                member = members[name]
                if not member.isfile() or member.size > limit:
                    raise ValueError
                with archive.extractfile(member) as stream:
                    return stream.read(limit + 1)

            manifest = json.loads(
                content("manifest.json", 128 * 1024), object_pairs_hook=unique_pairs
            )
            if not isinstance(manifest, list) or len(manifest) != 1:
                raise ValueError
            entry = manifest[0]
            if (
                expected_ref is not None
                and entry["RepoTags"] != [expected_ref]
                or expected_ref is None
                and entry["RepoTags"] not in (None, [])
            ):
                raise ValueError
            raw_config = content(entry["Config"], 2 * 1024 * 1024)
            config_sha = "sha256:" + hashlib.sha256(raw_config).hexdigest()
            config = json.loads(raw_config, object_pairs_hook=unique_pairs)
            if config["architecture"] != "amd64" or config["os"] != "linux":
                raise ValueError
            if config["config"]["Labels"]["org.opencontainers.image.revision"] != revision:
                raise ValueError
            diff_ids = config["rootfs"]["diff_ids"]
            layers = entry["Layers"]
            if (
                config["rootfs"]["type"] != "layers"
                or not diff_ids
                or len(layers) != len(diff_ids)
                or len(set(layers)) != len(layers)
            ):
                raise ValueError
            for name, expected in zip(layers, diff_ids, strict=True):
                image_id(expected)
                member = members[name]
                if not member.isfile() or member.size > 8 * 1024**3:
                    raise ValueError
                with archive.extractfile(member) as raw:
                    prefix = raw.read(2)
                    raw.seek(0)
                    # Legacy archives contain plain layers; OCI docker-save may
                    # contain gzip blobs. diff_ids always hash the decoded bytes.
                    with contextlib.ExitStack() as stack:
                        stream = (
                            stack.enter_context(gzip.GzipFile(fileobj=raw))
                            if prefix == b"\x1f\x8b"
                            else raw
                        )
                        checksum = hashlib.sha256()
                        size = 0
                        for block in iter(lambda stream=stream: stream.read(1024 * 1024), b""):
                            checksum.update(block)
                            size += len(block)
                            if size > 8 * 1024**3 or time.monotonic() > deadline:
                                raise ValueError
                        if "sha256:" + checksum.hexdigest() != expected:
                            raise ValueError
            return {
                "config_sha256": config_sha,
                "config_digest": config_digest(config["config"]),
                "architecture": config["architecture"],
                "os": config["os"],
                "variant": config.get("variant", ""),
                "rootfs_diff_ids": diff_ids,
            }
    except (OSError, ValueError, TypeError, KeyError, AttributeError, tarfile.TarError, EOFError):
        raise DeploymentError("Image archive identity invalid; contents withheld") from None


def match_image(inspected: dict, identity: dict, revision: str) -> str:
    try:
        actual = image_id(inspected["Id"])
        if (
            inspected["Architecture"] != identity["architecture"]
            or inspected["Os"] != identity["os"]
            or inspected.get("Variant", "") != identity["variant"]
            or inspected["RootFS"]["Type"] != "layers"
            or inspected["RootFS"]["Layers"] != identity["rootfs_diff_ids"]
            or config_digest(inspected["Config"]) != identity["config_digest"]
            or inspected["Config"]["Labels"].get("org.opencontainers.image.revision") != revision
        ):
            raise ValueError
        return actual
    except (ValueError, TypeError, KeyError, AttributeError):
        raise DeploymentError(
            "Image identity or architecture mismatch; contents withheld"
        ) from None


def check_manifest_identity(manifest: dict, identity: dict) -> None:
    image_id(manifest["image_id"])
    if manifest.get("image_identity") is not None:
        if manifest["image_identity"] != identity:
            raise DeploymentError("Exported image identity mismatch")
    elif manifest["image_id"] != identity["config_sha256"]:
        # Old manifests recorded a config ID. An unproved index ID is not enough.
        raise DeploymentError("Legacy source image configuration digest mismatch")
    if "digest" in manifest:
        image_id(manifest["digest"])


def loaded_image_binding(release: Path) -> dict | None:
    path = release / "loaded-image.json"
    if not path.exists():
        return None
    try:
        receipt = json.loads(path.read_text())
        manifest = json.loads((release / "manifest.json").read_text())
        check_manifest_identity(manifest, receipt["identity"])
        if (
            receipt["version"] != 1
            or receipt["revision"] != manifest["revision"]
            or receipt["source_image_id"] != manifest["image_id"]
            or receipt["archive_sha256"] != manifest["archive_sha256"]
            or receipt["registry_digest"] != manifest.get("digest")
            or (release / "image.env").read_text()
            != f"QS_AI_IMAGE={image_id(receipt['loaded_image_id'])}\n"
        ):
            raise ValueError
        return receipt
    except (OSError, ValueError, TypeError, KeyError):
        raise DeploymentError("Loaded image release binding invalid; contents withheld") from None


def release_image_id(release: Path, *, verify_identity: bool = False) -> str:
    receipt = loaded_image_binding(release)
    manifest = json.loads((release / "manifest.json").read_text())
    if receipt is not None:
        actual = receipt["loaded_image_id"]
        if verify_identity:
            inspected = json.loads(
                run("pinned image identity", ["sudo", "-n", "docker", "image", "inspect", actual])
            )[0]
            if match_image(inspected, receipt["identity"], manifest["revision"]) != actual:
                raise DeploymentError("Pinned image mismatch")
        return actual
    if manifest.get("image_identity") is not None:
        raise DeploymentError("Loaded image receipt missing")
    # Historical releases retain strict ID equality, never the new equivalence
    # exception. Check a mutable legacy tag before any rollback/preflight starts.
    expected = image_id(manifest["image_id"])
    reference = (release / "image.env").read_text().strip()
    if reference != f"QS_AI_IMAGE={expected}":
        if reference != f"QS_AI_IMAGE=qs-ai:{manifest['revision']}":
            raise DeploymentError("Historical image reference mismatch")
        inspected = json.loads(
            run(
                "legacy image identity",
                ["sudo", "-n", "docker", "image", "inspect", f"qs-ai:{manifest['revision']}"],
            )
        )[0]
        if inspected["Id"] != expected:
            raise DeploymentError("Historical image identity mismatch")
    return expected


def bind_loaded_image(release: Path, manifest: dict, identity: dict, actual: str) -> None:
    receipt = {
        "version": 1,
        "revision": manifest["revision"],
        "source_image_id": manifest["image_id"],
        "loaded_image_id": actual,
        "archive_sha256": manifest["archive_sha256"],
        "registry_digest": manifest.get("digest"),
        "identity": identity,
    }
    path = release / "loaded-image.json"
    if path.exists():
        if json.loads(path.read_text()) != receipt:
            raise DeploymentError("Existing loaded image binding cannot be overwritten")
    image_env = release / "image.env"
    temporary = release / ".image.env.tmp"
    temporary.write_text(f"QS_AI_IMAGE={actual}\n")
    temporary.chmod(0o600)
    temporary.replace(image_env)
    if not path.exists():
        # Publish only a complete receipt, without replacing a prior identity.
        # If interrupted before publication, apply can reconstruct it from the
        # still-present verified source archive; no service has started yet.
        with tempfile.NamedTemporaryFile(
            mode="w", dir=release, prefix=".loaded-", delete=False
        ) as stream:
            candidate = Path(stream.name)
            try:
                json.dump(receipt, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
                candidate.chmod(0o600)
                os.link(candidate, path)
            finally:
                candidate.unlink(missing_ok=True)


def secret_override(database_url: str) -> dict:
    if not database_url.startswith("mysql+asyncmy://") or any(
        char in database_url for char in "\r\n\x00"
    ):
        raise ValueError("Invalid database secret")
    return {
        "services": {
            "qs-ai": {
                "environment": {
                    "QS_AI_DATABASE_URL": database_url.replace("$", "$$"),
                }
            }
        }
    }


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def run(phase: str, args: list[str]) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=1200)
    if result.returncode:
        # Do not expose raw driver/Compose errors, which may contain credentials.
        detail = ""
        if phase == "database probe":
            try:
                diagnostic = json.loads(result.stdout.strip().splitlines()[-1])
                code = diagnostic.get("driver_code")
                if isinstance(code, int):
                    detail = f", database driver code {code}"
            except (ValueError, IndexError):
                pass
        raise DeploymentError(f"{phase} failed (exit {result.returncode}{detail})")
    return result.stdout


def save_loaded_archive(path: Path, actual: str) -> None:
    """The deploy caller owns the file; sudo only reads the exact daemon image."""
    image_id(actual)
    try:
        with path.open("xb") as output:
            path.chmod(0o600)
            result = subprocess.run(
                ["sudo", "-n", "docker", "save", actual],
                stdout=output,
                stderr=subprocess.PIPE,
                timeout=1200,
            )
            if result.returncode:
                raise DeploymentError("Loaded image export failed")
    except (OSError, subprocess.TimeoutExpired):
        raise DeploymentError("Loaded image export failed; details withheld") from None


def compose(release: Path, *args: str) -> list[str]:
    loaded_image_binding(release)  # Fail before starting anything if the pinned binding drifted.
    return [
        "sudo",
        "-n",
        "docker",
        "compose",
        "-p",
        "qs-ai",
        "--env-file",
        str(release / "image.env"),
        "-f",
        str(release / "compose.yaml"),
        "-f",
        str(release / "runtime.json"),
        *args,
    ]


def runtime_service(release: Path) -> str:
    services = run("compose services", compose(release, "config", "--services")).split()
    return "qs-ai" if "qs-ai" in services else "api"


def stop_release(release: Path) -> None:
    # Stop admission first. Then stop old consumers together and wait for drain.
    services = run("compose services", compose(release, "config", "--services")).split()
    ingress = [s for s in ("grpc", "api") if s in services]
    if ingress:
        run("stop old admission", compose(release, "stop", "-t", "210", *ingress))
    run("drain release", compose(release, "stop", "-t", "210"))
    running = run("verify stopped", compose(release, "ps", "--status", "running", "-q")).strip()
    if running:
        raise DeploymentError("Previous release still running")


def probe(release: Path, require_head: bool = False) -> dict:
    output = run(
        "database probe",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            runtime_service(release),
            "/app/.venv/bin/python",
            "-m",
            "qs_ai.bootstrap.database_check",
            *(["--require-head"] if require_head else []),
        ),
    )
    return json.loads(output.strip().splitlines()[-1])


def expected_heads(release: Path) -> list[str]:
    """Read the target image's migration contract without connecting to a database."""
    output = run(
        "image schema contract",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            runtime_service(release),
            "/app/.venv/bin/python",
            "-c",
            "import json; from alembic.config import Config; "
            "from alembic.script import ScriptDirectory; "
            "print(json.dumps(sorted(ScriptDirectory.from_config(Config('alembic.ini')).get_heads())))",
        ),
    )
    heads = json.loads(output.strip().splitlines()[-1])
    if not isinstance(heads, list) or not heads or any(not isinstance(v, str) for v in heads):
        raise DeploymentError("Image schema contract unavailable")
    return heads


def guard_schema_transition(current: list[str], expected: list[str]) -> None:
    """Never run destructive family conversion through ordinary deploy or rollback."""
    if not isinstance(current, list) or not isinstance(expected, list):
        raise DeploymentError("Database schema contract unavailable")
    for heads in (current, expected):
        if len(heads) > 1 or any(not isinstance(v, str) for v in heads):
            raise DeploymentError("Database schema contract unavailable")
    if not expected:
        raise DeploymentError("Image schema contract unavailable")

    def family(heads: list[str]) -> str:
        if not heads:
            return "empty"
        match = re.fullmatch(r"([0-9]{4})_[a-z0-9_]+", heads[0])
        if not match:
            return "unknown"
        number = int(match[1])
        return "legacy" if number <= 38 else ("transition" if number == 39 else "modules")

    source, target = family(current), family(expected)
    if "unknown" in (source, target):
        raise DeploymentError("Database schema contract unavailable")
    if "transition" in (source, target) or (
        source != target and "modules" in (source, target) and source != "empty"
    ):
        raise DeploymentError(
            "Schema conversion requires python -m qs_ai.maintenance.schema_refactor; "
            "ordinary deploy/rollback cannot preserve data across this boundary"
        )


def verify(release: Path) -> dict:
    expected_image = release_image_id(release, verify_identity=True)
    run(
        "service readiness",
        compose(
            release,
            "up",
            "-d",
            "--remove-orphans",
            "--pull",
            "never",
            "--wait",
            "--wait-timeout",
            "120",
        ),
    )
    services = run("compose services", compose(release, "config", "--services")).split()
    if not services:
        raise DeploymentError("No release services configured")
    for service in services:
        container = run("running container", compose(release, "ps", "-q", service)).strip()
        if not container:
            raise DeploymentError("Missing running service")
        image_id = run(
            "running image",
            ["sudo", "-n", "docker", "inspect", container, "--format", "{{.Image}}"],
        ).strip()
        if image_id != expected_image:
            raise DeploymentError("Running image does not match release")
    if "qs-ai" in services:
        run(
            "live mTLS probe",
            compose(
                release,
                "exec",
                "-T",
                "qs-ai",
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.bootstrap.grpc_probe",
            ),
        )
    return probe(release, True)


def release_path(name: str) -> Path:
    if not re.fullmatch(r"[0-9a-f]{40}-[0-9]+-[0-9]+", name):
        raise ValueError("Invalid release identifier")
    return ROOT / "releases" / name


def messaging_release(release: Path, manifest: dict | None = None) -> bool:
    """Read the release's frozen binding; never consult current mutable deployment vars."""
    path = release / "runtime.json"
    if not path.exists():
        return False  # Historical release/test fixture without MQ metadata.
    try:
        runtime = json.loads(path.read_text())
        service = runtime["services"]["qs-ai"]
        raw = service["environment"].get("QS_AI_MESSAGING")
        if raw is None:
            return False
        options = json.loads(raw)
        if options.get("enabled") is not True:
            raise ValueError
        if set(runtime["services"]) != {"qs-ai"}:
            raise ValueError
        if manifest is None:
            manifest = json.loads((release / "manifest.json").read_text())
        metadata = {"options": raw, "volumes": service.get("volumes", [])}
        digest = hashlib.sha256(
            json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if digest != manifest.get("messaging_binding_sha256"):
            raise ValueError
        paths = {
            options["signing_key_file"],
            options["qs_recipient_key_file"],
            *options["decrypt_key_files"].values(),
            *options["qs_signer_files"].values(),
        }
        targets = set()
        revisions = set()
        for mount in service["volumes"]:
            target = mount["target"]
            if not target.startswith("/run/qs-ai-jose/") or target in targets:
                raise ValueError
            source = mount["source"]
            match = re.fullmatch(
                r"/data/infra/qs-ai-messaging/versions/([a-z0-9][a-z0-9-]{0,63})/([a-z0-9._-]+\.json)",
                source,
            )
            if not match or target != "/run/qs-ai-jose/" + match[2]:
                raise ValueError
            if (
                mount["type"] != "bind"
                or mount["read_only"] is not True
                or mount["bind"]["create_host_path"] is not False
            ):
                raise ValueError
            targets.add(target)
            revisions.add(match[1])
        if targets != paths or len(revisions) != 1:
            raise ValueError
        return True
    except (ValueError, TypeError, KeyError, AttributeError, OSError):
        raise DeploymentError("Frozen MQ release binding invalid; contents withheld") from None


def messaging_transition(current: Path, target: Path) -> None:
    if messaging_release(current) and not messaging_release(target):
        raise DeploymentError("MQ ownership cannot fall back to a messaging-disabled release")


def messaging_key_preflight(release: Path) -> None:
    if messaging_release(release):
        run(
            "MQ key file preflight",
            compose(
                release,
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "qs-ai",
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.maintenance.messaging_preflight",
            ),
        )


def restore(state: dict) -> None:
    schema_followup_guard()
    previous = state.get("previous")
    if not previous:
        raise DeploymentError("No previous successful release")
    target = release_path(previous)
    messaging_transition(release_path(state["current"]), target)
    release_image_id(target, verify_identity=True)
    messaging_key_preflight(target)
    current = release_path(state["current"])
    before = probe(current, True)
    guard_schema_transition(before["current"], expected_heads(target))
    # Old image must recognize the current schema and require its own exact head.
    probe(target, True)
    try:
        stop_release(current)
        database = verify(target)
    except Exception:
        stop_release(target)
        probe(current, True)
        verify(current)
        raise
    write_json(ROOT / "state.json", {"current": previous, "previous": state["current"]})
    schema_followup_publish(target, database)
    print(json.dumps({"rollback": "passed", "release": previous}))


def _load_release_image(release: Path, manifest: dict) -> str:
    """Validate and pin an uploaded archive without accessing database or runtime."""
    revision = manifest["revision"]
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or not release.name.startswith(revision + "-"):
        raise ValueError("Invalid release revision")
    archive = release / "image.tar.gz"
    checksum = hashlib.sha256()
    with archive.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    digest = checksum.hexdigest()
    if digest != manifest["archive_sha256"]:
        raise DeploymentError("Image archive checksum mismatch")
    reference = manifest.get("image_ref", f"qs-ai:{revision}")
    if not isinstance(reference, str) or not re.fullmatch(
        r"qs-ai:[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}", reference
    ):
        raise DeploymentError("Invalid release image reference")
    identity = archive_identity(archive, reference, revision)
    check_manifest_identity(manifest, identity)
    run("image load", ["sudo", "-n", "docker", "load", "-i", str(archive)])
    inspected = json.loads(
        run("image identity", ["sudo", "-n", "docker", "image", "inspect", reference])
    )[0]
    actual = match_image(inspected, identity, revision)
    if actual != identity["config_sha256"]:
        # Containerd daemons may expose an index/manifest ID. Export that exact
        # immutable ID and verify its raw config digest, not just six Config fields.
        with tempfile.TemporaryDirectory(prefix=".loaded-image-", dir=release) as directory:
            saved = Path(directory) / "image.tar"
            save_loaded_archive(saved, actual)
            if archive_identity(saved, None, revision) != identity:
                raise DeploymentError("Loaded image configuration or layer digest mismatch")
    bind_loaded_image(release, manifest, identity, actual)
    return actual


def _offline_release_preflight(release: Path, enabled: bool) -> None:
    services = run("compose services", compose(release, "config", "--services")).split()
    if enabled:
        if services != ["qs-ai"]:
            raise DeploymentError("MQ release requires one qs-ai service")
        messaging_key_preflight(release)
    if "grpc" in services or "qs-ai" in services:
        run(
            "TLS file preflight",
            compose(
                release,
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "qs-ai" if "qs-ai" in services else "grpc",
                "/app/.venv/bin/python",
                "-m",
                "qs_ai.bootstrap.grpc_probe",
                "--check-files",
            ),
        )


def stage_prepare(release: Path) -> dict:
    """Prepare immutable image materials only; the caller owns both deployment locks."""
    materials = {
        key + "_sha256": hashlib.sha256((release / name).read_bytes()).hexdigest()
        for key, name in (
            ("manifest", "manifest.json"),
            ("runtime", "runtime.json"),
            ("compose", "compose.yaml"),
        )
    }
    prepared = release / "prepared.json"
    existing = json.loads(prepared.read_text()) if prepared.exists() else None
    if existing is not None and any(existing.get(key) != value for key, value in materials.items()):
        raise DeploymentError("Existing prepared release binding cannot be overwritten")
    path = ROOT / "state.json"
    state = json.loads(path.read_text()) if path.exists() else {}
    verified = release / "verification.json"
    if release.name in (state.get("current"), state.get("previous")) or (
        verified.exists() and json.loads(verified.read_text()).get("phase") == "ready"
    ):
        raise DeploymentError("Existing successful release cannot be overwritten")
    manifest = json.loads((release / "manifest.json").read_text())
    loaded_image_binding(release)
    enabled = messaging_release(release, manifest)
    actual = _load_release_image(release, manifest)
    _offline_release_preflight(release, enabled)
    heads = expected_heads(release)
    if len(heads) != 1 or not re.fullmatch(r"[0-9]{4}_[a-z0-9_]+", heads[0]):
        raise DeploymentError("Image schema contract unavailable")
    receipt = {
        "version": 1,
        "prepared": True,
        "release": release.name,
        "release_id": release.name,
        "revision": manifest["revision"],
        "image_id": actual,
        "source_image_id": manifest["image_id"],
        "archive_sha256": manifest["archive_sha256"],
        "registry_digest": manifest.get("digest"),
        "expected_heads": heads,
        **materials,
    }
    if existing is not None:
        if existing != receipt:
            raise DeploymentError("Existing prepared release binding cannot be overwritten")
    else:
        write_json(prepared, receipt)
    return receipt


def apply(release: Path, state: dict) -> None:
    schema_followup_guard()
    manifest = json.loads((release / "manifest.json").read_text())
    verified = release / "verification.json"
    if release.name in (state.get("current"), state.get("previous")) or (
        verified.exists() and json.loads(verified.read_text()).get("phase") == "ready"
    ):
        raise DeploymentError("Existing successful release cannot be overwritten")
    loaded_image_binding(release)
    enabled = messaging_release(release, manifest)
    if state.get("current"):
        messaging_transition(release_path(state["current"]), release)
    actual = _load_release_image(release, manifest)
    revision = manifest["revision"]
    archive = release / "image.tar.gz"
    _offline_release_preflight(release, enabled)
    before = probe(release)
    guard_schema_transition(before["current"], before["expected"])
    write_json(release / "verification.json", {"phase": "preflight", "database": before})
    # A migration failure never replaces a healthy service. MySQL DDL may be partial.
    run(
        "migration",
        compose(
            release,
            "run",
            "--rm",
            "--no-deps",
            "-T",
            runtime_service(release),
            "/app/.venv/bin/alembic",
            "upgrade",
            "head",
        ),
    )
    after = probe(release, True)
    try:
        if state.get("current"):
            stop_release(release_path(state["current"]))
        verify(release)
    except Exception:
        if state.get("current") and before["current"] == after["current"]:
            previous = release_path(state["current"])
            stop_release(release)
            messaging_transition(release, previous)
            messaging_key_preflight(previous)
            probe(previous, True)
            verify(previous)
            print("Service restored to previous successful release", flush=True)
        elif not state.get("current"):
            run("first release cleanup", compose(release, "down"))
        else:
            print("Schema changed; application rollback requires compatibility review", flush=True)
        raise
    write_json(
        release / "verification.json",
        {
            "phase": "ready",
            "database": after,
            "image_id": actual,
            "source_image_id": manifest["image_id"],
        },
    )
    write_json(ROOT / "state.json", {"current": release.name, "previous": state.get("current")})
    schema_followup_publish(release, after)
    # Loaded images and release metadata remain available for rollback.
    archive.unlink()
    print(json.dumps({"deployed": revision, "database": after, "release": release.name}))


@contextlib.contextmanager
def global_deploy_lock():
    directory = str(RETENTION_ROOT)
    path = RETENTION_ROOT / "deploy.lock"
    run("retention directory", ["sudo", "-n", "mkdir", "-p", directory])
    run("retention directory mode", ["sudo", "-n", "chmod", "0755", directory])
    if not path.exists():
        # sudoers permits chown/chmod/ln, not touch. Publish a root-owned inode
        # without replacing a concurrent initializer's file or held flock.
        # ROOT and RETENTION_ROOT must reside on the same filesystem.
        with tempfile.TemporaryDirectory(prefix=".retention-lock-", dir=ROOT) as temporary:
            candidate = Path(temporary) / "lock"
            candidate.touch(exist_ok=False)
            run("deployment lock owner", ["sudo", "-n", "chown", "root:root", str(candidate)])
            run("deployment lock mode", ["sudo", "-n", "chmod", "0666", str(candidate)])
            try:
                run("deployment lock", ["sudo", "-n", "ln", "--", str(candidate), str(path)])
            except DeploymentError:
                if not path.is_file():
                    raise
    with open(path, "r+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def retain_successful_image(release):
    # Cleanup has its own failure status; never trigger application rollback.
    try:
        actual = release_image_id(release)
        script = Path(__file__)
        helper = (
            script.with_name("image-retention.py")
            if script.name == "deploy.py"
            else (script.with_name(script.stem + "-retention.py"))
        )
        previous = json.loads((ROOT / "state.json").read_text()).get("previous")
        protected = []
        if previous:
            protected = ["--protect-image-id", release_image_id(release_path(previous))]
        followup = schema_followup_module()
        if followup is not None:
            for image in followup.protected_images(ROOT):
                protected.extend(["--protect-image-id", image])
        run(
            "image retention",
            [
                "sudo",
                "-n",
                "python3",
                str(helper),
                "--service",
                "qs-ai",
                "--image-ref",
                actual,
                "--apply",
                "--deployment-locked",
                *protected,
            ],
        )
    except Exception:
        print("::warning::Deployment succeeded, but image retention failed; inspect server audit.")


def main() -> None:
    os.umask(0o077)
    ROOT.mkdir(exist_ok=True)
    with global_deploy_lock(), (ROOT / "deploy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = ROOT / "state.json"
        state = json.loads(path.read_text()) if path.exists() else {}
        if len(sys.argv) == 3 and sys.argv[1] == "prepare":
            release = release_path(sys.argv[2])
            print(json.dumps(stage_prepare(release), sort_keys=True))
            return
        if len(sys.argv) != 2:
            raise DeploymentError("Expected release, rollback, or prepare <release>")
        if sys.argv[1] == "rollback":
            followup = schema_followup_module()
            if followup is None:
                restore(state)
            else:
                followup.ordinary_restore(FollowupDeployment(), state)
            retain_successful_image(release_path(state["previous"]))
        else:
            release = release_path(sys.argv[1])
            try:
                followup = schema_followup_module()
                if followup is None:
                    apply(release, state)
                else:
                    followup.ordinary_release(FollowupDeployment(), release.name)
            except Exception as error:
                write_json(
                    release / "failure.json",
                    {
                        "status": "failed",
                        "error_type": type(error).__name__,
                        "phase": str(error) if isinstance(error, DeploymentError) else "unexpected",
                    },
                )
                raise
            retain_successful_image(release)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        message = str(error) if isinstance(error, DeploymentError) else type(error).__name__
        print(f"Deployment failed: {message}", file=sys.stderr)
        raise SystemExit(1) from None
