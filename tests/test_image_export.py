import importlib.util
import io
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def exporter(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "image_export_test", ROOT / "scripts/cd/deploy.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    payload = tmp_path / "payload.tar"
    with tarfile.open(payload, "w") as archive:
        content = b"[]"
        member = tarfile.TarInfo("manifest.json")
        member.size = len(content)
        archive.addfile(member, io.BytesIO(content))
    script = tmp_path / "docker.py"
    script.write_text("""import os,sys,time
from pathlib import Path
mode = os.environ.get('MODE', 'success')
if mode == 'classified_error':
    sys.stderr.write('content digest sha256:private-registry-secret: not found')
    sys.exit(7)
if mode == 'hang':
    time.sleep(60)
if mode == 'empty':
    sys.stdout.buffer.write(bytes(10240))
elif mode == 'truncated':
    sys.stdout.buffer.write(Path(os.environ['PAYLOAD']).read_bytes()[:513])
elif mode == 'corrupt':
    sys.stdout.buffer.write(b'not a tar archive')
else:
    sys.stdout.buffer.write(Path(os.environ['PAYLOAD']).read_bytes())
sys.stdout.flush()
if mode == 'fail_after_tar':
    sys.exit(7)
""")
    original = subprocess.Popen
    processes = []

    def spawn(args, **kwargs):
        if args[0] == "docker":
            assert args == ["docker", "save", "test-image"]
            args = [sys.executable, str(script)]
        elif args[:3] == ["gzip", "-1", "-c"] and kwargs["env"].get("MODE") == "compression_fail":
            args = [sys.executable, "-c", "import sys; sys.stdout.write('partial'); sys.exit(9)"]
        process = original(args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(module.subprocess, "Popen", spawn)
    return module, {**os.environ, "PAYLOAD": str(payload)}, processes


def test_streaming_export_produces_valid_archive(exporter, tmp_path):
    module, environment, processes = exporter
    target = tmp_path / "image.tar.gz"
    module.export_image("test-image", target, environment, timeout=5)
    with tarfile.open(target, "r:gz") as archive:
        assert archive.getnames() == ["manifest.json"]
    assert all(p.returncode == 0 for p in processes)
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    "mode", ["fail_after_tar", "corrupt", "empty", "truncated", "compression_fail", "hang"]
)
def test_failed_export_keeps_previous_package_and_reaps_children(exporter, tmp_path, mode):
    module, environment, processes = exporter
    environment["MODE"] = mode
    target = tmp_path / "image.tar.gz"
    target.write_bytes(b"previous complete package")
    timeout = 0.3 if mode == "hang" else 5
    with pytest.raises((RuntimeError, subprocess.TimeoutExpired)):
        module.export_image("test-image", target, environment, timeout=timeout)
    assert target.read_bytes() == b"previous complete package"
    assert all(p.returncode is not None for p in processes)
    assert not list(tmp_path.glob("*.tmp"))


def test_export_failure_reports_only_allowlisted_category_and_exit_codes(exporter, tmp_path):
    module, environment, processes = exporter
    environment["MODE"] = "classified_error"
    target = tmp_path / "image.tar.gz"
    with pytest.raises(RuntimeError) as error:
        module.export_image("test-image", target, environment, timeout=5)
    message = str(error.value)
    assert "docker_exit=7 docker_kind=missing_content" in message
    assert "gzip_exit=0" in message and "archive_disk_free_mib=" in message
    assert "private-registry-secret" not in message
    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp"))
    assert all(p.returncode is not None for p in processes)


@pytest.mark.parametrize(
    "raw, expected",
    [
        (b"no space left on device", "no_space"),
        (b"Permission denied private-path", "permission_denied"),
        (b"Cannot connect to private daemon", "daemon_unavailable"),
        (b"unexpected sensitive message", "unclassified"),
    ],
)
def test_diagnostic_categories_never_return_raw_stderr(exporter, raw, expected):
    module, *_ = exporter
    assert module.export_failure_kind(raw) == expected


@pytest.mark.parametrize(
    "raw, category",
    [
        ("unauthorized: private-registry/private-token", "registry_auth"),
        ("manifest unknown: private-image", "missing_content"),
        ("toomanyrequests: private-account", "registry_rate_limit"),
        ("x509: private-host", "registry_tls"),
        ("dial tcp private-ip: i/o timeout", "network_unavailable"),
        ("Cannot connect to private daemon", "daemon_unavailable"),
        ("unknown private-secret", "unclassified"),
    ],
)
def test_failed_command_reports_category_without_credentials(exporter, monkeypatch, raw, category):
    module, *_ = exporter
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            ["docker", "pull", "private-image"], 7, "private-progress", raw
        ),
    )
    with pytest.raises(RuntimeError) as error:
        module.run("image pull", ["docker", "pull", "private-image"])
    assert str(error.value) == (f"image pull failed: exit=7 kind={category}; raw output suppressed")
