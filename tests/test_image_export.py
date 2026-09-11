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
