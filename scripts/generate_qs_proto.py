"""Generate from the vendored, checksum-verified qs-server contract."""

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
arguments = parser.parse_args()
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "integrations/qs_server/manifest.json").read_text())
source = root / "integrations/qs_server/proto"
destination = root / "src/qs_ai/infrastructure/qs_server/generated"
workspace = tempfile.TemporaryDirectory()
target = Path(workspace.name) if arguments.check else destination
for relative, expected in manifest["files"].items():
    actual = hashlib.sha256((source / relative).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"Contract checksum mismatch: {relative}")
subprocess.run(
    [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{source}",
        f"--python_out={target}",
        f"--pyi_out={target}",
        *[str(source / name) for name in sorted(manifest["files"])],
    ],
    check=True,
)
subprocess.run(
    [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{source}",
        f"--grpc_python_out={target}",
        str(source / "interpretation/interpretation.proto"),
    ],
    check=True,
)
for directory in (target / "evaluation", target / "interpretation"):
    (directory / "__init__.py").write_text("")
    for path in [*directory.glob("*.py"), *directory.glob("*.pyi")]:
        contents = path.read_text()
        for package in ("evaluation", "interpretation"):
            contents = contents.replace(
                f"from {package} import ",
                f"from qs_ai.infrastructure.qs_server.generated.{package} import ",
            )
        module = f"{directory.name}.{path.stem}"
        qualified = f"qs_ai.infrastructure.qs_server.generated.{module}"
        contents = contents.replace(repr(module), repr(qualified))
        path.write_text(contents)
print(f"Generated pinned qs-server contract {manifest['commit'][:12]}")

if arguments.check:
    generated = {
        path.relative_to(target): path.read_bytes() for path in target.rglob("*") if path.is_file()
    }
    for relative, content in generated.items():
        path = destination / relative
        if not path.exists() or path.read_bytes() != content:
            raise SystemExit(f"Generated contract drift: {relative}")
    print("Generated contract matches the locked toolchain")
workspace.cleanup()
