"""Generate additive MQ types, preserving the original workflow descriptor name.

The shared source uses the Go import root. Python's existing descriptor is named
workflow.proto; normalize only that import in a temporary compiler input. Neither
the canonical source nor the original workflow generated files are rewritten.
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
arguments = parser.parse_args()
source = root / "integrations/workflow/proto"
destination = root / "src/qs_ai/contracts/workflow"
with tempfile.TemporaryDirectory() as directory:
    target = Path(directory)
    temporary_source = target / "messaging.proto"
    temporary_source.write_text(
        (source / "messaging.proto")
        .read_text()
        .replace('import "aiworkflow/workflow.proto";', 'import "workflow.proto";')
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{target}",
            f"-I{source}",
            f"--python_out={target}",
            f"--pyi_out={target}",
            f"--grpc_python_out={target}",
            "messaging.proto",
        ],
        check=True,
    )
    for path in sorted(target.glob("messaging_pb2*")):
        content = path.read_text()
        for module in ("workflow_pb2", "messaging_pb2"):
            content = content.replace(
                f"import {module} as", f"from qs_ai.contracts.workflow import {module} as"
            ).replace(f"'{module}'", f"'qs_ai.contracts.workflow.{module}'")
        output = destination / path.name
        if arguments.check:
            if not output.exists() or output.read_text() != content:
                raise SystemExit(f"Generated MQ contract drift: {path.name}")
        else:
            output.write_text(content)
print("Messaging protocol verified" if arguments.check else "Messaging protocol generated")
