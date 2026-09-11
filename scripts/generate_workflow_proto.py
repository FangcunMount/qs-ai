"""Generate the qs-ai-owned bidirectional protocol. CI checks deterministic output."""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
arguments = parser.parse_args()
destination = root / "src/qs_ai/contracts/workflow"
with tempfile.TemporaryDirectory() as directory:
    target = Path(directory)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{root / 'integrations/workflow/proto'}",
            f"--python_out={target}",
            f"--pyi_out={target}",
            f"--grpc_python_out={target}",
            "workflow.proto",
        ],
        check=True,
    )
    for path in target.iterdir():
        content = path.read_text().replace(
            "import workflow_pb2 as", "from qs_ai.contracts.workflow import workflow_pb2 as"
        )
        content = content.replace("'workflow_pb2'", "'qs_ai.contracts.workflow.workflow_pb2'")
        output = destination / path.name
        if arguments.check:
            if not output.exists() or output.read_text() != content:
                raise SystemExit(f"Generated contract drift: {path.name}")
        else:
            output.write_text(content)
print("Workflow protocol verified" if arguments.check else "Workflow protocol generated")
