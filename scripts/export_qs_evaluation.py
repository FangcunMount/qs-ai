"""Export executable evaluation policies and original resources from a clean QS checkout."""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

PROGRAM = """package main
import (
 "encoding/json"
 "os"
 evaluation "QS_EVALUATION"
)
func main() {
 execution:=evaluation.CurrentEvaluationExecutionPolicy()
 gate:=evaluation.CurrentReleaseGatePolicy()
 ef,err:=execution.Fingerprint();if err!=nil{panic(err)}
 gf,err:=gate.Fingerprint();if err!=nil{panic(err)}
 er,err:=json.Marshal(execution);if err!=nil{panic(err)}
 gr,err:=json.Marshal(gate);if err!=nil{panic(err)}
 json.NewEncoder(os.Stdout).Encode(map[string]any{
  "execution_policy":map[string]any{"definition_json":string(er),"fingerprint":ef},
  "gate_policy":map[string]any{"definition_json":string(gr),"fingerprint":gf},
 })
}
"""
PROGRAM = PROGRAM.replace(
    "QS_EVALUATION",
    "github.com/FangcunMount/qs-server/internal/apiserver/"
    "domain/interpretation/aiexplanation/evaluation",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("checkout", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    checkout = args.checkout.resolve()
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout):
        raise SystemExit("Source checkout must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
    with tempfile.TemporaryDirectory(prefix="qs_ai_evaluation_", dir=checkout / "scripts") as name:
        source = Path(name) / "main.go"
        source.write_text(PROGRAM)
        policies = json.loads(subprocess.check_output(["go", "run", str(source)], cwd=checkout))
    output = Path(__file__).resolve().parents[1] / "integrations/qs_server/evaluation"
    resources = {
        "policies.json": (json.dumps(policies, ensure_ascii=False, indent=2) + "\n").encode()
    }
    names = [f"ai-explanation-prompt-evaluation-cases-v{i}.json" for i in range(1, 7)]
    names += [f"ai-explanation-semantic-evaluator-prompt-v{i}.md" for i in (1, 2)]
    names += [
        "ai-explanation-semantic-evaluation-output-v1.schema.json",
        "ai-explanation-evaluation-execution-policy-v1.schema.json",
        "ai-explanation-release-gate-policy-v1.schema.json",
    ]
    for filename in names:
        resources[filename] = subprocess.check_output(
            ["git", "show", f"{commit}:api/schema/interpretation/{filename}"], cwd=checkout
        )
    manifest = {
        "repository": "https://github.com/FangcunMount/qs-server",
        "commit": commit,
        "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in resources.items()},
        "status": "Source resources and executable policies only; no approval or activation",
    }
    resources["manifest.json"] = (json.dumps(manifest, indent=2) + "\n").encode()
    for name, raw in resources.items():
        if args.check:
            if (output / name).read_bytes() != raw:
                raise SystemExit(f"Evaluation resource drift: {name}")
        else:
            output.mkdir(parents=True, exist_ok=True)
            (output / name).write_bytes(raw)


if __name__ == "__main__":
    main()
