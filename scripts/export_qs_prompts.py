"""Export executable QS Prompt packages from a clean, explicit source checkout."""

import argparse
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

PROGRAM = """package main
import (
 "context"
 "encoding/json"
 "os"
 port "github.com/FangcunMount/qs-server/internal/apiserver/application/interpretation/aiexplanation/port"
 catalog "github.com/FangcunMount/qs-server/internal/apiserver/infra/aiexplanation/prompt"
)
func main(){
 packages:=[]port.PromptPackage{}
 for _,version:=range []string{"v1","v2","v3","v4","v5","v6"}{
  p,err:=catalog.NewCatalog().ResolvePromptPackage(context.Background(),catalog.ParticipantScaleTemplateID,version)
  if err!=nil{panic(err)};if err=p.Validate();err!=nil{panic(err)};packages=append(packages,p)
 }
 if err:=json.NewEncoder(os.Stdout).Encode(packages);err!=nil{panic(err)}
}
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("checkout", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    checkout = args.checkout.resolve()
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=checkout, text=True)
    if status:
        raise SystemExit("Source checkout must be clean")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
    with tempfile.TemporaryDirectory(
        prefix="qs_ai_prompt_export_", dir=checkout / "scripts"
    ) as name:
        directory = Path(name)
        (directory / "main.go").write_text(PROGRAM)
        packages = json.loads(
            subprocess.check_output(["go", "run", str(directory / "main.go")], cwd=checkout)
        )
    root = Path(__file__).resolve().parents[1] / "integrations/qs_server/prompts"
    root.mkdir(parents=True, exist_ok=True)
    files = {}
    for package in packages:
        name = package["Ref"]["Version"] + ".json"
        raw = (json.dumps(package, ensure_ascii=False, indent=2) + "\n").encode()
        if args.check:
            if not (root / name).exists() or (root / name).read_bytes() != raw:
                raise SystemExit(f"Executable Prompt drift: {name}")
        else:
            (root / name).write_bytes(raw)
        files[name] = hashlib.sha256(raw).hexdigest()
    manifest_text = (
        json.dumps(
            {
                "repository": "https://github.com/FangcunMount/qs-server",
                "commit": commit,
                "source": "internal/apiserver/infra/aiexplanation/prompt/catalog.go",
                "files": files,
                "status": "Exported executable packages; active Profile and input mapping not yet migrated",
            },
            indent=2,
        )
        + "\n"
    )
    if args.check:
        if (root / "manifest.json").read_text() != manifest_text:
            raise SystemExit("Prompt source manifest drift")
    else:
        (root / "manifest.json").write_text(manifest_text)
    print(f"Verified {len(files)} executable Prompt packages from {commit}")


if __name__ == "__main__":
    main()
