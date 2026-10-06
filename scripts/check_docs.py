"""Check documentation links, ownership, source baselines and selected runtime facts.

This is a narrow review guard, not a natural-language or production acceptance judge.
Only standard-library modules are used so the gate also works before dependency setup.
"""

import ast
import hashlib
import json
import re
import tomllib
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"(?<!!)\[[^\]]+\]\((<[^>]+>|[^\s)]+)\)")
DOC_STATES = {"aligned", "needs_review", "drifted", "planned"}
IMPL_STATES = {"implemented", "partial", "planned", "not_applicable"}


def prose(path: Path) -> str:
    lines = []
    fence = None
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip()
        if fence:
            if stripped.startswith(fence):
                fence = None
            continue
        if stripped.startswith(("```", "~~~")):
            fence = stripped[:3]
            continue
        lines.append(line)
    return "\n".join(lines)


def anchors(path: Path) -> set[str]:
    result: set[str] = set()
    seen: Counter[str] = Counter()
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", prose(path), re.MULTILINE):
        slug = re.sub(r"[^\w\s-]", "", heading.lower()).replace(" ", "-")
        suffix = f"-{seen[slug]}" if seen[slug] else ""
        result.add(slug + suffix)
        seen[slug] += 1
    result.update(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)', prose(path)))
    return result


def markdown_files(root: Path) -> list[Path]:
    files = [root / "README.md"]
    for directory in ("docs", "integrations", "configs", "deploy", "tests", "scripts"):
        files.extend(p for p in (root / directory).rglob("*.md") if "_archive" not in p.parts)
    return sorted(files)


def local_path(root: Path, name: str) -> Path | None:
    path = (root / name).resolve()
    if Path(name).is_absolute() or not path.is_relative_to(root.resolve()):
        return None
    return path


def check_links(root: Path, files: list[Path]) -> tuple[list[str], int]:
    errors = []
    count = 0
    for source in files:
        for target in LINK.findall(prose(source)):
            url = urlsplit(target.strip("<>"))
            if url.scheme or url.netloc:
                continue
            count += 1
            resolved = (source.parent / unquote(url.path)).resolve() if url.path else source
            label = source.relative_to(root)
            if not resolved.is_relative_to(root.resolve()):
                errors.append(f"{label}: link outside repository: {target}")
            elif not resolved.exists():
                errors.append(f"{label}: missing target: {target}")
            elif url.fragment and resolved.suffix == ".md":
                if unquote(url.fragment) not in anchors(resolved):
                    errors.append(f"{label}: missing anchor: {target}")
    return errors, count


def check_closure(root: Path, files: list[Path], manifest: dict) -> list[str]:
    errors = []
    if not re.fullmatch(r"[0-9a-f]{40}", manifest.get("source_commit", "")):
        errors.append("closure: source_commit must be a full reviewed commit")
    expected = {p.relative_to(root).as_posix() for p in files}
    declared = set()
    for entry in manifest.get("documents", []):
        name = entry.get("path", "")
        if name in declared:
            errors.append(f"closure: duplicate document {name}")
        declared.add(name)
        if not entry.get("owner"):
            errors.append(f"closure: missing owner: {name}")
        if entry.get("status") not in DOC_STATES:
            errors.append(f"closure: invalid document state: {name}")
        if entry.get("implementation_status") not in IMPL_STATES:
            errors.append(f"closure: invalid implementation state: {name}")
        if entry.get("runtime_status") != "unknown":
            errors.append(f"closure: runtime evidence must be independently bound: {name}")
        if not entry.get("verify"):
            errors.append(f"closure: missing verification entry: {name}")
        sources = entry.get("sources", [])
        if not sources:
            errors.append(f"closure: missing reviewed source: {name}")
        for source_name in sources:
            path = local_path(root, source_name)
            expected_hash = manifest.get("source_catalog", {}).get(source_name)
            if path is None or not path.is_file() or "_archive" in path.parts:
                errors.append(f"closure: invalid source: {name}: {source_name}")
            elif hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
                errors.append(f"closure: source-baseline-drift: {name}: {source_name}")
    for name in sorted(expected - declared):
        errors.append(f"closure: document-uncovered: {name}")
    for name in sorted(declared - expected):
        errors.append(f"closure: stale-document: {name}")
    return errors


def check_snapshot(root: Path, snapshot: dict, migration: dict) -> list[str]:
    errors = []
    originals = set()
    archives = set()
    for entry in snapshot.get("files", []):
        name = entry.get("original", "")
        if name in originals:
            errors.append(f"snapshot: duplicate original: {name}")
        originals.add(name)
        archives.add(entry.get("archive", ""))
        path = local_path(root, entry.get("archive", ""))
        if path is None or not path.is_file() or "_archive" not in path.parts:
            errors.append(f"snapshot: missing or unsafe archive: {name}")
        elif hashlib.sha256(path.read_bytes()).hexdigest() != entry.get("sha256"):
            errors.append(f"snapshot: original-bytes-changed: {name}")
    mapped = set()
    for entry in migration.get("files", []):
        name = entry.get("original", "")
        if name in mapped:
            errors.append(f"migration: duplicate original: {name}")
        mapped.add(name)
        if entry.get("archive") not in archives:
            errors.append(f"migration: archive not in snapshot: {name}")
        if not entry.get("successors"):
            errors.append(f"migration: no active successor: {name}")
        for successor in entry.get("successors", []):
            path = local_path(root, successor)
            if path is None or not path.is_file() or "_archive" in path.parts:
                errors.append(f"migration: invalid successor: {name}: {successor}")
    if originals != mapped:
        errors.append("migration: snapshot coverage mismatch")
    return errors


def function(tree: ast.AST, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
    )


def check_facts(root: Path) -> list[str]:
    errors = []

    def parse(name: str) -> ast.Module:
        return ast.parse((root / name).read_text(encoding="utf-8"))

    def require(condition: bool, message: str) -> None:
        if not condition:
            errors.append(f"facts: {message}")

    try:
        readme = (root / "README.md").read_text(encoding="utf-8")
        dependencies = tomllib.loads((root / "pyproject.toml").read_text())["project"][
            "dependencies"
        ]
        require(
            all(
                any(item.startswith(name + ">=") for item in dependencies)
                for name in ("langchain", "langgraph")
            ),
            "model/graph dependencies changed; review architecture",
        )
        require(
            "LangChain 承担模型适配，LangGraph 编排生成与评测步骤" in readme,
            "root model/graph statement missing",
        )
        server = function(parse("src/qs_ai/bootstrap/server.py"), "serve")
        require(
            any(
                isinstance(node, ast.If)
                and ast.unparse(node.test) == "not settings.messaging.enabled"
                and any(isinstance(child, ast.Raise) for child in node.body)
                for node in ast.walk(server)
            ),
            "unified server no longer requires MQ",
        )
        require("执行命令使用 MQ" in readme, "root MQ admission statement missing")
        cutoff = parse("src/qs_ai/transport/grpc/mq_cutover.py")
        methods = next(
            node.value
            for node in cutoff.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "MQ_EXECUTION_METHODS" for t in node.targets
            )
        )
        values = ast.literal_eval(methods.args[0])
        require(
            values
            == {
                "/qsai.workflow.v1.Commands/Start",
                "/qsai.workflow.v1.Commands/Change",
                "/qsai.workflow.v1.ParticipantManagement/Retry",
                "/qsai.workflow.v1.EvaluationManagement/Start",
                "/qsai.workflow.v1.EvaluationManagement/Cancel",
            },
            "retired execution RPC set changed",
        )
        generation = function(parse("src/qs_ai/application/execution/generation.py"), "_execute")
        require(
            any(
                isinstance(node, ast.If)
                and ast.unparse(node.test) == "call.status in {'dispatched', 'unknown'}"
                and any(
                    isinstance(child, ast.Raise) and "provider_result_unknown" in ast.unparse(child)
                    for child in node.body
                )
                for node in ast.walk(generation)
            ),
            "unknown-call blocking branch changed",
        )
        graph = parse("src/qs_ai/infrastructure/workflows/report.py")
        nodes = {
            node.args[0].value
            for node in ast.walk(graph)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_node"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        }
        require(nodes == {"prepare", "generate", "validate"}, "report graph stages changed")
        require(
            not any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "compile"
                and any(k.arg == "checkpointer" for k in node.keywords)
                for node in ast.walk(graph)
            ),
            "report graph persistence ownership changed",
        )
        api = function(parse("src/qs_ai/bootstrap/api.py"), "create_app")
        routers = {
            ast.unparse(node.args[0])
            for node in ast.walk(api)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "include_router"
            and node.args
        }
        require(routers == {"router", "metrics_router"}, "HTTP router set changed")
    except (OSError, ValueError, KeyError, StopIteration, SyntaxError, AttributeError) as error:
        errors.append(f"facts: cannot inspect source: {type(error).__name__}")
    return errors


def main() -> int:
    files = markdown_files(ROOT)
    errors, count = check_links(ROOT, files)
    try:
        closure = json.loads((ROOT / "docs/document-closure.json").read_text())
        snapshot = json.loads(
            (ROOT / "docs/_archive/2026-10-doc-system/snapshot-manifest.json").read_text()
        )
        migration = json.loads((ROOT / "docs/migration-map.json").read_text())
        errors.extend(check_closure(ROOT, files, closure))
        errors.extend(check_snapshot(ROOT, snapshot, migration))
    except (OSError, ValueError) as error:
        errors.append(f"metadata: cannot load manifest: {type(error).__name__}")
    errors.extend(check_facts(ROOT))
    for error in errors:
        print(error)
    print(f"Checked {len(files)} Markdown files and {count} local links: {len(errors)} errors")
    print("Checked document coverage, source hashes, archived bytes and selected runtime facts")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
