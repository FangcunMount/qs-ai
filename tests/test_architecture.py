import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "src" / "qs_ai"


def imports(path: Path) -> set[str]:
    result = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"Use absolute imports for boundary checks: {path}"
            result.add(node.module or "")
    return result


def test_dependency_direction_and_no_cycles() -> None:
    graph = {}
    for path in ROOT.rglob("*.py"):
        module = "qs_ai." + ".".join(path.relative_to(ROOT).with_suffix("").parts)
        dependencies = imports(path)
        graph[module] = {name for name in dependencies if name.startswith("qs_ai.")}
        layer = path.relative_to(ROOT).parts[0]
        if layer in {"domain", "application"}:
            forbidden = (
                "fastapi",
                "dishka",
                "sqlalchemy",
                "langchain",
                "langgraph",
                "grpc",
                "pydantic",
                "qs_ai.config",
                "qs_ai.contracts",
                "qs_ai.bootstrap",
                "qs_ai.transport",
                "qs_ai.infrastructure",
            )
            assert not any(name.startswith(forbidden) for name in dependencies), path
        if layer == "domain":
            assert not any(name.startswith("qs_ai.application") for name in dependencies), path
        if layer == "transport":
            assert not any(
                name.startswith(("qs_ai.infrastructure", "qs_ai.bootstrap"))
                for name in dependencies
            ), path
        if layer == "infrastructure":
            assert not any(
                name.startswith(("qs_ai.transport", "qs_ai.bootstrap")) for name in dependencies
            ), path
        if layer in {"domain", "application", "infrastructure", "transport"}:
            tree = ast.parse(path.read_text())
            assert not any(
                isinstance(node, ast.Attribute)
                and node.attr in {"dishka_container", "make_async_container"}
                for node in ast.walk(tree)
            ), path

    def visit(module: str, ancestors: set[str]) -> None:
        assert module not in ancestors, f"Import cycle: {ancestors} -> {module}"
        for dependency in graph.get(module, set()):
            visit(dependency, ancestors | {module})

    for module in graph:
        visit(module, set())
