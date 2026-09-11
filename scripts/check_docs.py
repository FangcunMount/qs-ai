"""Check local Markdown links without network access or additional dependencies."""

import re
from collections import Counter
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
LINK = re.compile(r"(?<!!)\[[^\]]+\]\(([^\s)]+)\)")


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


def main() -> int:
    files = [ROOT / "README.md"]
    for directory in ("docs", "integrations"):
        files.extend(p for p in (ROOT / directory).rglob("*.md") if "_archive" not in p.parts)
    errors: list[str] = []
    count = 0
    for source in files:
        for target in LINK.findall(prose(source)):
            url = urlsplit(target.strip("<>"))
            if url.scheme or url.netloc:
                continue
            count += 1
            resolved = (source.parent / unquote(url.path)).resolve() if url.path else source
            if not resolved.is_relative_to(ROOT):
                errors.append(f"{source.relative_to(ROOT)}: link outside repository: {target}")
            elif not resolved.exists():
                errors.append(f"{source.relative_to(ROOT)}: missing target: {target}")
            elif url.fragment and resolved.suffix == ".md":
                if unquote(url.fragment) not in anchors(resolved):
                    errors.append(f"{source.relative_to(ROOT)}: missing anchor: {target}")
    for error in errors:
        print(error)
    print(f"Checked {len(files)} Markdown files and {count} local links: {len(errors)} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
