"""Regression checks for documentation failure detection, independent of AI services."""

import hashlib
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("docs_checker", ROOT / "scripts/check_docs.py")
assert SPEC and SPEC.loader
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def test_active_links_and_duplicate_heading_anchors(tmp_path):
    page = tmp_path / "README.md"
    page.write_text("# Topic\n# Topic\n[ok](#topic-1)\n[bad](#missing)\n")
    errors, count = CHECK.check_links(tmp_path, [page])
    assert count == 2
    assert len(errors) == 1 and "missing anchor" in errors[0]


def test_links_cannot_escape_repository_or_follow_escaping_symlink(tmp_path):
    page = tmp_path / "README.md"
    (tmp_path / "outside").symlink_to(tmp_path.parent)
    page.write_text("[escape](../secret)\n[symlink](outside/secret)\n")
    errors, _ = CHECK.check_links(tmp_path, [page])
    assert len(errors) == 2
    assert all("outside repository" in error for error in errors)


def fixture(tmp_path):
    page = tmp_path / "README.md"
    page.write_text("# Doc\n")
    source = tmp_path / "source.py"
    source.write_text("VALUE = 1\n")
    entry = {
        "path": "README.md",
        "owner": "overview",
        "status": "aligned",
        "implementation_status": "partial",
        "runtime_status": "unknown",
        "verify": ["python scripts/check_docs.py"],
        "sources": ["source.py"],
    }
    return (
        page,
        source,
        {
            "source_commit": "a" * 40,
            "documents": [entry],
            "source_catalog": {"source.py": hashlib.sha256(source.read_bytes()).hexdigest()},
        },
    )


def test_partial_implementation_can_have_aligned_document(tmp_path):
    page, _, manifest = fixture(tmp_path)
    assert CHECK.check_closure(tmp_path, [page], manifest) == []


def test_unreviewed_source_change_is_detected(tmp_path):
    page, source, manifest = fixture(tmp_path)
    source.write_text("VALUE = 2\n")
    assert any(
        "source-baseline-drift" in e for e in CHECK.check_closure(tmp_path, [page], manifest)
    )


def test_new_document_requires_owner_and_closure_coverage(tmp_path):
    page, _, manifest = fixture(tmp_path)
    new = tmp_path / "new.md"
    new.write_text("# New\n")
    manifest["documents"][0]["owner"] = ""
    errors = CHECK.check_closure(tmp_path, [page, new], manifest)
    assert any("missing owner" in error for error in errors)
    assert any("document-uncovered: new.md" in error for error in errors)


@pytest.mark.parametrize("path", ["../outside", "/tmp/absolute", "_archive/old.py"])
def test_review_sources_must_be_current_repository_files(tmp_path, path):
    page, _, manifest = fixture(tmp_path)
    manifest["documents"][0]["sources"][0] = path
    assert any("invalid source" in e for e in CHECK.check_closure(tmp_path, [page], manifest))


def test_test_evidence_cannot_be_promoted_to_runtime_acceptance(tmp_path):
    page, _, manifest = fixture(tmp_path)
    manifest["documents"][0]["runtime_status"] = "accepted"
    assert any("independently bound" in e for e in CHECK.check_closure(tmp_path, [page], manifest))


def test_archive_byte_change_and_missing_successor_are_detected(tmp_path):
    archive = tmp_path / "_archive/old.md"
    archive.parent.mkdir()
    archive.write_text("Original dated evidence\n")
    snapshot = {
        "files": [
            {
                "original": "old.md",
                "archive": "_archive/old.md",
                "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            }
        ]
    }
    successor = tmp_path / "new.md"
    successor.write_text("Current explanation\n")
    migration = {
        "files": [{"original": "old.md", "archive": "_archive/old.md", "successors": ["new.md"]}]
    }
    assert CHECK.check_snapshot(tmp_path, snapshot, migration) == []
    archive.write_text("History silently refreshed\n")
    successor.unlink()
    errors = CHECK.check_snapshot(tmp_path, snapshot, migration)
    assert any("original-bytes-changed" in error for error in errors)
    assert any("invalid successor" in error for error in errors)


def test_repository_runtime_facts_match_current_design():
    assert CHECK.check_facts(ROOT) == []


def test_removing_mq_requirement_is_detected(tmp_path):
    names = [
        "README.md",
        "pyproject.toml",
        "src/qs_ai/bootstrap/server.py",
        "src/qs_ai/transport/grpc/mq_cutover.py",
        "src/qs_ai/application/execution/generation.py",
        "src/qs_ai/infrastructure/workflows/report.py",
        "src/qs_ai/bootstrap/api.py",
    ]
    for name in names:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((ROOT / name).read_bytes())
    server = tmp_path / "src/qs_ai/bootstrap/server.py"
    server.write_text(server.read_text().replace("if not settings.messaging.enabled:", "if False:"))
    assert any("no longer requires MQ" in error for error in CHECK.check_facts(tmp_path))
