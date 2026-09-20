"""Frozen judge bodies are independent of the current asset catalog and files."""

from dataclasses import fields
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity, FrozenContractRef
from qs_ai.infrastructure.persistence.mysql.evaluation_contracts import semantic_contract
from qs_ai.infrastructure.qs_server.semantic_assets import load_semantic_assets


async def test_frozen_judge_uses_exact_bytes_without_database_or_file_access(monkeypatch):
    asset = load_semantic_assets()
    references = {
        f.name: FrozenContractRef(f.name, "v1", "sha256:" + "a" * 64)
        for f in fields(EvidenceReleaseIdentity)
    }
    references.update(semantic_prompt=asset.prompt, semantic_output_schema=asset.output_schema)
    release = EvidenceReleaseIdentity(**references)
    frozen = {
        "semantic_prompt_markdown": asset.prompt_markdown,
        "semantic_output_schema_json": asset.output_schema_json,
    }

    def unavailable(*args, **kwargs):
        raise AssertionError("Frozen asset reads must not consult current files")

    monkeypatch.setattr("pathlib.Path.read_bytes", unavailable)
    db = cast(AsyncSession, object())
    assert await semantic_contract(db, release, 1, frozen=frozen) == asset
    with pytest.raises(ValueError, match="fingerprint"):
        await semantic_contract(
            db,
            release,
            1,
            frozen={**frozen, "semantic_prompt_markdown": asset.prompt_markdown + "\n"},
        )
    with pytest.raises(ValueError, match="Incomplete"):
        await semantic_contract(
            db, release, 1, frozen={"semantic_prompt_markdown": asset.prompt_markdown}
        )

    with pytest.raises(ValueError, match="owner"):
        await semantic_contract(
            db, release, 1, frozen={**frozen, "semantic_owner_organization_id": 2}
        )
    assert (
        await semantic_contract(
            db, release, 1, frozen={**frozen, "semantic_owner_organization_id": 1}
        )
        == asset
    )
