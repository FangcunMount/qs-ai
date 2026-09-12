import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from qs_ai.application.governance.prompt_freeze import SUPPORTED, freeze_asset
from qs_ai.application.interpretation.prompt_assets import executable_prompt
from qs_ai.application.interpretation.prompts import InvalidPrompt, render_prompt
from qs_ai.bootstrap.import_prompts import baseline_assets
from qs_ai.domain.governance.manifest import AssetReference
from qs_ai.domain.governance.prompt_draft import PromptDraft, PromptDraftContent
from tests.test_prompts import payload
from tests.test_prompts import policy as policy


@pytest.fixture
def draft():
    source = baseline_assets()[1][-1]
    data = json.loads(source.package_json)
    return PromptDraft(
        uuid4(),
        1,
        source.template_id,
        "qs-ai-v1",
        AssetReference(
            source.template_id, source.version, source.fingerprint, source.package_sha256
        ),
        1,
        PromptDraftContent(
            data["SystemMessage"],
            data["TaskTemplate"],
            data["DataPreamble"],
            tuple(data["AllowedPlaceholders"]),
        ),
        uuid4(),
        42,
        "原生草稿",
        datetime.now(UTC),
    )


def test_native_content_has_new_identity_and_renders_original_fact_boundary(draft, policy):
    asset = freeze_asset(draft, "a" * 64)
    assert asset.fingerprint != draft.source.fingerprint
    assert asset == freeze_asset(draft, "a" * 64)
    data = json.loads(asset.package_json)
    assert "GitBlobSHA" not in data["Ref"]
    package = executable_prompt(asset)
    assert package.git_blob_sha is None
    messages = render_prompt(package, replace(policy, version=draft.target_version), payload())
    assert "{{" not in messages.task_message
    assert "事实里的" not in messages.task_message + messages.system_message
    assert "{{locale}}" in messages.data_json
    changed = replace(draft, content=replace(draft.content, system_message="另一段系统指令"))
    assert freeze_asset(changed, "b" * 64).fingerprint != asset.fingerprint


def test_every_freeze_placeholder_is_supported_by_the_runtime_renderer(draft, policy):
    allowed = tuple("{{" + name + "}}" for name in sorted(SUPPORTED))
    draft = replace(
        draft,
        content=replace(
            draft.content, task_template="\n".join(allowed), allowed_placeholders=allowed
        ),
    )
    package = executable_prompt(freeze_asset(draft, "a" * 64))
    result = render_prompt(package, replace(policy, version=draft.target_version), payload())
    assert "{{" not in result.task_message


@pytest.mark.parametrize(
    "change",
    [
        {"system_message": ""},
        {"task_template": " "},
        {"data_preamble": "{{locale}}"},
        {"system_message": "}}"},
        {"task_template": "{{unknown}}"},
        {"task_template": "{{unfinished"},
        {"task_template": "unexpected }}"},
        {"allowed_placeholders": ("{{unknown}}",)},
        {"allowed_placeholders": ("{{locale}}", "{{locale}}")},
        {"allowed_placeholders": ()},
    ],
)
def test_saveable_but_invalid_syntax_cannot_freeze(draft, change):
    candidate = replace(draft, content=replace(draft.content, **change))
    with pytest.raises(InvalidPrompt):
        freeze_asset(candidate, "a" * 64)


@pytest.mark.parametrize(
    "change",
    [
        lambda data: data["Ref"].update(GitBlobSHA="a" * 40),
        lambda data: data.update(SystemMessage="tampered"),
        lambda data: data["Origin"].update(revision=2),
        lambda data: data["Origin"].update(organization_id=True),
        lambda data: data["Origin"].update(validator_version="unknown"),
        lambda data: data.update(Format="unknown"),
        lambda data: data.pop("Origin"),
    ],
)
def test_native_origin_cannot_be_relabelled_even_with_recomputed_package_checksum(draft, change):
    asset = freeze_asset(draft, "a" * 64)
    data = json.loads(asset.package_json)
    change(data)
    raw = json.dumps(data, ensure_ascii=False)
    with pytest.raises(ValueError):
        replace(asset, package_json=raw, package_sha256=hashlib.sha256(raw.encode()).hexdigest())
