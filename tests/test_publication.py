"""Pure publication rules; baseline assets and synthetic approval, no release acceptance."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from qs_ai.application.interpretation.manifest import build_generation_manifest
from qs_ai.domain.evaluation.finalization import FinalReview
from qs_ai.domain.governance.publication import (
    PublicationAudit,
    PublicationConflict,
    PublicationEvidence,
    PublicationPointer,
    PublishedConfiguration,
    ReleaseSelector,
    change_publication,
    resolve_publication,
)
from tests.test_generation_manifest import assets as assets
from tests.test_generation_manifest import complete_release as complete_release
from tests.test_generation_manifest import evaluation_release as evaluation_release

AT = datetime(2026, 9, 13, tzinfo=UTC)


@pytest.fixture
async def evidence(assets, complete_release):
    stores, selection = assets
    manifest = await build_generation_manifest(*stores, **selection)
    return PublicationEvidence(
        stores[0].get.return_value,
        manifest,
        complete_release,
        uuid4(),
        73,
        FinalReview("user:42", "完整审核通过", AT, True),
        manifest.fingerprint(),
    )


def published(evidence, at=AT):
    return PublishedConfiguration(
        uuid4(), evidence, PublicationAudit("user:42", "发布已批准配置", at)
    )


def move(pointer, target, action="publish", *, at=None, **changes):
    audit = (
        target.audit if action == "publish" else PublicationAudit("user:43", "核对后变更", at or AT)
    )
    args = dict(
        expected_version=pointer.version,
        expected_active_id=pointer.active.publication_id if pointer.active else None,
    )
    args.update(changes)
    return change_publication(pointer, target, action, audit, **args)


def with_selector(evidence, selector):
    definition = json.loads(evidence.profile.definition_json)
    definition["selector"].update(selector)
    raw = json.dumps(definition, ensure_ascii=False, separators=(",", ":"))
    sha = hashlib.sha256(raw.encode()).hexdigest()
    profile = replace(evidence.profile, definition_json=raw, fingerprint="sha256:" + sha)
    manifest = replace(
        evidence.manifest,
        profile=replace(
            evidence.manifest.profile, fingerprint=profile.fingerprint, content_sha256=sha
        ),
    )
    release = replace(
        evidence.release, profile=replace(evidence.release.profile, fingerprint=profile.fingerprint)
    )
    return replace(
        evidence,
        profile=profile,
        manifest=manifest,
        release=release,
        evaluated_manifest_fingerprint=manifest.fingerprint(),
    )


def test_publish_replace_disable_and_rollback_preserve_old_execution_bindings(evidence):
    empty = PublicationPointer(evidence.selector)
    first = published(evidence)
    pointer = move(empty, first).current
    assert empty.version == 0 and empty.active is None
    assert pointer.version == 1 and pointer.active == first
    original_manifest = first.evidence.manifest.canonical_json()
    # A subsequent evaluation approved a distinct immutable route revision.
    route = replace(
        evidence.manifest.generation_route,
        version="v9",
        fingerprint="sha256:" + "b" * 64,
        content_sha256="b" * 64,
    )
    newer = replace(
        evidence,
        manifest=replace(evidence.manifest, generation_route=route),
        release=replace(
            evidence.release,
            generation_route=replace(
                evidence.release.generation_route,
                version=route.version,
                fingerprint=route.fingerprint,
            ),
        ),
        run_id=uuid4(),
        evaluated_manifest_fingerprint=replace(
            evidence.manifest, generation_route=route
        ).fingerprint(),
    )
    second = published(newer, AT + timedelta(seconds=1))
    pointer = move(pointer, second).current
    assert (
        pointer.version == 2 and pointer.active.evidence.manifest.generation_route.version == "v9"
    )
    disabled = move(pointer, None, "disable", at=AT + timedelta(seconds=2)).current
    assert disabled.version == 3 and disabled.active is None
    change = move(disabled, first, "rollback", at=AT + timedelta(seconds=3))
    assert change.current.version == 4 and change.current.active == first
    assert change.current.active.audit.at == AT  # Rollback has its own audit.
    assert change.audit.actor == "user:43" and change.audit.at > first.audit.at
    assert first.evidence.manifest.canonical_json() == original_manifest
    assert first.evidence.manifest.generation_route.version == "v8"


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_version": 0},
        {"expected_version": True},
        {"expected_active_id": None},
        {"expected_active_id": UUID(int=1)},
    ],
)
def test_replacement_requires_exact_pointer_and_previous_publication(evidence, changes):
    first = published(evidence)
    pointer = move(PublicationPointer(evidence.selector), first).current
    with pytest.raises(PublicationConflict):
        move(pointer, published(evidence), **changes)
    assert pointer.active == first and pointer.version == 1


def test_disable_and_rollback_cannot_manufacture_initial_publication(evidence):
    initial = PublicationPointer(evidence.selector)
    with pytest.raises(PublicationConflict):
        move(initial, None, "disable")
    with pytest.raises(ValueError):
        move(initial, published(evidence), "rollback")
    first = published(evidence)
    pointer = move(initial, first).current
    with pytest.raises(PublicationConflict):
        move(pointer, first, "rollback")
    other = published(with_selector(evidence, {"model_code": "OTHER"}))
    with pytest.raises(PublicationConflict):
        move(pointer, other)


@pytest.mark.parametrize(
    "field", ["profile", "prompt", "generation_route", "input_schema", "output_schema"]
)
def test_publication_rejects_asset_reference_not_bound_by_evaluation(evidence, field):
    release = replace(
        evidence.release,
        **{field: replace(getattr(evidence.release, field), fingerprint="sha256:" + "0" * 64)},
    )
    with pytest.raises(ValueError, match="evaluated release"):
        replace(evidence, release=release)


def test_rejected_or_incomplete_identity_never_becomes_publication(evidence):
    for changes in (
        {"run_id": UUID(int=0)},
        {"run_version": 0},
        {"run_version": True},
        {"final_review": replace(evidence.final_review, passed=False)},
    ):
        with pytest.raises(ValueError):
            replace(evidence, **changes)
    for audit in (PublicationAudit("user:42", "发布", AT - timedelta(seconds=1)),):
        with pytest.raises(ValueError):
            PublishedConfiguration(uuid4(), evidence, audit)


def test_profile_policy_and_manifest_content_checksum_must_both_match(evidence):
    with pytest.raises(ValueError, match="evaluated manifest"):
        replace(
            evidence,
            manifest=replace(
                evidence.manifest,
                profile=replace(evidence.manifest.profile, content_sha256="0" * 64),
            ),
        )
    data = json.loads(evidence.profile.definition_json)
    data["generation_policy"]["prompt_version"] = "v999"
    raw = json.dumps(data, ensure_ascii=False)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    profile = replace(evidence.profile, definition_json=raw, fingerprint="sha256:" + digest)
    manifest = replace(
        evidence.manifest,
        profile=replace(
            evidence.manifest.profile, fingerprint=profile.fingerprint, content_sha256=digest
        ),
    )
    release = replace(
        evidence.release, profile=replace(evidence.release.profile, fingerprint=profile.fingerprint)
    )
    with pytest.raises(ValueError, match="policy differs"):
        replace(
            evidence,
            profile=profile,
            manifest=manifest,
            release=release,
            evaluated_manifest_fingerprint=manifest.fingerprint(),
        )


def test_current_qs_selector_precedence_and_disabled_fallback_are_preserved(evidence):
    selectors = ({}, {"model_code": "SCL"}, {"model_code": "SCL", "model_version": "v2"})
    pointers = []
    for fields in selectors:
        target = published(with_selector(evidence, fields))
        pointers.append(move(PublicationPointer(target.evidence.selector), target).current)
    query = ReleaseSelector("participant", "scale", "score_range", "SCL", "v2")
    assert resolve_publication(tuple(pointers), query) == pointers[2].active
    pointers[2] = move(pointers[2], None, "disable").current
    assert resolve_publication(tuple(pointers), query) == pointers[1].active
    assert (
        resolve_publication(tuple(pointers), replace(query, model_code="OTHER"))
        == pointers[0].active
    )
    assert resolve_publication((), query) is None
    with pytest.raises(PublicationConflict, match="Ambiguous"):
        resolve_publication((pointers[1], pointers[1]), query)
    assert len({p.selector.key() for p in pointers}) == 3


@pytest.mark.parametrize(
    "fields",
    [
        {"audience": "operator"},
        {"model_kind": "typology"},
        {"decision_kind": "ranking"},
        {"model_code": " "},
        {"model_version": "v1"},
        {"model_code": "SCL", "model_version": "bad version"},
    ],
)
def test_unsupported_or_incomplete_selector_rejected(fields):
    args = dict(audience="participant", model_kind="scale", decision_kind="score_range")
    args.update(fields)
    with pytest.raises(ValueError):
        ReleaseSelector(**args)


@pytest.mark.parametrize(
    "fields",
    [
        {"actor": "user:0"},
        {"actor": "user:01"},
        {"actor": "user:9223372036854775808"},
        {"reason": " "},
        {"reason": "<bad>"},
        {"reason": "字" * 334},
        {"at": AT.replace(tzinfo=None)},
    ],
)
def test_publication_requires_bounded_trusted_audit(fields):
    args = dict(actor="user:42", reason="发布", at=AT)
    args.update(fields)
    with pytest.raises(ValueError):
        PublicationAudit(**args)


def test_pointer_time_and_new_publication_audit_cannot_drift(evidence):
    first = published(evidence, AT + timedelta(seconds=1))
    pointer = move(PublicationPointer(evidence.selector), first).current
    with pytest.raises(ValueError):
        move(pointer, None, "disable", at=AT)
    second = published(evidence, AT + timedelta(seconds=2))
    with pytest.raises(ValueError, match="same audit"):
        change_publication(
            pointer,
            second,
            "publish",
            PublicationAudit("user:43", "伪造发布人", second.audit.at),
            expected_version=1,
            expected_active_id=first.publication_id,
        )


def test_unchanged_source_prompt_fingerprint_does_not_hide_changed_package_bytes(evidence):
    changed = replace(
        evidence.manifest, prompt=replace(evidence.manifest.prompt, content_sha256="a" * 64)
    )
    assert changed.prompt.fingerprint == evidence.manifest.prompt.fingerprint
    with pytest.raises(ValueError, match="evaluated manifest"):
        replace(evidence, manifest=changed)
