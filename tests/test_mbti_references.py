"""Synthetic reference material tests; no source content or quality approval."""

import json
from itertools import product

import pytest

from qs_ai.domain.governance.mbti_references import (
    MBTI_TOPICS,
    InvalidMBTIReferences,
    decode_mbti_reference_material,
)
from qs_ai.domain.governance.scenes import MBTI_AXES, MBTI_MODEL, MBTI_VERSION


def material():
    return {
        "schema_version": "mbti-reference-material/v1",
        "version": "synthetic-v1",
        "model_code": MBTI_MODEL,
        "model_version": MBTI_VERSION,
        "sources": [
            {
                "source_id": "synthetic-source",
                "title": "合成来源，不是生产参考",
                "url": "https://example.org/synthetic-reference",
                "accessed_on": "2026-10-02",
                "support_scope": "只用于验证引用契约，不证明任何人格特征。",
            }
        ],
        "entries": [
            {
                "entry_id": f"{topic}.{axis.lower()}.{pole.lower()}",
                "topic": topic,
                "axis": axis,
                "pole": pole,
                "content": f"合成 {topic}/{axis}/{pole} 参考，不用于真实解读。",
                "source_ids": ["synthetic-source"],
                "usage_boundary": "结构验证不等于内容支持或人工批准。",
            }
            for topic in MBTI_TOPICS
            for axis, *poles in MBTI_AXES
            for pole in poles
        ],
    }


@pytest.mark.parametrize("poles", list(product(*(axis[1:] for axis in MBTI_AXES))))
def test_all_sixteen_types_select_only_matching_references(poles):
    selected = decode_mbti_reference_material(material()).select("".join(poles))
    assert len(selected.entries) == 12
    assert {item.topic for item in selected.entries} == set(MBTI_TOPICS)
    assert {item.source_id for item in selected.sources} == {"synthetic-source"}
    for entry in selected.entries:
        assert poles[[axis[0] for axis in MBTI_AXES].index(entry.axis)] == entry.pole
        selected.validate_reference("reference:" + entry.entry_id, entry.topic, ("model_result",))
        selected.validate_reference(
            "reference:" + entry.entry_id, entry.topic, ("dimension:" + entry.axis,)
        )


def test_frozen_selection_does_not_read_or_share_mutable_source():
    raw = material()
    selected = decode_mbti_reference_material(raw).select("ISFJ")
    frozen_json, fingerprint = selected.canonical_json, selected.fingerprint
    raw["entries"][0]["content"] = "新的未冻结内容"
    raw["sources"][0]["title"] = "新的来源标题"
    assert selected.canonical_json == frozen_json
    assert selected.fingerprint == fingerprint
    assert decode_mbti_reference_material(raw).select("ISFJ").fingerprint != fingerprint
    assert "model_result" not in json.loads(
        frozen_json
    )  # Reference data stays separate from facts.


def test_reordering_material_has_stable_selection_bytes():
    raw = material()
    expected = decode_mbti_reference_material(raw).select("ENFP")
    raw["entries"].reverse()
    assert decode_mbti_reference_material(raw).select("ENFP") == expected


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), "mbti-reference-material/v2"),
        (("version",), ""),
        (("version",), 1),
        (("model_code",), "MBTI_OTHER"),
        (("model_version",), "latest"),
        (("sources",), []),
        (("sources", 0, "source_id"), "Invalid ID"),
        (("sources", 0, "title"), " "),
        (("sources", 0, "url"), "http://example.org/reference"),
        (("sources", 0, "url"), "https://secret:password@example.org/reference"),
        (("sources", 0, "url"), "https://example.org/\nreference"),
        (("sources", 0, "accessed_on"), "2026-02-30"),
        (("sources", 0, "accessed_on"), "20261002"),
        (("sources", 0, "accessed_on"), False),
        (("sources", 0, "support_scope"), ""),
        (("entries",), []),
        (("entries", 0, "entry_id"), "bad id"),
        (("entries", 0, "topic"), "hiring"),
        (("entries", 0, "topic"), {}),
        (("entries", 0, "axis"), "cognitive-functions"),
        (("entries", 0, "axis"), []),
        (("entries", 0, "pole"), "N"),
        (("entries", 0, "pole"), 1),
        (("entries", 0, "content"), "<script>untrusted</script>"),
        (("entries", 0, "content"), "x" * 1001),
        (("entries", 0, "usage_boundary"), ""),
        (("entries", 0, "source_ids"), []),
        (("entries", 0, "source_ids"), ["missing-source"]),
        (("entries", 0, "source_ids"), ["synthetic-source", "synthetic-source"]),
    ],
)
def test_invalid_material_rejected_without_echoing_input(path, value):
    raw = material()
    node = raw
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(InvalidMBTIReferences) as error:
        decode_mbti_reference_material(raw)
    assert "secret" not in str(error.value)
    assert "password" not in str(error.value)
    assert "untrusted" not in str(error.value)


@pytest.mark.parametrize("location", [(), ("sources", 0), ("entries", 0)])
def test_unknown_fields_cannot_claim_approval(location):
    raw = material()
    node = raw
    for key in location:
        node = node[key]
    node["approved"] = True
    with pytest.raises(InvalidMBTIReferences, match="fields"):
        decode_mbti_reference_material(raw)


@pytest.mark.parametrize("target", ["sources", "entries"])
def test_duplicate_identity_rejected(target):
    raw = material()
    raw[target].append(raw[target][0].copy())
    with pytest.raises(InvalidMBTIReferences, match="Duplicate"):
        decode_mbti_reference_material(raw)


def test_missing_pole_topic_is_rejected_before_generation():
    raw = material()
    replacement = raw["entries"][1].copy()
    replacement["entry_id"] = "distinct-but-same-coverage"
    raw["entries"][0] = replacement
    with pytest.raises(InvalidMBTIReferences, match="coverage"):
        decode_mbti_reference_material(raw)


def test_unused_source_is_not_silently_carried_into_frozen_material():
    raw = material()
    raw["sources"].append({**raw["sources"][0], "source_id": "unused-source"})
    with pytest.raises(InvalidMBTIReferences, match="Unreferenced"):
        decode_mbti_reference_material(raw)


def test_reference_pack_cannot_exceed_existing_profile_storage_limit():
    raw = material()
    raw["entries"] += [
        {**entry, "entry_id": entry["entry_id"] + ".extra"} for entry in raw["entries"]
    ]
    for entry in raw["entries"]:
        entry["content"] = "文" * 1000
        entry["usage_boundary"] = "界" * 500
    with pytest.raises(InvalidMBTIReferences, match="size limit"):
        decode_mbti_reference_material(raw)


@pytest.mark.parametrize("type_code", ["ISF", "ISFJJ", "ISXX", "isfj", None, 16])
def test_invalid_type_has_no_fallback(type_code):
    with pytest.raises(InvalidMBTIReferences, match="type"):
        decode_mbti_reference_material(material()).select(type_code)


@pytest.mark.parametrize(
    "ref,topic,evidence",
    [
        ("reference:missing", "personality", ("model_result",)),
        ("reference:personality.ei.e", "personality", ("model_result",)),
        ("reference:personality.ei.i", "career", ("model_result",)),
        ("reference:personality.ei.i", "personality", ("overall_result",)),
        ("reference:personality.ei.i", "personality", ("dimension:JP",)),
    ],
)
def test_unknown_wrong_pole_wrong_topic_and_unrelated_evidence_rejected(ref, topic, evidence):
    with pytest.raises(InvalidMBTIReferences):
        decode_mbti_reference_material(material()).select("ISFJ").validate_reference(
            ref, topic, evidence
        )
