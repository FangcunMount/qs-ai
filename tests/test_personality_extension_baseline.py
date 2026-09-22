"""Protect the existing scale contract before adding a separate MBTI input contract.

Fixtures are de-identified projections of an authorized test account's records.
They contain no production subject/request IDs, credentials or raw answers.
No network, database or model invocation is used by these tests.
"""

import hashlib
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import NotApplicable, assemble_input
from qs_ai.application.interpretation.prompts import render_prompt
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile
from qs_ai.infrastructure.qs_server.prompts import load_prompt

FIXTURES = Path(__file__).parent / "fixtures" / "personality_baseline"


def read_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_frozen_scale_input_remains_byte_identical() -> None:
    fixture = read_fixture("scale_replay.json")
    release = decode_published_profile(read_fixture("scale_profile.json"))
    assembled = assemble_input(
        json.dumps(fixture["snapshot"], ensure_ascii=False),
        release.input_policy,
        locale=fixture["locale"],
        focus_areas=tuple(fixture["focus_areas"]),
    )
    assert assembled.canonical_json == fixture["expected_canonical_json"]
    assert assembled.provider_payload == fixture["expected_provider_payload"]
    assert assembled.fingerprint == fixture["expected_input_fingerprint"]


def test_frozen_scale_prompt_messages_remain_byte_identical() -> None:
    fixture = read_fixture("scale_replay.json")
    release = decode_published_profile(read_fixture("scale_profile.json"))
    policy = release.render_policy
    messages = render_prompt(
        load_prompt(policy.template_id, policy.version),
        policy,
        fixture["expected_provider_payload"],
    )
    raw = json.dumps(asdict(messages), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert hashlib.sha256(raw.encode()).hexdigest() == fixture["expected_prompt_messages_sha256"]


def test_real_mbti_identity_cannot_fall_back_to_scale_v1() -> None:
    snapshot = read_fixture("scale_replay.json")["snapshot"]
    mbti = read_fixture("mbti-report.anonymized.json")
    release = decode_published_profile(read_fixture("scale_profile.json"))
    # This is deliberately not a proposed MBTI input: it proves the old contract
    # cannot be opened by substituting a real typology identity into a scale body.
    snapshot["model"] = mbti["report"]["model"]
    snapshot["runtime"]["decision_kind"] = mbti["outcome"]["decision_kind"]
    with pytest.raises(NotApplicable, match="scale/score_range"):
        assemble_input(json.dumps(snapshot, ensure_ascii=False), release.input_policy)
