from dataclasses import asdict, replace
from pathlib import Path

import pytest

from qs_ai.application.interpretation.input import InputPolicy
from qs_ai.application.interpretation.preparation import prepare_report_input
from qs_ai.application.interpretation.service import fingerprint
from qs_ai.domain.interpretation.model import (
    Actor,
    EvidenceItem,
    EvidenceSet,
    Fact,
    RuleViolation,
    Session,
)


def bound_case() -> tuple[Session, EvidenceSet, InputPolicy]:
    raw = (Path(__file__).parent / "fixtures" / "report_snapshot.json").read_text()
    item = EvidenceItem("42", "7", "99", "standard-v1:101", (Fact("standard_report", raw),))
    evidence = EvidenceSet("evidence", "session", fingerprint([asdict(item)]), (item,))
    session = Session(
        "session",
        Actor("1", "parent"),
        "7",
        ("42",),
        "解读",
        evidence_set_id="evidence",
        workflow_version="qs-snapshot-v1",
    )
    policy = InputPolicy(
        "case", "v6", "sha256:" + "a" * 64, None, None, 2, 12, (), (), (), True, False
    )
    return session, evidence, policy


def test_frozen_report_bound_to_session_and_source() -> None:
    session, evidence, policy = bound_case()
    assert prepare_report_input(session, evidence, policy).provider_payload


@pytest.mark.parametrize(
    "change",
    [
        {"session_id": "other"},
        {"id": "other"},
        {"fingerprint": "wrong"},
    ],
)
def test_rejects_other_or_corrupted_evidence(change: dict) -> None:
    session, evidence, policy = bound_case()
    with pytest.raises(RuleViolation):
        prepare_report_input(session, replace(evidence, **change), policy)


@pytest.mark.parametrize(
    "change",
    [
        {"assessment_id": "43"},
        {"testee_id": "8"},
        {"report_id": "100"},
        {"source_version": "standard-v1:102"},
        {"source_version": "other:101"},
    ],
)
def test_rejects_rebound_snapshot_even_with_recomputed_hash(change: dict) -> None:
    session, evidence, policy = bound_case()
    item = replace(evidence.items[0], **change)
    evidence = replace(evidence, items=(item,), fingerprint=fingerprint([asdict(item)]))
    with pytest.raises(RuleViolation):
        prepare_report_input(session, evidence, policy)


def test_tampered_fact_detected_before_input_assembly() -> None:
    session, evidence, policy = bound_case()
    item = replace(evidence.items[0], facts=(Fact("standard_report", "invalid JSON"),))
    with pytest.raises(RuleViolation, match="evidence_binding_mismatch"):
        prepare_report_input(session, replace(evidence, items=(item,)), policy)


def test_single_report_workflow_rejects_multiple_reports() -> None:
    session, evidence, policy = bound_case()
    second = replace(evidence.items[0], assessment_id="43")
    items = (*evidence.items, second)
    evidence = replace(evidence, items=items, fingerprint=fingerprint([asdict(i) for i in items]))
    session = replace(session, assessment_ids=("42", "43"))
    with pytest.raises(RuleViolation, match="single_report_required"):
        prepare_report_input(session, evidence, policy)


def test_unknown_fact_and_workflow_fail_closed() -> None:
    session, evidence, policy = bound_case()
    with pytest.raises(RuleViolation):
        prepare_report_input(
            replace(session, workflow_version="interpretation-v1"), evidence, policy
        )
    item = replace(evidence.items[0], facts=(Fact("unknown", "{}"),))
    evidence = replace(evidence, items=(item,), fingerprint=fingerprint([asdict(item)]))
    with pytest.raises(RuleViolation, match="report_fact_missing"):
        prepare_report_input(session, evidence, policy)
