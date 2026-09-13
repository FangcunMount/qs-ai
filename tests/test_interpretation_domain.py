import pytest

from qs_ai.domain.interpretation.model import Actor, RuleViolation, Session, Status


def test_question_identity_and_terminal_state():
    session = Session("s", Actor("1", "u"), "2", ("3",), "goal")
    session.queue("r1")
    session.running()
    session.await_answer("q1")
    with pytest.raises(RuleViolation, match="question_conflict"):
        session.queue("r2", question_id="old-question")
    assert session.status == Status.AWAITING_ANSWER
    session.queue("r2", question_id="q1")
    session.cancel()
    with pytest.raises(RuleViolation, match="invalid_state"):
        session.running()


@pytest.mark.parametrize(
    "case", ["missing_report", "wrong_subject", "missing_item", "duplicate_ref"]
)
def test_invalid_evidence_cannot_be_frozen(case):
    from qs_ai.domain.interpretation.model import EvidenceItem, EvidenceSet, Fact

    facts = (Fact("ref", "fact"),)
    if case == "duplicate_ref":
        facts = facts + facts
    item = EvidenceItem(
        "42",
        "8" if case == "wrong_subject" else "7",
        "" if case == "missing_report" else "report-1",
        "v1",
        facts,
    )
    evidence = EvidenceSet("e", "s", "fingerprint", () if case == "missing_item" else (item,))
    with pytest.raises(RuleViolation):
        evidence.validate("7", ("42",))


@pytest.mark.parametrize("status", [s for s in Status if s != Status.BLOCKED])
def test_manual_retry_cannot_restart_active_cancelled_or_completed_session(status):
    session = Session(
        "s",
        Actor("1", "u"),
        "2",
        ("3",),
        "goal",
        status=status,
        active_run_id="old",
        workflow_version="qs-snapshot-v1",
    )
    with pytest.raises(RuleViolation, match="participant_retry_conflict"):
        session.retry("old", "new")
    assert session.status == status and session.active_run_id == "old" and session.version == 1


def test_manual_retry_preserves_session_evidence_and_rejects_the_wrong_attempt():
    session = Session(
        "s",
        Actor("1", "u"),
        "2",
        ("3",),
        "goal",
        status=Status.BLOCKED,
        active_run_id="old",
        evidence_set_id="frozen",
        workflow_version="qs-published-snapshot-v1",
        failure_code="provider_timeout",
    )
    with pytest.raises(RuleViolation, match="participant_retry_conflict"):
        session.retry("stale", "new")
    session.retry("old", "new")
    assert (
        session.evidence_set_id == "frozen"
        and session.workflow_version == "qs-published-snapshot-v1"
    )
    assert session.failure_code is None and session.status == Status.QUEUED and session.version == 2
