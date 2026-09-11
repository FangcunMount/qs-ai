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


def test_default_api_fails_closed():
    from fastapi.testclient import TestClient

    from qs_ai.config import Settings
    from qs_ai.main import create_app

    with TestClient(create_app(Settings(_env_file=None))) as client:
        body = {"testee_id": "1", "assessment_ids": ["2"], "goal": "goal"}
        headers = {"Idempotency-Key": "key"}
        assert (
            client.post("/v1/interpretation-sessions", json=body, headers=headers).status_code
            == 401
        )
        headers["Authorization"] = "Bearer untrusted-token"
        response = client.post("/v1/interpretation-sessions", json=body, headers=headers)
        assert response.status_code == 503
        assert "untrusted-token" not in response.text


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
