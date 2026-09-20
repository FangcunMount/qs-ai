from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Actor(_message.Message):
    __slots__ = ("org_id", "subject_id")
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    org_id: str
    subject_id: str
    def __init__(self, org_id: _Optional[str] = ..., subject_id: _Optional[str] = ...) -> None: ...

class StartCommand(_message.Message):
    __slots__ = ("request_id", "actor", "testee_id", "assessment_ids", "goal", "evidence")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    GOAL_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    actor: Actor
    testee_id: str
    assessment_ids: _containers.RepeatedScalarFieldContainer[str]
    goal: str
    evidence: _containers.RepeatedCompositeFieldContainer[EvidenceItem]
    def __init__(self, request_id: _Optional[str] = ..., actor: _Optional[_Union[Actor, _Mapping]] = ..., testee_id: _Optional[str] = ..., assessment_ids: _Optional[_Iterable[str]] = ..., goal: _Optional[str] = ..., evidence: _Optional[_Iterable[_Union[EvidenceItem, _Mapping]]] = ...) -> None: ...

class ChangeCommand(_message.Message):
    __slots__ = ("command_id", "session_id", "actor", "action", "expected_version", "question_id", "answer", "skip")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    QUESTION_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    SKIP_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    session_id: str
    actor: Actor
    action: str
    expected_version: int
    question_id: str
    answer: str
    skip: bool
    def __init__(self, command_id: _Optional[str] = ..., session_id: _Optional[str] = ..., actor: _Optional[_Union[Actor, _Mapping]] = ..., action: _Optional[str] = ..., expected_version: _Optional[int] = ..., question_id: _Optional[str] = ..., answer: _Optional[str] = ..., skip: _Optional[bool] = ...) -> None: ...

class Receipt(_message.Message):
    __slots__ = ("session_id", "run_id", "status", "version")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    run_id: str
    status: str
    version: int
    def __init__(self, session_id: _Optional[str] = ..., run_id: _Optional[str] = ..., status: _Optional[str] = ..., version: _Optional[int] = ...) -> None: ...

class StateEvent(_message.Message):
    __slots__ = ("event_id", "request_id", "session_id", "actor", "testee_id", "version", "status", "question_id", "question", "can_skip", "failure_code", "artifact_json")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    QUESTION_ID_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    CAN_SKIP_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_JSON_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    request_id: str
    session_id: str
    actor: Actor
    testee_id: str
    version: int
    status: str
    question_id: str
    question: str
    can_skip: bool
    failure_code: str
    artifact_json: str
    def __init__(self, event_id: _Optional[str] = ..., request_id: _Optional[str] = ..., session_id: _Optional[str] = ..., actor: _Optional[_Union[Actor, _Mapping]] = ..., testee_id: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., question_id: _Optional[str] = ..., question: _Optional[str] = ..., can_skip: _Optional[bool] = ..., failure_code: _Optional[str] = ..., artifact_json: _Optional[str] = ...) -> None: ...

class Acknowledgement(_message.Message):
    __slots__ = ("event_id",)
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    def __init__(self, event_id: _Optional[str] = ...) -> None: ...

class Fact(_message.Message):
    __slots__ = ("ref", "value")
    REF_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ref: str
    value: str
    def __init__(self, ref: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class EvidenceItem(_message.Message):
    __slots__ = ("assessment_id", "testee_id", "report_id", "source_version", "facts")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    FACTS_FIELD_NUMBER: _ClassVar[int]
    assessment_id: str
    testee_id: str
    report_id: str
    source_version: str
    facts: _containers.RepeatedCompositeFieldContainer[Fact]
    def __init__(self, assessment_id: _Optional[str] = ..., testee_id: _Optional[str] = ..., report_id: _Optional[str] = ..., source_version: _Optional[str] = ..., facts: _Optional[_Iterable[_Union[Fact, _Mapping]]] = ...) -> None: ...

class EvaluationExecutionQuery(_message.Message):
    __slots__ = ("scope", "expected_version", "cursor", "limit", "execution_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    cursor: str
    limit: int
    execution_id: str
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., cursor: _Optional[str] = ..., limit: _Optional[int] = ..., execution_id: _Optional[str] = ...) -> None: ...

class EvaluationExecutionSummary(_message.Message):
    __slots__ = ("execution_id", "invocation_id", "kind", "case_id", "slot_ordinal", "execution_ordinal", "status", "raw_output_bytes", "normalized_output_bytes", "evidence_json")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    SLOT_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RAW_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    NORMALIZED_OUTPUT_BYTES_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_JSON_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    invocation_id: str
    kind: str
    case_id: str
    slot_ordinal: int
    execution_ordinal: int
    status: str
    raw_output_bytes: int
    normalized_output_bytes: int
    evidence_json: str
    def __init__(self, execution_id: _Optional[str] = ..., invocation_id: _Optional[str] = ..., kind: _Optional[str] = ..., case_id: _Optional[str] = ..., slot_ordinal: _Optional[int] = ..., execution_ordinal: _Optional[int] = ..., status: _Optional[str] = ..., raw_output_bytes: _Optional[int] = ..., normalized_output_bytes: _Optional[int] = ..., evidence_json: _Optional[str] = ...) -> None: ...

class EvaluationExecutionPage(_message.Message):
    __slots__ = ("run_id", "version", "executions", "next_cursor")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    executions: _containers.RepeatedCompositeFieldContainer[EvaluationExecutionSummary]
    next_cursor: str
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., executions: _Optional[_Iterable[_Union[EvaluationExecutionSummary, _Mapping]]] = ..., next_cursor: _Optional[str] = ...) -> None: ...

class EvaluationExecutionOutput(_message.Message):
    __slots__ = ("run_id", "version", "execution", "raw_output", "normalized_output", "raw_sha256", "normalized_sha256")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    RAW_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    NORMALIZED_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    RAW_SHA256_FIELD_NUMBER: _ClassVar[int]
    NORMALIZED_SHA256_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    execution: EvaluationExecutionSummary
    raw_output: bytes
    normalized_output: bytes
    raw_sha256: str
    normalized_sha256: str
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., execution: _Optional[_Union[EvaluationExecutionSummary, _Mapping]] = ..., raw_output: _Optional[bytes] = ..., normalized_output: _Optional[bytes] = ..., raw_sha256: _Optional[str] = ..., normalized_sha256: _Optional[str] = ...) -> None: ...

class EvaluationPlanQuery(_message.Message):
    __slots__ = ("scope", "suite", "generation_route", "semantic_route")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SUITE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ROUTE_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_ROUTE_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    suite: FrozenEvaluationRef
    generation_route: FrozenEvaluationRef
    semantic_route: FrozenEvaluationRef
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., suite: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., generation_route: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., semantic_route: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ...) -> None: ...

class EvaluationPlan(_message.Message):
    __slots__ = ("schema_version", "plan_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PLAN_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    plan_json: str
    def __init__(self, schema_version: _Optional[str] = ..., plan_json: _Optional[str] = ...) -> None: ...

class EvaluationReopenCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "reason", "confirm")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    reason: str
    confirm: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ...) -> None: ...

class EvaluationCancelCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "reason", "confirm", "discard")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    DISCARD_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    reason: str
    confirm: bool
    discard: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ..., discard: _Optional[bool] = ...) -> None: ...

class EvaluationFinalizeCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "expected_passed", "reason", "confirm")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_PASSED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    expected_passed: bool
    reason: str
    confirm: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., expected_passed: _Optional[bool] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ...) -> None: ...

class EvaluationGateQuery(_message.Message):
    __slots__ = ("scope", "expected_version")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ...) -> None: ...

class EvaluationGatePreview(_message.Message):
    __slots__ = ("run_id", "version", "release_fingerprint", "gate_result_json")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    GATE_RESULT_JSON_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    release_fingerprint: str
    gate_result_json: str
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., release_fingerprint: _Optional[str] = ..., gate_result_json: _Optional[str] = ...) -> None: ...

class EvaluationCandidateSummary(_message.Message):
    __slots__ = ("candidate_id", "case_id", "slot_ordinal")
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    SLOT_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    candidate_id: str
    case_id: str
    slot_ordinal: int
    def __init__(self, candidate_id: _Optional[str] = ..., case_id: _Optional[str] = ..., slot_ordinal: _Optional[int] = ...) -> None: ...

class EvaluationCandidateIndex(_message.Message):
    __slots__ = ("run_id", "version", "candidates")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    candidates: _containers.RepeatedCompositeFieldContainer[EvaluationCandidateSummary]
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., candidates: _Optional[_Iterable[_Union[EvaluationCandidateSummary, _Mapping]]] = ...) -> None: ...

class EvaluationCandidateQuery(_message.Message):
    __slots__ = ("scope", "candidate_id", "expected_version")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    candidate_id: str
    expected_version: int
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., candidate_id: _Optional[str] = ..., expected_version: _Optional[int] = ...) -> None: ...

class EvaluationCandidateEvidence(_message.Message):
    __slots__ = ("run_id", "version", "candidate_id", "normalized_output", "semantic_output", "evidence_json")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    NORMALIZED_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_JSON_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    candidate_id: str
    normalized_output: bytes
    semantic_output: bytes
    evidence_json: str
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., candidate_id: _Optional[str] = ..., normalized_output: _Optional[bytes] = ..., semantic_output: _Optional[bytes] = ..., evidence_json: _Optional[str] = ...) -> None: ...

class EvaluationQuery(_message.Message):
    __slots__ = ("run_id", "organization_id", "operator_user_id")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_USER_ID_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    organization_id: int
    operator_user_id: int
    def __init__(self, run_id: _Optional[str] = ..., organization_id: _Optional[int] = ..., operator_user_id: _Optional[int] = ...) -> None: ...

class UnknownResolutionCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "execution_id", "decision", "reason", "confirm", "acknowledged_duplicate_call_and_cost_risk")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    ACKNOWLEDGED_DUPLICATE_CALL_AND_COST_RISK_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    execution_id: str
    decision: str
    reason: str
    confirm: bool
    acknowledged_duplicate_call_and_cost_risk: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., execution_id: _Optional[str] = ..., decision: _Optional[str] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ..., acknowledged_duplicate_call_and_cost_risk: _Optional[bool] = ...) -> None: ...

class EvaluationUnknownQuery(_message.Message):
    __slots__ = ("scope", "expected_version")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ...) -> None: ...

class EvaluationUnknownExecution(_message.Message):
    __slots__ = ("execution_id", "invocation_id", "kind", "case_id", "slot_ordinal", "candidate_id", "execution_ordinal", "started_at", "finished_at", "provider_call_count", "failure_stage", "failure_code", "target_execution_count", "target_execution_limit", "stage_execution_count", "stage_execution_limit", "replacement_allowed")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    SLOT_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_CALL_COUNT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_STAGE_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    TARGET_EXECUTION_COUNT_FIELD_NUMBER: _ClassVar[int]
    TARGET_EXECUTION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    STAGE_EXECUTION_COUNT_FIELD_NUMBER: _ClassVar[int]
    STAGE_EXECUTION_LIMIT_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_ALLOWED_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    invocation_id: str
    kind: str
    case_id: str
    slot_ordinal: int
    candidate_id: str
    execution_ordinal: int
    started_at: str
    finished_at: str
    provider_call_count: int
    failure_stage: str
    failure_code: str
    target_execution_count: int
    target_execution_limit: int
    stage_execution_count: int
    stage_execution_limit: int
    replacement_allowed: bool
    def __init__(self, execution_id: _Optional[str] = ..., invocation_id: _Optional[str] = ..., kind: _Optional[str] = ..., case_id: _Optional[str] = ..., slot_ordinal: _Optional[int] = ..., candidate_id: _Optional[str] = ..., execution_ordinal: _Optional[int] = ..., started_at: _Optional[str] = ..., finished_at: _Optional[str] = ..., provider_call_count: _Optional[int] = ..., failure_stage: _Optional[str] = ..., failure_code: _Optional[str] = ..., target_execution_count: _Optional[int] = ..., target_execution_limit: _Optional[int] = ..., stage_execution_count: _Optional[int] = ..., stage_execution_limit: _Optional[int] = ..., replacement_allowed: _Optional[bool] = ...) -> None: ...

class EvaluationUnknownIndex(_message.Message):
    __slots__ = ("run_id", "version", "release_fingerprint", "status", "unresolved_result_unknown_count", "can_resolve", "executions")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UNRESOLVED_RESULT_UNKNOWN_COUNT_FIELD_NUMBER: _ClassVar[int]
    CAN_RESOLVE_FIELD_NUMBER: _ClassVar[int]
    EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    release_fingerprint: str
    status: str
    unresolved_result_unknown_count: int
    can_resolve: bool
    executions: _containers.RepeatedCompositeFieldContainer[EvaluationUnknownExecution]
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., release_fingerprint: _Optional[str] = ..., status: _Optional[str] = ..., unresolved_result_unknown_count: _Optional[int] = ..., can_resolve: _Optional[bool] = ..., executions: _Optional[_Iterable[_Union[EvaluationUnknownExecution, _Mapping]]] = ...) -> None: ...

class EvaluationState(_message.Message):
    __slots__ = ("run_id", "version", "status", "unresolved_result_unknown_count", "resolutions_json", "reviews_json", "finalization_json", "reopenings_json", "creation_json", "cancellation_json", "can_reopen_review")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    UNRESOLVED_RESULT_UNKNOWN_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESOLUTIONS_JSON_FIELD_NUMBER: _ClassVar[int]
    REVIEWS_JSON_FIELD_NUMBER: _ClassVar[int]
    FINALIZATION_JSON_FIELD_NUMBER: _ClassVar[int]
    REOPENINGS_JSON_FIELD_NUMBER: _ClassVar[int]
    CREATION_JSON_FIELD_NUMBER: _ClassVar[int]
    CANCELLATION_JSON_FIELD_NUMBER: _ClassVar[int]
    CAN_REOPEN_REVIEW_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    version: int
    status: str
    unresolved_result_unknown_count: int
    resolutions_json: str
    reviews_json: str
    finalization_json: str
    reopenings_json: str
    creation_json: str
    cancellation_json: str
    can_reopen_review: bool
    def __init__(self, run_id: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., unresolved_result_unknown_count: _Optional[int] = ..., resolutions_json: _Optional[str] = ..., reviews_json: _Optional[str] = ..., finalization_json: _Optional[str] = ..., reopenings_json: _Optional[str] = ..., creation_json: _Optional[str] = ..., cancellation_json: _Optional[str] = ..., can_reopen_review: _Optional[bool] = ...) -> None: ...

class SemanticContradictionReview(_message.Message):
    __slots__ = ("policy_version", "execution_id", "output_fingerprint", "assertion_ordinal", "original_detail", "candidate_excerpt", "reason")
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    ASSERTION_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_DETAIL_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_EXCERPT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    policy_version: str
    execution_id: str
    output_fingerprint: str
    assertion_ordinal: int
    original_detail: str
    candidate_excerpt: str
    reason: str
    def __init__(self, policy_version: _Optional[str] = ..., execution_id: _Optional[str] = ..., output_fingerprint: _Optional[str] = ..., assertion_ordinal: _Optional[int] = ..., original_detail: _Optional[str] = ..., candidate_excerpt: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CandidateReviewItem(_message.Message):
    __slots__ = ("candidate_id", "decision", "reason", "semantic_review")
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_REVIEW_FIELD_NUMBER: _ClassVar[int]
    candidate_id: str
    decision: str
    reason: str
    semantic_review: SemanticContradictionReview
    def __init__(self, candidate_id: _Optional[str] = ..., decision: _Optional[str] = ..., reason: _Optional[str] = ..., semantic_review: _Optional[_Union[SemanticContradictionReview, _Mapping]] = ...) -> None: ...

class EvaluationReviewCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "role", "reviews")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    REVIEWS_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    role: str
    reviews: _containers.RepeatedCompositeFieldContainer[CandidateReviewItem]
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., role: _Optional[str] = ..., reviews: _Optional[_Iterable[_Union[CandidateReviewItem, _Mapping]]] = ...) -> None: ...

class EvaluationStartCommand(_message.Message):
    __slots__ = ("scope", "expected_version", "reason", "confirm")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    expected_version: int
    reason: str
    confirm: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., expected_version: _Optional[int] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ...) -> None: ...

class FrozenEvaluationRef(_message.Message):
    __slots__ = ("id", "version", "fingerprint")
    ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    id: str
    version: str
    fingerprint: str
    def __init__(self, id: _Optional[str] = ..., version: _Optional[str] = ..., fingerprint: _Optional[str] = ...) -> None: ...

class EvaluationRelease(_message.Message):
    __slots__ = ("suite", "prompt", "profile", "input_schema", "output_schema", "generation_route", "semantic_prompt", "semantic_output_schema", "semantic_route", "execution_policy", "gate_policy")
    SUITE_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ROUTE_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_PROMPT_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SEMANTIC_ROUTE_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_POLICY_FIELD_NUMBER: _ClassVar[int]
    GATE_POLICY_FIELD_NUMBER: _ClassVar[int]
    suite: FrozenEvaluationRef
    prompt: FrozenEvaluationRef
    profile: FrozenEvaluationRef
    input_schema: FrozenEvaluationRef
    output_schema: FrozenEvaluationRef
    generation_route: FrozenEvaluationRef
    semantic_prompt: FrozenEvaluationRef
    semantic_output_schema: FrozenEvaluationRef
    semantic_route: FrozenEvaluationRef
    execution_policy: FrozenEvaluationRef
    gate_policy: FrozenEvaluationRef
    def __init__(self, suite: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., prompt: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., profile: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., input_schema: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., output_schema: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., generation_route: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., semantic_prompt: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., semantic_output_schema: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., semantic_route: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., execution_policy: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., gate_policy: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ...) -> None: ...

class EvaluationCreateCommand(_message.Message):
    __slots__ = ("scope", "release", "reason", "confirm")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    scope: EvaluationQuery
    release: EvaluationRelease
    reason: str
    confirm: bool
    def __init__(self, scope: _Optional[_Union[EvaluationQuery, _Mapping]] = ..., release: _Optional[_Union[EvaluationRelease, _Mapping]] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ...) -> None: ...

class PublicationScope(_message.Message):
    __slots__ = ("organization_id", "operator_user_id")
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_USER_ID_FIELD_NUMBER: _ClassVar[int]
    organization_id: int
    operator_user_id: int
    def __init__(self, organization_id: _Optional[int] = ..., operator_user_id: _Optional[int] = ...) -> None: ...

class PublicationSelector(_message.Message):
    __slots__ = ("audience", "model_kind", "decision_kind", "model_code", "model_version")
    AUDIENCE_FIELD_NUMBER: _ClassVar[int]
    MODEL_KIND_FIELD_NUMBER: _ClassVar[int]
    DECISION_KIND_FIELD_NUMBER: _ClassVar[int]
    MODEL_CODE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    audience: str
    model_kind: str
    decision_kind: str
    model_code: str
    model_version: str
    def __init__(self, audience: _Optional[str] = ..., model_kind: _Optional[str] = ..., decision_kind: _Optional[str] = ..., model_code: _Optional[str] = ..., model_version: _Optional[str] = ...) -> None: ...

class PublicationExpectation(_message.Message):
    __slots__ = ("selector", "version", "active_publication_id")
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PUBLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    selector: PublicationSelector
    version: int
    active_publication_id: str
    def __init__(self, selector: _Optional[_Union[PublicationSelector, _Mapping]] = ..., version: _Optional[int] = ..., active_publication_id: _Optional[str] = ...) -> None: ...

class PublicationPublishCommand(_message.Message):
    __slots__ = ("scope", "command_id", "expected", "reason", "confirm", "run_id", "run_version", "release_fingerprint")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    expected: PublicationExpectation
    reason: str
    confirm: bool
    run_id: str
    run_version: int
    release_fingerprint: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., expected: _Optional[_Union[PublicationExpectation, _Mapping]] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ..., run_id: _Optional[str] = ..., run_version: _Optional[int] = ..., release_fingerprint: _Optional[str] = ...) -> None: ...

class PublicationRollbackCommand(_message.Message):
    __slots__ = ("scope", "command_id", "expected", "reason", "confirm", "target_publication_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    TARGET_PUBLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    expected: PublicationExpectation
    reason: str
    confirm: bool
    target_publication_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., expected: _Optional[_Union[PublicationExpectation, _Mapping]] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ..., target_publication_id: _Optional[str] = ...) -> None: ...

class PublicationDisableCommand(_message.Message):
    __slots__ = ("scope", "command_id", "expected", "reason", "confirm")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    expected: PublicationExpectation
    reason: str
    confirm: bool
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., expected: _Optional[_Union[PublicationExpectation, _Mapping]] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ...) -> None: ...

class PublicationQuery(_message.Message):
    __slots__ = ("scope", "selector")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    selector: PublicationSelector
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., selector: _Optional[_Union[PublicationSelector, _Mapping]] = ...) -> None: ...

class PublicationReceiptQuery(_message.Message):
    __slots__ = ("scope", "command_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ...) -> None: ...

class PublicationHistoryQuery(_message.Message):
    __slots__ = ("scope", "selector", "before_version", "limit")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    BEFORE_VERSION_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    selector: PublicationSelector
    before_version: int
    limit: int
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., selector: _Optional[_Union[PublicationSelector, _Mapping]] = ..., before_version: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class PublicationHistoryVersionQuery(_message.Message):
    __slots__ = ("scope", "selector", "version")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    selector: PublicationSelector
    version: int
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., selector: _Optional[_Union[PublicationSelector, _Mapping]] = ..., version: _Optional[int] = ...) -> None: ...

class PublicationHistoryPage(_message.Message):
    __slots__ = ("schema_version", "payload_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    payload_json: str
    def __init__(self, schema_version: _Optional[str] = ..., payload_json: _Optional[str] = ...) -> None: ...

class PublicationState(_message.Message):
    __slots__ = ("selector", "version", "active_publication_id", "publication_json", "changed_at")
    SELECTOR_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_PUBLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLICATION_JSON_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_FIELD_NUMBER: _ClassVar[int]
    selector: PublicationSelector
    version: int
    active_publication_id: str
    publication_json: str
    changed_at: str
    def __init__(self, selector: _Optional[_Union[PublicationSelector, _Mapping]] = ..., version: _Optional[int] = ..., active_publication_id: _Optional[str] = ..., publication_json: _Optional[str] = ..., changed_at: _Optional[str] = ...) -> None: ...

class PublicationReceipt(_message.Message):
    __slots__ = ("command_id", "previous", "current", "action", "actor", "reason", "changed_at")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CHANGED_AT_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    previous: PublicationState
    current: PublicationState
    action: str
    actor: str
    reason: str
    changed_at: str
    def __init__(self, command_id: _Optional[str] = ..., previous: _Optional[_Union[PublicationState, _Mapping]] = ..., current: _Optional[_Union[PublicationState, _Mapping]] = ..., action: _Optional[str] = ..., actor: _Optional[str] = ..., reason: _Optional[str] = ..., changed_at: _Optional[str] = ...) -> None: ...

class PromptDraftFreezeCommand(_message.Message):
    __slots__ = ("scope", "draft_id", "command_id", "expected_revision", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    draft_id: str
    command_id: str
    expected_revision: int
    reason: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., draft_id: _Optional[str] = ..., command_id: _Optional[str] = ..., expected_revision: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class PromptDraftFreezeReceipt(_message.Message):
    __slots__ = ("schema_version", "command_id", "receipt_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    command_id: str
    receipt_json: str
    def __init__(self, schema_version: _Optional[str] = ..., command_id: _Optional[str] = ..., receipt_json: _Optional[str] = ...) -> None: ...

class PromptDraftSource(_message.Message):
    __slots__ = ("identity", "version", "fingerprint", "content_sha256")
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SHA256_FIELD_NUMBER: _ClassVar[int]
    identity: str
    version: str
    fingerprint: str
    content_sha256: str
    def __init__(self, identity: _Optional[str] = ..., version: _Optional[str] = ..., fingerprint: _Optional[str] = ..., content_sha256: _Optional[str] = ...) -> None: ...

class PromptDraftContent(_message.Message):
    __slots__ = ("system_message", "task_template", "data_preamble", "allowed_placeholders")
    SYSTEM_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TASK_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    DATA_PREAMBLE_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_PLACEHOLDERS_FIELD_NUMBER: _ClassVar[int]
    system_message: str
    task_template: str
    data_preamble: str
    allowed_placeholders: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, system_message: _Optional[str] = ..., task_template: _Optional[str] = ..., data_preamble: _Optional[str] = ..., allowed_placeholders: _Optional[_Iterable[str]] = ...) -> None: ...

class PromptDraftCreateCommand(_message.Message):
    __slots__ = ("scope", "draft_id", "command_id", "source", "template_id", "target_version", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    draft_id: str
    command_id: str
    source: PromptDraftSource
    template_id: str
    target_version: str
    reason: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., draft_id: _Optional[str] = ..., command_id: _Optional[str] = ..., source: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., template_id: _Optional[str] = ..., target_version: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class PromptDraftReviseCommand(_message.Message):
    __slots__ = ("scope", "draft_id", "command_id", "expected_revision", "content", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_REVISION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    draft_id: str
    command_id: str
    expected_revision: int
    content: PromptDraftContent
    reason: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., draft_id: _Optional[str] = ..., command_id: _Optional[str] = ..., expected_revision: _Optional[int] = ..., content: _Optional[_Union[PromptDraftContent, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class PromptDraftQuery(_message.Message):
    __slots__ = ("scope", "draft_id", "revision")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    draft_id: str
    revision: int
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., draft_id: _Optional[str] = ..., revision: _Optional[int] = ...) -> None: ...

class PromptDraftReceiptQuery(_message.Message):
    __slots__ = ("scope", "command_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ...) -> None: ...

class PromptDraftState(_message.Message):
    __slots__ = ("schema_version", "draft_id", "revision", "snapshot_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DRAFT_ID_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    draft_id: str
    revision: int
    snapshot_json: str
    def __init__(self, schema_version: _Optional[str] = ..., draft_id: _Optional[str] = ..., revision: _Optional[int] = ..., snapshot_json: _Optional[str] = ...) -> None: ...

class PromptDraftLifecycle(_message.Message):
    __slots__ = ("schema_version", "draft", "status", "frozen")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DRAFT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FROZEN_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    draft: PromptDraftState
    status: str
    frozen: PromptDraftFrozenVersion
    def __init__(self, schema_version: _Optional[str] = ..., draft: _Optional[_Union[PromptDraftState, _Mapping]] = ..., status: _Optional[str] = ..., frozen: _Optional[_Union[PromptDraftFrozenVersion, _Mapping]] = ...) -> None: ...

class PromptDraftFrozenVersion(_message.Message):
    __slots__ = ("asset", "revision", "frozen_at")
    ASSET_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    FROZEN_AT_FIELD_NUMBER: _ClassVar[int]
    asset: PromptDraftSource
    revision: int
    frozen_at: str
    def __init__(self, asset: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., revision: _Optional[int] = ..., frozen_at: _Optional[str] = ...) -> None: ...

class ProfileRegisterCommand(_message.Message):
    __slots__ = ("scope", "command_id", "source", "definition_json", "prompt", "generation_route", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DEFINITION_JSON_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ROUTE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    source: PromptDraftSource
    definition_json: str
    prompt: PromptDraftSource
    generation_route: PromptDraftSource
    reason: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., source: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., definition_json: _Optional[str] = ..., prompt: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., generation_route: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class ProfileRegistrationQuery(_message.Message):
    __slots__ = ("scope", "command_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ...) -> None: ...

class ProfileRegistrationReceipt(_message.Message):
    __slots__ = ("schema_version", "command_id", "receipt_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    command_id: str
    receipt_json: str
    def __init__(self, schema_version: _Optional[str] = ..., command_id: _Optional[str] = ..., receipt_json: _Optional[str] = ...) -> None: ...

class SuiteRegisterCommand(_message.Message):
    __slots__ = ("scope", "command_id", "source", "suite_id", "suite_version", "profile", "prompt", "generation_route", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SUITE_ID_FIELD_NUMBER: _ClassVar[int]
    SUITE_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ROUTE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    source: FrozenEvaluationRef
    suite_id: str
    suite_version: str
    profile: PromptDraftSource
    prompt: PromptDraftSource
    generation_route: PromptDraftSource
    reason: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., source: _Optional[_Union[FrozenEvaluationRef, _Mapping]] = ..., suite_id: _Optional[str] = ..., suite_version: _Optional[str] = ..., profile: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., prompt: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., generation_route: _Optional[_Union[PromptDraftSource, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class SuiteRegistrationQuery(_message.Message):
    __slots__ = ("scope", "command_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ...) -> None: ...

class SuiteRegistrationReceipt(_message.Message):
    __slots__ = ("schema_version", "command_id", "receipt_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    command_id: str
    receipt_json: str
    def __init__(self, schema_version: _Optional[str] = ..., command_id: _Optional[str] = ..., receipt_json: _Optional[str] = ...) -> None: ...

class AssetCatalogQuery(_message.Message):
    __slots__ = ("scope", "kind", "identity", "limit", "cursor")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    kind: str
    identity: str
    limit: int
    cursor: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., kind: _Optional[str] = ..., identity: _Optional[str] = ..., limit: _Optional[int] = ..., cursor: _Optional[str] = ...) -> None: ...

class AssetCatalogGetQuery(_message.Message):
    __slots__ = ("scope", "kind", "identity", "version")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    kind: str
    identity: str
    version: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., kind: _Optional[str] = ..., identity: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class AssetCatalogResponse(_message.Message):
    __slots__ = ("schema_version", "payload_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    payload_json: str
    def __init__(self, schema_version: _Optional[str] = ..., payload_json: _Optional[str] = ...) -> None: ...

class EvaluationCatalogQuery(_message.Message):
    __slots__ = ("scope", "status", "limit", "cursor")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    status: str
    limit: int
    cursor: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., status: _Optional[str] = ..., limit: _Optional[int] = ..., cursor: _Optional[str] = ...) -> None: ...

class EvaluationCatalogPage(_message.Message):
    __slots__ = ("items", "next_cursor")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[EvaluationSummary]
    next_cursor: str
    def __init__(self, items: _Optional[_Iterable[_Union[EvaluationSummary, _Mapping]]] = ..., next_cursor: _Optional[str] = ...) -> None: ...

class EvaluationSummary(_message.Message):
    __slots__ = ("run_id", "organization_id", "version", "status", "created_at", "requested_by", "profile_id", "profile_version", "prompt_id", "prompt_version", "release_fingerprint", "unresolved_result_unknown_count", "review_count", "required_candidates", "accepted_candidates", "review_ready_candidates", "last_cause", "last_reason")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    PROFILE_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROMPT_ID_FIELD_NUMBER: _ClassVar[int]
    PROMPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    UNRESOLVED_RESULT_UNKNOWN_COUNT_FIELD_NUMBER: _ClassVar[int]
    REVIEW_COUNT_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    REVIEW_READY_CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    LAST_CAUSE_FIELD_NUMBER: _ClassVar[int]
    LAST_REASON_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    organization_id: int
    version: int
    status: str
    created_at: str
    requested_by: str
    profile_id: str
    profile_version: str
    prompt_id: str
    prompt_version: str
    release_fingerprint: str
    unresolved_result_unknown_count: int
    review_count: int
    required_candidates: int
    accepted_candidates: int
    review_ready_candidates: int
    last_cause: str
    last_reason: str
    def __init__(self, run_id: _Optional[str] = ..., organization_id: _Optional[int] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., created_at: _Optional[str] = ..., requested_by: _Optional[str] = ..., profile_id: _Optional[str] = ..., profile_version: _Optional[str] = ..., prompt_id: _Optional[str] = ..., prompt_version: _Optional[str] = ..., release_fingerprint: _Optional[str] = ..., unresolved_result_unknown_count: _Optional[int] = ..., review_count: _Optional[int] = ..., required_candidates: _Optional[int] = ..., accepted_candidates: _Optional[int] = ..., review_ready_candidates: _Optional[int] = ..., last_cause: _Optional[str] = ..., last_reason: _Optional[str] = ...) -> None: ...

class EvaluationCapacityReservation(_message.Message):
    __slots__ = ("run_id", "provider_calls", "requested_by", "reserved_at")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_CALLS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    RESERVED_AT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    provider_calls: int
    requested_by: str
    reserved_at: str
    def __init__(self, run_id: _Optional[str] = ..., provider_calls: _Optional[int] = ..., requested_by: _Optional[str] = ..., reserved_at: _Optional[str] = ...) -> None: ...

class EvaluationCapacitySnapshot(_message.Message):
    __slots__ = ("organization_id", "budget_day", "daily_provider_calls", "reserved_provider_calls", "remaining_provider_calls", "full_run_provider_calls", "remaining_full_runs", "max_active_runs", "active_runs", "reservation_count", "reservations", "reservations_truncated")
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    BUDGET_DAY_FIELD_NUMBER: _ClassVar[int]
    DAILY_PROVIDER_CALLS_FIELD_NUMBER: _ClassVar[int]
    RESERVED_PROVIDER_CALLS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_PROVIDER_CALLS_FIELD_NUMBER: _ClassVar[int]
    FULL_RUN_PROVIDER_CALLS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FULL_RUNS_FIELD_NUMBER: _ClassVar[int]
    MAX_ACTIVE_RUNS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_RUNS_FIELD_NUMBER: _ClassVar[int]
    RESERVATION_COUNT_FIELD_NUMBER: _ClassVar[int]
    RESERVATIONS_FIELD_NUMBER: _ClassVar[int]
    RESERVATIONS_TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    organization_id: int
    budget_day: str
    daily_provider_calls: int
    reserved_provider_calls: int
    remaining_provider_calls: int
    full_run_provider_calls: int
    remaining_full_runs: int
    max_active_runs: int
    active_runs: int
    reservation_count: int
    reservations: _containers.RepeatedCompositeFieldContainer[EvaluationCapacityReservation]
    reservations_truncated: bool
    def __init__(self, organization_id: _Optional[int] = ..., budget_day: _Optional[str] = ..., daily_provider_calls: _Optional[int] = ..., reserved_provider_calls: _Optional[int] = ..., remaining_provider_calls: _Optional[int] = ..., full_run_provider_calls: _Optional[int] = ..., remaining_full_runs: _Optional[int] = ..., max_active_runs: _Optional[int] = ..., active_runs: _Optional[int] = ..., reservation_count: _Optional[int] = ..., reservations: _Optional[_Iterable[_Union[EvaluationCapacityReservation, _Mapping]]] = ..., reservations_truncated: _Optional[bool] = ...) -> None: ...

class ParticipantCapacityQuery(_message.Message):
    __slots__ = ("scope", "subject_id", "assessment_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    subject_id: str
    assessment_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., subject_id: _Optional[str] = ..., assessment_id: _Optional[str] = ...) -> None: ...

class ParticipantCapacityPolicy(_message.Message):
    __slots__ = ("daily_org", "daily_user", "daily_assessment", "active_org", "active_user", "active_assessment")
    DAILY_ORG_FIELD_NUMBER: _ClassVar[int]
    DAILY_USER_FIELD_NUMBER: _ClassVar[int]
    DAILY_ASSESSMENT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ORG_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_USER_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_ASSESSMENT_FIELD_NUMBER: _ClassVar[int]
    daily_org: int
    daily_user: int
    daily_assessment: int
    active_org: int
    active_user: int
    active_assessment: int
    def __init__(self, daily_org: _Optional[int] = ..., daily_user: _Optional[int] = ..., daily_assessment: _Optional[int] = ..., active_org: _Optional[int] = ..., active_user: _Optional[int] = ..., active_assessment: _Optional[int] = ...) -> None: ...

class ParticipantCapacityUsage(_message.Message):
    __slots__ = ("identity", "daily_reserved", "daily_remaining", "active", "active_remaining")
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    DAILY_RESERVED_FIELD_NUMBER: _ClassVar[int]
    DAILY_REMAINING_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_REMAINING_FIELD_NUMBER: _ClassVar[int]
    identity: str
    daily_reserved: int
    daily_remaining: int
    active: int
    active_remaining: int
    def __init__(self, identity: _Optional[str] = ..., daily_reserved: _Optional[int] = ..., daily_remaining: _Optional[int] = ..., active: _Optional[int] = ..., active_remaining: _Optional[int] = ...) -> None: ...

class ParticipantReservation(_message.Message):
    __slots__ = ("run_id", "session_id", "subject_id", "assessment_ids", "budget_day", "reserved_at", "active", "acquired_at")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    BUDGET_DAY_FIELD_NUMBER: _ClassVar[int]
    RESERVED_AT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    ACQUIRED_AT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    session_id: str
    subject_id: str
    assessment_ids: _containers.RepeatedScalarFieldContainer[str]
    budget_day: str
    reserved_at: str
    active: bool
    acquired_at: str
    def __init__(self, run_id: _Optional[str] = ..., session_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., assessment_ids: _Optional[_Iterable[str]] = ..., budget_day: _Optional[str] = ..., reserved_at: _Optional[str] = ..., active: _Optional[bool] = ..., acquired_at: _Optional[str] = ...) -> None: ...

class ParticipantCapacitySnapshot(_message.Message):
    __slots__ = ("organization_id", "budget_day", "policy", "organization", "subject", "assessment", "daily_reservations", "active_reservations", "daily_truncated", "active_truncated")
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    BUDGET_DAY_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_FIELD_NUMBER: _ClassVar[int]
    DAILY_RESERVATIONS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_RESERVATIONS_FIELD_NUMBER: _ClassVar[int]
    DAILY_TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    organization_id: int
    budget_day: str
    policy: ParticipantCapacityPolicy
    organization: ParticipantCapacityUsage
    subject: ParticipantCapacityUsage
    assessment: ParticipantCapacityUsage
    daily_reservations: _containers.RepeatedCompositeFieldContainer[ParticipantReservation]
    active_reservations: _containers.RepeatedCompositeFieldContainer[ParticipantReservation]
    daily_truncated: bool
    active_truncated: bool
    def __init__(self, organization_id: _Optional[int] = ..., budget_day: _Optional[str] = ..., policy: _Optional[_Union[ParticipantCapacityPolicy, _Mapping]] = ..., organization: _Optional[_Union[ParticipantCapacityUsage, _Mapping]] = ..., subject: _Optional[_Union[ParticipantCapacityUsage, _Mapping]] = ..., assessment: _Optional[_Union[ParticipantCapacityUsage, _Mapping]] = ..., daily_reservations: _Optional[_Iterable[_Union[ParticipantReservation, _Mapping]]] = ..., active_reservations: _Optional[_Iterable[_Union[ParticipantReservation, _Mapping]]] = ..., daily_truncated: _Optional[bool] = ..., active_truncated: _Optional[bool] = ...) -> None: ...

class ParticipantExecutionQuery(_message.Message):
    __slots__ = ("scope", "session_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    session_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., session_id: _Optional[str] = ...) -> None: ...

class ParticipantRetryReceiptQuery(_message.Message):
    __slots__ = ("scope", "command_id")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ...) -> None: ...

class ParticipantRetryCommand(_message.Message):
    __slots__ = ("scope", "session_id", "command_id", "expected_run_id", "expected_version", "reason", "confirm", "expected_provider_invocations", "accept_result_unknown_risk")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CONFIRM_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_PROVIDER_INVOCATIONS_FIELD_NUMBER: _ClassVar[int]
    ACCEPT_RESULT_UNKNOWN_RISK_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    session_id: str
    command_id: str
    expected_run_id: str
    expected_version: int
    reason: str
    confirm: bool
    expected_provider_invocations: int
    accept_result_unknown_risk: bool
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., session_id: _Optional[str] = ..., command_id: _Optional[str] = ..., expected_run_id: _Optional[str] = ..., expected_version: _Optional[int] = ..., reason: _Optional[str] = ..., confirm: _Optional[bool] = ..., expected_provider_invocations: _Optional[int] = ..., accept_result_unknown_risk: _Optional[bool] = ...) -> None: ...

class ParticipantExecution(_message.Message):
    __slots__ = ("organization_id", "session_id", "request_id", "run_id", "version", "status", "subject_id", "testee_id", "assessment_ids", "failure_code", "model_call_status", "invocation_id", "source_run_id", "can_retry", "unknown_result_risk", "retry_provider_invocations")
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    MODEL_CALL_STATUS_FIELD_NUMBER: _ClassVar[int]
    INVOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CAN_RETRY_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_RESULT_RISK_FIELD_NUMBER: _ClassVar[int]
    RETRY_PROVIDER_INVOCATIONS_FIELD_NUMBER: _ClassVar[int]
    organization_id: int
    session_id: str
    request_id: str
    run_id: str
    version: int
    status: str
    subject_id: str
    testee_id: str
    assessment_ids: _containers.RepeatedScalarFieldContainer[str]
    failure_code: str
    model_call_status: str
    invocation_id: str
    source_run_id: str
    can_retry: bool
    unknown_result_risk: bool
    retry_provider_invocations: int
    def __init__(self, organization_id: _Optional[int] = ..., session_id: _Optional[str] = ..., request_id: _Optional[str] = ..., run_id: _Optional[str] = ..., version: _Optional[int] = ..., status: _Optional[str] = ..., subject_id: _Optional[str] = ..., testee_id: _Optional[str] = ..., assessment_ids: _Optional[_Iterable[str]] = ..., failure_code: _Optional[str] = ..., model_call_status: _Optional[str] = ..., invocation_id: _Optional[str] = ..., source_run_id: _Optional[str] = ..., can_retry: _Optional[bool] = ..., unknown_result_risk: _Optional[bool] = ..., retry_provider_invocations: _Optional[int] = ...) -> None: ...

class ProfileLifecycleQuery(_message.Message):
    __slots__ = ("scope", "identity", "version", "status", "limit", "cursor")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    identity: str
    version: str
    status: str
    limit: int
    cursor: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., identity: _Optional[str] = ..., version: _Optional[str] = ..., status: _Optional[str] = ..., limit: _Optional[int] = ..., cursor: _Optional[str] = ...) -> None: ...

class SolutionQuery(_message.Message):
    __slots__ = ("scope", "solution_id", "command_id", "cursor")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    solution_id: str
    command_id: str
    cursor: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., solution_id: _Optional[str] = ..., command_id: _Optional[str] = ..., cursor: _Optional[str] = ...) -> None: ...

class SolutionWrite(_message.Message):
    __slots__ = ("scope", "solution_id", "command_json")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    SOLUTION_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_JSON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    solution_id: str
    command_json: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., solution_id: _Optional[str] = ..., command_json: _Optional[str] = ...) -> None: ...

class SolutionResponse(_message.Message):
    __slots__ = ("schema_version", "data_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DATA_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    data_json: str
    def __init__(self, schema_version: _Optional[str] = ..., data_json: _Optional[str] = ...) -> None: ...

class QuotaQuery(_message.Message):
    __slots__ = ("scope", "command_id", "before_revision")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    BEFORE_REVISION_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_id: str
    before_revision: int
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_id: _Optional[str] = ..., before_revision: _Optional[int] = ...) -> None: ...

class QuotaWrite(_message.Message):
    __slots__ = ("scope", "command_json")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    COMMAND_JSON_FIELD_NUMBER: _ClassVar[int]
    scope: PublicationScope
    command_json: str
    def __init__(self, scope: _Optional[_Union[PublicationScope, _Mapping]] = ..., command_json: _Optional[str] = ...) -> None: ...

class QuotaResponse(_message.Message):
    __slots__ = ("schema_version", "data_json")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    DATA_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    data_json: str
    def __init__(self, schema_version: _Optional[str] = ..., data_json: _Optional[str] = ...) -> None: ...
