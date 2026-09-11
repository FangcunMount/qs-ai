from qs_ai.infrastructure.qs_server.generated.evaluation import evaluation_pb2 as _evaluation_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Suggestion(_message.Message):
    __slots__ = ("category", "content", "factor_code")
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    FACTOR_CODE_FIELD_NUMBER: _ClassVar[int]
    category: str
    content: str
    factor_code: str
    def __init__(self, category: _Optional[str] = ..., content: _Optional[str] = ..., factor_code: _Optional[str] = ...) -> None: ...

class NormReference(_message.Message):
    __slots__ = ("score_kind", "benchmark", "table_version", "form_variant", "min_age_months", "max_age_months", "gender")
    SCORE_KIND_FIELD_NUMBER: _ClassVar[int]
    BENCHMARK_FIELD_NUMBER: _ClassVar[int]
    TABLE_VERSION_FIELD_NUMBER: _ClassVar[int]
    FORM_VARIANT_FIELD_NUMBER: _ClassVar[int]
    MIN_AGE_MONTHS_FIELD_NUMBER: _ClassVar[int]
    MAX_AGE_MONTHS_FIELD_NUMBER: _ClassVar[int]
    GENDER_FIELD_NUMBER: _ClassVar[int]
    score_kind: str
    benchmark: float
    table_version: str
    form_variant: str
    min_age_months: int
    max_age_months: int
    gender: str
    def __init__(self, score_kind: _Optional[str] = ..., benchmark: _Optional[float] = ..., table_version: _Optional[str] = ..., form_variant: _Optional[str] = ..., min_age_months: _Optional[int] = ..., max_age_months: _Optional[int] = ..., gender: _Optional[str] = ...) -> None: ...

class DimensionInterpret(_message.Message):
    __slots__ = ("factor_code", "factor_name", "raw_score", "risk_level", "description", "max_score", "suggestion", "derived_scores", "level", "norm_reference")
    FACTOR_CODE_FIELD_NUMBER: _ClassVar[int]
    FACTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    RAW_SCORE_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MAX_SCORE_FIELD_NUMBER: _ClassVar[int]
    SUGGESTION_FIELD_NUMBER: _ClassVar[int]
    DERIVED_SCORES_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    NORM_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    factor_code: str
    factor_name: str
    raw_score: float
    risk_level: str
    description: str
    max_score: float
    suggestion: str
    derived_scores: _containers.RepeatedCompositeFieldContainer[_evaluation_pb2.ScoreValue]
    level: _evaluation_pb2.ResultLevel
    norm_reference: NormReference
    def __init__(self, factor_code: _Optional[str] = ..., factor_name: _Optional[str] = ..., raw_score: _Optional[float] = ..., risk_level: _Optional[str] = ..., description: _Optional[str] = ..., max_score: _Optional[float] = ..., suggestion: _Optional[str] = ..., derived_scores: _Optional[_Iterable[_Union[_evaluation_pb2.ScoreValue, _Mapping]]] = ..., level: _Optional[_Union[_evaluation_pb2.ResultLevel, _Mapping]] = ..., norm_reference: _Optional[_Union[NormReference, _Mapping]] = ...) -> None: ...

class ModelRarity(_message.Message):
    __slots__ = ("percent", "label", "one_in_x")
    PERCENT_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    ONE_IN_X_FIELD_NUMBER: _ClassVar[int]
    percent: float
    label: str
    one_in_x: int
    def __init__(self, percent: _Optional[float] = ..., label: _Optional[str] = ..., one_in_x: _Optional[int] = ...) -> None: ...

class ModelExtra(_message.Message):
    __slots__ = ("kind", "type_code", "type_name", "one_liner", "image_url", "match_percent", "is_special", "special_trigger", "commentary", "rarity")
    KIND_FIELD_NUMBER: _ClassVar[int]
    TYPE_CODE_FIELD_NUMBER: _ClassVar[int]
    TYPE_NAME_FIELD_NUMBER: _ClassVar[int]
    ONE_LINER_FIELD_NUMBER: _ClassVar[int]
    IMAGE_URL_FIELD_NUMBER: _ClassVar[int]
    MATCH_PERCENT_FIELD_NUMBER: _ClassVar[int]
    IS_SPECIAL_FIELD_NUMBER: _ClassVar[int]
    SPECIAL_TRIGGER_FIELD_NUMBER: _ClassVar[int]
    COMMENTARY_FIELD_NUMBER: _ClassVar[int]
    RARITY_FIELD_NUMBER: _ClassVar[int]
    kind: str
    type_code: str
    type_name: str
    one_liner: str
    image_url: str
    match_percent: float
    is_special: bool
    special_trigger: str
    commentary: str
    rarity: ModelRarity
    def __init__(self, kind: _Optional[str] = ..., type_code: _Optional[str] = ..., type_name: _Optional[str] = ..., one_liner: _Optional[str] = ..., image_url: _Optional[str] = ..., match_percent: _Optional[float] = ..., is_special: _Optional[bool] = ..., special_trigger: _Optional[str] = ..., commentary: _Optional[str] = ..., rarity: _Optional[_Union[ModelRarity, _Mapping]] = ...) -> None: ...

class AssessmentReport(_message.Message):
    __slots__ = ("assessment_id", "conclusion", "dimensions", "suggestions", "created_at", "model_extra", "model", "primary_score", "level")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CONCLUSION_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    SUGGESTIONS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    MODEL_EXTRA_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_SCORE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    conclusion: str
    dimensions: _containers.RepeatedCompositeFieldContainer[DimensionInterpret]
    suggestions: _containers.RepeatedCompositeFieldContainer[Suggestion]
    created_at: str
    model_extra: ModelExtra
    model: _evaluation_pb2.ModelIdentity
    primary_score: _evaluation_pb2.ScoreValue
    level: _evaluation_pb2.ResultLevel
    def __init__(self, assessment_id: _Optional[int] = ..., conclusion: _Optional[str] = ..., dimensions: _Optional[_Iterable[_Union[DimensionInterpret, _Mapping]]] = ..., suggestions: _Optional[_Iterable[_Union[Suggestion, _Mapping]]] = ..., created_at: _Optional[str] = ..., model_extra: _Optional[_Union[ModelExtra, _Mapping]] = ..., model: _Optional[_Union[_evaluation_pb2.ModelIdentity, _Mapping]] = ..., primary_score: _Optional[_Union[_evaluation_pb2.ScoreValue, _Mapping]] = ..., level: _Optional[_Union[_evaluation_pb2.ResultLevel, _Mapping]] = ...) -> None: ...

class GetAssessmentReportRequest(_message.Message):
    __slots__ = ("assessment_id", "testee_id")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    testee_id: int
    def __init__(self, assessment_id: _Optional[int] = ..., testee_id: _Optional[int] = ...) -> None: ...

class GetAssessmentReportResponse(_message.Message):
    __slots__ = ("report",)
    REPORT_FIELD_NUMBER: _ClassVar[int]
    report: AssessmentReport
    def __init__(self, report: _Optional[_Union[AssessmentReport, _Mapping]] = ...) -> None: ...

class ListMyReportsRequest(_message.Message):
    __slots__ = ("testee_id", "page", "page_size")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    page: int
    page_size: int
    def __init__(self, testee_id: _Optional[int] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ...) -> None: ...

class ListMyReportsResponse(_message.Message):
    __slots__ = ("items", "total", "page", "page_size", "total_pages")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PAGES_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[AssessmentReport]
    total: int
    page: int
    page_size: int
    total_pages: int
    def __init__(self, items: _Optional[_Iterable[_Union[AssessmentReport, _Mapping]]] = ..., total: _Optional[int] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., total_pages: _Optional[int] = ...) -> None: ...

class GenerateReportFromAssessmentRequest(_message.Message):
    __slots__ = ("assessment_id", "outcome_id")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_ID_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    outcome_id: str
    def __init__(self, assessment_id: _Optional[int] = ..., outcome_id: _Optional[str] = ...) -> None: ...

class GenerateReportFromOutcomeRequest(_message.Message):
    __slots__ = ("outcome_id",)
    OUTCOME_ID_FIELD_NUMBER: _ClassVar[int]
    outcome_id: str
    def __init__(self, outcome_id: _Optional[str] = ...) -> None: ...

class GenerateReportFromAssessmentResponse(_message.Message):
    __slots__ = ("success", "status", "message", "retryable", "run_id", "failure_kind", "generation_id", "report_id", "failure_code", "retry_disposition", "attempt_origin", "current_attempt", "max_automatic_attempts", "remaining_automatic_attempts", "next_attempt_at", "retry_event_id", "action_request_id")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FAILURE_KIND_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    RETRY_DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ORIGIN_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_AUTOMATIC_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_AUTOMATIC_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    RETRY_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    success: bool
    status: str
    message: str
    retryable: bool
    run_id: str
    failure_kind: str
    generation_id: str
    report_id: str
    failure_code: str
    retry_disposition: str
    attempt_origin: str
    current_attempt: int
    max_automatic_attempts: int
    remaining_automatic_attempts: int
    next_attempt_at: str
    retry_event_id: str
    action_request_id: str
    def __init__(self, success: _Optional[bool] = ..., status: _Optional[str] = ..., message: _Optional[str] = ..., retryable: _Optional[bool] = ..., run_id: _Optional[str] = ..., failure_kind: _Optional[str] = ..., generation_id: _Optional[str] = ..., report_id: _Optional[str] = ..., failure_code: _Optional[str] = ..., retry_disposition: _Optional[str] = ..., attempt_origin: _Optional[str] = ..., current_attempt: _Optional[int] = ..., max_automatic_attempts: _Optional[int] = ..., remaining_automatic_attempts: _Optional[int] = ..., next_attempt_at: _Optional[str] = ..., retry_event_id: _Optional[str] = ..., action_request_id: _Optional[str] = ...) -> None: ...

class ExecuteAIExplanationRequest(_message.Message):
    __slots__ = ("generation_id", "trace_id", "event_id", "expected_attempt", "attempt_origin", "action_request_id", "expected_run_id", "expected_lease_expires_at", "expected_invocation_phase")
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ORIGIN_FIELD_NUMBER: _ClassVar[int]
    ACTION_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_LEASE_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_INVOCATION_PHASE_FIELD_NUMBER: _ClassVar[int]
    generation_id: str
    trace_id: str
    event_id: str
    expected_attempt: int
    attempt_origin: str
    action_request_id: str
    expected_run_id: str
    expected_lease_expires_at: str
    expected_invocation_phase: str
    def __init__(self, generation_id: _Optional[str] = ..., trace_id: _Optional[str] = ..., event_id: _Optional[str] = ..., expected_attempt: _Optional[int] = ..., attempt_origin: _Optional[str] = ..., action_request_id: _Optional[str] = ..., expected_run_id: _Optional[str] = ..., expected_lease_expires_at: _Optional[str] = ..., expected_invocation_phase: _Optional[str] = ...) -> None: ...

class ExecuteAIExplanationResponse(_message.Message):
    __slots__ = ("success", "status", "generation_id", "run_id", "artifact_id", "failure_kind", "failure_code", "safe_message", "retryable")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    FAILURE_KIND_FIELD_NUMBER: _ClassVar[int]
    FAILURE_CODE_FIELD_NUMBER: _ClassVar[int]
    SAFE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    status: str
    generation_id: str
    run_id: str
    artifact_id: str
    failure_kind: str
    failure_code: str
    safe_message: str
    retryable: bool
    def __init__(self, success: _Optional[bool] = ..., status: _Optional[str] = ..., generation_id: _Optional[str] = ..., run_id: _Optional[str] = ..., artifact_id: _Optional[str] = ..., failure_kind: _Optional[str] = ..., failure_code: _Optional[str] = ..., safe_message: _Optional[str] = ..., retryable: _Optional[bool] = ...) -> None: ...

class ExecutePromptEvaluationStepRequest(_message.Message):
    __slots__ = ("org_id", "run_id", "case_id", "attempt", "requested_by", "event_id", "recheck_id", "evidence_version", "execution_kind", "slot_ordinal", "candidate_id", "execution_ordinal")
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    RECHECK_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_KIND_FIELD_NUMBER: _ClassVar[int]
    SLOT_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    org_id: int
    run_id: str
    case_id: str
    attempt: int
    requested_by: str
    event_id: str
    recheck_id: str
    evidence_version: str
    execution_kind: str
    slot_ordinal: int
    candidate_id: str
    execution_ordinal: int
    def __init__(self, org_id: _Optional[int] = ..., run_id: _Optional[str] = ..., case_id: _Optional[str] = ..., attempt: _Optional[int] = ..., requested_by: _Optional[str] = ..., event_id: _Optional[str] = ..., recheck_id: _Optional[str] = ..., evidence_version: _Optional[str] = ..., execution_kind: _Optional[str] = ..., slot_ordinal: _Optional[int] = ..., candidate_id: _Optional[str] = ..., execution_ordinal: _Optional[int] = ...) -> None: ...

class ExecutePromptEvaluationStepResponse(_message.Message):
    __slots__ = ("success", "run_id", "case_id", "attempt", "status", "run_status", "next_case_id", "next_attempt", "recheck_id", "evidence_version", "execution_kind", "slot_ordinal", "candidate_id", "execution_ordinal")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    CASE_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RUN_STATUS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CASE_ID_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    RECHECK_ID_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_KIND_FIELD_NUMBER: _ClassVar[int]
    SLOT_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ORDINAL_FIELD_NUMBER: _ClassVar[int]
    success: bool
    run_id: str
    case_id: str
    attempt: int
    status: str
    run_status: str
    next_case_id: str
    next_attempt: int
    recheck_id: str
    evidence_version: str
    execution_kind: str
    slot_ordinal: int
    candidate_id: str
    execution_ordinal: int
    def __init__(self, success: _Optional[bool] = ..., run_id: _Optional[str] = ..., case_id: _Optional[str] = ..., attempt: _Optional[int] = ..., status: _Optional[str] = ..., run_status: _Optional[str] = ..., next_case_id: _Optional[str] = ..., next_attempt: _Optional[int] = ..., recheck_id: _Optional[str] = ..., evidence_version: _Optional[str] = ..., execution_kind: _Optional[str] = ..., slot_ordinal: _Optional[int] = ..., candidate_id: _Optional[str] = ..., execution_ordinal: _Optional[int] = ...) -> None: ...

class AIExplanationEvidenceRef(_message.Message):
    __slots__ = ("kind", "ref")
    KIND_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    kind: str
    ref: str
    def __init__(self, kind: _Optional[str] = ..., ref: _Optional[str] = ...) -> None: ...

class AIExplanationIntegratedInsight(_message.Message):
    __slots__ = ("kind", "title", "content", "why_it_matters", "evidence_refs")
    KIND_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    WHY_IT_MATTERS_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_REFS_FIELD_NUMBER: _ClassVar[int]
    kind: str
    title: str
    content: str
    why_it_matters: str
    evidence_refs: _containers.RepeatedCompositeFieldContainer[AIExplanationEvidenceRef]
    def __init__(self, kind: _Optional[str] = ..., title: _Optional[str] = ..., content: _Optional[str] = ..., why_it_matters: _Optional[str] = ..., evidence_refs: _Optional[_Iterable[_Union[AIExplanationEvidenceRef, _Mapping]]] = ...) -> None: ...

class AIExplanationSuggestion(_message.Message):
    __slots__ = ("origin", "category", "title", "goal", "actions", "rationale", "evidence_refs", "source_suggestion_refs", "caution")
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    GOAL_FIELD_NUMBER: _ClassVar[int]
    ACTIONS_FIELD_NUMBER: _ClassVar[int]
    RATIONALE_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_REFS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SUGGESTION_REFS_FIELD_NUMBER: _ClassVar[int]
    CAUTION_FIELD_NUMBER: _ClassVar[int]
    origin: str
    category: str
    title: str
    goal: str
    actions: _containers.RepeatedScalarFieldContainer[str]
    rationale: str
    evidence_refs: _containers.RepeatedCompositeFieldContainer[AIExplanationEvidenceRef]
    source_suggestion_refs: _containers.RepeatedScalarFieldContainer[str]
    caution: str
    def __init__(self, origin: _Optional[str] = ..., category: _Optional[str] = ..., title: _Optional[str] = ..., goal: _Optional[str] = ..., actions: _Optional[_Iterable[str]] = ..., rationale: _Optional[str] = ..., evidence_refs: _Optional[_Iterable[_Union[AIExplanationEvidenceRef, _Mapping]]] = ..., source_suggestion_refs: _Optional[_Iterable[str]] = ..., caution: _Optional[str] = ...) -> None: ...

class AIExplanationContent(_message.Message):
    __slots__ = ("schema_version", "summary", "integrated_insights", "suggestions", "limitations")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    INTEGRATED_INSIGHTS_FIELD_NUMBER: _ClassVar[int]
    SUGGESTIONS_FIELD_NUMBER: _ClassVar[int]
    LIMITATIONS_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    summary: str
    integrated_insights: _containers.RepeatedCompositeFieldContainer[AIExplanationIntegratedInsight]
    suggestions: _containers.RepeatedCompositeFieldContainer[AIExplanationSuggestion]
    limitations: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, schema_version: _Optional[str] = ..., summary: _Optional[str] = ..., integrated_insights: _Optional[_Iterable[_Union[AIExplanationIntegratedInsight, _Mapping]]] = ..., suggestions: _Optional[_Iterable[_Union[AIExplanationSuggestion, _Mapping]]] = ..., limitations: _Optional[_Iterable[str]] = ...) -> None: ...

class AIExplanationFailure(_message.Message):
    __slots__ = ("code", "safe_message", "retryable")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SAFE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    code: str
    safe_message: str
    retryable: bool
    def __init__(self, code: _Optional[str] = ..., safe_message: _Optional[str] = ..., retryable: _Optional[bool] = ...) -> None: ...

class GetAIExplanationCapabilityRequest(_message.Message):
    __slots__ = ("assessment_id", "testee_id", "locale", "focus_areas")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    FOCUS_AREAS_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    testee_id: int
    locale: str
    focus_areas: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, assessment_id: _Optional[int] = ..., testee_id: _Optional[int] = ..., locale: _Optional[str] = ..., focus_areas: _Optional[_Iterable[str]] = ...) -> None: ...

class RequestAIExplanationRequest(_message.Message):
    __slots__ = ("assessment_id", "testee_id", "locale", "focus_areas")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    LOCALE_FIELD_NUMBER: _ClassVar[int]
    FOCUS_AREAS_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    testee_id: int
    locale: str
    focus_areas: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, assessment_id: _Optional[int] = ..., testee_id: _Optional[int] = ..., locale: _Optional[str] = ..., focus_areas: _Optional[_Iterable[str]] = ...) -> None: ...

class GetAIExplanationRequest(_message.Message):
    __slots__ = ("assessment_id", "testee_id", "generation_id")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    testee_id: int
    generation_id: str
    def __init__(self, assessment_id: _Optional[int] = ..., testee_id: _Optional[int] = ..., generation_id: _Optional[str] = ...) -> None: ...

class AIExplanationResponse(_message.Message):
    __slots__ = ("status", "reason_code", "generation_id", "artifact_id", "source_report_id", "source_state", "content", "failure", "created_at", "updated_at")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REASON_CODE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_STATE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    status: str
    reason_code: str
    generation_id: str
    artifact_id: str
    source_report_id: str
    source_state: str
    content: AIExplanationContent
    failure: AIExplanationFailure
    created_at: str
    updated_at: str
    def __init__(self, status: _Optional[str] = ..., reason_code: _Optional[str] = ..., generation_id: _Optional[str] = ..., artifact_id: _Optional[str] = ..., source_report_id: _Optional[str] = ..., source_state: _Optional[str] = ..., content: _Optional[_Union[AIExplanationContent, _Mapping]] = ..., failure: _Optional[_Union[AIExplanationFailure, _Mapping]] = ..., created_at: _Optional[str] = ..., updated_at: _Optional[str] = ...) -> None: ...

class ExportAIExplanationsRequest(_message.Message):
    __slots__ = ("testee_id", "page_size", "cursor")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    CURSOR_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    page_size: int
    cursor: str
    def __init__(self, testee_id: _Optional[int] = ..., page_size: _Optional[int] = ..., cursor: _Optional[str] = ...) -> None: ...

class AIExplanationExportSourceReceipt(_message.Message):
    __slots__ = ("assessment_id", "report_id", "outcome_id", "report_type", "template_version", "content_schema_version", "builder_identity", "report_generated_at")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    BUILDER_IDENTITY_FIELD_NUMBER: _ClassVar[int]
    REPORT_GENERATED_AT_FIELD_NUMBER: _ClassVar[int]
    assessment_id: str
    report_id: str
    outcome_id: str
    report_type: str
    template_version: str
    content_schema_version: str
    builder_identity: str
    report_generated_at: str
    def __init__(self, assessment_id: _Optional[str] = ..., report_id: _Optional[str] = ..., outcome_id: _Optional[str] = ..., report_type: _Optional[str] = ..., template_version: _Optional[str] = ..., content_schema_version: _Optional[str] = ..., builder_identity: _Optional[str] = ..., report_generated_at: _Optional[str] = ...) -> None: ...

class AIExplanationExportReleaseReceipt(_message.Message):
    __slots__ = ("profile_id", "profile_version", "profile_fingerprint", "prompt_template_id", "prompt_version", "prompt_fingerprint", "prompt_git_blob_sha", "provider_route", "provider_route_revision", "resolved_provider", "resolved_model", "execution_spec_fingerprint", "input_schema", "output_schema", "safety_policy", "schema_validator_version", "reference_validator_version", "profile_validator_version", "safety_validator_version", "validated_at")
    PROFILE_ID_FIELD_NUMBER: _ClassVar[int]
    PROFILE_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROFILE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    PROMPT_TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    PROMPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    PROMPT_GIT_BLOB_SHA_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ROUTE_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ROUTE_REVISION_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_PROVIDER_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_MODEL_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_SPEC_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SAFETY_POLICY_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VALIDATOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_VALIDATOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROFILE_VALIDATOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    SAFETY_VALIDATOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    VALIDATED_AT_FIELD_NUMBER: _ClassVar[int]
    profile_id: str
    profile_version: str
    profile_fingerprint: str
    prompt_template_id: str
    prompt_version: str
    prompt_fingerprint: str
    prompt_git_blob_sha: str
    provider_route: str
    provider_route_revision: str
    resolved_provider: str
    resolved_model: str
    execution_spec_fingerprint: str
    input_schema: str
    output_schema: str
    safety_policy: str
    schema_validator_version: str
    reference_validator_version: str
    profile_validator_version: str
    safety_validator_version: str
    validated_at: str
    def __init__(self, profile_id: _Optional[str] = ..., profile_version: _Optional[str] = ..., profile_fingerprint: _Optional[str] = ..., prompt_template_id: _Optional[str] = ..., prompt_version: _Optional[str] = ..., prompt_fingerprint: _Optional[str] = ..., prompt_git_blob_sha: _Optional[str] = ..., provider_route: _Optional[str] = ..., provider_route_revision: _Optional[str] = ..., resolved_provider: _Optional[str] = ..., resolved_model: _Optional[str] = ..., execution_spec_fingerprint: _Optional[str] = ..., input_schema: _Optional[str] = ..., output_schema: _Optional[str] = ..., safety_policy: _Optional[str] = ..., schema_validator_version: _Optional[str] = ..., reference_validator_version: _Optional[str] = ..., profile_validator_version: _Optional[str] = ..., safety_validator_version: _Optional[str] = ..., validated_at: _Optional[str] = ...) -> None: ...

class AIExplanationSubjectExportItem(_message.Message):
    __slots__ = ("generation_id", "artifact_id", "source", "release", "content", "generated_at")
    GENERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    GENERATED_AT_FIELD_NUMBER: _ClassVar[int]
    generation_id: str
    artifact_id: str
    source: AIExplanationExportSourceReceipt
    release: AIExplanationExportReleaseReceipt
    content: AIExplanationContent
    generated_at: str
    def __init__(self, generation_id: _Optional[str] = ..., artifact_id: _Optional[str] = ..., source: _Optional[_Union[AIExplanationExportSourceReceipt, _Mapping]] = ..., release: _Optional[_Union[AIExplanationExportReleaseReceipt, _Mapping]] = ..., content: _Optional[_Union[AIExplanationContent, _Mapping]] = ..., generated_at: _Optional[str] = ...) -> None: ...

class AIExplanationSubjectExportResponse(_message.Message):
    __slots__ = ("schema_version", "org_id", "testee_id", "exported_at", "snapshot_at", "items", "next_cursor")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    EXPORTED_AT_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_AT_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_CURSOR_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    org_id: int
    testee_id: int
    exported_at: str
    snapshot_at: str
    items: _containers.RepeatedCompositeFieldContainer[AIExplanationSubjectExportItem]
    next_cursor: str
    def __init__(self, schema_version: _Optional[str] = ..., org_id: _Optional[int] = ..., testee_id: _Optional[int] = ..., exported_at: _Optional[str] = ..., snapshot_at: _Optional[str] = ..., items: _Optional[_Iterable[_Union[AIExplanationSubjectExportItem, _Mapping]]] = ..., next_cursor: _Optional[str] = ...) -> None: ...
