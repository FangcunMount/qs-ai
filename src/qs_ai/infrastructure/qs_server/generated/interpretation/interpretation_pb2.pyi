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

class RequestAIWorkflowRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id", "report_id", "request_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    report_id: int
    request_id: str
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ..., report_id: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class AIWorkflowAccepted(_message.Message):
    __slots__ = ("request_id", "status")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    status: str
    def __init__(self, request_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class AIWorkflowAccessRequest(_message.Message):
    __slots__ = ("org_id", "subject_id", "testee_id", "assessment_ids")
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    SUBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_IDS_FIELD_NUMBER: _ClassVar[int]
    org_id: str
    subject_id: str
    testee_id: str
    assessment_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, org_id: _Optional[str] = ..., subject_id: _Optional[str] = ..., testee_id: _Optional[str] = ..., assessment_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class AIWorkflowAccessResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetAIWorkflowRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id", "request_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    request_id: str
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ..., request_id: _Optional[str] = ...) -> None: ...

class AIWorkflowResult(_message.Message):
    __slots__ = ("request_id", "status", "version", "content_json", "artifact_id", "report_id", "source_version")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CONTENT_JSON_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_ID_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    status: str
    version: int
    content_json: str
    artifact_id: str
    report_id: str
    source_version: str
    def __init__(self, request_id: _Optional[str] = ..., status: _Optional[str] = ..., version: _Optional[int] = ..., content_json: _Optional[str] = ..., artifact_id: _Optional[str] = ..., report_id: _Optional[str] = ..., source_version: _Optional[str] = ...) -> None: ...

class GetAIWorkflowSourceRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ...) -> None: ...

class AIWorkflowSource(_message.Message):
    __slots__ = ("status", "report_id", "source_version")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_VERSION_FIELD_NUMBER: _ClassVar[int]
    status: str
    report_id: str
    source_version: str
    def __init__(self, status: _Optional[str] = ..., report_id: _Optional[str] = ..., source_version: _Optional[str] = ...) -> None: ...
