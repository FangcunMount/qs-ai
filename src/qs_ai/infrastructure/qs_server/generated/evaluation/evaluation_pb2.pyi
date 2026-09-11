from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ModelIdentity(_message.Message):
    __slots__ = ("kind", "algorithm", "code", "version", "title", "decision_kind")
    KIND_FIELD_NUMBER: _ClassVar[int]
    ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    DECISION_KIND_FIELD_NUMBER: _ClassVar[int]
    kind: str
    algorithm: str
    code: str
    version: str
    title: str
    decision_kind: str
    def __init__(self, kind: _Optional[str] = ..., algorithm: _Optional[str] = ..., code: _Optional[str] = ..., version: _Optional[str] = ..., title: _Optional[str] = ..., decision_kind: _Optional[str] = ...) -> None: ...

class ScoreValue(_message.Message):
    __slots__ = ("kind", "value", "label", "max")
    KIND_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    kind: str
    value: float
    label: str
    max: float
    def __init__(self, kind: _Optional[str] = ..., value: _Optional[float] = ..., label: _Optional[str] = ..., max: _Optional[float] = ...) -> None: ...

class ResultLevel(_message.Message):
    __slots__ = ("code", "label", "severity")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    code: str
    label: str
    severity: str
    def __init__(self, code: _Optional[str] = ..., label: _Optional[str] = ..., severity: _Optional[str] = ...) -> None: ...

class AssessmentSummary(_message.Message):
    __slots__ = ("id", "questionnaire_code", "questionnaire_version", "origin_type", "status", "created_at", "submitted_at", "answer_sheet_id", "model", "primary_score", "level")
    ID_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_CODE_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    ANSWER_SHEET_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_SCORE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    id: int
    questionnaire_code: str
    questionnaire_version: str
    origin_type: str
    status: str
    created_at: str
    submitted_at: str
    answer_sheet_id: int
    model: ModelIdentity
    primary_score: ScoreValue
    level: ResultLevel
    def __init__(self, id: _Optional[int] = ..., questionnaire_code: _Optional[str] = ..., questionnaire_version: _Optional[str] = ..., origin_type: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[str] = ..., submitted_at: _Optional[str] = ..., answer_sheet_id: _Optional[int] = ..., model: _Optional[_Union[ModelIdentity, _Mapping]] = ..., primary_score: _Optional[_Union[ScoreValue, _Mapping]] = ..., level: _Optional[_Union[ResultLevel, _Mapping]] = ...) -> None: ...

class AssessmentDetail(_message.Message):
    __slots__ = ("id", "org_id", "testee_id", "questionnaire_code", "questionnaire_version", "answer_sheet_id", "origin_type", "origin_id", "status", "created_at", "submitted_at", "failed_at", "failure_reason", "model", "primary_score", "level")
    ID_FIELD_NUMBER: _ClassVar[int]
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_CODE_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ANSWER_SHEET_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_SCORE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    id: int
    org_id: int
    testee_id: int
    questionnaire_code: str
    questionnaire_version: str
    answer_sheet_id: int
    origin_type: str
    origin_id: str
    status: str
    created_at: str
    submitted_at: str
    failed_at: str
    failure_reason: str
    model: ModelIdentity
    primary_score: ScoreValue
    level: ResultLevel
    def __init__(self, id: _Optional[int] = ..., org_id: _Optional[int] = ..., testee_id: _Optional[int] = ..., questionnaire_code: _Optional[str] = ..., questionnaire_version: _Optional[str] = ..., answer_sheet_id: _Optional[int] = ..., origin_type: _Optional[str] = ..., origin_id: _Optional[str] = ..., status: _Optional[str] = ..., created_at: _Optional[str] = ..., submitted_at: _Optional[str] = ..., failed_at: _Optional[str] = ..., failure_reason: _Optional[str] = ..., model: _Optional[_Union[ModelIdentity, _Mapping]] = ..., primary_score: _Optional[_Union[ScoreValue, _Mapping]] = ..., level: _Optional[_Union[ResultLevel, _Mapping]] = ...) -> None: ...

class FactorScore(_message.Message):
    __slots__ = ("factor_code", "factor_name", "raw_score", "risk_level", "is_total_score")
    FACTOR_CODE_FIELD_NUMBER: _ClassVar[int]
    FACTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    RAW_SCORE_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    IS_TOTAL_SCORE_FIELD_NUMBER: _ClassVar[int]
    factor_code: str
    factor_name: str
    raw_score: float
    risk_level: str
    is_total_score: bool
    def __init__(self, factor_code: _Optional[str] = ..., factor_name: _Optional[str] = ..., raw_score: _Optional[float] = ..., risk_level: _Optional[str] = ..., is_total_score: _Optional[bool] = ...) -> None: ...

class TrendPoint(_message.Message):
    __slots__ = ("assessment_id", "score", "risk_level", "created_at")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    score: float
    risk_level: str
    created_at: str
    def __init__(self, assessment_id: _Optional[int] = ..., score: _Optional[float] = ..., risk_level: _Optional[str] = ..., created_at: _Optional[str] = ...) -> None: ...

class AuthorizeAssessmentRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ...) -> None: ...

class AuthorizeAssessmentResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetMyAssessmentRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ...) -> None: ...

class GetMyAssessmentResponse(_message.Message):
    __slots__ = ("assessment",)
    ASSESSMENT_FIELD_NUMBER: _ClassVar[int]
    assessment: AssessmentDetail
    def __init__(self, assessment: _Optional[_Union[AssessmentDetail, _Mapping]] = ...) -> None: ...

class ResolveAssessmentByAnswerSheetIDRequest(_message.Message):
    __slots__ = ("answer_sheet_id",)
    ANSWER_SHEET_ID_FIELD_NUMBER: _ClassVar[int]
    answer_sheet_id: int
    def __init__(self, answer_sheet_id: _Optional[int] = ...) -> None: ...

class ResolveAssessmentByAnswerSheetIDResponse(_message.Message):
    __slots__ = ("testee_id", "assessment_id", "readiness_phase", "assessment_status", "failure_reason")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    READINESS_PHASE_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_STATUS_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    readiness_phase: str
    assessment_status: str
    failure_reason: str
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ..., readiness_phase: _Optional[str] = ..., assessment_status: _Optional[str] = ..., failure_reason: _Optional[str] = ...) -> None: ...

class EnsureAssessmentRequest(_message.Message):
    __slots__ = ("org_id", "answer_sheet_id", "questionnaire_code", "questionnaire_version", "testee_id", "filler_id", "task_id", "origin_type", "origin_id", "admission")
    ORG_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_SHEET_ID_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_CODE_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_VERSION_FIELD_NUMBER: _ClassVar[int]
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    FILLER_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_FIELD_NUMBER: _ClassVar[int]
    org_id: int
    answer_sheet_id: int
    questionnaire_code: str
    questionnaire_version: str
    testee_id: int
    filler_id: int
    task_id: str
    origin_type: str
    origin_id: str
    admission: AssessmentAdmission
    def __init__(self, org_id: _Optional[int] = ..., answer_sheet_id: _Optional[int] = ..., questionnaire_code: _Optional[str] = ..., questionnaire_version: _Optional[str] = ..., testee_id: _Optional[int] = ..., filler_id: _Optional[int] = ..., task_id: _Optional[str] = ..., origin_type: _Optional[str] = ..., origin_id: _Optional[str] = ..., admission: _Optional[_Union[AssessmentAdmission, _Mapping]] = ...) -> None: ...

class AssessmentAdmission(_message.Message):
    __slots__ = ("purpose", "questionnaire_code", "questionnaire_version", "model_kind", "model_algorithm", "model_code", "model_version", "model_title")
    PURPOSE_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_CODE_FIELD_NUMBER: _ClassVar[int]
    QUESTIONNAIRE_VERSION_FIELD_NUMBER: _ClassVar[int]
    MODEL_KIND_FIELD_NUMBER: _ClassVar[int]
    MODEL_ALGORITHM_FIELD_NUMBER: _ClassVar[int]
    MODEL_CODE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    MODEL_TITLE_FIELD_NUMBER: _ClassVar[int]
    purpose: str
    questionnaire_code: str
    questionnaire_version: str
    model_kind: str
    model_algorithm: str
    model_code: str
    model_version: str
    model_title: str
    def __init__(self, purpose: _Optional[str] = ..., questionnaire_code: _Optional[str] = ..., questionnaire_version: _Optional[str] = ..., model_kind: _Optional[str] = ..., model_algorithm: _Optional[str] = ..., model_code: _Optional[str] = ..., model_version: _Optional[str] = ..., model_title: _Optional[str] = ...) -> None: ...

class EnsureAssessmentResponse(_message.Message):
    __slots__ = ("assessment_id", "created", "auto_submitted")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    AUTO_SUBMITTED_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    created: bool
    auto_submitted: bool
    def __init__(self, assessment_id: _Optional[int] = ..., created: _Optional[bool] = ..., auto_submitted: _Optional[bool] = ...) -> None: ...

class ExecuteEvaluationRequest(_message.Message):
    __slots__ = ("assessment_id",)
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    def __init__(self, assessment_id: _Optional[int] = ...) -> None: ...

class ExecuteEvaluationResponse(_message.Message):
    __slots__ = ("status", "model", "primary_score", "level", "outcome_id", "retryable", "run_id", "failure_kind", "failure_message", "trace_id", "input_snapshot_ref", "retry_disposition", "attempt_origin", "current_attempt", "max_automatic_attempts", "remaining_automatic_attempts", "next_attempt_at", "retry_event_id", "action_request_id")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_SCORE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_ID_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    FAILURE_KIND_FIELD_NUMBER: _ClassVar[int]
    FAILURE_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_SNAPSHOT_REF_FIELD_NUMBER: _ClassVar[int]
    RETRY_DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_ORIGIN_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    MAX_AUTOMATIC_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    REMAINING_AUTOMATIC_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    RETRY_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    status: str
    model: ModelIdentity
    primary_score: ScoreValue
    level: ResultLevel
    outcome_id: str
    retryable: bool
    run_id: str
    failure_kind: str
    failure_message: str
    trace_id: str
    input_snapshot_ref: str
    retry_disposition: str
    attempt_origin: str
    current_attempt: int
    max_automatic_attempts: int
    remaining_automatic_attempts: int
    next_attempt_at: str
    retry_event_id: str
    action_request_id: str
    def __init__(self, status: _Optional[str] = ..., model: _Optional[_Union[ModelIdentity, _Mapping]] = ..., primary_score: _Optional[_Union[ScoreValue, _Mapping]] = ..., level: _Optional[_Union[ResultLevel, _Mapping]] = ..., outcome_id: _Optional[str] = ..., retryable: _Optional[bool] = ..., run_id: _Optional[str] = ..., failure_kind: _Optional[str] = ..., failure_message: _Optional[str] = ..., trace_id: _Optional[str] = ..., input_snapshot_ref: _Optional[str] = ..., retry_disposition: _Optional[str] = ..., attempt_origin: _Optional[str] = ..., current_attempt: _Optional[int] = ..., max_automatic_attempts: _Optional[int] = ..., remaining_automatic_attempts: _Optional[int] = ..., next_attempt_at: _Optional[str] = ..., retry_event_id: _Optional[str] = ..., action_request_id: _Optional[str] = ...) -> None: ...

class ListMyAssessmentsRequest(_message.Message):
    __slots__ = ("testee_id", "status", "page", "page_size", "scale_code", "risk_level", "date_from", "date_to", "model_kind", "model_code", "model_kinds")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    SCALE_CODE_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    DATE_FROM_FIELD_NUMBER: _ClassVar[int]
    DATE_TO_FIELD_NUMBER: _ClassVar[int]
    MODEL_KIND_FIELD_NUMBER: _ClassVar[int]
    MODEL_CODE_FIELD_NUMBER: _ClassVar[int]
    MODEL_KINDS_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    status: str
    page: int
    page_size: int
    scale_code: str
    risk_level: str
    date_from: str
    date_to: str
    model_kind: str
    model_code: str
    model_kinds: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, testee_id: _Optional[int] = ..., status: _Optional[str] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., scale_code: _Optional[str] = ..., risk_level: _Optional[str] = ..., date_from: _Optional[str] = ..., date_to: _Optional[str] = ..., model_kind: _Optional[str] = ..., model_code: _Optional[str] = ..., model_kinds: _Optional[_Iterable[str]] = ...) -> None: ...

class ListMyAssessmentsResponse(_message.Message):
    __slots__ = ("items", "total", "page", "page_size", "total_pages")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_PAGES_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[AssessmentSummary]
    total: int
    page: int
    page_size: int
    total_pages: int
    def __init__(self, items: _Optional[_Iterable[_Union[AssessmentSummary, _Mapping]]] = ..., total: _Optional[int] = ..., page: _Optional[int] = ..., page_size: _Optional[int] = ..., total_pages: _Optional[int] = ...) -> None: ...

class GetAssessmentScoresRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ...) -> None: ...

class GetAssessmentScoresResponse(_message.Message):
    __slots__ = ("assessment_id", "total_score", "risk_level", "factor_scores")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TOTAL_SCORE_FIELD_NUMBER: _ClassVar[int]
    RISK_LEVEL_FIELD_NUMBER: _ClassVar[int]
    FACTOR_SCORES_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    total_score: float
    risk_level: str
    factor_scores: _containers.RepeatedCompositeFieldContainer[FactorScore]
    def __init__(self, assessment_id: _Optional[int] = ..., total_score: _Optional[float] = ..., risk_level: _Optional[str] = ..., factor_scores: _Optional[_Iterable[_Union[FactorScore, _Mapping]]] = ...) -> None: ...

class GetFactorTrendRequest(_message.Message):
    __slots__ = ("testee_id", "factor_code", "limit")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    FACTOR_CODE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    factor_code: str
    limit: int
    def __init__(self, testee_id: _Optional[int] = ..., factor_code: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class GetFactorTrendResponse(_message.Message):
    __slots__ = ("testee_id", "factor_code", "factor_name", "data_points")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    FACTOR_CODE_FIELD_NUMBER: _ClassVar[int]
    FACTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    DATA_POINTS_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    factor_code: str
    factor_name: str
    data_points: _containers.RepeatedCompositeFieldContainer[TrendPoint]
    def __init__(self, testee_id: _Optional[int] = ..., factor_code: _Optional[str] = ..., factor_name: _Optional[str] = ..., data_points: _Optional[_Iterable[_Union[TrendPoint, _Mapping]]] = ...) -> None: ...

class GetHighRiskFactorsRequest(_message.Message):
    __slots__ = ("testee_id", "assessment_id")
    TESTEE_ID_FIELD_NUMBER: _ClassVar[int]
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    testee_id: int
    assessment_id: int
    def __init__(self, testee_id: _Optional[int] = ..., assessment_id: _Optional[int] = ...) -> None: ...

class GetHighRiskFactorsResponse(_message.Message):
    __slots__ = ("assessment_id", "has_high_risk", "high_risk_factors", "needs_urgent_care")
    ASSESSMENT_ID_FIELD_NUMBER: _ClassVar[int]
    HAS_HIGH_RISK_FIELD_NUMBER: _ClassVar[int]
    HIGH_RISK_FACTORS_FIELD_NUMBER: _ClassVar[int]
    NEEDS_URGENT_CARE_FIELD_NUMBER: _ClassVar[int]
    assessment_id: int
    has_high_risk: bool
    high_risk_factors: _containers.RepeatedCompositeFieldContainer[FactorScore]
    needs_urgent_care: bool
    def __init__(self, assessment_id: _Optional[int] = ..., has_high_risk: _Optional[bool] = ..., high_risk_factors: _Optional[_Iterable[_Union[FactorScore, _Mapping]]] = ..., needs_urgent_care: _Optional[bool] = ...) -> None: ...
