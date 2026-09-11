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
