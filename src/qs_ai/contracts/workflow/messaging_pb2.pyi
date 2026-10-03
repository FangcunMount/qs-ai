from qs_ai.contracts.workflow import workflow_pb2 as _workflow_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MessagingKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MESSAGING_KIND_UNSPECIFIED: _ClassVar[MessagingKind]
    START: _ClassVar[MessagingKind]
    CHANGE: _ClassVar[MessagingKind]
    PARTICIPANT_RETRY: _ClassVar[MessagingKind]
    EVALUATION_START: _ClassVar[MessagingKind]
    EVALUATION_CANCEL: _ClassVar[MessagingKind]
    COMMAND_RECEIPT: _ClassVar[MessagingKind]
    INTERPRETATION_STATE: _ClassVar[MessagingKind]
    EVALUATION_STATE: _ClassVar[MessagingKind]
    EVENT_ACKNOWLEDGEMENT: _ClassVar[MessagingKind]

class MessagingDecision(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MESSAGING_DECISION_UNSPECIFIED: _ClassVar[MessagingDecision]
    ACCEPTED: _ClassVar[MessagingDecision]
    REJECTED: _ClassVar[MessagingDecision]
    HELD: _ClassVar[MessagingDecision]
MESSAGING_KIND_UNSPECIFIED: MessagingKind
START: MessagingKind
CHANGE: MessagingKind
PARTICIPANT_RETRY: MessagingKind
EVALUATION_START: MessagingKind
EVALUATION_CANCEL: MessagingKind
COMMAND_RECEIPT: MessagingKind
INTERPRETATION_STATE: MessagingKind
EVALUATION_STATE: MessagingKind
EVENT_ACKNOWLEDGEMENT: MessagingKind
MESSAGING_DECISION_UNSPECIFIED: MessagingDecision
ACCEPTED: MessagingDecision
REJECTED: MessagingDecision
HELD: MessagingDecision

class MessagingEnvelope(_message.Message):
    __slots__ = ("schema_version", "producer", "destination", "message_id", "kind", "aggregate_key", "correlation_command_id", "body_sha256", "body_length", "inline_body", "payload_reference", "original_occurred_at")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    AGGREGATE_KEY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    BODY_SHA256_FIELD_NUMBER: _ClassVar[int]
    BODY_LENGTH_FIELD_NUMBER: _ClassVar[int]
    INLINE_BODY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_REFERENCE_FIELD_NUMBER: _ClassVar[int]
    ORIGINAL_OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    schema_version: str
    producer: str
    destination: str
    message_id: str
    kind: MessagingKind
    aggregate_key: str
    correlation_command_id: str
    body_sha256: str
    body_length: int
    inline_body: bytes
    payload_reference: MessagePayloadReference
    original_occurred_at: str
    def __init__(self, schema_version: _Optional[str] = ..., producer: _Optional[str] = ..., destination: _Optional[str] = ..., message_id: _Optional[str] = ..., kind: _Optional[_Union[MessagingKind, str]] = ..., aggregate_key: _Optional[str] = ..., correlation_command_id: _Optional[str] = ..., body_sha256: _Optional[str] = ..., body_length: _Optional[int] = ..., inline_body: _Optional[bytes] = ..., payload_reference: _Optional[_Union[MessagePayloadReference, _Mapping]] = ..., original_occurred_at: _Optional[str] = ...) -> None: ...

class MessagePayloadReference(_message.Message):
    __slots__ = ("producer", "destination", "message_id", "body_sha256", "body_length", "organization_id")
    PRODUCER_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    BODY_SHA256_FIELD_NUMBER: _ClassVar[int]
    BODY_LENGTH_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    producer: str
    destination: str
    message_id: str
    body_sha256: str
    body_length: int
    organization_id: str
    def __init__(self, producer: _Optional[str] = ..., destination: _Optional[str] = ..., message_id: _Optional[str] = ..., body_sha256: _Optional[str] = ..., body_length: _Optional[int] = ..., organization_id: _Optional[str] = ...) -> None: ...

class MessagingBody(_message.Message):
    __slots__ = ("start", "change", "participant_retry", "evaluation_start", "evaluation_cancel", "command_receipt", "interpretation_state", "evaluation_state", "event_acknowledgement")
    START_FIELD_NUMBER: _ClassVar[int]
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    PARTICIPANT_RETRY_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_START_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_CANCEL_FIELD_NUMBER: _ClassVar[int]
    COMMAND_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    INTERPRETATION_STATE_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_STATE_FIELD_NUMBER: _ClassVar[int]
    EVENT_ACKNOWLEDGEMENT_FIELD_NUMBER: _ClassVar[int]
    start: _workflow_pb2.StartCommand
    change: _workflow_pb2.ChangeCommand
    participant_retry: _workflow_pb2.ParticipantRetryCommand
    evaluation_start: _workflow_pb2.EvaluationStartCommand
    evaluation_cancel: _workflow_pb2.EvaluationCancelCommand
    command_receipt: MessagingCommandReceipt
    interpretation_state: _workflow_pb2.StateEvent
    evaluation_state: EvaluationRuntimeState
    event_acknowledgement: MessagingEventAcknowledgement
    def __init__(self, start: _Optional[_Union[_workflow_pb2.StartCommand, _Mapping]] = ..., change: _Optional[_Union[_workflow_pb2.ChangeCommand, _Mapping]] = ..., participant_retry: _Optional[_Union[_workflow_pb2.ParticipantRetryCommand, _Mapping]] = ..., evaluation_start: _Optional[_Union[_workflow_pb2.EvaluationStartCommand, _Mapping]] = ..., evaluation_cancel: _Optional[_Union[_workflow_pb2.EvaluationCancelCommand, _Mapping]] = ..., command_receipt: _Optional[_Union[MessagingCommandReceipt, _Mapping]] = ..., interpretation_state: _Optional[_Union[_workflow_pb2.StateEvent, _Mapping]] = ..., evaluation_state: _Optional[_Union[EvaluationRuntimeState, _Mapping]] = ..., event_acknowledgement: _Optional[_Union[MessagingEventAcknowledgement, _Mapping]] = ...) -> None: ...

class MessagingCommandReceipt(_message.Message):
    __slots__ = ("command_id", "command_body_sha256", "decision", "code", "grpc_status_code", "workflow_receipt", "evaluation_receipt")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_BODY_SHA256_FIELD_NUMBER: _ClassVar[int]
    DECISION_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    GRPC_STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    WORKFLOW_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_RECEIPT_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    command_body_sha256: str
    decision: MessagingDecision
    code: str
    grpc_status_code: int
    workflow_receipt: _workflow_pb2.Receipt
    evaluation_receipt: _workflow_pb2.EvaluationState
    def __init__(self, command_id: _Optional[str] = ..., command_body_sha256: _Optional[str] = ..., decision: _Optional[_Union[MessagingDecision, str]] = ..., code: _Optional[str] = ..., grpc_status_code: _Optional[int] = ..., workflow_receipt: _Optional[_Union[_workflow_pb2.Receipt, _Mapping]] = ..., evaluation_receipt: _Optional[_Union[_workflow_pb2.EvaluationState, _Mapping]] = ...) -> None: ...

class EvaluationRuntimeState(_message.Message):
    __slots__ = ("run_id", "organization_id", "version", "event_sequence", "status", "release_fingerprint")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ORGANIZATION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    EVENT_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RELEASE_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    organization_id: str
    version: int
    event_sequence: int
    status: str
    release_fingerprint: str
    def __init__(self, run_id: _Optional[str] = ..., organization_id: _Optional[str] = ..., version: _Optional[int] = ..., event_sequence: _Optional[int] = ..., status: _Optional[str] = ..., release_fingerprint: _Optional[str] = ...) -> None: ...

class MessagingEventAcknowledgement(_message.Message):
    __slots__ = ("event_id", "event_body_sha256", "event_kind", "outcome")
    class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        OUTCOME_UNSPECIFIED: _ClassVar[MessagingEventAcknowledgement.Outcome]
        STORED: _ClassVar[MessagingEventAcknowledgement.Outcome]
        TECHNICALLY_HELD: _ClassVar[MessagingEventAcknowledgement.Outcome]
    OUTCOME_UNSPECIFIED: MessagingEventAcknowledgement.Outcome
    STORED: MessagingEventAcknowledgement.Outcome
    TECHNICALLY_HELD: MessagingEventAcknowledgement.Outcome
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_BODY_SHA256_FIELD_NUMBER: _ClassVar[int]
    EVENT_KIND_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    event_body_sha256: str
    event_kind: MessagingKind
    outcome: MessagingEventAcknowledgement.Outcome
    def __init__(self, event_id: _Optional[str] = ..., event_body_sha256: _Optional[str] = ..., event_kind: _Optional[_Union[MessagingKind, str]] = ..., outcome: _Optional[_Union[MessagingEventAcknowledgement.Outcome, str]] = ...) -> None: ...

class MessagePayload(_message.Message):
    __slots__ = ("reference", "body")
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    reference: MessagePayloadReference
    body: bytes
    def __init__(self, reference: _Optional[_Union[MessagePayloadReference, _Mapping]] = ..., body: _Optional[bytes] = ...) -> None: ...
