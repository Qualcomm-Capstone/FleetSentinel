from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RecordKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RECORD_KIND_UNSPECIFIED: _ClassVar[RecordKind]
    RECORD_KIND_SIGNAL: _ClassVar[RecordKind]
    RECORD_KIND_SEGMENT_REF: _ClassVar[RecordKind]
    RECORD_KIND_PERCEPTION: _ClassVar[RecordKind]
RECORD_KIND_UNSPECIFIED: RecordKind
RECORD_KIND_SIGNAL: RecordKind
RECORD_KIND_SEGMENT_REF: RecordKind
RECORD_KIND_PERCEPTION: RecordKind

class IngestRecord(_message.Message):
    __slots__ = ("seq", "kind", "payload")
    SEQ_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    seq: int
    kind: RecordKind
    payload: bytes
    def __init__(self, seq: _Optional[int] = ..., kind: _Optional[_Union[RecordKind, str]] = ..., payload: _Optional[bytes] = ...) -> None: ...

class Ack(_message.Message):
    __slots__ = ("ack_seq",)
    ACK_SEQ_FIELD_NUMBER: _ClassVar[int]
    ack_seq: int
    def __init__(self, ack_seq: _Optional[int] = ...) -> None: ...
