"""Typed application inputs; transport schemas and validation stay at their boundaries."""

from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class StartCommand:
    action: ClassVar[str] = "start"
    expected_version: int


@dataclass(frozen=True)
class AnswerCommand:
    action: ClassVar[str] = "answer"
    expected_version: int
    question_id: str
    answer: str | None = None
    skip: bool = False


@dataclass(frozen=True)
class CancelCommand:
    action: ClassVar[str] = "cancel"
    expected_version: int
