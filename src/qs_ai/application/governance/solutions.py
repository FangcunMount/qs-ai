"""A persistent editing workspace; evaluation and publication remain authoritative."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.prompt_draft import PromptDraftContent, valid_reason


def title_required(value: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 100 or "\0" in value:
        raise ValueError("Title required within 100 characters")


def revision_required(value: int) -> None:
    if type(value) is not int or not 1 <= value < 2**63:
        raise ValueError("Positive expected revision required")


@dataclass(frozen=True, kw_only=True)
class Command:
    command_id: UUID
    reason: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.command_id, UUID)
            or not self.command_id.int
            or not valid_reason(self.reason)
        ):
            raise ValueError("Command identity and reason required")


@dataclass(frozen=True, kw_only=True)
class CreateSolution(Command):
    title: str
    publication_id: UUID | None = None
    source_run_id: UUID | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        title_required(self.title)
        if (self.publication_id is None) == (self.source_run_id is None):
            raise ValueError("Exactly one immutable source required")
        ref = self.publication_id or self.source_run_id
        if not isinstance(ref, UUID) or not ref.int:
            raise ValueError("Source identity required")


@dataclass(frozen=True)
class ModelSelection:
    model: str
    max_output_tokens: int
    timeout_milliseconds: int
    reasoning_effort: str
    model_key: str | None = None
    catalog_revision: str | None = None
    thinking: Literal["enabled", "disabled"] | None = None
    temperature: float | None = None
    top_p: float | None = None

    def __post_init__(self) -> None:
        if self.model_key is None and any(
            v is not None
            for v in (self.catalog_revision, self.thinking, self.temperature, self.top_p)
        ):
            raise ValueError("Explicit model identity required for v2 parameters")
        if self.model_key is not None and (
            not isinstance(self.model_key, str)
            or not self.model_key.strip()
            or not isinstance(self.catalog_revision, str)
            or not self.catalog_revision.strip()
        ):
            raise ValueError("Model identity and catalog revision required")
        if not isinstance(self.model, str) or not self.model.strip() or len(self.model) > 128:
            raise ValueError("Model required")
        if type(self.max_output_tokens) is not int or not 1 <= self.max_output_tokens <= 12000:
            raise ValueError("Invalid output limit")
        if (
            type(self.timeout_milliseconds) is not int
            or not 1000 <= self.timeout_milliseconds <= 180000
        ):
            raise ValueError("Invalid timeout")
        if self.reasoning_effort not in (
            "",
            "none",
            "minimal",
            "low",
            "medium",
            "high",
            "xhigh",
            "max",
        ):
            raise ValueError("Unsupported reasoning effort")


@dataclass(frozen=True, kw_only=True)
class SaveSolution(Command):
    expected_revision: int
    title: str
    content: PromptDraftContent
    generation: ModelSelection
    semantic: ModelSelection
    evaluation_suite: FrozenContractRef | None = None
    semantic_prompt: FrozenContractRef | None = None
    semantic_owner_organization_id: int = 0

    def __post_init__(self) -> None:
        super().__post_init__()
        title_required(self.title)
        revision_required(self.expected_revision)
        if any(
            value is not None and not isinstance(value, FrozenContractRef)
            for value in (self.evaluation_suite, self.semantic_prompt)
        ):
            raise ValueError("Exact optional evaluation asset reference required")
        if (
            type(self.semantic_owner_organization_id) is not int
            or not 0 <= self.semantic_owner_organization_id < 2**63
        ):
            raise ValueError("Invalid semantic owner")
        if self.semantic_prompt is None and self.semantic_owner_organization_id:
            raise ValueError("Semantic owner requires an explicit prompt")
        if not isinstance(self.content, PromptDraftContent) or not all(
            isinstance(v, ModelSelection) for v in (self.generation, self.semantic)
        ):
            raise ValueError("Prompt content and model selections required")


@dataclass(frozen=True, kw_only=True)
class PrepareSolution(Command):
    expected_revision: int

    def __post_init__(self) -> None:
        super().__post_init__()
        revision_required(self.expected_revision)


class SolutionStore(Protocol):
    def capabilities(self) -> dict[str, Any]: ...
    async def list(self, scope: DraftScope, cursor: str = "") -> dict[str, Any]: ...
    async def get(self, scope: DraftScope, solution_id: UUID) -> dict[str, Any]: ...
    async def receipt(self, scope: DraftScope, command_id: UUID) -> dict[str, Any]: ...
    async def apply(
        self,
        scope: DraftScope,
        solution_id: UUID,
        command: CreateSolution | SaveSolution | PrepareSolution,
        at: datetime,
    ) -> dict[str, Any]: ...
