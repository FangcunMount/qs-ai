"""Register a new immutable evaluation binding; existing approvals are never reused."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.domain.governance.prompt_draft import nonzero_uuid, valid_reason


@dataclass(frozen=True)
class RegisterSuite:
    command_id: UUID
    source: FrozenContractRef
    suite_id: str
    suite_version: str
    profile: AssetReference
    prompt: AssetReference
    generation_route: AssetReference
    reason: str
    case_edits_json: str = ""
    semantic_prompt: FrozenContractRef | None = None
    semantic_owner_organization_id: int = 0

    def __post_init__(self) -> None:
        from qs_ai.domain.evaluation.case_edits import parse

        parse(self.case_edits_json)
        if (
            (
                self.semantic_prompt is not None
                and not isinstance(self.semantic_prompt, FrozenContractRef)
            )
            or type(self.semantic_owner_organization_id) is not int
            or not 0 <= self.semantic_owner_organization_id < 2**63
        ):
            raise ValueError("Explicit semantic asset and owner required")
        if self.semantic_prompt is None and self.semantic_owner_organization_id != 0:
            raise ValueError("Semantic owner requires an explicit reference")
        FrozenContractRef(self.suite_id, self.suite_version, "sha256:" + "0" * 64)
        if (
            not nonzero_uuid(self.command_id)
            or not valid_reason(self.reason)
            or not isinstance(self.source, FrozenContractRef)
        ):
            raise ValueError("Explicit source and audited command required")
        if not all(
            isinstance(v, AssetReference)
            for v in (self.profile, self.prompt, self.generation_route)
        ):
            raise ValueError("Complete executable asset references required")


@dataclass(frozen=True)
class SuiteRegistrationReceipt:
    scope: DraftScope
    command: RegisterSuite
    suite: FrozenContractRef
    manifest: GenerationManifest
    registered_at: datetime

    def __post_init__(self) -> None:
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("Registration time requires a timezone")
        if (self.command.suite_id, self.command.suite_version) != (
            self.suite.id,
            self.suite.version,
        ) or (self.command.profile, self.command.prompt, self.command.generation_route) != (
            self.manifest.profile,
            self.manifest.prompt,
            self.manifest.generation_route,
        ):
            raise ValueError("Suite receipt differs from confirmed command")


class SuiteRegistrar(Protocol):
    async def register(
        self, scope: DraftScope, command: RegisterSuite, at: datetime
    ) -> SuiteRegistrationReceipt: ...
    async def get_receipt(
        self, scope: DraftScope, command_id: UUID
    ) -> SuiteRegistrationReceipt: ...
