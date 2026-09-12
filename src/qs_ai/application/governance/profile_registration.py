"""Register a validated immutable Profile; evaluation and publication remain separate."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.domain.governance.manifest import AssetReference, GenerationManifest
from qs_ai.domain.governance.prompt_draft import nonzero_uuid, valid_reason


@dataclass(frozen=True)
class RegisterProfile:
    command_id: UUID
    source: AssetReference
    definition_json: str
    prompt: AssetReference
    generation_route: AssetReference
    reason: str

    def __post_init__(self) -> None:
        if (
            not nonzero_uuid(self.command_id)
            or not valid_reason(self.reason)
            or not isinstance(self.definition_json, str)
            or not 1 <= len(self.definition_json.encode()) <= 131072
            or not all(
                isinstance(x, AssetReference)
                for x in (self.source, self.prompt, self.generation_route)
            )
        ):
            raise ValueError("Explicit bounded Profile definition, references and audit required")


@dataclass(frozen=True)
class ProfileRegistrationReceipt:
    scope: DraftScope
    command: RegisterProfile
    manifest: GenerationManifest
    registered_at: datetime

    def __post_init__(self) -> None:
        if self.registered_at.tzinfo is None or self.registered_at.utcoffset() is None:
            raise ValueError("Registration time requires a timezone")
        if (
            self.manifest.prompt != self.command.prompt
            or self.manifest.generation_route != self.command.generation_route
        ):
            raise ValueError("Registered references differ from confirmed command")


class ProfileRegistrar(Protocol):
    async def register(
        self, scope: DraftScope, command: RegisterProfile, at: datetime
    ) -> ProfileRegistrationReceipt: ...
    async def get_receipt(
        self, scope: DraftScope, command_id: UUID
    ) -> ProfileRegistrationReceipt: ...
