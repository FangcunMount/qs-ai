"""Safe immutable model dispatch identity retained with received results."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelExecutionIdentity:
    provider: str
    requested_model: str
    protocol: str
    adapter_contract: str
    binding_id: str
    binding_revision: str
    route_fingerprint: str
