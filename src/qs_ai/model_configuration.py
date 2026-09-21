"""Deployment-owned bindings and model capabilities. No credentials in catalog responses."""

from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ModelCatalogConflict(ValueError):
    """The client selected a stale capability revision."""


class Configuration(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)


class ModelBinding(Configuration):
    binding_id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,63}$")
    revision: str = Field(min_length=1, max_length=128)
    provider: Literal["deepseek", "zhipu"]
    protocol: Literal["responses", "chat_completions"]
    adapter_contract: Literal["deepseek-responses/v1", "zhipu-chat/v1"]
    endpoint: str = Field(repr=False)
    credential_slot: Literal["deepseek", "zhipu"]
    enabled: bool = True

    @model_validator(mode="after")
    def validate_binding(self) -> "ModelBinding":
        expected = {
            "deepseek": ("responses", "deepseek-responses/v1", "api.deepseek.com", "/responses"),
            "zhipu": (
                "chat_completions",
                "zhipu-chat/v1",
                "open.bigmodel.cn",
                "/api/paas/v4/chat/completions",
            ),
        }
        protocol, contract, host, path = expected[self.provider]
        url = urlsplit(self.endpoint)
        if (self.protocol, self.adapter_contract, self.credential_slot) != (
            protocol,
            contract,
            self.provider,
        ):
            raise ValueError("Provider binding mismatch")
        if (
            url.scheme != "https"
            or url.hostname != host
            or url.path != path
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.port not in (None, 443)
        ):
            raise ValueError("Unapproved provider endpoint")
        return self


class ModelCapability(Configuration):
    model_key: str = Field(pattern=r"^[a-z][a-z0-9._/-]{0,127}$")
    model_id: str = Field(min_length=1, max_length=128)
    catalog_revision: str = Field(min_length=1, max_length=128)
    binding_id: str
    binding_revision: str
    purposes: tuple[Literal["generation", "semantic"], ...] = ("generation",)
    # Discovery is not proof. Execution stays unavailable until evidence is registered.
    verified: bool = False
    evidence_ref: str | None = None
    max_output_tokens: int = Field(default=12000, strict=True, ge=1, le=12000)
    max_timeout_milliseconds: int = Field(default=180000, strict=True, ge=1000, le=180000)
    reasoning_efforts: tuple[Literal["none", "low", "high", "max"], ...]

    @model_validator(mode="after")
    def validate_evidence(self) -> "ModelCapability":
        if not self.purposes or len(set(self.purposes)) != len(self.purposes):
            raise ValueError("Distinct model purposes required")
        if not self.reasoning_efforts or len(set(self.reasoning_efforts)) != len(
            self.reasoning_efforts
        ):
            raise ValueError("Distinct reasoning efforts required")
        if self.verified and (not self.evidence_ref or not self.evidence_ref.strip()):
            raise ValueError("Verified model requires evidence")
        return self


class ModelConfiguration(Configuration):
    v2_writes_enabled: bool = False
    bindings: tuple[ModelBinding, ...] = ()
    catalog: tuple[ModelCapability, ...] = ()

    @model_validator(mode="after")
    def validate_references(self) -> "ModelConfiguration":
        identities = [(v.binding_id, v.revision) for v in self.bindings]
        if len(set(identities)) != len(identities):
            raise ValueError("Duplicate model binding")
        models = [v.model_key for v in self.catalog]
        if len(set(models)) != len(models):
            raise ValueError("Duplicate model key")
        for entry in self.catalog:
            if (entry.binding_id, entry.binding_revision) not in identities:
                raise ValueError("Model binding missing")
        return self

    def resolve(
        self, key: str, revision: str | None, purpose: str
    ) -> tuple[ModelCapability, ModelBinding]:
        entry = next((v for v in self.catalog if v.model_key == key), None)
        if entry is None or not entry.verified or purpose not in entry.purposes:
            raise ValueError("model_not_enabled")
        if revision != entry.catalog_revision:
            raise ModelCatalogConflict("model_capability_changed")
        binding = next(
            v
            for v in self.bindings
            if (v.binding_id, v.revision) == (entry.binding_id, entry.binding_revision)
        )
        if not binding.enabled:
            raise ValueError("binding_unavailable")
        return entry, binding
