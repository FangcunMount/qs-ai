"""Exact suite-level evaluation references, independent of the selected model routes."""

from dataclasses import dataclass

from qs_ai.domain.evaluation.identity import FrozenContractRef


@dataclass(frozen=True)
class SuiteContracts:
    execution_policy: FrozenContractRef
    gate_policy: FrozenContractRef
    semantic_prompt: FrozenContractRef
    semantic_output_schema: FrozenContractRef
    semantic_owner_organization_id: int = 0

    def __post_init__(self) -> None:
        if (
            type(self.semantic_owner_organization_id) is not int
            or not 0 <= self.semantic_owner_organization_id < 2**63
            or not all(
                isinstance(ref, FrozenContractRef)
                for ref in (
                    self.execution_policy,
                    self.gate_policy,
                    self.semantic_prompt,
                    self.semantic_output_schema,
                )
            )
        ):
            raise ValueError("Exact suite evaluation references and owner required")
