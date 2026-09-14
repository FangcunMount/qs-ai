"""Resolve an accepted task's immutable configuration, never the latest pointer."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from qs_ai.application.execution.generation import DurableGeneration
from qs_ai.application.execution.report_workflow import ReportWorkflow
from qs_ai.application.interpretation.output import OutputParser
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.application.interpretation.prompts import PromptPackage
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.application.interpretation.release import ExplanationRelease
from qs_ai.domain.interpretation.model import EvidenceSet


class ConfigurationUnavailable(ValueError):
    pass


@dataclass(frozen=True)
class ExecutionConfiguration:
    publication_id: str
    manifest_fingerprint: str
    release: ExplanationRelease
    package: PromptPackage
    route: ModelRoute
    schema: dict[str, Any]
    parser: OutputParser
    validate_input: Callable[[str], None]


class ConfigurationReader(Protocol):
    async def get(self, claim: Claim, evidence: EvidenceSet) -> ExecutionConfiguration: ...


class PublishedReportWorkflow:
    def __init__(self, reader: ConfigurationReader, generation: DurableGeneration) -> None:
        self.reader, self.generation = reader, generation

    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        if claim.session.workflow_version != "qs-published-snapshot-v1":
            return WorkflowResult("", failure_code="configuration_invalid")
        try:
            config = await self.reader.get(claim, evidence)
        except ConfigurationUnavailable:
            return WorkflowResult("", failure_code="configuration_invalid")
        return await ReportWorkflow(
            self.generation,
            config.release,
            config.package,
            config.route,
            config.schema,
            config.parser,
            publication_id=config.publication_id,
            manifest_fingerprint=config.manifest_fingerprint,
            validate_input=config.validate_input,
        ).execute(claim, evidence)
