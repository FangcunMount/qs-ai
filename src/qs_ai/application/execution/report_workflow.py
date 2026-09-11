"""Execute the migrated single-report interpretation with durable provider receipts."""

from typing import Any

from qs_ai.application.execution.artifact import build_artifact
from qs_ai.application.execution.generation import DurableGeneration, FrozenGeneration
from qs_ai.application.interpretation.input import InvalidInput, NotApplicable
from qs_ai.application.interpretation.output import InvalidOutput, OutputParser
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.prompts import InvalidPrompt, PromptPackage
from qs_ai.application.interpretation.provider import ModelRoute, ProviderFailure
from qs_ai.application.interpretation.release import ExplanationRelease
from qs_ai.domain.interpretation.model import EvidenceSet


class ReportWorkflow:
    def __init__(
        self,
        generation: DurableGeneration,
        release: ExplanationRelease,
        package: PromptPackage,
        route: ModelRoute,
        schema: dict[str, Any],
        parser: OutputParser,
    ) -> None:
        self.generation = generation
        self.release = release
        self.package = package
        self.route = route
        self.schema = schema
        self.parser = parser

    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        # ExecuteNext owns authorization before execution and before acceptance.
        # Storage/cancellation errors propagate; retries consult the durable receipt.
        try:
            prepared = prepare_explanation(claim.session, evidence, self.release, self.package)
            generated = await self.generation.execute(
                claim, FrozenGeneration(prepared, self.route, self.schema)
            )
            artifact = build_artifact(claim, evidence, generated, self.parser)
        except NotApplicable:
            return WorkflowResult("", failure_code="report_not_applicable")
        except InvalidInput:
            return WorkflowResult("", failure_code="report_input_invalid")
        except InvalidPrompt:
            return WorkflowResult("", failure_code="prompt_invalid")
        except InvalidOutput as error:
            return WorkflowResult("", failure_code=error.code)
        except ProviderFailure as error:
            return WorkflowResult(
                "", failure_code="provider_result_unknown" if error.result_unknown else error.code
            )
        return WorkflowResult("", artifact=artifact)
