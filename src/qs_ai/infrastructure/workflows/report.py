"""Execute the migrated single-report interpretation with durable provider receipts."""

from collections.abc import Callable
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langsmith import tracing_context

from qs_ai.application.execution.artifact import build_artifact
from qs_ai.application.execution.configuration import ExecutionConfiguration
from qs_ai.application.execution.generation import (
    DurableGeneration,
    FrozenGeneration,
    GeneratedExplanation,
)
from qs_ai.application.interpretation.input import InvalidInput, NotApplicable
from qs_ai.application.interpretation.output import InvalidOutput, OutputParser
from qs_ai.application.interpretation.ports import Claim, WorkflowResult
from qs_ai.application.interpretation.preparation import PreparedExplanation, prepare_explanation
from qs_ai.application.interpretation.prompts import InvalidPrompt, PromptPackage
from qs_ai.application.interpretation.provider import ModelRoute, ProviderFailure
from qs_ai.application.interpretation.release import ExplanationRelease
from qs_ai.application.operations.diagnostics import operation
from qs_ai.domain.interpretation.model import EvidenceSet


class ReportState(TypedDict, total=False):
    claim: Claim
    evidence: EvidenceSet
    prepared: PreparedExplanation
    generated: GeneratedExplanation
    result: WorkflowResult


class ReportWorkflow:
    def __init__(
        self,
        generation: DurableGeneration,
        release: ExplanationRelease,
        package: PromptPackage,
        route: ModelRoute,
        schema: dict[str, Any],
        parser: OutputParser,
        *,
        publication_id: str | None = None,
        manifest_fingerprint: str | None = None,
        validate_input: Callable[[str], None] | None = None,
    ) -> None:
        self.generation = generation
        self.release = release
        self.package = package
        self.route = route
        self.schema = schema
        self.parser = parser
        self.publication_id = publication_id
        self.manifest_fingerprint = manifest_fingerprint
        self.validate_input = validate_input

        graph = StateGraph(ReportState)
        graph.add_node("prepare", self._prepare)
        graph.add_node("generate", self._generate)
        graph.add_node("validate", self._validate)
        graph.add_edge(START, "prepare")
        graph.add_edge("prepare", "generate")
        graph.add_edge("generate", "validate")
        graph.add_edge("validate", END)
        self.graph = graph.compile()

    async def _prepare(self, state: ReportState) -> ReportState:
        with operation("graph.generation.prepare", "generation"):
            prepared = prepare_explanation(
                state["claim"].session, state["evidence"], self.release, self.package
            )
            if self.validate_input is not None:
                self.validate_input(prepared.assembled_input.canonical_json)
            return {"prepared": prepared}

    async def _generate(self, state: ReportState) -> ReportState:
        with operation("graph.generation.generate", "generation"):
            generated = await self.generation.execute(
                state["claim"],
                FrozenGeneration(
                    state["prepared"],
                    self.route,
                    self.schema,
                    publication_id=self.publication_id,
                    manifest_fingerprint=self.manifest_fingerprint,
                ),
            )
            return {"generated": generated}

    async def _validate(self, state: ReportState) -> ReportState:
        with operation("graph.generation.validate", "generation"):
            artifact = build_artifact(
                state["claim"], state["evidence"], state["generated"], self.parser
            )
            return {"result": WorkflowResult("", artifact=artifact)}

    async def execute(self, claim: Claim, evidence: EvidenceSet) -> WorkflowResult:
        # ExecuteNext owns authorization before execution and before acceptance.
        # Storage/cancellation errors propagate; retries consult the durable receipt.
        try:
            with tracing_context(enabled=False):
                state = await self.graph.ainvoke({"claim": claim, "evidence": evidence})
            return state["result"]
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


def create_report_workflow(
    generation: DurableGeneration, config: "ExecutionConfiguration"
) -> ReportWorkflow:
    return ReportWorkflow(
        generation,
        config.release,
        config.package,
        config.route,
        config.schema,
        config.parser,
        publication_id=config.publication_id,
        manifest_fingerprint=config.manifest_fingerprint,
        validate_input=config.validate_input,
    )
