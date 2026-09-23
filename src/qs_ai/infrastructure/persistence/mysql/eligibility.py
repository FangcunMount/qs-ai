"""Capability reads share publication/input validators with actual admission."""

from jsonschema.exceptions import SchemaError
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound

from qs_ai.application.execution.configuration import ConfigurationUnavailable
from qs_ai.application.governance.solution_models import EditableModelPolicy, validate_v2_admission
from qs_ai.application.interpretation.eligibility import Eligibility, eligibility_source
from qs_ai.application.interpretation.input import InvalidInput
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.domain.governance.publication import resolve_publication
from qs_ai.domain.interpretation.model import Actor, EvidenceItem, RuleViolation
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.execution_configurations import compile_configuration
from qs_ai.infrastructure.persistence.mysql.publication_records import load_pointer
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers as pointers,
)


class MySQLEligibilityReader:
    def __init__(self, transactions: Transactions, models: EditableModelPolicy) -> None:
        self.transactions = transactions
        self.models = models

    async def check(
        self,
        actor: Actor,
        testee_id: str,
        assessment_ids: tuple[str, ...],
        items: tuple[EvidenceItem, ...],
    ) -> Eligibility:
        source = eligibility_source(actor, testee_id, assessment_ids, items)
        if isinstance(source, Eligibility):
            return source
        session, evidence, query = source
        async with self.transactions.open() as db:
            rows = (
                (
                    await db.execute(
                        select(pointers).where(
                            pointers.c.selector_key.in_(
                                [q.key() for q in query.admission_candidates()]
                            )
                        )
                    )
                )
                .mappings()
                .all()
            )
            try:
                candidates = tuple([await load_pointer(db, row) for row in rows])
                publication = resolve_publication(candidates, query)
                if publication is None:
                    return Eligibility(
                        "unavailable", "publication_paused" if candidates else "publication_missing"
                    )
                config = await compile_configuration(db, publication)
                validate_v2_admission(config.route, self.models.configuration, "generation")
            except (
                ValueError,
                ConfigurationUnavailable,
                NoResultFound,
                NotFound,
                SchemaError,
                KeyError,
                TypeError,
                RuleViolation,
            ):
                return Eligibility("unavailable", "asset_invalid")
            try:
                prepared = prepare_explanation(session, evidence, config.release, config.package)
                config.validate_input(prepared.assembled_input.canonical_json)
            except (InvalidInput, RuleViolation):
                return Eligibility("unavailable", "source_incomplete")
            # No commit, locks, reservation, session creation or provider dependency.
            # Start revalidates against its own transaction and current capacity.
            return Eligibility("available")
