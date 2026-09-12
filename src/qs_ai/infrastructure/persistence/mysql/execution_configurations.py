"""Admission and recovery use one snapshot; no latest lookup for accepted sessions."""

import hashlib
import json
from dataclasses import asdict, replace
from uuid import UUID

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from qs_ai.application.execution.configuration import (
    ConfigurationUnavailable,
    ExecutionConfiguration,
)
from qs_ai.application.execution.generation import FrozenGeneration
from qs_ai.application.governance.publication_codec import canonical, publication_json
from qs_ai.application.interpretation.input import InvalidInput
from qs_ai.application.interpretation.ports import Claim, NotFound
from qs_ai.application.interpretation.preparation import prepare_explanation
from qs_ai.application.interpretation.prompts import PromptPackage
from qs_ai.application.interpretation.provider import ModelRoute
from qs_ai.application.interpretation.selection import report_selector
from qs_ai.domain.governance.prompt import PromptAsset
from qs_ai.domain.governance.publication import PublishedConfiguration, resolve_publication
from qs_ai.domain.governance.route import RouteAsset
from qs_ai.domain.governance.schema import SchemaAsset
from qs_ai.domain.interpretation.model import EvidenceSet, RuleViolation, Session
from qs_ai.infrastructure.persistence.model_call_codec import JSONModelCallCodec
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import (
    AssetSnapshotReader,
    generation_snapshot,
)
from qs_ai.infrastructure.persistence.mysql.database import Transactions
from qs_ai.infrastructure.persistence.mysql.publication_records import (
    load_pointer,
    load_publication,
    load_receipt,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_changes as changes,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers as pointers,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    execution_configurations as bindings,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    prompt_assets,
    route_assets,
    schema_assets,
)
from qs_ai.infrastructure.qs_server.evaluation_suite import PUBLISHED_INPUT_VERSION, load_suite
from qs_ai.infrastructure.qs_server.output import QSOutputParser
from qs_ai.infrastructure.qs_server.profiles import decode_published_profile


def publication_hash(publication: PublishedConfiguration) -> str:
    return hashlib.sha256(publication_json(publication).encode()).hexdigest()


async def compile_configuration(
    db: AsyncSession, publication: PublishedConfiguration
) -> ExecutionConfiguration:
    proof = publication.evidence
    suite = load_suite(proof.release.suite)
    if (
        suite.input_construction_version != PUBLISHED_INPUT_VERSION
        or suite.input_schema != proof.release.input_schema
    ):
        raise ConfigurationUnavailable("Publication was not evaluated for this input construction")
    profile, manifest = await generation_snapshot(db, proof.release)
    if profile != proof.profile or manifest != proof.manifest:
        raise ConfigurationUnavailable("Published assets changed")
    prompt = await AssetSnapshotReader(db, prompt_assets, PromptAsset).get(
        manifest.prompt.identity, manifest.prompt.version
    )
    route = await AssetSnapshotReader(db, route_assets, RouteAsset).get(
        manifest.generation_route.identity, manifest.generation_route.version
    )
    schemas = AssetSnapshotReader(db, schema_assets, SchemaAsset)
    output = await schemas.get(manifest.output_schema.identity, manifest.output_schema.version)
    input_asset = await schemas.get(manifest.input_schema.identity, manifest.input_schema.version)
    if prompt is None or route is None or output is None or input_asset is None:
        raise ConfigurationUnavailable("Published asset missing")
    release = decode_published_profile(
        {
            "definition": json.loads(profile.definition_json),
            "fingerprint": profile.fingerprint,
            "status": "published",
        }
    )
    data = json.loads(prompt.package_json)
    package = PromptPackage(
        prompt.template_id,
        prompt.version,
        prompt.fingerprint,
        data["Ref"]["GitBlobSHA"],
        data["SystemMessage"],
        data["TaskTemplate"],
        data["DataPreamble"],
        tuple(data["AllowedPlaceholders"]),
    )
    definition = json.loads(route.definition_json)
    structured = definition.pop("structured_output")
    definition.setdefault("protocol", "responses")
    definition.setdefault("structured_output_mode", "json_schema")
    definition.setdefault("reasoning_effort", "")
    model = ModelRoute(**definition)
    if (
        not structured
        or model.provider != "deepseek"
        or model.protocol != "responses"
        or model.structured_output_mode != "json_schema"
        or model.fingerprint() != route.fingerprint
    ):
        raise ConfigurationUnavailable("Unsupported published model route")
    parser = QSOutputParser.from_schema(json.loads(output.definition_json))
    input_schema = json.loads(input_asset.definition_json)
    Draft202012Validator.check_schema(input_schema)
    validator = Draft202012Validator(input_schema)

    def validate_input(raw: str) -> None:
        if not validator.is_valid(json.loads(raw)):
            raise InvalidInput("Frozen input schema rejected prepared report")

    return ExecutionConfiguration(
        str(publication.publication_id),
        manifest.fingerprint(),
        release,
        package,
        model,
        parser.schema(),
        parser,
        validate_input,
    )


async def bind_configuration(db: AsyncSession, session: Session, evidence: EvidenceSet) -> None:
    """Caller owns the same transaction as task acceptance and idempotency receipt."""
    query = report_selector(session, evidence)
    selectors = {
        query,
        replace(query, model_version=None),
        replace(query, model_code=None, model_version=None),
    }
    rows = (
        (
            await db.execute(
                select(pointers).where(pointers.c.selector_key.in_([q.key() for q in selectors]))
            )
        )
        .mappings()
        .all()
    )
    candidates = [await load_pointer(db, row) for row in rows]
    publication = resolve_publication(tuple(candidates), query)
    if publication is None:
        raise RuleViolation("configuration_unavailable")
    config = await compile_configuration(db, publication)
    prepared = prepare_explanation(session, evidence, config.release, config.package)
    config.validate_input(prepared.assembled_input.canonical_json)
    pointer = next(p for p in candidates if p.active == publication)
    await db.execute(
        insert(bindings).values(
            session_id=session.id,
            evidence_set_id=evidence.id,
            evidence_fingerprint=evidence.fingerprint,
            publication_id=str(publication.publication_id),
            publication_sha256=publication_hash(publication),
            pointer_version=pointer.version,
            selector_query=canonical(asdict(query)),
        )
    )


async def read_configuration(
    db: AsyncSession, session: Session, evidence: EvidenceSet
) -> ExecutionConfiguration:
    query = report_selector(session, evidence)
    row = (
        (await db.execute(select(bindings).where(bindings.c.session_id == session.id)))
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["evidence_set_id"] != evidence.id
        or row["evidence_fingerprint"] != evidence.fingerprint
        or row["selector_query"] != canonical(asdict(query))
    ):
        raise ConfigurationUnavailable("Accepted configuration missing or bound to other evidence")
    publication, _ = await load_publication(db, UUID(row["publication_id"]))
    if row["publication_sha256"] != publication_hash(
        publication
    ) or not publication.evidence.selector.matches(query):
        raise ConfigurationUnavailable("Accepted publication content changed")
    audit = (
        (
            await db.execute(
                select(changes).where(
                    changes.c.selector_key == publication.evidence.selector.key(),
                    changes.c.version == row["pointer_version"],
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if audit is None or (await load_receipt(db, audit)).change.current.active != publication:
        raise ConfigurationUnavailable("Accepted pointer version has no matching publication audit")
    return await compile_configuration(db, publication)


class MySQLExecutionConfigurations:
    def __init__(self, transactions: Transactions) -> None:
        self.transactions = transactions

    async def get(self, claim: Claim, evidence: EvidenceSet) -> ExecutionConfiguration:
        try:
            async with self.transactions.open() as db:
                await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
                return await read_configuration(db, claim.session, evidence)
        except (ValueError, RuleViolation, NotFound, SchemaError, KeyError, TypeError):
            raise ConfigurationUnavailable("Cannot load accepted configuration") from None


async def validate_generation(
    db: AsyncSession, session: Session, evidence: EvidenceSet | None, raw: str
) -> tuple[ExecutionConfiguration, FrozenGeneration]:
    """Recheck the stored admission before dispatch or artifact acceptance."""
    try:
        if evidence is None:
            raise ConfigurationUnavailable("Accepted evidence missing")
        config = await read_configuration(db, session, evidence)
        prepared = prepare_explanation(session, evidence, config.release, config.package)
        config.validate_input(prepared.assembled_input.canonical_json)
        expected = FrozenGeneration(
            prepared,
            config.route,
            config.schema,
            publication_id=config.publication_id,
            manifest_fingerprint=config.manifest_fingerprint,
        )
        frozen = JSONModelCallCodec().decode_request(raw)
        if frozen != expected:
            raise ConfigurationUnavailable("Model call differs from accepted configuration")
        return config, frozen
    except (ValueError, RuleViolation, NotFound, SchemaError, KeyError, TypeError):
        raise ConfigurationUnavailable("Invalid accepted generation configuration") from None
