"""Real MySQL gate/asset/publication transactions, synthetic model and review inputs."""

import asyncio
import copy
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.publication import (
    MovePublication,
    PublicationHistoryQuery,
    PublicationScope,
    PublishConfiguration,
)
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.bootstrap.import_schemas import baseline_assets as schemas
from qs_ai.domain.evaluation.identity import FrozenContractRef
from qs_ai.domain.governance.publication import PublicationConflict, PublicationPointer
from qs_ai.infrastructure.persistence.mysql.asset_snapshot import generation_snapshot
from qs_ai.infrastructure.persistence.mysql.evaluation_runs import create_run
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications, apply_publication
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_changes as changes,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers as pointers,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publications as publications,
)
from qs_ai.infrastructure.persistence.mysql.schema import (
    evaluation_runs,
    prompt_assets,
)
from tests.integration.test_evaluation_completions import dispatched as dispatched
from tests.integration.test_evaluation_creation_interop import persisted_assets as persisted_assets
from tests.integration.test_evaluation_finalization import commit, reviewed
from tests.integration.test_evaluation_finalization import passing_reviewable as passing_reviewable
from tests.integration.test_evaluation_finalization import passing_semantics as passing_semantics
from tests.integration.test_evaluation_reviews import reviewable as reviewable
from tests.integration.test_evaluation_runs import rows
from tests.integration.test_evaluation_runs import setup_run as setup_run
from tests.integration.test_semantic_completions import judge as judge

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
async def freeze_creation(setup_run, persisted_assets, monkeypatch, request):
    from tests.integration import test_evaluation_completions as fixtures

    async def create(db, run_id, release):
        schema = schemas()[1][0]
        release = replace(
            release,
            suite=getattr(request, "param", release.suite),
            input_schema=FrozenContractRef(
                schema.schema_id,
                schema.schema_id + "/" + schema.version,
                schema.fingerprint,
            ),
        )
        _, manifest = await generation_snapshot(db, release)
        return await create_run(
            db,
            run_id,
            release,
            1,
            "actor:1",
            "冻结实际资产评测",
            datetime(2026, 9, 12, tzinfo=UTC),
            generation_manifest=manifest,
        )

    # Freeze at creation, before any candidate is dispatched. No after-the-fact backfill.
    monkeypatch.setattr(fixtures, "create", create)
    yield
    tx, run_id, _ = setup_run
    async with tx.open() as db:
        keys = list(
            (
                await db.execute(
                    select(publications.c.selector_key).where(publications.c.run_id == str(run_id))
                )
            ).scalars()
        )
        if keys:
            await db.execute(delete(changes).where(changes.c.selector_key.in_(keys)))
            await db.execute(delete(pointers).where(pointers.c.selector_key.in_(keys)))
            await db.execute(delete(publications).where(publications.c.run_id == str(run_id)))
        await db.commit()


@pytest.fixture
async def ready(passing_reviewable):
    tx, management, _, value = passing_reviewable
    version = await reviewed(passing_reviewable)
    state = await commit(passing_reviewable, version=version, passed=True)
    run, _, _ = await rows(tx, management.run_id)
    definition = json.loads(run["definition_json"])
    async with tx.open() as db:
        from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity

        release = EvidenceReleaseIdentity(
            **{k: FrozenContractRef(**v) for k, v in definition["release"].items()}
        )
        profile, _ = await generation_snapshot(db, release)
    from qs_ai.domain.governance.publication import ReleaseSelector

    selector = ReleaseSelector(**json.loads(profile.definition_json)["selector"])
    scope = PublicationScope(1, 42)
    command = PublishConfiguration(
        uuid4(),
        selector,
        0,
        None,
        "发布已完整评测配置",
        True,
        management.run_id,
        state.version,
        release.fingerprint(),
    )
    return tx, scope, command, value.reviewed_at + timedelta(seconds=2)


async def inventory(tx):
    async with tx.open() as db:
        return [
            list((await db.execute(select(table))).mappings())
            for table in (publications, pointers, changes)
        ]


async def test_publish_replace_rollback_disable_and_replay_preserve_history(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    before_run = await rows(tx, command.run_id)
    first = await store.apply(scope, command, at)
    old = first.change.current.active
    assert old is not None and first.change.current.version == 1
    assert await store.get(command.selector) == first.change.current
    second_command = replace(
        command, command_id=uuid4(), expected_version=1, expected_active_id=old.publication_id
    )
    second = await store.apply(scope, second_command, at + timedelta(seconds=1))
    new = second.change.current.active
    assert new is not None and new.publication_id != old.publication_id
    rollback = MovePublication(
        uuid4(), command.selector, 2, new.publication_id, "回退原发布", True, old.publication_id
    )
    back = await store.apply(PublicationScope(2, 43), rollback, at + timedelta(seconds=2))
    assert back.change.current.active == old and back.change.current.version == 3
    stop = MovePublication(uuid4(), command.selector, 3, old.publication_id, "停用", True, None)
    stopped = await store.apply(scope, stop, at + timedelta(seconds=3))
    assert stopped.change.current.active is None and stopped.change.current.version == 4
    assert await store.get(command.selector) == stopped.change.current
    inventory_before = await inventory(tx)
    assert await store.apply(scope, command, at + timedelta(days=1)) == first
    assert await store.apply(scope, stop, at + timedelta(days=1)) == stopped
    assert await inventory(tx) == inventory_before
    assert [len(v) for v in inventory_before] == [2, 1, 4]
    assert await rows(tx, command.run_id) == before_run


async def test_uncommitted_publish_leaves_no_record_pointer_or_audit(ready):
    tx, scope, command, at = ready
    before = await inventory(tx)
    async with tx.open() as db:
        await apply_publication(db, scope, command, at)
        # Caller cancellation/failure before commit must roll back all three tables.
    assert await inventory(tx) == before
    assert await MySQLPublications(tx).get(command.selector) == PublicationPointer(command.selector)


async def test_concurrent_initial_publications_and_duplicate_retries(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    other = replace(command, command_id=uuid4())
    results = await asyncio.gather(
        store.apply(scope, command, at),
        store.apply(scope, other, at),
        return_exceptions=True,
    )
    assert sum(isinstance(value, PublicationConflict) for value in results) == 1
    assert [len(v) for v in await inventory(tx)] == [1, 1, 1]
    winner = 0 if not isinstance(results[0], Exception) else 1
    accepted_command = (command, other)[winner]
    replay = await asyncio.gather(*(store.apply(scope, accepted_command, at) for _ in range(3)))
    assert all(value == results[winner] for value in replay)


@pytest.mark.parametrize(
    "case", ["foreign", "selector", "release", "missing_manifest", "bytes", "approval"]
)
async def test_untrusted_or_changed_publication_evidence_cannot_activate(ready, case):
    tx, scope, command, at = ready
    before = await inventory(tx)
    changed_prompt = None
    if case == "foreign":
        scope = replace(scope, organization_id=2)
    elif case == "selector":
        command = replace(command, selector=replace(command.selector, model_code="another"))
    elif case == "release":
        command = replace(command, release_fingerprint="sha256:" + "0" * 64)
    else:
        run, _, _ = await rows(tx, command.run_id)
        definition = json.loads(run["definition_json"])
        async with tx.open() as db:
            if case == "missing_manifest":
                definition.pop("generation_manifest_json")
                await db.execute(
                    update(evaluation_runs)
                    .where(evaluation_runs.c.run_id == str(command.run_id))
                    .values(definition_json=json.dumps(definition))
                )
            elif case == "approval":
                progress = copy.deepcopy(run["progress_json"])
                progress["human_reviews"].pop()
                await db.execute(
                    update(evaluation_runs)
                    .where(evaluation_runs.c.run_id == str(command.run_id))
                    .values(progress_json=progress)
                )
            else:
                ref = definition["release"]["prompt"]
                condition = (prompt_assets.c.template_id == ref["id"]) & (
                    prompt_assets.c.version == ref["version"]
                )
                row = (await db.execute(select(prompt_assets).where(condition))).mappings().one()
                changed_prompt = (condition, dict(row))
                raw = row["package_json"] + " "
                await db.execute(
                    update(prompt_assets)
                    .where(condition)
                    .values(
                        package_json=raw,
                        package_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                    )
                )
            await db.commit()
    try:
        with pytest.raises((ValueError, NotFound)):
            await MySQLPublications(tx).apply(scope, command, at)
        assert await inventory(tx) == before
    finally:
        if changed_prompt:
            condition, row = changed_prompt
            async with tx.open() as db:
                await db.execute(update(prompt_assets).where(condition).values(**row))
                await db.commit()


async def test_command_reuse_and_stale_pointer_are_conflicts(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    await store.apply(scope, command, at)
    before = await inventory(tx)
    for changed in (replace(command, reason="different"), replace(command, command_id=uuid4())):
        with pytest.raises(PublicationConflict):
            await store.apply(scope, changed, at)
    with pytest.raises(PublicationConflict):
        await store.apply(replace(scope, operator_user_id=43), command, at)
    assert await inventory(tx) == before


async def test_disable_does_not_require_current_quality_but_rollback_revalidates(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    first = await store.apply(scope, command, at)
    active = first.change.current.active
    stop = MovePublication(
        uuid4(), command.selector, 1, active.publication_id, "停用异常配置", True, None
    )
    run, _, _ = await rows(tx, command.run_id)
    progress = copy.deepcopy(run["progress_json"])
    progress["human_reviews"].pop()
    async with tx.open() as db:
        await db.execute(
            update(evaluation_runs)
            .where(evaluation_runs.c.run_id == str(command.run_id))
            .values(progress_json=progress)
        )
        await db.commit()
    await store.apply(scope, stop, at + timedelta(seconds=1))
    before = await inventory(tx)
    rollback = MovePublication(
        uuid4(), command.selector, 2, None, "不可恢复异常配置", True, active.publication_id
    )
    with pytest.raises(ValueError):
        await store.apply(scope, rollback, at + timedelta(seconds=2))
    assert await inventory(tx) == before


@pytest.mark.parametrize(
    "case", ["missing_target", "wrong_selector", "missing_audit", "corrupt_record"]
)
async def test_retained_target_and_pointer_integrity(ready, case):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    first = await store.apply(scope, command, at)
    active = first.change.current.active
    assert active is not None
    if case in {"missing_target", "wrong_selector"}:
        rollback = MovePublication(
            uuid4(),
            command.selector,
            1,
            active.publication_id,
            "核对回退",
            True,
            uuid4() if case == "missing_target" else active.publication_id,
        )
        if case == "wrong_selector":
            rollback = replace(rollback, selector=replace(command.selector, model_code="other"))
        before = await inventory(tx)
        with pytest.raises((NotFound, PublicationConflict)):
            await store.apply(scope, rollback, at)
        assert await inventory(tx) == before
    else:
        async with tx.open() as db:
            if case == "missing_audit":
                await db.execute(
                    delete(changes).where(changes.c.command_id == str(command.command_id))
                )
            else:
                await db.execute(
                    update(publications)
                    .where(publications.c.publication_id == str(active.publication_id))
                    .values(content_sha256="0" * 64)
                )
            await db.commit()
        with pytest.raises(ValueError):
            await store.get(command.selector)


async def test_publication_waits_for_final_approval_before_reading_snapshot(passing_reviewable):
    from qs_ai.domain.evaluation.identity import EvidenceReleaseIdentity
    from qs_ai.domain.governance.publication import ReleaseSelector
    from qs_ai.infrastructure.persistence.mysql.evaluation_finalization import finalize

    tx, management, _, value = passing_reviewable
    version = await reviewed(passing_reviewable)
    run, _, _ = await rows(tx, management.run_id)
    definition = json.loads(run["definition_json"])
    release = EvidenceReleaseIdentity(
        **{key: FrozenContractRef(**ref) for key, ref in definition["release"].items()}
    )
    async with tx.open() as db:
        profile, _ = await generation_snapshot(db, release)
    selector = ReleaseSelector(**json.loads(profile.definition_json)["selector"])
    command = PublishConfiguration(
        uuid4(),
        selector,
        0,
        None,
        "批准后立即发布",
        True,
        management.run_id,
        version + 1,
        release.fingerprint(),
    )
    pending = None
    try:
        async with tx.open() as db:
            await finalize(
                db, management, version, True, "完整审核", value.reviewed_at, confirm=True
            )
            pending = asyncio.create_task(
                MySQLPublications(tx).apply(
                    PublicationScope(1, 42), command, value.reviewed_at + timedelta(seconds=1)
                )
            )
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending), 0.15)
            await db.commit()
        receipt = await asyncio.wait_for(pending, 10)
        assert receipt.change.current.active.evidence.run_version == version + 1
    finally:
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("field", ["reason", "scope", "target"])
async def test_replay_and_pointer_reads_bind_the_original_confirmed_command(ready, field):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    first = await store.apply(scope, command, at)
    active = first.change.current.active
    stop = MovePublication(
        uuid4(), command.selector, 1, active.publication_id, "确认停用", True, None
    )
    await store.apply(scope, stop, at + timedelta(seconds=1))
    async with tx.open() as db:
        row = (
            (await db.execute(select(changes).where(changes.c.command_id == str(stop.command_id))))
            .mappings()
            .one()
        )
        if field == "reason":
            data = json.loads(row["receipt_json"])
            data["audit"]["reason"] = "未经确认的新原因"
            from qs_ai.application.governance.publication_codec import canonical

            values = {"receipt_json": canonical(data)}
        elif field == "scope":
            values = {"organization_id": 2}
        else:
            data = json.loads(row["request_json"])
            data["command"]["expected_active_id"] = str(uuid4())
            from qs_ai.application.governance.publication_codec import canonical

            values = {"request_json": canonical(data)}
        await db.execute(
            update(changes).where(changes.c.command_id == str(stop.command_id)).values(**values)
        )
        await db.commit()
    with pytest.raises(ValueError):
        await store.get(command.selector)
    with pytest.raises(ValueError):
        await store.apply(scope, stop, at + timedelta(days=1))


async def test_two_independently_approved_runs_contend_for_one_selector(ready, monkeypatch):
    from contextlib import AsyncExitStack, asynccontextmanager

    from qs_ai.infrastructure.persistence.mysql import publications as implementation

    tx, scope, first_command, at = ready
    async with AsyncExitStack() as stack:
        # Build a second complete ledger through the same real preparation,
        # completion, review and finalization paths; do not copy an approval row.
        second_setup = await stack.enter_async_context(asynccontextmanager(setup_run.__wrapped__)())
        second_dispatch = await stack.enter_async_context(
            asynccontextmanager(dispatched.__wrapped__)(second_setup)
        )
        second_judge = await stack.enter_async_context(
            asynccontextmanager(judge.__wrapped__)(second_dispatch)
        )
        second_review = await reviewable.__wrapped__(second_judge)
        version = await reviewed(second_review)
        approved = await commit(second_review, version=version, passed=True)
        second_command = replace(
            first_command, command_id=uuid4(), run_id=second_setup[1], run_version=approved.version
        )
        arrived = asyncio.Event()
        count = 0
        original_lock = implementation.lock_run_in_transaction

        async def locked(db, management, expected):
            nonlocal count
            result = await original_lock(db, management, expected)
            count += 1
            if count == 2:
                arrived.set()
            await asyncio.wait_for(arrived.wait(), 10)
            return result

        monkeypatch.setattr(implementation, "lock_run_in_transaction", locked)
        try:
            store = MySQLPublications(tx)
            results = await asyncio.gather(
                store.apply(scope, first_command, at),
                store.apply(scope, second_command, at),
                return_exceptions=True,
            )
            assert count == 2
            assert sum(isinstance(value, PublicationConflict) for value in results) == 1
            accepted = [value for value in results if not isinstance(value, Exception)]
            assert len(accepted) == 1
            assert [len(v) for v in await inventory(tx)] == [1, 1, 1]
            assert await store.get(first_command.selector) == accepted[0].change.current
        finally:
            # Release the publication FK before the second fixture deletes its Run.
            async with tx.open() as db:
                key = first_command.selector.key()
                await db.execute(delete(changes).where(changes.c.selector_key == key))
                await db.execute(delete(pointers).where(pointers.c.selector_key == key))
                await db.execute(delete(publications).where(publications.c.selector_key == key))
                await db.commit()


async def test_history_pages_and_exact_version_reads_preserve_all_publication_audits(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    first = await store.apply(scope, command, at)
    old = first.change.current.active
    assert old is not None
    second = await store.apply(
        scope,
        replace(
            command, command_id=uuid4(), expected_version=1, expected_active_id=old.publication_id
        ),
        at + timedelta(seconds=1),
    )
    new = second.change.current.active
    assert new is not None
    back = await store.apply(
        PublicationScope(2, 43),
        MovePublication(
            uuid4(), command.selector, 2, new.publication_id, "回退核验", True, old.publication_id
        ),
        at + timedelta(seconds=2),
    )
    stopped = await store.apply(
        PublicationScope(2, 43),
        MovePublication(uuid4(), command.selector, 3, old.publication_id, "停用核验", True, None),
        at + timedelta(seconds=3),
    )
    before = await inventory(tx)
    before_run = await rows(tx, command.run_id)
    page = await store.list_history(PublicationHistoryQuery(command.selector, limit=2))
    assert [v.version for v in page.entries] == [4, 3] and page.next_before_version == 3
    assert [v.action for v in page.entries] == ["disable", "rollback"]
    assert [v.actor for v in page.entries] == ["user:43", "user:43"]
    assert page.entries[0].publication_id is None and page.entries[0].run_id is None
    assert page.entries[1].publication_id == old.publication_id
    assert page.entries[1].profile_id == old.evidence.profile.profile_id
    for version, expected in enumerate((first, second, back, stopped), 1):
        assert await store.get_history(command.selector, version) == expected
    with pytest.raises(NotFound):
        await store.get_receipt(PublicationScope(2, 43), first.command_id)
    assert await inventory(tx) == before and await rows(tx, command.run_id) == before_run
    # A later publication must not move an existing exclusive cursor or repeat entries.
    await store.apply(
        scope,
        replace(command, command_id=uuid4(), expected_version=4, expected_active_id=None),
        at + timedelta(seconds=4),
    )
    remaining = await store.list_history(PublicationHistoryQuery(command.selector, 3, 2))
    assert [v.version for v in remaining.entries] == [2, 1] and remaining.next_before_version == 0
    assert remaining.entries[0].publication_id == new.publication_id
    assert remaining.entries[1].publication_id == old.publication_id
    empty = await store.list_history(PublicationHistoryQuery(command.selector, 1, 2))
    assert not empty.entries and empty.next_before_version == 0


async def test_history_reads_are_exact_selector_and_never_create_empty_pointers(ready):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    await store.apply(scope, command, at)
    other = replace(command.selector, model_code="unpublished-history-selector")
    before = await inventory(tx)
    assert not (await store.list_history(PublicationHistoryQuery(other))).entries
    for selector, version in ((other, 1), (command.selector, 2)):
        with pytest.raises(NotFound):
            await store.get_history(selector, version)
    assert await inventory(tx) == before


@pytest.mark.parametrize("corrupt", ["audit_actor", "content_digest"])
async def test_history_rejects_corrupted_retained_records(ready, corrupt):
    tx, scope, command, at = ready
    store = MySQLPublications(tx)
    await store.apply(scope, command, at)
    async with tx.open() as db:
        if corrupt == "audit_actor":
            await db.execute(
                update(changes)
                .where(changes.c.command_id == str(command.command_id))
                .values(operator_user_id=999)
            )
        else:
            await db.execute(
                update(publications)
                .where(publications.c.run_id == str(command.run_id))
                .values(content_sha256="0" * 64)
            )
        await db.commit()
    before = await inventory(tx)
    with pytest.raises(ValueError):
        await store.list_history(PublicationHistoryQuery(command.selector))
    with pytest.raises(ValueError):
        await store.get_history(command.selector, 1)
    assert await inventory(tx) == before


async def test_policy_references_find_canonical_publication_and_isolate_org(ready):
    from qs_ai.application.governance.asset_references import ReferenceQuery
    from qs_ai.application.governance.prompt_drafts import DraftScope
    from qs_ai.infrastructure.persistence.mysql.asset_references import MySQLPolicyReferences

    tx, scope, command, at = ready
    receipt = await MySQLPublications(tx).apply(scope, command, at)
    publication = receipt.change.current.active
    assert publication is not None
    reader = MySQLPolicyReferences(tx)
    for kind in ("execution_policy", "gate_policy"):
        query = ReferenceQuery(kind, getattr(publication.evidence.release, kind), "publication", 1)
        found = []
        for _ in range(100):
            page = await reader.get(
                DraftScope(scope.organization_id, scope.operator_user_id), query
            )
            found.extend(item["identity"] for item in page["items"])
            if not page["next_cursor"]:
                break
            query = replace(query, cursor=page["next_cursor"])
        else:
            pytest.fail("publication reference pagination did not terminate")
        assert str(publication.publication_id) in found
        hidden = await reader.get(DraftScope(2, 43), replace(query, cursor=""))
        assert str(publication.publication_id) not in [v["identity"] for v in hidden["items"]]
