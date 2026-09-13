"""Real asset and publication records drive lifecycle; imports alone are not releases."""

import hashlib
import json
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update

from qs_ai.application.governance.profile_lifecycle import ProfileLifecycleQuery
from qs_ai.application.governance.prompt_drafts import DraftScope
from qs_ai.application.governance.publication import MovePublication
from qs_ai.application.interpretation.ports import NotFound
from qs_ai.domain.governance.profile import ProfileAsset
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.profile_lifecycle import MySQLProfileLifecycle
from qs_ai.infrastructure.persistence.mysql.publications import MySQLPublications
from qs_ai.infrastructure.persistence.mysql.schema import (
    configuration_publication_pointers,
    profile_assets,
)
from tests.integration.test_publications import dispatched as dispatched
from tests.integration.test_publications import freeze_creation as freeze_creation
from tests.integration.test_publications import judge as judge
from tests.integration.test_publications import passing_reviewable as passing_reviewable
from tests.integration.test_publications import passing_semantics as passing_semantics
from tests.integration.test_publications import persisted_assets as persisted_assets
from tests.integration.test_publications import ready as ready
from tests.integration.test_publications import reviewable as reviewable
from tests.integration.test_publications import setup_run as setup_run

pytestmark = pytest.mark.integration


async def test_profile_lifecycle_uses_committed_publications_and_retains_original_reference(ready):
    tx, pub_scope, command, at = ready
    store = MySQLProfileLifecycle(tx)
    scope = DraftScope(pub_scope.organization_id, pub_scope.operator_user_id)
    async with tx.open() as db:
        row = (await db.execute(select(profile_assets))).mappings().one()
    identity, version = row["profile_id"], row["version"]
    draft = await store.get(scope, identity, version)
    assert draft.status == "draft" and not draft.active_publication_id
    assert (
        draft.source_ref == row["source_ref"] and draft.reference.fingerprint == row["fingerprint"]
    )
    assert (await store.list(scope, ProfileLifecycleQuery(identity, "published"))).items == ()
    publication = await MySQLPublications(tx).apply(pub_scope, command, at)
    first = publication.change.current.active
    current = await store.get(scope, identity, version)
    assert current.status == "published" and current.reference == draft.reference
    assert current.active_publication_id == str(first.publication_id)
    assert current.active_run_id == str(first.evidence.run_id) and current.selector_version == 1
    assert (await store.list(scope, ProfileLifecycleQuery(identity, "published"))).items == (
        current,
    )
    assert (
        await store.get(DraftScope(2, 99), identity, version) == current
    )  # Shared configuration catalog, no actor audit data.
    disabled = await MySQLPublications(tx).apply(
        pub_scope,
        MovePublication(uuid4(), command.selector, 1, first.publication_id, "停用验证", True, None),
        at + timedelta(seconds=1),
    )
    inactive = await store.get(scope, identity, version)
    assert inactive.status == "disabled" and inactive.inactive_reason == "disabled"
    assert not inactive.active_publication_id and inactive.reference == draft.reference
    assert (await store.list(scope, ProfileLifecycleQuery(identity, "disabled"))).items == (
        inactive,
    )
    await MySQLPublications(tx).apply(
        pub_scope,
        MovePublication(
            uuid4(),
            command.selector,
            disabled.change.current.version,
            None,
            "回退验证",
            True,
            first.publication_id,
        ),
        at + timedelta(seconds=2),
    )
    restored = await store.get(scope, identity, version)
    assert (
        restored.status == "published"
        and restored.reference == draft.reference
        and restored.selector_version == 3
    )


async def test_profile_lifecycle_filters_before_keyset_pagination_and_is_exact(ready):
    tx, pub_scope, command, at = ready
    scope = DraftScope(pub_scope.organization_id, pub_scope.operator_user_id)
    store = MySQLProfileLifecycle(tx)
    async with tx.open() as db:
        row = (await db.execute(select(profile_assets))).mappings().one()
    identity = row["profile_id"]
    versions = ["lifecycle-A", "lifecycle-a", "lifecycle-v10", "lifecycle-v2"]
    try:
        for version in versions:
            definition = {**json.loads(row["definition_json"]), "version": version}
            raw = json.dumps(definition, ensure_ascii=False, separators=(",", ":"))
            await MySQLProfileAssets(tx).put(
                ProfileAsset(
                    identity, version, "sha256:" + hashlib.sha256(raw.encode()).hexdigest(), raw
                ),
                "qs-server:retained-source",
                "test-only",
            )
        await MySQLPublications(tx).apply(pub_scope, command, at)
        cursor, actual = "", []
        for _ in range(10):
            query = ProfileLifecycleQuery(identity, "draft", 1, cursor)
            page = await store.list(scope, query)
            actual.extend(item.reference.version for item in page.items)
            if not page.next_cursor:
                break
            with pytest.raises(ValueError):
                replace(query, status="published", cursor=page.next_cursor)
            cursor = page.next_cursor
        assert actual == versions
        assert not (await store.list(scope, ProfileLifecycleQuery(identity.upper()))).items
        with pytest.raises(NotFound):
            await store.get(scope, identity, "missing-version")
    finally:
        async with tx.open() as db:
            await db.execute(
                delete(profile_assets).where(
                    profile_assets.c.profile_id == identity, profile_assets.c.version.in_(versions)
                )
            )
            await db.commit()


async def test_profile_lifecycle_does_not_report_unaudited_pointer_as_published(ready):
    tx, scope, command, at = ready
    result = await MySQLPublications(tx).apply(scope, command, at)
    profile = result.change.current.active.evidence.profile
    async with tx.open() as db:
        await db.execute(update(configuration_publication_pointers).values(version=99))
        await db.commit()
    with pytest.raises(ValueError, match="audit"):
        await MySQLProfileLifecycle(tx).get(
            DraftScope(scope.organization_id, scope.operator_user_id),
            profile.profile_id,
            profile.version,
        )
