import pytest

from qs_ai.infrastructure.persistence.mysql.messaging import outbox
from qs_ai.infrastructure.persistence.mysql.schema import result_outbox
from qs_ai.maintenance.schema_layout import (
    handoff_tables,
    require_handoff_head,
    require_known_head,
    retained_physical_table,
)


def test_0041_legacy_handoff_does_not_authorize_old_layout_conversion():
    head = "0041_transaction_messaging"
    assert require_handoff_head([head]) == head
    assert handoff_tables(head) == (result_outbox, outbox)
    assert retained_physical_table(head, "ai_messaging_outbox") == "messaging_outbox"
    assert retained_physical_table(head, "result_outbox") == result_outbox.name
    with pytest.raises(ValueError, match="recognized complete"):
        require_known_head([head])


@pytest.mark.parametrize(
    "heads",
    [
        None,
        [],
        ["0042_unknown"],
        ["0039_data_consolidation"],
        ["0041_transaction_messaging", "0040_module_table_names"],
    ],
)
def test_legacy_handoff_refuses_unknown_partial_and_multiple_heads(heads):
    with pytest.raises(ValueError, match="recognized complete"):
        require_handoff_head(heads)
