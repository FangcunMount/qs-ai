import pytest

from qs_ai.config import Settings
from qs_ai.model_configuration import ModelBinding, ModelCapability, ModelConfiguration


def binding(**changes):
    return ModelBinding(
        **dict(
            dict(
                binding_id="zhipu-official",
                revision="v1",
                provider="zhipu",
                protocol="chat_completions",
                adapter_contract="zhipu-chat/v1",
                endpoint="https://open.bigmodel.cn/api/paas/v4/chat/completions",
                credential_slot="zhipu",
            ),
            **changes,
        )
    )


def capability(**changes):
    return ModelCapability(
        **dict(
            dict(
                model_key="zhipu/quality",
                model_id="glm-5.3",
                catalog_revision="20260921-1",
                binding_id="zhipu-official",
                binding_revision="v1",
                reasoning_efforts=("low", "high", "max"),
            ),
            **changes,
        )
    )


def test_default_does_not_enable_v2_or_require_new_credentials():
    s = Settings()
    assert not s.models.v2_writes_enabled
    assert s.zhipu_api_key is None
    assert not s.models.catalog


@pytest.mark.parametrize(
    "change",
    [
        {"endpoint": "https://evil.invalid/chat/completions"},
        {"endpoint": "https://open.bigmodel.cn/api/paas/v4/chat/completions?api_key=secret"},
        {"credential_slot": "deepseek"},
        {"protocol": "responses"},
    ],
)
def test_binding_rejects_credential_mismatch_and_unapproved_destination(change):
    with pytest.raises(ValueError):
        binding(**change)


def test_catalog_requires_unique_identity_and_explicit_binding():
    b, c = binding(), capability()
    assert not ModelConfiguration(bindings=(b,), catalog=(c,)).catalog[0].verified
    for values in ({"bindings": (b, b)}, {"catalog": (c,)}, {"bindings": (b,), "catalog": (c, c)}):
        with pytest.raises(ValueError):
            ModelConfiguration(**values)
    with pytest.raises(ValueError):
        capability(verified=True)
    assert capability(verified=True, evidence_ref="acceptance/run-1").verified
