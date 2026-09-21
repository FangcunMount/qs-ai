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


def test_deepseek_credential_aliases_are_explicit_and_conflicts_are_redacted():
    for values in (
        {"model_api_key": "old-private"},
        {"deepseek_api_key": "old-private"},
        {"model_api_key": "old-private", "deepseek_api_key": "old-private"},
    ):
        assert Settings(**values).effective_deepseek_api_key.get_secret_value() == "old-private"
    with pytest.raises(ValueError, match="aliases disagree") as error:
        Settings(model_api_key="old-private", deepseek_api_key="new-private")
    assert "old-private" not in str(error.value)
    assert "new-private" not in str(error.value)


def test_production_catalog_defaults_are_explicit_and_keep_writes_closed():
    from pathlib import Path

    import yaml

    config = ModelConfiguration.model_validate(
        yaml.safe_load(Path("configs/production.yaml").read_text())["models"]
    )
    assert not config.v2_writes_enabled
    assert {entry.model_id for entry in config.catalog} == {
        "deepseek-v4-pro",
        "deepseek-flash",
        "glm-5.3",
        "glm-5.3-flash",
    }
    for entry in config.catalog:
        assert set(entry.defaults) == set(entry.purposes)
        if entry.model_id.startswith("glm-"):
            assert entry.thinking_modes == ("enabled",)
            assert all(value.thinking == "enabled" for value in entry.defaults.values())
        assert not entry.sampling_parameters
