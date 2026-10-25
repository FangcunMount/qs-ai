import json
from unittest.mock import AsyncMock

import pytest

from qs_ai.bootstrap.messaging import MessagingRuntime
from qs_ai.config import MessagingOptions
from tests.test_mq_deployment import binding, runtime


def rocketmq_binding():
    return binding()


def test_rocketmq_deployment_freezes_provider_files_and_ca_environment():
    service = runtime(rocketmq_binding())["services"]["qs-ai"]
    options = MessagingOptions.model_validate_json(service["environment"]["QS_AI_MESSAGING"])
    assert options.provider == "rocketmq"
    assert not options.publish_enabled
    assert not options.nsqd
    assert service["environment"]["GRPC_DEFAULT_SSL_ROOTS_FILE_PATH"] == options.rocketmq.ca_file
    assert len(service["volumes"]) == 7
    assert all(
        mount["read_only"] and not mount["bind"]["create_host_path"] for mount in service["volumes"]
    )
    assert "password" not in json.dumps(service)


def test_rocketmq_binding_rejects_unreviewed_credentials_mount():
    value = rocketmq_binding()
    value["rocketmq"]["credentials_file"] = "/tmp/unreviewed.json"
    with pytest.raises(ValueError):
        runtime(value)


async def test_maintenance_gate_precedes_outbox_scan():
    value = MessagingRuntime()
    value.publish_enabled = False
    value.publisher_ready.set()
    value.relay = AsyncMock()
    assert await value.step() == 0
    value.relay.step.assert_not_awaited()
