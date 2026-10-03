import json
from types import SimpleNamespace

import pytest
from reliable_messaging.wire import FAILED_CHANNEL, failed_topic

from qs_ai.infrastructure.workflow_transport.messaging import CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.mq_failure import FailureTopology


async def test_failure_topology_is_one_bounded_read_on_configured_origin():
    calls = []
    topic = failed_topic(COMMANDS, CHANNELS[COMMANDS])

    class HTTP:
        async def fetch(self, request):
            calls.append(request)
            return SimpleNamespace(
                code=200,
                body=json.dumps(
                    {
                        "topics": [
                            {"topic_name": topic, "channels": [{"channel_name": FAILED_CHANNEL}]}
                        ]
                    }
                ).encode(),
            )

    check = FailureTopology({"node:4150": "http://127.0.0.1:4151"}, HTTP(), (COMMANDS,))
    assert not calls
    await check("node:4150", topic, FAILED_CHANNEL)
    assert len(calls) == 1
    request = calls[0]
    assert request.method == "GET" and request.request_timeout == 5
    assert request.connect_timeout == 5 and not request.follow_redirects
    assert request.url.startswith("http://127.0.0.1:4151/stats?")
    for args in (("other:4150", topic, FAILED_CHANNEL), ("node:4150", "bad", FAILED_CHANNEL)):
        with pytest.raises(ValueError):
            await check(*args)
    assert len(calls) == 1  # no endpoint discovery or mutation


async def test_missing_failure_subscription_fails_once_without_creating_it():
    calls = []

    class HTTP:
        async def fetch(self, request):
            calls.append(request)
            return SimpleNamespace(code=200, body=b'{"topics":[]}')

    check = FailureTopology({"node:4150": "http://127.0.0.1:4151"}, HTTP(), (COMMANDS,))
    with pytest.raises(ValueError, match="pre-provisioned"):
        await check("node:4150", failed_topic(COMMANDS, CHANNELS[COMMANDS]), FAILED_CHANNEL)
    assert len(calls) == 1 and calls[0].method == "GET"
