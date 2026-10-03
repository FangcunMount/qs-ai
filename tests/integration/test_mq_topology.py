"""Required read-only readiness proof on a dedicated disposable NSQ 1.3.0."""

import json
import os
from urllib.parse import urlencode

import pytest
from reliable_messaging.wire import FAILED_CHANNEL, failed_topic
from tornado.httpclient import AsyncHTTPClient, HTTPClientError, HTTPRequest

from qs_ai.infrastructure.workflow_transport.messaging import ACKS, CHANNELS, COMMANDS
from qs_ai.infrastructure.workflow_transport.mq_failure import FailureTopology

pytestmark = pytest.mark.integration


async def test_preprovisioned_failure_topology_with_real_nsq():
    origin = os.environ.get("QS_MQ_NSQ_HTTP_URL")
    if not origin:
        pytest.fail("Required dedicated disposable QS_MQ_NSQ_HTTP_URL is missing")
    client = AsyncHTTPClient(force_instance=True)  # test host owns this instance
    created = []
    try:
        response = await client.fetch(origin + "/info", request_timeout=5)
        assert json.loads(response.body)["version"] == "1.3.0"
        response = await client.fetch(origin + "/stats?format=json", request_timeout=5)
        existing = {t["topic_name"] for t in json.loads(response.body)["topics"]}
        check = FailureTopology({"test-node:4150": origin}, client, (COMMANDS, ACKS))
        for original in (COMMANDS, ACKS):
            topic = failed_topic(original, CHANNELS[original])
            if topic not in existing:
                with pytest.raises((ValueError, HTTPClientError)):
                    await check("test-node:4150", topic, FAILED_CHANNEL)
                # Explicit test provisioning, outside the application's readiness callback.
                await client.fetch(
                    HTTPRequest(
                        origin + "/topic/create?" + urlencode({"topic": topic}),
                        method="POST",
                        body=b"",
                        request_timeout=5,
                    )
                )
                created.append(topic)
            await client.fetch(
                HTTPRequest(
                    origin
                    + "/channel/create?"
                    + urlencode({"topic": topic, "channel": FAILED_CHANNEL}),
                    method="POST",
                    body=b"",
                    request_timeout=5,
                )
            )
            await check("test-node:4150", topic, FAILED_CHANNEL)
    finally:
        for topic in created:  # only the topics this test created, never pre-existing queues
            await client.fetch(
                HTTPRequest(
                    origin + "/topic/delete?" + urlencode({"topic": topic}),
                    method="POST",
                    body=b"",
                    request_timeout=5,
                )
            )
        client.close()
