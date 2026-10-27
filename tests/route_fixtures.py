"""Explicit trusted v2 identities for storage-only/unit fixtures."""

from reliable_messaging.transport import RouteBinding, RouteKey, RouteResolver, physical_topic_v2

from qs_ai.infrastructure.workflow_transport.messaging import ACKS, COMMANDS, EVENTS


def legacy_routes() -> RouteResolver:
    return RouteResolver(
        tuple(
            RouteBinding(
                topic=topic,
                physical_topic=physical_topic_v2(topic, "TRANSACTION", owner, "m3-1"),
                message_type="TRANSACTION",
                owner=owner,
                route_revision="m3-1",
            )
            for topic, owner in ((COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql"))
        )
    )


COMMAND_TOPIC = (
    legacy_routes()
    .publish_route(RouteKey(COMMANDS, "TRANSACTION", "qs.mysql", "m3-1"))
    .physical_topic
)
