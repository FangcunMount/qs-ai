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


def readable_routes(*, history: bool = True) -> RouteResolver:
    from reliable_messaging.transport import READABLE_NAMING_V1, physical_topic_v3

    old = tuple(
        legacy_routes().publish_route(RouteKey(topic, "TRANSACTION", owner, "m3-1"))
        for topic, owner in ((COMMANDS, "qs.mysql"), (ACKS, "qs.mysql"), (EVENTS, "ai.mysql"))
    )
    current = tuple(
        RouteBinding(
            topic=route.topic,
            physical_topic=physical_topic_v3(
                route.topic, route.message_type, route.owner, route.route_revision
            ),
            message_type=route.message_type,
            owner=route.owner,
            route_revision=route.route_revision,
            naming_scheme=READABLE_NAMING_V1,
        )
        for route in old
    )
    return RouteResolver(current, old if history else ())
