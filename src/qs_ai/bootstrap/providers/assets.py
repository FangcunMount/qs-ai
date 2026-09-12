"""Shared immutable asset ports for governance and evaluation execution."""

from dishka import Provider, Scope, provide

from qs_ai.application.interpretation.profile_assets import ProfileAssets
from qs_ai.application.interpretation.prompt_assets import PromptAssets
from qs_ai.application.interpretation.route_assets import RouteAssets
from qs_ai.application.interpretation.schema_assets import SchemaAssets
from qs_ai.infrastructure.persistence.mysql.profile_assets import MySQLProfileAssets
from qs_ai.infrastructure.persistence.mysql.prompt_assets import MySQLPromptAssets
from qs_ai.infrastructure.persistence.mysql.route_assets import MySQLRouteAssets
from qs_ai.infrastructure.persistence.mysql.schema_assets import MySQLSchemaAssets


class AssetProvider(Provider):
    profiles = provide(MySQLProfileAssets, provides=ProfileAssets, scope=Scope.REQUEST)
    prompts = provide(MySQLPromptAssets, provides=PromptAssets, scope=Scope.REQUEST)
    routes = provide(MySQLRouteAssets, provides=RouteAssets, scope=Scope.REQUEST)
    schemas = provide(MySQLSchemaAssets, provides=SchemaAssets, scope=Scope.REQUEST)
