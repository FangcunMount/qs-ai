from dishka import AsyncContainer, Provider, make_async_container
from dishka.entities.validation_settings import STRICT_VALIDATION

from qs_ai.bootstrap.providers.assets import AssetProvider
from qs_ai.bootstrap.providers.core import OperationsProvider, PersistenceProvider, RuntimeProvider
from qs_ai.bootstrap.providers.generation import GenerationProvider
from qs_ai.bootstrap.providers.integration import IntegrationProvider
from qs_ai.bootstrap.providers.interpretation import InterpretationProvider
from qs_ai.config import Settings


def create_container(settings: Settings, *overrides: Provider) -> AsyncContainer:
    return make_async_container(
        RuntimeProvider(),
        PersistenceProvider(),
        OperationsProvider(),
        InterpretationProvider(),
        GenerationProvider(),
        IntegrationProvider(),
        AssetProvider(),
        *overrides,
        context={Settings: settings},
        validation_settings=STRICT_VALIDATION,
    )
