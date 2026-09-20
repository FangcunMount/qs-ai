"""One-shot result delivery; production uses bootstrap.server."""

import argparse
import asyncio
import json

from qs_ai.application.integration.events import DeliverResults
from qs_ai.bootstrap.container import create_container
from qs_ai.bootstrap.server import DeliveryProvider
from qs_ai.config import Settings


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["deliver"])
    parser.parse_args()
    settings = Settings()
    container = create_container(settings, DeliveryProvider())
    try:
        async with container() as operation:
            count = await (await operation.get(DeliverResults)).once(settings.delivery.batch_size)
            print(json.dumps({"delivered": count}))
    finally:
        await container.close()


if __name__ == "__main__":
    asyncio.run(main())
