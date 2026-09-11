"""HTTP process entry using the same settings loader as workers and migrations."""

import uvicorn

from qs_ai.bootstrap.api import create_app
from qs_ai.config import Settings


def main() -> None:
    settings = Settings()
    uvicorn.run(create_app(settings), **settings.http.model_dump())


if __name__ == "__main__":
    main()
