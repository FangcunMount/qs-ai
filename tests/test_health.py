from dishka import Provider, Scope, provide
from fastapi.testclient import TestClient

from qs_ai.config import Settings
from qs_ai.infrastructure.persistence.mysql.database import Database
from qs_ai.main import create_app


def test_live_without_database_but_not_ready() -> None:
    with TestClient(create_app(Settings(_env_file=None, database_url=None))) as client:
        assert client.get("/healthz").json() == {"status": "ok", "service": "qs-ai"}
        response = client.get("/readyz")
        assert response.status_code == 503
        assert response.json()["database"] == "not_configured"


def test_database_failure_is_redacted() -> None:
    class BrokenEngine:
        def connect(self):
            raise RuntimeError("mysql://user:secret@example/database")

    class BrokenDatabase(Provider):
        @provide(scope=Scope.APP, provides=Database, override=True)
        def database(self) -> Database:
            database = Database(None)
            database.engine = BrokenEngine()
            return database

    app = create_app(Settings(_env_file=None, database_url=None), providers=(BrokenDatabase(),))
    with TestClient(app) as client:
        response = client.get("/readyz")
        assert response.status_code == 503
        assert "secret" not in response.text
        assert response.json()["database"] == "unavailable"


def test_retired_session_http_routes_are_not_registered() -> None:
    app = create_app(Settings(_env_file=None, database_url=None))
    with TestClient(app) as client:
        assert client.post("/v1/interpretation-sessions", json={}).status_code == 404
        assert client.get("/v1/interpretation-sessions/example").status_code == 404
        assert not any("interpretation-sessions" in route for route in app.openapi()["paths"])
