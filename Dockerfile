FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY configs ./configs
COPY integrations/qs_server/prompts ./integrations/qs_server/prompts
COPY integrations/qs_server/schemas ./integrations/qs_server/schemas
COPY integrations/qs_server/routes ./integrations/qs_server/routes
COPY integrations/qs_server/evaluation ./integrations/qs_server/evaluation
COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /usr/local/bin/uv
RUN uv sync --frozen --no-dev --no-editable
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --uid 10001 --create-home app
USER app
EXPOSE 8000
CMD ["/app/.venv/bin/python", "-m", "qs_ai.bootstrap.server"]
