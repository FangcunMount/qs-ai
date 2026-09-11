FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /usr/local/bin/uv
RUN uv sync --frozen --no-dev --no-editable
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --uid 10001 --create-home app
USER app
EXPOSE 8000
CMD ["/app/.venv/bin/uvicorn", "qs_ai.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
