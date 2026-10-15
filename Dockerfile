FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH"
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app
COPY demo ./demo

RUN adduser --system --no-create-home app
USER app
