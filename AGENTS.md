# AGENTS.md

Async payment processing service: FastAPI, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ via FastStream, Alembic.
README.md is the spec: keep code and README in sync. Design decisions live in README, not in docstrings.

## Commands
- Install: `uv sync`
- Tests: `uv run pytest` (needs Docker: testcontainers starts PostgreSQL and RabbitMQ)
- Lint: `uv run ruff check . && uv run ruff format --check . && uv run mypy app`
- Run: `docker compose up --build`

## Rules
- Python 3.13, dependencies pinned in uv.lock. A new dependency needs a reason in the commit message.
- TDD, one behavior per cycle: failing test first, then the minimal code. Never weaken a test to make code pass.
- Tests never read DATABASE_URL or RABBITMQ_URL from the environment.
- Money is Decimal end to end. The `metadata` column maps to attribute `metadata_` (name reserved by SQLAlchemy).
- Commit messages in English, README in Russian.
