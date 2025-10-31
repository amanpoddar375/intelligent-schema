# Intelligent Schema-Aware Query Executor (ISAQE)

ISAQE turns natural-language questions into safe, optimized SQL against PostgreSQL and returns concise answers. It combines schema intelligence, LLM reasoning, guardrails, and observability in a FastAPI service.

## Getting Started

1. Install dependencies:
   ```bash
   pip install -e .[dev]
   ```
2. Export LLM credentials (optional; without a key, a deterministic stub client is used):
   ```bash
   export LLM_API_KEY="sk-..."
   ```
3. Configure Postgres/Redis/LLM endpoints in `config.yaml`.
4. Run the API:
   ```bash
   uvicorn app.api:app --reload
   ```

## Project Structure

- `app/` – application modules (schema discovery, ranking, LLM harnesses, SQL generation and validation, guardrails, execution, synthesis, API).
- `prompts/` – few-shot examples and JSON schemas for LLM responses.
- `scripts/` – operational utilities (embedding precomputation).
- `tests/` – unit and integration test scaffolding.
- `loadtest/` – Locust load profile.
- `docs/` – roadmap and operational guidance.

## Testing

```bash
pytest
```

## Key Modules

- `SchemaExtractor` – pulls metadata from Postgres catalogs, caches snapshots.
- `SchemaRanker` – semantic ranking via TF-IDF or precomputed embeddings.
- `LLMReasoner` – prompts LLM with compressed schema slice and validates JSON.
- `SQLGenerator` – deterministic SELECT generation with safe defaults.
- `SQLValidator` – SQL AST enforcement (`SELECT`-only, limit clamping, function allowlist).
- `GuardrailEngine` – `EXPLAIN` inspection, cost/row heuristics.
- `QueryExecutor` – asyncpg execution with sampling.
- `ResponseSynthesizer` – LLM or stub-generated narratives.

## Observability & Security

- Prometheus metrics, audit log, Structlog logging, rate limiting, read-only SQL enforcement.

## Deployment

Package ships as FastAPI app; run via Uvicorn/Gunicorn in Docker or Kubernetes. Configure connection pooling with `pgbouncer`, caching with Redis, and integrate tracing via OpenTelemetry.
