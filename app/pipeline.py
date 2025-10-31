from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import asyncpg

from .audit import AuditLogger
from .cache import CacheClient
from .config import LLMConfig, PostgresConfig, SchemaConfig, SQLGuardrailConfig, SecurityConfig
from .executor import QueryExecutor
from .guardrails import GuardrailEngine
from .logging_utils import get_logger
from .llm_reasoner import LLMReasoner
from .models import QueryRequest, QueryResponse
from .observability import REQUEST_COUNTER, record_latency
from .rate_limiter import RateLimiter
from .schema_extractor import SchemaExtractor
from .schema_ranker import SchemaRanker
from .schema_selector import select_schema_slice
from .sql_generator import SQLGenerator
from .sql_validator import SQLValidator
from .synthesizer import ResponseSynthesizer

logger = get_logger(__name__)


class RateLimitExceeded(Exception):
    pass


class QueryPipeline:
    def __init__(
        self,
        pg_cfg: PostgresConfig,
        schema_cfg: SchemaConfig,
        guard_cfg: SQLGuardrailConfig,
        security_cfg: SecurityConfig,
        schema_extractor: SchemaExtractor,
        schema_ranker: SchemaRanker,
        reasoner: LLMReasoner,
        sql_generator: SQLGenerator,
        sql_validator: SQLValidator,
        guardrail_engine: GuardrailEngine,
        executor: QueryExecutor,
        synthesizer: ResponseSynthesizer,
        cache_client: CacheClient,
        audit_logger: AuditLogger,
        rate_limiter: RateLimiter,
    ):
        self._pg_cfg = pg_cfg
        self._schema_cfg = schema_cfg
        self._guard_cfg = guard_cfg
        self._security_cfg = security_cfg
        self._schema_extractor = schema_extractor
        self._schema_ranker = schema_ranker
        self._reasoner = reasoner
        self._sql_generator = sql_generator
        self._sql_validator = sql_validator
        self._guardrail_engine = guardrail_engine
        self._executor = executor
        self._synthesizer = synthesizer
        self._cache = cache_client
        self._audit = audit_logger
        self._rate_limiter = rate_limiter
        self._schema_lock = asyncio.Lock()

    async def handle(self, conn: asyncpg.Connection, request: QueryRequest) -> QueryResponse:
        user_key = request.user_id or "anonymous"
        if not await self._rate_limiter.allow(user_key):
            REQUEST_COUNTER.labels(status="rate_limited").inc()
            raise RateLimitExceeded("Rate limit exceeded")

        with record_latency("total"):
            schema_snapshot = await self._get_schema_snapshot(conn, refresh=request.refresh_schema)
            with record_latency("ranking"):
                ranked_tables = self._schema_ranker.rank_tables(
                    request.query,
                    schema_snapshot,
                    top_n=self._schema_cfg.ranker_top_n,
                )
            schema_slice = select_schema_slice(schema_snapshot, ranked_tables, self._schema_cfg)
            with record_latency("reasoner"):
                reasoner_output = await self._reasoner.reason_schema_with_llm(request.query, schema_slice)
            with record_latency("sql_generation"):
                plans = await self._sql_generator.generate(
                    reasoner_output.get("query_intent", request.query),
                    reasoner_output.get("schema_context", {}),
                    reasoner_output.get("relevant_tables", []),
                    reasoner_output.get("foreign_keys_map", []),
                )
            if not plans:
                raise ValueError("SQL generator returned no plans")
            primary_sql = plans[0]["sql"]
            with record_latency("validation"):
                sanitized_sql = self._sql_validator.validate_and_sanitize(primary_sql)
            with record_latency("guardrails"):
                allowed, guard_metrics = await self._guardrail_engine.guardrail_check(
                    conn,
                    sanitized_sql,
                    schema_snapshot.get("table_stats", {}),
                )
                if not allowed:
                    REQUEST_COUNTER.labels(status="rejected").inc()
                    raise ValueError("Guardrails rejected query")
            with record_latency("execution"):
                execution_result = await self._executor.execute_sql(conn, sanitized_sql)
            with record_latency("synthesis"):
                answer = await self._synthesizer.synthesize(
                    request.query,
                    sanitized_sql,
                    execution_result["data"],
                    execution_result["metadata"],
                )

        REQUEST_COUNTER.labels(status="success").inc()
        self._audit.write({
            "user_id": user_key,
            "query": request.query,
            "sql": sanitized_sql,
            "metadata": execution_result.get("metadata", {}),
            "guard_metrics": guard_metrics,
        })

        return QueryResponse(
            answer=answer,
            sql=sanitized_sql,
            rows=execution_result["data"],
            metadata=execution_result["metadata"],
        )

    async def _get_schema_snapshot(self, conn: asyncpg.Connection, refresh: bool) -> Dict[str, Any]:
        cache_key = "schema_snapshot"
        if not refresh:
            cached = await self._cache.get_json(cache_key)
            if cached:
                return cached
        async with self._schema_lock:
            snapshot = await self._schema_extractor.get_schema_snapshot(conn, refresh=refresh)
            ttl = self._schema_cfg.refresh_interval_s
            await self._cache.set_json(cache_key, snapshot, ttl)
            return snapshot


__all__ = ["QueryPipeline", "RateLimitExceeded"]
