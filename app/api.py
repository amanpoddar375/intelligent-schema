from __future__ import annotations

import os

from fastapi import Depends, FastAPI, HTTPException

from .audit import AuditLogger
from .cache import CacheClient
from .config import Settings, get_settings
from .db import Database
from .executor import QueryExecutor
from .guardrails import GuardrailEngine
from .llm_client import build_llm_client
from .llm_reasoner import LLMReasoner
from .logging_utils import configure_logging, get_logger
from .models import QueryRequest, QueryResponse
from .observability import init_metrics_server
from .pipeline import QueryPipeline, RateLimitExceeded
from .prompts import PromptResources
from .rate_limiter import RateLimiter
from .schema_extractor import SchemaExtractor
from .schema_ranker import SchemaRanker
from .sql_generator import SQLGenerator
from .sql_validator import SQLValidator
from .synthesizer import ResponseSynthesizer

logger = get_logger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.app.log_level)
    init_metrics_server(settings.observability)

    app = FastAPI(title="ISAQE", version="0.1.0")

    db = Database(settings.postgres)
    cache = CacheClient(settings.redis)
    audit = AuditLogger(settings.observability)
    rate_limiter = RateLimiter(settings.security)
    prompts = PromptResources(settings.prompts)
    api_key = os.environ.get("LLM_API_KEY", "")
    llm_client = build_llm_client(settings.llm, api_key)

    schema_extractor = SchemaExtractor(settings.schema)
    schema_ranker = SchemaRanker(settings.schema)
    reasoner = LLMReasoner(settings.llm, llm_client, prompts)
    sql_generator = SQLGenerator(settings.postgres)
    sql_validator = SQLValidator(settings.postgres, settings.sql_guardrails)
    guardrails = GuardrailEngine(settings.sql_guardrails)
    executor = QueryExecutor(settings.postgres)
    synthesizer = ResponseSynthesizer(settings.llm, llm_client, prompts)

    pipeline = QueryPipeline(
        settings.postgres,
        settings.schema,
        settings.sql_guardrails,
        settings.security,
        schema_extractor,
        schema_ranker,
        reasoner,
        sql_generator,
        sql_validator,
        guardrails,
        executor,
        synthesizer,
        cache,
        audit,
        rate_limiter,
    )

    @app.on_event("startup")
    async def _startup() -> None:
        await db.connect()
        await cache.connect()
        logger.info("app_started")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await db.close()
        await cache.close()
        if hasattr(llm_client, "aclose"):
            await llm_client.aclose()  # type: ignore[attr-defined]
        logger.info("app_shutdown")

    async def get_pipeline() -> QueryPipeline:
        return pipeline

    async def get_connection():
        conn = await db.acquire()
        try:
            yield conn
        finally:
            await db.release(conn)

    @app.post("/query", response_model=QueryResponse)
    async def run_query(
        request: QueryRequest,
        pipeline: QueryPipeline = Depends(get_pipeline),
        conn=Depends(get_connection),
    ) -> QueryResponse:
        try:
            return await pipeline.handle(conn, request)
        except RateLimitExceeded as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception("query_failed", error=str(exc))
            raise HTTPException(status_code=500, detail="Query processing failed") from exc

    return app


app = create_app()


__all__ = ["create_app", "app"]
