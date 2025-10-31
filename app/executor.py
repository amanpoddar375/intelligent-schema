from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import asyncpg

from .config import PostgresConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


class QueryExecutor:
    def __init__(self, cfg: PostgresConfig):
        self._cfg = cfg

    async def execute_sql(self, conn: asyncpg.Connection, sql: str, timeout_s: int | None = None) -> Dict[str, Any]:
        timeout_s = timeout_s or self._cfg.statement_timeout_ms / 1000
        logger.info("execute_sql", sql=sql)
        try:
            rows = await asyncio.wait_for(conn.fetch(sql), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            raise TimeoutError("Query execution timed out") from exc
        data = [dict(row) for row in rows[: self._cfg.sample_limit]]
        metadata = {
            "rows_returned": len(data),
            "truncated": len(rows) > len(data),
        }
        return {
            "status": "success",
            "data": data,
            "metadata": metadata,
        }


async def execute_sql(conn: asyncpg.Connection, sql: str, timeout_s: int = 5) -> Dict[str, Any]:
    cfg = PostgresConfig(dsn="postgresql://placeholder", statement_timeout_ms=int(timeout_s * 1000))
    executor = QueryExecutor(cfg)
    return await executor.execute_sql(conn, sql, timeout_s=timeout_s)


__all__ = ["QueryExecutor", "execute_sql"]
