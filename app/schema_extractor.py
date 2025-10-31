from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any, Dict

import asyncpg

from .config import SchemaConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


_SCHEMA_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    d.description AS table_description,
    c.reltuples AS row_estimate,
    pg_total_relation_size(c.oid) AS size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
"""

_COLUMNS_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    a.attname AS column_name,
    format_type(a.atttypid, a.atttypmod) AS data_type,
    pg_get_expr(ad.adbin, ad.adrelid) AS default_value,
    a.attnotnull AS is_not_null,
    col_description(a.attrelid, a.attnum) AS column_description
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind = 'r'
"""

_FK_SQL = """
SELECT
    conrelid::regclass::text AS table_name,
    confrelid::regclass::text AS foreign_table_name,
    pg_get_constraintdef(oid) AS definition,
    conname AS constraint_name
FROM pg_constraint
WHERE contype = 'f'
"""

_INDEX_SQL = """
SELECT
    t.relname AS table_name,
    i.relname AS index_name,
    pg_get_indexdef(i.oid) AS index_definition,
    ix.indisunique AS is_unique
FROM pg_index ix
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
"""


class SchemaExtractor:
    def __init__(self, cfg: SchemaConfig):
        self._cfg = cfg
        self._lock = asyncio.Lock()
        self._snapshot: Dict[str, Any] = {}
        self._timestamp: dt.datetime | None = None

    async def get_schema_snapshot(self, conn: asyncpg.Connection, refresh: bool = False) -> Dict[str, Any]:
        if refresh or self._is_stale():
            async with self._lock:
                if refresh or self._is_stale():
                    self._snapshot = await self._collect(conn)
                    self._timestamp = dt.datetime.utcnow()
                    logger.info("schema_snapshot_refreshed", tables=len(self._snapshot.get("tables", {})))
        return self._snapshot

    def _is_stale(self) -> bool:
        if not self._snapshot or self._timestamp is None:
            return True
        return (dt.datetime.utcnow() - self._timestamp).total_seconds() > self._cfg.refresh_interval_s

    async def _collect(self, conn: asyncpg.Connection) -> Dict[str, Any]:
        tables = await conn.fetch(_SCHEMA_SQL)
        columns = await conn.fetch(_COLUMNS_SQL)
        foreign_keys = await conn.fetch(_FK_SQL)
        indexes = await conn.fetch(_INDEX_SQL)

        snapshot: Dict[str, Any] = {
            "generated_at": dt.datetime.utcnow().isoformat(),
            "tables": {},
            "foreign_keys": [],
            "indexes": {},
            "table_stats": {},
        }

        for row in tables:
            key = row["schema_name"] + "." + row["table_name"]
            snapshot["tables"][key] = {
                "schema": row["schema_name"],
                "name": row["table_name"],
                "description": row["table_description"],
                "row_estimate": int(row["row_estimate"] or 0),
                "size_bytes": int(row["size_bytes"] or 0),
                "columns": {},
            }
            snapshot["table_stats"][key] = {
                "row_estimate": int(row["row_estimate"] or 0),
                "size_bytes": int(row["size_bytes"] or 0),
            }

        for col in columns:
            key = col["schema_name"] + "." + col["table_name"]
            table = snapshot["tables"].setdefault(key, {
                "schema": col["schema_name"],
                "name": col["table_name"],
                "description": None,
                "row_estimate": 0,
                "size_bytes": 0,
                "columns": {},
            })
            table["columns"][col["column_name"]] = {
                "data_type": col["data_type"],
                "default_value": col["default_value"],
                "is_not_null": col["is_not_null"],
                "description": col["column_description"],
            }

        for fk in foreign_keys:
            snapshot["foreign_keys"].append({
                "constraint": fk["constraint_name"],
                "definition": fk["definition"],
                "table": fk["table_name"],
                "foreign_table": fk["foreign_table_name"],
            })

        for ix in indexes:
            snapshot["indexes"].setdefault(ix["table_name"], []).append({
                "index": ix["index_name"],
                "definition": ix["index_definition"],
                "is_unique": ix["is_unique"],
            })

        return snapshot


async def get_schema_snapshot(conn: asyncpg.Connection, refresh: bool = False) -> Dict[str, Any]:
    extractor = SchemaExtractor(SchemaConfig())
    return await extractor.get_schema_snapshot(conn, refresh=refresh)


__all__ = ["SchemaExtractor", "get_schema_snapshot"]
