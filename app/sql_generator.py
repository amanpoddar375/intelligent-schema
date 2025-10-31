from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List

from dateutil import parser as date_parser

from .config import PostgresConfig


@dataclass
class SQLPlan:
    sql: str
    purpose: str
    expected_rows: str


class SQLGenerator:
    def __init__(self, cfg: PostgresConfig):
        self._cfg = cfg

    async def generate(self, query_intent: str, schema_context: Dict, relevant_tables: List[str], foreign_keys_map: List[List[str]]) -> List[Dict]:
        select_cols = self._build_select_columns(schema_context, relevant_tables)
        from_clause = self._build_from_clause(relevant_tables, foreign_keys_map)
        where_clauses = self._build_where_clauses(query_intent)
        sql = self._compose_sql(select_cols, from_clause, where_clauses)
        plan = SQLPlan(sql=sql, purpose=query_intent, expected_rows="unknown")
        return [plan.__dict__]

    def _build_select_columns(self, schema_context: Dict, relevant_tables: List[str]) -> List[str]:
        columns = []
        for table in relevant_tables:
            table_columns = schema_context.get(table, {}).get("columns", [])
            for column in table_columns[:5]:
                alias = f"{table.replace('.', '_')}_{column}"
                columns.append(f"{table}.{column} AS {alias}")
        return columns or ["*"]

    def _build_from_clause(self, relevant_tables: List[str], foreign_keys_map: List[List[str]]) -> str:
        if not relevant_tables:
            raise ValueError("No tables provided for SQL generation")
        base = relevant_tables[0]
        joins = []
        for fk in foreign_keys_map:
            left_table, left_col, right_table, right_col = fk
            if left_table in relevant_tables and right_table in relevant_tables:
                joins.append(f"LEFT JOIN {right_table} ON {left_table}.{left_col} = {right_table}.{right_col}")
        return " ".join([base, *joins])

    def _build_where_clauses(self, query_intent: str) -> List[str]:
        clauses: List[str] = []
        lowered = query_intent.lower()
        if "last" in lowered and "day" in lowered:
            match = re.search(r"last (\d+) day", lowered)
            days = int(match.group(1)) if match else 30
            clauses.append(f"created_at >= CURRENT_DATE - INTERVAL '{days} days'")
        if "active" in lowered:
            clauses.append("status = 'active'")
        if date_match := re.search(r"(\d{4}-\d{2}-\d{2})", lowered):
            try:
                iso_date = date_parser.parse(date_match.group(1)).date().isoformat()
                clauses.append(f"created_at >= DATE '{iso_date}'")
            except (ValueError, OverflowError):
                pass
        return clauses

    def _compose_sql(self, select_cols: List[str], from_clause: str, where_clauses: List[str]) -> str:
        select_clause = ",\n       ".join(select_cols)
        where_clause = ""
        if where_clauses:
            where_clause = "\nWHERE " + " AND ".join(where_clauses)
        limit_clause = f"\nLIMIT {self._cfg.sample_limit}"
        return f"SELECT\n       {select_clause}\nFROM {from_clause}{where_clause}{limit_clause};"


async def generate_sql(query_intent: str, schema_context: Dict) -> List[Dict]:
    raise NotImplementedError("Use SQLGenerator class with dependencies")


__all__ = ["SQLGenerator", "SQLPlan", "generate_sql"]
