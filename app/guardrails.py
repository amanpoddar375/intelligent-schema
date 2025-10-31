from __future__ import annotations

import json
from typing import Dict, Tuple

import asyncpg

from .config import SQLGuardrailConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


class GuardrailEngine:
    def __init__(self, cfg: SQLGuardrailConfig):
        self._cfg = cfg

    async def guardrail_check(self, conn: asyncpg.Connection, sql: str, table_stats: Dict) -> Tuple[bool, Dict]:
        explain = await self._run_explain(conn, sql)
        metrics = self._extract_metrics(explain)
        allowed = self._apply_rules(metrics, table_stats)
        logger.info("guardrail_decision", allowed=allowed, metrics=metrics)
        return allowed, metrics

    async def _run_explain(self, conn: asyncpg.Connection, sql: str) -> Dict:
        query = f"EXPLAIN (FORMAT JSON) {sql}"
        rows = await conn.fetch(query)
        plan_json = rows[0][0]
        if isinstance(plan_json, str):
            return json.loads(plan_json)[0]
        return plan_json[0]

    def _extract_metrics(self, plan: Dict) -> Dict:
        root = plan.get("Plan", {})
        return {
            "plan_rows": root.get("Plan Rows", 0),
            "plan_width": root.get("Plan Width", 0),
            "total_cost": root.get("Total Cost", 0.0),
            "node_type": root.get("Node Type", ""),
        }

    def _apply_rules(self, metrics: Dict, table_stats: Dict) -> bool:
        if metrics["plan_rows"] > self._cfg.row_threshold:
            return False
        if metrics["total_cost"] > self._cfg.cost_threshold:
            return False
        if metrics["node_type"].lower() == "seq scan" and metrics["plan_rows"] > (self._cfg.row_threshold / 10):
            return False
        return True


async def guardrail_check(conn: asyncpg.Connection, sql: str, table_stats: Dict) -> Tuple[bool, Dict]:
    engine = GuardrailEngine(SQLGuardrailConfig())
    return await engine.guardrail_check(conn, sql, table_stats)


__all__ = ["GuardrailEngine", "guardrail_check"]
