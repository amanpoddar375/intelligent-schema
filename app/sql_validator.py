from __future__ import annotations

from typing import List

import sqlglot
from sqlglot import expressions as exp

from .config import PostgresConfig, SQLGuardrailConfig


class SQLValidationError(ValueError):
    pass


class SQLValidator:
    def __init__(self, pg_cfg: PostgresConfig, guard_cfg: SQLGuardrailConfig):
        self._pg_cfg = pg_cfg
        self._guard_cfg = guard_cfg

    def validate_and_sanitize(self, sql: str) -> str:
        try:
            parsed = sqlglot.parse_one(sql, read="postgres")
        except sqlglot.errors.ParseError as exc:
            raise SQLValidationError(f"Invalid SQL: {exc}") from exc
        self._enforce_select_only(parsed)
        self._enforce_limit(parsed)
        self._enforce_disallowed_functions(parsed)
        return parsed.sql()

    def _enforce_select_only(self, expr: exp.Expression) -> None:
        if not isinstance(expr, exp.Select):
            raise SQLValidationError("Only SELECT statements are allowed")
        if expr.args.get("from") is None:
            raise SQLValidationError("SELECT must include FROM clause")

    def _enforce_limit(self, expr: exp.Select) -> None:
        limit = expr.args.get("limit")
        max_limit = self._pg_cfg.max_limit
        if limit is None:
            expr.set("limit", exp.Limit(this=exp.Literal.number(max_limit)))
            return
        value = limit.this
        if isinstance(value, exp.Literal) and value.is_number:
            if int(value.this) > max_limit:
                limit.set("expression", exp.Literal.number(max_limit))
        else:
            raise SQLValidationError("LIMIT must be numeric literal")

    def _enforce_disallowed_functions(self, expr: exp.Expression) -> None:
        disallowed = {fn.lower() for fn in self._guard_cfg.disallowed_functions}
        for node in expr.walk():
            if isinstance(node, exp.Func) and node.name.lower() in disallowed:
                raise SQLValidationError(f"Function {node.name} is not allowed")


def validate_and_sanitize(sql: str, max_limit: int = 1000) -> str:
    pg_cfg = PostgresConfig(
        dsn="postgresql://placeholder",
        max_limit=max_limit,
        sample_limit=min(max_limit, 500),
    )
    guard_cfg = SQLGuardrailConfig()
    validator = SQLValidator(pg_cfg, guard_cfg)
    return validator.validate_and_sanitize(sql)


__all__ = ["SQLValidator", "SQLValidationError", "validate_and_sanitize"]
