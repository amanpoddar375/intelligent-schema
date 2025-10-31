from __future__ import annotations

import pytest

from app.config import PostgresConfig, SQLGuardrailConfig
from app.sql_validator import SQLValidationError, SQLValidator


@pytest.fixture
def validator() -> SQLValidator:
    pg_cfg = PostgresConfig(dsn="postgresql://placeholder", max_limit=100, sample_limit=50)
    guard_cfg = SQLGuardrailConfig(disallowed_functions=["pg_sleep"])
    return SQLValidator(pg_cfg, guard_cfg)


def test_enforces_select_only(validator: SQLValidator) -> None:
    with pytest.raises(SQLValidationError):
        validator.validate_and_sanitize("DELETE FROM users")


def test_adds_limit_when_missing(validator: SQLValidator) -> None:
    sql = "SELECT id FROM users"
    sanitized = validator.validate_and_sanitize(sql)
    assert "LIMIT 100" in sanitized


def test_clamps_limit(validator: SQLValidator) -> None:
    sql = "SELECT id FROM users LIMIT 1000"
    sanitized = validator.validate_and_sanitize(sql)
    assert "LIMIT 100" in sanitized


def test_rejects_disallowed_function(validator: SQLValidator) -> None:
    with pytest.raises(SQLValidationError):
        validator.validate_and_sanitize("SELECT pg_sleep(1)")
