from __future__ import annotations

import functools
import pathlib
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator


class RetryConfig(BaseModel):
    attempts: int = Field(default=3, ge=1)
    backoff_seconds: float = Field(default=1.0, ge=0)


class AppConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "info"
    max_concurrency: int = Field(default=100, ge=1)
    request_timeout_s: int = Field(default=30, ge=1)


class PostgresConfig(BaseModel):
    dsn: str
    min_pool_size: int = Field(default=5, ge=1)
    max_pool_size: int = Field(default=20, ge=1)
    statement_timeout_ms: int = Field(default=5000, ge=100)
    sample_limit: int = Field(default=500, ge=1)
    max_limit: int = Field(default=1000, ge=1)

    @validator("max_pool_size")
    def validate_pool_sizes(cls, v: int, values: Dict[str, int]) -> int:
        min_size = values.get("min_pool_size", 1)
        if v < min_size:
            raise ValueError("max_pool_size must be >= min_pool_size")
        return v


class RedisConfig(BaseModel):
    url: str
    schema_cache_ttl_s: int = Field(default=7200, ge=60)
    embedding_cache_ttl_s: int = Field(default=86400, ge=60)


class LLMConfig(BaseModel):
    provider: str = "openai"
    model: str
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    max_tokens: int = Field(default=1200, ge=1)
    rate_limit_per_minute: int = Field(default=100, ge=1)
    reasoner_retry_config: RetryConfig = Field(default_factory=RetryConfig)
    synthesizer_retry_config: RetryConfig = Field(default_factory=RetryConfig)


class SchemaConfig(BaseModel):
    refresh_interval_s: int = Field(default=3600, ge=60)
    max_schema_slice_bytes: int = Field(default=8192, ge=1024)
    ranker_top_n: int = Field(default=8, ge=1)
    fk_depth: int = Field(default=2, ge=0, le=4)


class SQLGuardrailConfig(BaseModel):
    row_threshold: int = Field(default=500_000, ge=1)
    cost_threshold: int = Field(default=100_000, ge=1)
    max_estimated_time_ms: int = Field(default=2000, ge=1)
    require_where_for_large_tables: bool = True
    disallowed_functions: List[str] = Field(default_factory=list)


class ObservabilityConfig(BaseModel):
    enable_tracing: bool = True
    service_name: str = "isaqe"
    metrics_port: int = Field(default=9000, ge=0)
    audit_log_path: str = "logs/audit.log"


class SecurityConfig(BaseModel):
    enforce_read_only_role: bool = True
    enable_rate_limiting: bool = True
    max_requests_per_minute: int = Field(default=60, ge=1)
    ip_whitelist: List[str] = Field(default_factory=list)


class PromptsConfig(BaseModel):
    examples_path: str
    reasoner_schema: str
    synthesizer_schema: str


class Settings(BaseModel):
    environment: str = "development"
    app: AppConfig = Field(default_factory=AppConfig)
    postgres: PostgresConfig
    redis: RedisConfig
    llm: LLMConfig
    schema: SchemaConfig = Field(default_factory=SchemaConfig)
    sql_guardrails: SQLGuardrailConfig = Field(default_factory=SQLGuardrailConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    prompts: PromptsConfig


def _load_yaml(path: pathlib.Path) -> Dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@functools.lru_cache(maxsize=1)
def load_settings(path: Optional[str] = None) -> Settings:
    cfg_path = pathlib.Path(path or "config.yaml").resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found at {cfg_path}")
    raw = _load_yaml(cfg_path)
    return Settings(**raw)


def get_settings() -> Settings:
    return load_settings()
