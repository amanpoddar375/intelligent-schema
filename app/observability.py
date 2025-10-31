from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Iterator

from prometheus_client import Counter, Histogram, start_http_server

from .config import ObservabilityConfig

REQUEST_LATENCY = Histogram("isaqe_request_latency_seconds", "Latency for user queries", ["stage"])
REQUEST_COUNTER = Counter("isaqe_requests_total", "Total processed queries", ["status"])


def init_metrics_server(cfg: ObservabilityConfig) -> None:
    if cfg.metrics_port > 0:
        start_http_server(cfg.metrics_port)


@contextmanager
def record_latency(stage: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        REQUEST_LATENCY.labels(stage=stage).observe(time.perf_counter() - start)


__all__ = ["init_metrics_server", "record_latency", "REQUEST_LATENCY", "REQUEST_COUNTER"]
