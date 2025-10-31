from __future__ import annotations

import json
import pathlib
import time
from typing import Any, Dict

from .config import ObservabilityConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


class AuditLogger:
    def __init__(self, cfg: ObservabilityConfig):
        self._path = pathlib.Path(cfg.audit_log_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, payload: Dict[str, Any]) -> None:
        entry = {
            "timestamp": time.time(),
            **payload,
        }
        with self._path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")
        logger.info("audit_event", **payload)


__all__ = ["AuditLogger"]
