from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from typing import Deque, Dict

from .config import SecurityConfig


class RateLimiter:
    def __init__(self, cfg: SecurityConfig):
        self._cfg = cfg
        self._window = 60
        self._requests: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        if not self._cfg.enable_rate_limiting:
            return True
        async with self._lock:
            now = time.time()
            window_start = now - self._window
            queue = self._requests[key]
            while queue and queue[0] < window_start:
                queue.popleft()
            if len(queue) >= self._cfg.max_requests_per_minute:
                return False
            queue.append(now)
            return True


__all__ = ["RateLimiter"]
