from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, Optional

from redis import asyncio as redis_async
from redis.exceptions import RedisError

from .config import RedisConfig


class CacheClient:
    """Thin async Redis wrapper with JSON serialization."""

    def __init__(self, cfg: RedisConfig):
        self._cfg = cfg
        self._redis: Optional[redis_async.Redis] = None
        self._lock = asyncio.Lock()
        self._fallback: Dict[str, Any] = {}
        self._unavailable = False

    async def connect(self) -> None:
        async with self._lock:
            if self._redis is None:
                try:
                    self._redis = redis_async.from_url(
                        self._cfg.url,
                        encoding="utf-8",
                        decode_responses=True,
                    )
                except RedisError:
                    self._unavailable = True

    async def close(self) -> None:
        async with self._lock:
            if self._redis is not None:
                await self._redis.close()
                self._redis = None

    async def get_json(self, key: str) -> Optional[Any]:
        if self._unavailable:
            return self._fallback.get(key)
        redis = await self._ensure()
        try:
            payload = await redis.get(key)
        except RedisError:
            self._unavailable = True
            return self._fallback.get(key)
        if payload is None:
            return None
        return json.loads(payload)

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        if self._unavailable:
            self._fallback[key] = value
            return
        redis = await self._ensure()
        try:
            await redis.set(key, json.dumps(value), ex=ttl_seconds)
        except RedisError:
            self._unavailable = True
            self._fallback[key] = value

    async def _ensure(self) -> redis_async.Redis:
        if self._redis is None:
            await self.connect()
        assert self._redis is not None
        return self._redis


__all__ = ["CacheClient"]
