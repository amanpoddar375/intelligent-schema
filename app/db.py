from __future__ import annotations

import asyncpg

from .config import PostgresConfig


class Database:
    def __init__(self, cfg: PostgresConfig):
        self._cfg = cfg
        self._pool: asyncpg.pool.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=self._cfg.dsn,
                min_size=self._cfg.min_pool_size,
                max_size=self._cfg.max_pool_size,
                timeout=self._cfg.statement_timeout_ms / 1000,
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def acquire(self) -> asyncpg.Connection:
        if self._pool is None:
            await self.connect()
        assert self._pool is not None
        return await self._pool.acquire()

    async def release(self, conn: asyncpg.Connection) -> None:
        if self._pool is None:
            return
        await self._pool.release(conn)


__all__ = ["Database"]
