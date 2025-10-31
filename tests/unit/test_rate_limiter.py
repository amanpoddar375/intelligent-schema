from __future__ import annotations

import pytest

from app.config import SecurityConfig
from app.rate_limiter import RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_blocks_after_threshold() -> None:
    cfg = SecurityConfig(max_requests_per_minute=2)
    limiter = RateLimiter(cfg)
    assert await limiter.allow("user")
    assert await limiter.allow("user")
    assert not await limiter.allow("user")
