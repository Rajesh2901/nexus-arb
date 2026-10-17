import asyncio
import time
from typing import Dict
import structlog

logger = structlog.get_logger(__name__)


class AsyncTokenBucket:
    """
    Token bucket algorithm for rate-limiting async HTTP requests.
    Supports burst capacity and continuous fractional token replenishment.
    """

    def __init__(self, rate_per_minute: float, burst_capacity: int = 1):
        self.capacity = float(burst_capacity)
        self.tokens = float(burst_capacity)
        self.fill_rate = rate_per_minute / 60.0  # tokens added per second
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> float:
        """
        Wait until enough tokens are available, then consume them.
        Returns the seconds waited (0.0 if tokens were immediately available).
        """
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now

            # Replenish tokens based on elapsed time, capped at capacity
            self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate wait time needed to replenish required tokens
            deficit = tokens - self.tokens
            wait_time = deficit / self.fill_rate

        # Sleep outside the lock so other tasks can evaluate if needed
        logger.debug("Rate limit hit, pacing request", wait_seconds=round(wait_time, 2))
        await asyncio.sleep(wait_time)

        async with self.lock:
            self.tokens = max(0.0, self.tokens - tokens)
            self.last_update = time.monotonic()
            return wait_time


class RateLimiterRegistry:
    """Registry maintaining distinct token buckets per external API provider."""

    def __init__(self):
        self._limiters: Dict[str, AsyncTokenBucket] = {}

    def register(self, provider_name: str, requests_per_minute: float, burst: int = 1):
        self._limiters[provider_name] = AsyncTokenBucket(
            rate_per_minute=requests_per_minute,
            burst_capacity=burst,
        )

    async def acquire(self, provider_name: str) -> float:
        if provider_name in self._limiters:
            return await self._limiters[provider_name].acquire()
        return 0.0


limiter_registry = RateLimiterRegistry()
