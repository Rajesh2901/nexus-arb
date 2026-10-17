"""
Smoke test to verify models, deduplication hashing, and token bucket rate limiter.
"""
import asyncio
from datetime import datetime, timezone
from config import settings
from models import RawNewsItem, RegionEnum, ManualNewsInjectionRequest
from rate_limiter import AsyncTokenBucket
from publisher import NewsStreamPublisher


async def run_tests():
    print("Testing News Ingestion Components...")

    # 1. Test Model Serialization
    item = RawNewsItem(
        id="test-hash-12345",
        source="TDNET",
        region=RegionEnum.JAPAN,
        language="ja",
        title="7203.T Toyota reports record operating profit",
        symbols=["7203.T", "TM"],
        published_at=datetime.now(timezone.utc),
    )
    json_data = item.model_dump_json()
    assert "7203.T" in json_data
    print("[OK] Model Serialization passed.")

    # 2. Test Content Hashing
    pub = NewsStreamPublisher()
    h1 = pub.generate_hash("TDNET", "Toyota announces EV expansion")
    h2 = pub.generate_hash("TDNET", "Toyota announces EV expansion")
    h3 = pub.generate_hash("TDNET", "Toyota announces EV expansion 2")
    assert h1 == h2
    assert h1 != h3
    print("[OK] Deterministic Deduplication Hashing passed.")

    # 3. Test Token Bucket Rate Limiter
    bucket = AsyncTokenBucket(rate_per_minute=60.0, burst_capacity=2)
    w1 = await bucket.acquire(1.0)
    w2 = await bucket.acquire(1.0)
    assert w1 == 0.0
    assert w2 == 0.0
    print("[OK] Token Bucket Burst passed.")

    print("All component tests passed successfully!")


if __name__ == "__main__":
    asyncio.run(run_tests())
