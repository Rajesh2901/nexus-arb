import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple
import redis.asyncio as aioredis
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import settings
from models import RawNewsItem, RegionEnum

logger = structlog.get_logger(__name__)


class NewsStreamPublisher:
    """
    Manages Redis connection, stream writes (XADD for persistent queues),
    real-time Pub/Sub broadcasts, and Redis-backed deduplication.
    """

    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None
        self.total_ingested = 0
        self.duplicates_suppressed = 0
        self.by_region: Dict[str, int] = {r.value: 0 for r in RegionEnum}
        self.last_event_time: Optional[datetime] = None

    async def connect(self):
        """Initialize Redis connection pool."""
        logger.info("Connecting to Redis", url=settings.REDIS_URL)
        self.redis = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
        )
        try:
            await self.redis.ping()
            logger.info("Successfully connected to Redis instance")
        except Exception as e:
            logger.error("Failed initial Redis ping, will retry in background", error=str(e))

    async def close(self):
        """Close connection cleanly."""
        if self.redis:
            await self.redis.aclose()
            logger.info("Redis connection closed")

    def generate_hash(self, source: str, title: str) -> str:
        """Deterministic SHA-256 hash based on normalized title and source."""
        normalized = f"{source.strip().lower()}:{title.strip().lower()}"
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    async def is_duplicate(self, content_hash: str) -> bool:
        """Check if story hash has been seen within the deduplication TTL."""
        if not self.redis:
            return False

        try:
            # SADD returns 1 if new, 0 if already existed in the set
            was_added = await self.redis.sadd(settings.DEDUP_SET_KEY, content_hash)
            if was_added == 1:
                # Ensure the key has expiration set
                await self.redis.expire(settings.DEDUP_SET_KEY, settings.DEDUP_TTL_SECONDS)
                return False
            else:
                return True
        except Exception as e:
            logger.warning("Redis dedup check error; failing open to avoid drop", error=str(e))
            return False

    async def publish_news(self, item: RawNewsItem) -> Tuple[bool, Optional[str]]:
        """
        Deduplicates, writes to Redis Stream (durable queue), and Pub/Sub channel (real-time).
        Returns (success_flag, item_id_or_reason).
        """
        # Deduplication check
        if await self.is_duplicate(item.id):
            self.duplicates_suppressed += 1
            logger.debug("Duplicate story suppressed", id=item.id, title=item.title[:60])
            return False, "DUPLICATE_SUPPRESSED"

        payload = item.model_dump_json()

        try:
            if self.redis:
                # 1. Persistent Stream (XADD) with approximate trimming to 50k items
                stream_id = await self.redis.xadd(
                    name=settings.REDIS_STREAM_KEY,
                    fields={"data": payload},
                    maxlen=50000,
                    approximate=True,
                )

                # 2. Ephemeral Pub/Sub Broadcast (PUBLISH) for active UI listeners
                await self.redis.publish(settings.REDIS_PUBSUB_CHANNEL, payload)

                # Update operational metrics
                self.total_ingested += 1
                self.by_region[item.region.value] = self.by_region.get(item.region.value, 0) + 1
                self.last_event_time = datetime.now(timezone.utc)

                logger.info(
                    "News item ingested & published",
                    source=item.source,
                    region=item.region.value,
                    symbols=item.symbols,
                    title=item.title[:70],
                    stream_id=stream_id,
                )
                return True, item.id
            else:
                logger.warning("Redis client not initialized; item not published", id=item.id)
                return False, "REDIS_UNAVAILABLE"

        except Exception as e:
            logger.error("Failed to publish to Redis stream/pubsub", error=str(e), id=item.id)
            return False, str(e)


publisher = NewsStreamPublisher()
