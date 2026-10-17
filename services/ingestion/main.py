import asyncio
import json
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog
import uvicorn

from config import settings
from collectors import collector_manager
from models import IngestionMetrics, ManualNewsInjectionRequest, RawNewsItem, RegionEnum
from publisher import publisher


# ---------------------------------------------------------------------------
# Structured Logging Setup (structlog)
# ---------------------------------------------------------------------------
def setup_logging():
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    if settings.ENVIRONMENT == "production":
        # Format as strict JSON for Datadog / CloudWatch / ELK
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development human-friendly console output
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(colors=False),
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


setup_logging()
logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Application Lifespan (Startup & Shutdown)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "Booting Nexus News Ingestion Microservice",
        env=settings.ENVIRONMENT,
        redis=settings.REDIS_URL,
    )
    # 1. Connect Redis publisher & dedup pool
    await publisher.connect()

    # 2. Launch background collector tasks
    await collector_manager.start_all()

    yield

    # Clean teardown
    logger.info("Initiating graceful shutdown...")
    await collector_manager.stop_all()
    await publisher.close()
    logger.info("Microservice stopped cleanly.")


app = FastAPI(
    title="NexusArb News Ingestion Pipeline",
    description="Real-time multi-regional financial news ingestion for cross-border arbitrage",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for institutional React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Monitoring"])
async def health_check():
    """Liveness and readiness check verifying Redis connectivity."""
    redis_alive = False
    if publisher.redis:
        try:
            await publisher.redis.ping()
            redis_alive = True
        except Exception:
            redis_alive = False

    status_str = "healthy" if redis_alive else "degraded"
    status_code = status.HTTP_200_OK if redis_alive else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "status": status_str,
            "service": settings.SERVICE_NAME,
            "redis_connected": redis_alive,
            "active_collectors": len(collector_manager.get_collector_names()),
        },
    )


@app.get("/api/v1/metrics", response_model=IngestionMetrics, tags=["Metrics"])
async def get_metrics():
    """Retrieve throughput statistics, regional counts, and deduplication efficiency."""
    redis_alive = False
    if publisher.redis:
        try:
            await publisher.redis.ping()
            redis_alive = True
        except Exception:
            redis_alive = False

    return IngestionMetrics(
        total_ingested=publisher.total_ingested,
        duplicates_suppressed=publisher.duplicates_suppressed,
        by_region=publisher.by_region,
        last_event_time=publisher.last_event_time,
        redis_connected=redis_alive,
        active_collectors=collector_manager.get_collector_names(),
    )


@app.post("/api/v1/simulate/inject", tags=["Simulation & Testing"])
async def inject_news_event(req: ManualNewsInjectionRequest):
    """
    Manually inject an international market catalyst into the ingestion pipeline.
    Used for testing the downstream LLM analysis pipeline and execution simulator.
    """
    item_id = publisher.generate_hash(req.source, req.title)
    item = RawNewsItem(
        id=item_id,
        source=req.source,
        region=req.region,
        language=req.language,
        title=req.title,
        body=req.body,
        symbols=req.symbols,
        published_at=datetime.now(timezone.utc),
        metadata={"manual_injection": True},
    )
    published, reason = await publisher.publish_news(item)
    if not published:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"News event not ingested: {reason}",
        )
    return {"status": "success", "id": item.id, "published": True}


@app.get("/api/v1/stream/recent", tags=["Inspection"])
async def get_recent_stream_items(count: int = 20):
    """Inspect the most recent raw news events directly from the Redis Stream."""
    if not publisher.redis:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Redis unavailable")

    try:
        # XREVRANGE key + - COUNT N
        entries = await publisher.redis.xrevrange(settings.REDIS_STREAM_KEY, max="+", min="-", count=count)
        result = []
        for stream_id, fields in entries:
            raw_data = fields.get("data")
            if raw_data:
                result.append({"stream_id": stream_id, "item": json.loads(raw_data)})
        return {"count": len(result), "items": result}
    except Exception as e:
        logger.error("Failed to query Redis stream", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# WebSocket Hub: Real-Time Stream Broadcast to Frontends / Consumers
# ---------------------------------------------------------------------------

@app.websocket("/ws/news-feed")
async def websocket_news_feed(websocket: WebSocket):
    """
    Live WebSocket streaming connection.
    Subscribes to Redis Pub/Sub channel and pushes new news items instantaneously.
    """
    await websocket.accept()
    logger.info("New WebSocket client connected to news feed")

    if not publisher.redis:
        await websocket.send_text(json.dumps({"error": "Redis broker unavailable"}))
        await websocket.close()
        return

    pubsub = publisher.redis.pubsub()
    await pubsub.subscribe(settings.REDIS_PUBSUB_CHANNEL)

    async def forward_messages():
        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    await websocket.send_text(message["data"])
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("WebSocket client connection closed", error=str(e))

    forward_task = asyncio.create_task(forward_messages())

    try:
        # Keep connection open and listen for client pings
        while True:
            client_msg = await websocket.receive_text()
            if client_msg == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    finally:
        forward_task.cancel()
        await pubsub.unsubscribe(settings.REDIS_PUBSUB_CHANNEL)
        await pubsub.aclose()


# ---------------------------------------------------------------------------
# Entrypoint for direct execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.ENVIRONMENT == "development",
        log_level=settings.LOG_LEVEL.lower(),
    )
