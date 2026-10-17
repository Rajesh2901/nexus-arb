import asyncio
import sys
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import structlog
import uvicorn

from telemetry_hub import telemetry_hub
from telemetry_models import ExecutionGraphNode, TelemetryDashboardPayload

# Configure logging
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.dev.ConsoleRenderer(colors=False),
    ],
    logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
)
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing NexusArb Telemetry API Gateway...")
    telemetry_hub.start()
    yield
    logger.info("Stopping NexusArb Telemetry API Gateway...")
    telemetry_hub.stop()


app = FastAPI(
    title="NexusArb Telemetry Gateway",
    description="Real-time WebSocket & REST Telemetry API for institutional cross-border trading dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

# Enable CORS for React/Vite development server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# WebSocket Endpoint: Real-time Telemetry Stream
# ---------------------------------------------------------------------------

@app.websocket("/ws/telemetry")
async def websocket_telemetry_endpoint(websocket: WebSocket):
    """
    Primary WebSocket channel streaming real-time system metrics to the frontend dashboard.
    Emits continuous telemetry packets (1 Hz) containing:
      - Latency breakdown (ingestion, filter, LLM inference, valuation, matching in ms)
      - Processing load (CPU %, memory %, queue depth, msg/sec)
      - Content tokens processed per second (TPS)
      - Active international scanning nodes / regions (Japan, Europe, Australia, MENA)
      - Real-time execution graph array (Source Node -> LLM Agent -> Execution Ticker)
    """
    await telemetry_hub.connect(websocket)
    try:
        while True:
            # Maintain active connection and process client heartbeat / pings
            msg = await websocket.receive_text()
            if msg == "ping":
                await websocket.send_text('{"type":"pong"}')
    except WebSocketDisconnect:
        telemetry_hub.disconnect(websocket)
    except Exception as e:
        logger.debug("Telemetry WebSocket connection closed", error=str(e))
        telemetry_hub.disconnect(websocket)


# ---------------------------------------------------------------------------
# REST Snapshot Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Monitoring"])
async def health_check():
    """Liveness probe for orchestration and load balancers."""
    return {
        "status": "healthy",
        "service": "nexus-telemetry-gateway",
        "active_ws_clients": len(telemetry_hub.active_sockets),
    }


@app.get(
    "/api/v1/telemetry/snapshot",
    response_model=TelemetryDashboardPayload,
    tags=["Telemetry"],
)
async def get_telemetry_snapshot():
    """Retrieve an instantaneous snapshot of the complete telemetry state."""
    return telemetry_hub.sample_telemetry_snapshot()


@app.get(
    "/api/v1/telemetry/execution-graph",
    response_model=List[ExecutionGraphNode],
    tags=["Telemetry"],
)
async def get_execution_graph():
    """Retrieve the recent sliding execution graph array."""
    return list(telemetry_hub.execution_stream)


@app.post(
    "/api/v1/telemetry/execution-node",
    status_code=status.HTTP_201_CREATED,
    tags=["Telemetry Ingestion"],
)
async def ingest_execution_trace(node: ExecutionGraphNode):
    """
    Called by the upstream execution/strategy engine to register a new
    arbitrage trade trace into the real-time execution graph.
    """
    telemetry_hub.record_execution_node(node)
    logger.info(
        "Recorded execution graph trace",
        trace_id=node.trace_id,
        source=node.source_node,
        target=node.execution_ticker,
        action=node.action,
        net_alpha_bps=node.net_alpha_bps,
    )
    return {"status": "success", "trace_id": node.trace_id}


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8002, reload=False)
