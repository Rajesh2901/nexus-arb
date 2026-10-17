from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class RegionStatus(str, Enum):
    SCANNING = "SCANNING"
    IDLE = "IDLE"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"


class ExecutionStage(str, Enum):
    INGESTED = "INGESTED"
    FILTERED = "FILTERED"
    LLM_ANALYZED = "LLM_ANALYZED"
    DISLOCATION_CONFIRMED = "DISLOCATION_CONFIRMED"
    ROUTING_TO_BOOK = "ROUTING_TO_BOOK"
    SIMULATED_FILLED = "SIMULATED_FILLED"
    REJECTED_RISK = "REJECTED_RISK"


class LatencyBreakdownMs(BaseModel):
    """Sub-millisecond granular telemetry latency metrics."""
    ingestion_transit_ms: float = Field(..., description="Wire source to parser transit time")
    dedup_and_filter_ms: float = Field(..., description="Tier 0 regex / ONNX fast filter latency")
    llm_inference_ms: float = Field(..., description="Tier 1 structured LLM inference latency")
    valuation_engine_ms: float = Field(..., description="Fair value & dislocation calculation time")
    matching_simulation_ms: float = Field(..., description="L2 order book matching & slippage simulation time")
    total_e2e_ms: float = Field(..., description="Total pipeline latency from event arrival to signal/order")


class ProcessingLoadMetrics(BaseModel):
    """System compute, worker, and queue utilization metrics."""
    cpu_usage_pct: float = Field(..., description="Worker pool CPU utilization percentage")
    memory_usage_pct: float = Field(..., description="Process memory utilization percentage")
    active_workers: int = Field(..., description="Active async ingestion and valuation workers")
    queue_depth: int = Field(..., description="Pending unprocessed events in Redis Stream")
    messages_per_sec: float = Field(..., description="Throughput velocity of incoming wire stories")


class RegionTelemetry(BaseModel):
    """Operational status of a monitored global market hub."""
    region_id: str = Field(..., description="e.g. 'APAC_JAPAN', 'EMEA_EUROPE', 'MENA_MIDDLE_EAST'")
    display_name: str = Field(..., description="Human-readable exchange & wire description")
    status: RegionStatus
    active_nodes: int = Field(..., description="Active feeds / spiders scanning this market")
    feed_latency_ms: int = Field(..., description="Ping latency to local exchange gateway")
    items_scanned_last_min: int = Field(..., description="Disclosures ingested in the last 60 seconds")


class ExecutionGraphNode(BaseModel):
    """
    Execution graph node mapping:
    Source Node (Foreign Exchange/Ticker) -> LLM Agent (Model/Classifier) -> Execution Target (US Ticker)
    """
    trace_id: str = Field(..., description="Unique distributed tracing ID for this arbitrage event")
    source_node: str = Field(..., description="e.g. 'TSE (8035.T)', 'EURONEXT (ASML.AS)'")
    source_region: str = Field(..., description="'APAC_JAPAN', 'EMEA_EUROPE', 'MENA_MIDDLE_EAST'")
    headline: str = Field(..., description="Triggering market disclosure headline")
    llm_agent: str = Field(..., description="Model identifier (e.g. 'vLLM-Qwen-2.5-7B-Quant', 'Gemini-Flash')")
    sentiment_score: float = Field(..., ge=-1.0, le=1.0)
    execution_ticker: str = Field(..., description="U.S. traded symbol (e.g. 'NVDA', 'AMAT', 'ASML')")
    relationship: str = Field(..., description="'DIRECT_ADR', 'SUPPLY_CHAIN', 'COMPETITOR'")
    action: str = Field(..., description="'BUY', 'SELL', 'HOLD'")
    fair_value: float = Field(..., description="Model calculated theoretical fair value")
    current_market: float = Field(..., description="Current pre-market trading price")
    dislocation_bps: int = Field(..., description="Gross dislocation in basis points")
    net_alpha_bps: int = Field(..., description="Expected net edge after spread and slippage")
    pipeline_latency_ms: int = Field(..., description="Time taken through all 3 stages in ms")
    stage: ExecutionStage = Field(..., description="Current status along the execution pipeline")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class TelemetryDashboardPayload(BaseModel):
    """
    Strict real-time telemetry payload broadcast over WebSockets
    to power the institutional frontend monitoring console.
    """
    packet_sequence: int
    system_status: str = Field(default="OPTIMAL", description="'OPTIMAL', 'DEGRADED', 'CIRCUIT_TRIPPED'")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # 1. Latency Breakdown
    latency: LatencyBreakdownMs
    
    # 2. Processing Load
    processing_load: ProcessingLoadMetrics
    
    # 3. Content Token Metrics
    content_tokens_per_second: float = Field(..., description="Token throughput through LLM analysis layer")
    total_tokens_processed_today: int
    
    # 4. Active Global Nodes / Regions
    active_regions: List[RegionTelemetry]
    
    # 5. Stream of Current Signals with Execution Graph Array
    execution_graph: List[ExecutionGraphNode]
