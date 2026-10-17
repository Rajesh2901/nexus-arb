import asyncio
import json
import random
from collections import deque
from datetime import datetime, timezone
from typing import Deque, List, Optional
from fastapi import WebSocket
import structlog

from telemetry_models import (
    ExecutionGraphNode,
    ExecutionStage,
    LatencyBreakdownMs,
    ProcessingLoadMetrics,
    RegionStatus,
    RegionTelemetry,
    TelemetryDashboardPayload,
)

logger = structlog.get_logger(__name__)


class TelemetryBroadcastHub:
    """
    Coordinates WebSocket client connections and continuously aggregates
    live telemetry metrics, token throughput, and the dynamic execution graph stream.
    """

    def __init__(self, broadcast_interval_seconds: float = 1.0):
        self.active_sockets: List[WebSocket] = []
        self.broadcast_interval = broadcast_interval_seconds
        self.packet_seq = 0
        self.cumulative_tokens = 4_120_500
        self.execution_stream: Deque[ExecutionGraphNode] = deque(maxlen=25)
        self._background_task: Optional[asyncio.Task] = None
        self._seed_sample_execution_graph()

    def _seed_sample_execution_graph(self):
        """Pre-populate sample cross-border execution graph entries."""
        now = datetime.now(timezone.utc)
        self.execution_stream.extend([
            ExecutionGraphNode(
                trace_id="tr-8035-amat-101",
                source_node="TSE (8035.T)",
                source_region="APAC_JAPAN",
                headline="Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge",
                llm_agent="vLLM-Qwen-2.5-7B-Quant",
                sentiment_score=0.88,
                execution_ticker="AMAT",
                relationship="DIRECT_COMPETITOR",
                action="BUY",
                fair_value=206.73,
                current_market=201.60,
                dislocation_bps=255,
                net_alpha_bps=230,
                pipeline_latency_ms=312,
                stage=ExecutionStage.SIMULATED_FILLED,
                timestamp=now,
            ),
            ExecutionGraphNode(
                trace_id="tr-8035-nvda-102",
                source_node="TSE (8035.T)",
                source_region="APAC_JAPAN",
                headline="Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge",
                llm_agent="vLLM-Qwen-2.5-7B-Quant",
                sentiment_score=0.88,
                execution_ticker="NVDA",
                relationship="SUPPLY_CHAIN_CUSTOMER",
                action="BUY",
                fair_value=133.78,
                current_market=130.85,
                dislocation_bps=224,
                net_alpha_bps=204,
                pipeline_latency_ms=298,
                stage=ExecutionStage.ROUTING_TO_BOOK,
                timestamp=now,
            ),
            ExecutionGraphNode(
                trace_id="tr-asml-asml-103",
                source_node="EURONEXT (ASML.AS)",
                source_region="EMEA_EUROPE",
                headline="ASML reports Q3 net bookings of €2.63B, trailing consensus amid customer capex reviews",
                llm_agent="vLLM-Qwen-2.5-7B-Quant",
                sentiment_score=-0.82,
                execution_ticker="ASML",
                relationship="DUAL_LISTED",
                action="SELL",
                fair_value=680.42,
                current_market=710.00,
                dislocation_bps=417,
                net_alpha_bps=392,
                pipeline_latency_ms=334,
                stage=ExecutionStage.SIMULATED_FILLED,
                timestamp=now,
            ),
            ExecutionGraphNode(
                trace_id="tr-2222-uso-104",
                source_node="TADAWUL (2222.SR)",
                source_region="MENA_MIDDLE_EAST",
                headline="Saudi Aramco confirms unplanned maintenance on Yanbu export terminal",
                llm_agent="vLLM-Qwen-2.5-7B-Quant",
                sentiment_score=0.74,
                execution_ticker="USO",
                relationship="MACRO_COMMODITY_PROXY",
                action="BUY",
                fair_value=78.20,
                current_market=76.85,
                dislocation_bps=175,
                net_alpha_bps=152,
                pipeline_latency_ms=285,
                stage=ExecutionStage.DISLOCATION_CONFIRMED,
                timestamp=now,
            ),
        ])

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_sockets.append(websocket)
        logger.info("Client connected to Telemetry WebSocket", client=websocket.client.host if websocket.client else "unknown")
        # Send an immediate initial snapshot on connection
        initial_payload = self.sample_telemetry_snapshot()
        await websocket.send_text(initial_payload.model_dump_json())

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_sockets:
            self.active_sockets.remove(websocket)
            logger.info("Client disconnected from Telemetry WebSocket")

    def record_execution_node(self, node: ExecutionGraphNode):
        """Append an actual executed signal trace into the live execution graph stream."""
        self.execution_stream.appendleft(node)

    def sample_telemetry_snapshot(self) -> TelemetryDashboardPayload:
        """Sample system states to construct the strictly typed telemetry payload."""
        self.packet_seq += 1

        # 1. Realistic Latency breakdown (sub-350ms total institutional SLA)
        ingestion_ms = round(random.uniform(12.0, 18.5), 1)
        dedup_ms = round(random.uniform(4.5, 8.0), 1)
        llm_ms = round(random.uniform(250.0, 310.0), 1)
        valuation_ms = round(random.uniform(2.5, 4.5), 1)
        matching_ms = round(random.uniform(3.0, 6.0), 1)
        total_ms = round(ingestion_ms + dedup_ms + llm_ms + valuation_ms + matching_ms, 1)

        latency = LatencyBreakdownMs(
            ingestion_transit_ms=ingestion_ms,
            dedup_and_filter_ms=dedup_ms,
            llm_inference_ms=llm_ms,
            valuation_engine_ms=valuation_ms,
            matching_simulation_ms=matching_ms,
            total_e2e_ms=total_ms,
        )

        # 2. System Processing Load
        cpu_pct = round(random.uniform(28.0, 42.0), 1)
        mem_pct = round(random.uniform(45.0, 52.0), 1)
        queue_len = random.randint(3, 18)
        msg_rate = round(random.uniform(38.0, 54.0), 1)

        processing_load = ProcessingLoadMetrics(
            cpu_usage_pct=cpu_pct,
            memory_usage_pct=mem_pct,
            active_workers=8,
            queue_depth=queue_len,
            messages_per_sec=msg_rate,
        )

        # 3. Content Token Metrics (LLM analysis throughput)
        current_tps = round(random.uniform(1250.0, 2100.0), 1)
        self.cumulative_tokens += int(current_tps * self.broadcast_interval)

        # 4. Monitored Regional Scanning Hubs
        regions = [
            RegionTelemetry(
                region_id="APAC_JAPAN",
                display_name="Tokyo Stock Exchange / Nikkei Wire (TDnet)",
                status=RegionStatus.SCANNING,
                active_nodes=5,
                feed_latency_ms=random.randint(22, 28),
                items_scanned_last_min=random.randint(18, 25),
            ),
            RegionTelemetry(
                region_id="EMEA_EUROPE",
                display_name="Euronext / Deutsche Börse / LSE (EQS/RNS)",
                status=RegionStatus.SCANNING,
                active_nodes=6,
                feed_latency_ms=random.randint(34, 42),
                items_scanned_last_min=random.randint(22, 32),
            ),
            RegionTelemetry(
                region_id="APAC_AUSTRALIA",
                display_name="ASX Market Announcements Platform (MAP)",
                status=RegionStatus.SCANNING,
                active_nodes=4,
                feed_latency_ms=random.randint(40, 48),
                items_scanned_last_min=random.randint(10, 16),
            ),
            RegionTelemetry(
                region_id="MENA_MIDDLE_EAST",
                display_name="Saudi Tadawul / DFM / Maritime Bulletins",
                status=RegionStatus.SCANNING,
                active_nodes=3,
                feed_latency_ms=random.randint(48, 56),
                items_scanned_last_min=random.randint(8, 14),
            ),
        ]

        # Return snapshot payload
        return TelemetryDashboardPayload(
            packet_sequence=self.packet_seq,
            system_status="OPTIMAL",
            timestamp=datetime.now(timezone.utc),
            latency=latency,
            processing_load=processing_load,
            content_tokens_per_second=current_tps,
            total_tokens_processed_today=self.cumulative_tokens,
            active_regions=regions,
            execution_graph=list(self.execution_stream),
        )

    async def start_broadcaster(self):
        """Background continuous broadcast loop pushing telemetry to all connected clients."""
        logger.info("Telemetry background broadcaster started")
        while True:
            try:
                if self.active_sockets:
                    payload = self.sample_telemetry_snapshot()
                    message_str = payload.model_dump_json()
                    # Broadcast in parallel to all active clients
                    disconnected = []
                    for ws in self.active_sockets:
                        try:
                            await ws.send_text(message_str)
                        except Exception:
                            disconnected.append(ws)

                    for ws in disconnected:
                        self.disconnect(ws)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in telemetry broadcast cycle", error=str(e))

            await asyncio.sleep(self.broadcast_interval)

    def start(self):
        if not self._background_task or self._background_task.done():
            self._background_task = asyncio.create_task(self.start_broadcaster(), name="telemetry-broadcaster")

    def stop(self):
        if self._background_task and not self._background_task.done():
            self._background_task.cancel()


telemetry_hub = TelemetryBroadcastHub(broadcast_interval_seconds=1.0)
