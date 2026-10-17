"""
Automated verification test for the Telemetry WebSocket endpoint and REST snapshot APIs.
Uses Starlette/FastAPI TestClient to test WebSocket connectivity and schema assertions.
"""
import json
from fastapi.testclient import TestClient
from server import app
from telemetry_models import TelemetryDashboardPayload


def test_telemetry_websocket_and_rest():
    client = TestClient(app)

    print("Running Telemetry API & WebSocket Verification Tests...")

    # 1. Test REST Snapshot Endpoint
    res = client.get("/api/v1/telemetry/snapshot")
    assert res.status_code == 200, f"Expected 200, got {res.status_code}"
    data = res.json()

    # Validate against strict Pydantic model
    parsed_payload = TelemetryDashboardPayload(**data)
    assert parsed_payload.system_status == "OPTIMAL"
    assert parsed_payload.latency.total_e2e_ms > 0
    assert parsed_payload.content_tokens_per_second > 0
    assert len(parsed_payload.active_regions) == 4
    assert len(parsed_payload.execution_graph) >= 1
    print("[OK] REST Snapshot schema validation passed.")

    # 2. Test WebSocket Live Streaming Endpoint
    with client.websocket_connect("/ws/telemetry") as websocket:
        # Receive the first immediate telemetry payload
        message = websocket.receive_text()
        ws_data = json.loads(message)

        # Assert all required visualization sections exist
        assert "latency" in ws_data
        assert "processing_load" in ws_data
        assert "content_tokens_per_second" in ws_data
        assert "active_regions" in ws_data
        assert "execution_graph" in ws_data

        # Verify Latency metrics
        latency = ws_data["latency"]
        print(f"  -> Total E2E Latency: {latency['total_e2e_ms']} ms (LLM: {latency['llm_inference_ms']} ms)")
        assert latency["total_e2e_ms"] > 0
        assert latency["llm_inference_ms"] > 0

        # Verify Processing Load
        load = ws_data["processing_load"]
        print(f"  -> CPU Load: {load['cpu_usage_pct']}%, Queue Depth: {load['queue_depth']}, Velocity: {load['messages_per_sec']} msg/s")
        assert load["cpu_usage_pct"] > 0

        # Verify Content Tokens Per Second
        tps = ws_data["content_tokens_per_second"]
        print(f"  -> Content Tokens/Sec: {tps} TPS")
        assert tps > 0

        # Verify Active Monitored Scanning Regions
        regions = ws_data["active_regions"]
        region_ids = [r["region_id"] for r in regions]
        print(f"  -> Active Scanning Regions: {region_ids}")
        assert "APAC_JAPAN" in region_ids
        assert "EMEA_EUROPE" in region_ids
        assert "APAC_AUSTRALIA" in region_ids
        assert "MENA_MIDDLE_EAST" in region_ids

        # Verify Execution Graph (Source Node -> LLM Agent -> Execution Ticker)
        graph = ws_data["execution_graph"]
        assert len(graph) > 0
        first_node = graph[0]
        print(f"  -> Execution Trace: [{first_node['source_node']}] -> ({first_node['llm_agent']}) -> [{first_node['execution_ticker']}] | Action: {first_node['action']} (+{first_node['net_alpha_bps']} bps)")
        assert "source_node" in first_node
        assert "llm_agent" in first_node
        assert "execution_ticker" in first_node
        assert "action" in first_node

        # Send heartbeat ping
        websocket.send_text("ping")
        resp = websocket.receive_text()
        assert "pong" in resp
        print("[OK] WebSocket ping-pong heartbeat passed.")

    print("\nAll Telemetry WebSocket and schema verification tests passed successfully!")


if __name__ == "__main__":
    test_telemetry_websocket_and_rest()
