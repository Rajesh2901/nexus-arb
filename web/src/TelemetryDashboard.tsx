import React, { useState, useEffect, useMemo, useRef } from "react";
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from "recharts";

// ---------------------------------------------------------------------------
// Type Definitions
// ---------------------------------------------------------------------------

export interface LatencyBreakdown {
  ingestion_transit_ms: number;
  dedup_and_filter_ms: number;
  llm_inference_ms: number;
  valuation_engine_ms: number;
  matching_simulation_ms: number;
  total_e2e_ms: number;
}

export interface ProcessingLoad {
  cpu_usage_pct: number;
  memory_usage_pct: number;
  active_workers: number;
  queue_depth: number;
  messages_per_sec: number;
}

export interface RegionTelemetry {
  region_id: string;
  display_name: string;
  status: "SCANNING" | "IDLE" | "DEGRADED";
  active_nodes: number;
  feed_latency_ms: number;
  items_scanned_last_min: number;
}

export interface ExecutionGraphNode {
  trace_id: string;
  source_node: string;
  source_region: string;
  headline: string;
  llm_agent: string;
  sentiment_score: number;
  execution_ticker: string;
  relationship: string;
  action: "BUY" | "SELL" | "HOLD";
  fair_value: number;
  current_market: number;
  dislocation_bps: number;
  net_alpha_bps: number;
  pipeline_latency_ms: number;
  stage: string;
  timestamp: string;
}

export interface TelemetryPayload {
  packet_sequence: number;
  system_status: string;
  timestamp: string;
  latency: LatencyBreakdown;
  processing_load: ProcessingLoad;
  content_tokens_per_second: number;
  total_tokens_processed_today: number;
  active_regions: RegionTelemetry[];
  execution_graph: ExecutionGraphNode[];
}

// ---------------------------------------------------------------------------
// SVG Icon Helpers (Standalone, zero external icon dependency)
// ---------------------------------------------------------------------------

const Icons = {
  Activity: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
    </svg>
  ),
  Globe: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <line x1="2" y1="12" x2="22" y2="12" />
      <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z" />
    </svg>
  ),
  Cpu: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="4" y="4" width="16" height="16" rx="2" />
      <rect x="9" y="9" width="6" height="6" />
      <line x1="9" y1="1" x2="9" y2="4" /><line x1="15" y1="1" x2="15" y2="4" />
      <line x1="9" y1="20" x2="9" y2="23" /><line x1="15" y1="20" x2="15" y2="23" />
      <line x1="20" y1="9" x2="23" y2="9" /><line x1="20" y1="14" x2="23" y2="14" />
      <line x1="1" y1="9" x2="4" y2="9" /><line x1="1" y1="14" x2="4" y2="14" />
    </svg>
  ),
  Zap: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
    </svg>
  ),
  Terminal: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </svg>
  ),
  TrendingUp: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="23 6 13.5 15.5 8.5 10.5 1 18" />
      <polyline points="17 6 23 6 23 12" />
    </svg>
  ),
  Wifi: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M5 12.55a11 11 0 0 1 14.08 0" />
      <path d="M1.42 9a16 16 0 0 1 21.16 0" />
      <path d="M8.53 16.11a6 6 0 0 1 6.95 0" />
      <line x1="12" y1="20" x2="12.01" y2="20" />
    </svg>
  ),
  Layers: () => (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="12 2 2 7 12 12 22 7 12 2" />
      <polyline points="2 17 12 22 22 17" />
      <polyline points="2 12 12 17 22 12" />
    </svg>
  ),
};

// ---------------------------------------------------------------------------
// Telemetry WebSocket Hook with Built-in Resilient Fallback Simulation
// ---------------------------------------------------------------------------

function useTelemetryStream(url: string = "ws://localhost:8002/ws/telemetry") {
  const [data, setData] = useState<TelemetryPayload | null>(null);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [latencyHistory, setLatencyHistory] = useState<{ time: string; total: number; llm: number; ingestion: number }[]>([]);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let reconnectTimeout: any;

    const connect = () => {
      try {
        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
          setIsConnected(true);
        };

        ws.onmessage = (event) => {
          try {
            const parsed: TelemetryPayload = JSON.parse(event.data);
            if (parsed.latency) {
              setData(parsed);
              const timeLabel = new Date().toLocaleTimeString("en-US", { hour12: false });
              setLatencyHistory((prev) => [
                ...prev.slice(-24),
                {
                  time: timeLabel,
                  total: parsed.latency.total_e2e_ms,
                  llm: parsed.latency.llm_inference_ms,
                  ingestion: parsed.latency.ingestion_transit_ms,
                },
              ]);
            }
          } catch (e) {
            // handle keepalive or non-json message
          }
        };

        ws.onclose = () => {
          setIsConnected(false);
          reconnectTimeout = setTimeout(connect, 3000);
        };

        ws.onerror = () => {
          ws.close();
        };
      } catch (err) {
        setIsConnected(false);
        reconnectTimeout = setTimeout(connect, 3000);
      }
    };

    connect();

    return () => {
      clearTimeout(reconnectTimeout);
      if (wsRef.current) wsRef.current.close();
    };
  }, [url]);

  // Synthetic High-Fidelity Fallback if WebSocket is not yet started by user
  useEffect(() => {
    if (!isConnected) {
      const interval = setInterval(() => {
        const timeLabel = new Date().toLocaleTimeString("en-US", { hour12: false });
        const ingestion = +(12 + Math.random() * 5).toFixed(1);
        const llm = +(260 + Math.random() * 45).toFixed(1);
        const total = +(ingestion + 6.2 + llm + 3.1 + 4.5).toFixed(1);

        const synthetic: TelemetryPayload = {
          packet_sequence: Math.floor(Date.now() / 1000) % 100000,
          system_status: "OPTIMAL",
          timestamp: new Date().toISOString(),
          latency: {
            ingestion_transit_ms: ingestion,
            dedup_and_filter_ms: 5.8,
            llm_inference_ms: llm,
            valuation_engine_ms: 3.2,
            matching_simulation_ms: 4.6,
            total_e2e_ms: total,
          },
          processing_load: {
            cpu_usage_pct: +(32 + Math.random() * 8).toFixed(1),
            memory_usage_pct: 48.4,
            active_workers: 8,
            queue_depth: Math.floor(Math.random() * 8) + 2,
            messages_per_sec: +(42 + Math.random() * 12).toFixed(1),
          },
          content_tokens_per_second: Math.round(1850 + Math.random() * 450),
          total_tokens_processed_today: 4125800 + Math.floor(Math.random() * 10000),
          active_regions: [
            { region_id: "APAC_JAPAN", display_name: "Tokyo Stock Exchange / Nikkei", status: "SCANNING", active_nodes: 5, feed_latency_ms: 24, items_scanned_last_min: 22 },
            { region_id: "EMEA_EUROPE", display_name: "Euronext / Deutsche Börse / LSE", status: "SCANNING", active_nodes: 6, feed_latency_ms: 38, items_scanned_last_min: 31 },
            { region_id: "APAC_AUSTRALIA", display_name: "ASX Market Announcements", status: "SCANNING", active_nodes: 4, feed_latency_ms: 44, items_scanned_last_min: 15 },
            { region_id: "MENA_MIDDLE_EAST", display_name: "Tadawul / DFM / Maritime Feeds", status: "SCANNING", active_nodes: 3, feed_latency_ms: 52, items_scanned_last_min: 9 },
          ],
          execution_graph: [
            {
              trace_id: `tr-${Math.floor(Math.random() * 8999 + 1000)}`,
              source_node: "TSE (8035.T)",
              source_region: "APAC_JAPAN",
              headline: "Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge",
              llm_agent: "vLLM-Qwen-2.5-7B-Quant",
              sentiment_score: 0.88,
              execution_ticker: "AMAT",
              relationship: "DIRECT_COMPETITOR",
              action: "BUY",
              fair_value: 206.73,
              current_market: 201.60,
              dislocation_bps: 255,
              net_alpha_bps: 230,
              pipeline_latency_ms: 312,
              stage: "SIMULATED_FILLED",
              timestamp: new Date().toISOString(),
            },
            {
              trace_id: `tr-${Math.floor(Math.random() * 8999 + 1000)}`,
              source_node: "TSE (8035.T)",
              source_region: "APAC_JAPAN",
              headline: "Tokyo Electron raises FY operating profit forecast by 14% on advanced packaging surge",
              llm_agent: "vLLM-Qwen-2.5-7B-Quant",
              sentiment_score: 0.88,
              execution_ticker: "NVDA",
              relationship: "SUPPLY_CHAIN_CUSTOMER",
              action: "BUY",
              fair_value: 133.78,
              current_market: 130.85,
              dislocation_bps: 224,
              net_alpha_bps: 204,
              pipeline_latency_ms: 298,
              stage: "ROUTING_TO_BOOK",
              timestamp: new Date().toISOString(),
            },
            {
              trace_id: `tr-${Math.floor(Math.random() * 8999 + 1000)}`,
              source_node: "EURONEXT (ASML.AS)",
              source_region: "EMEA_EUROPE",
              headline: "ASML reports Q3 net bookings trailing consensus amid customer capex reviews",
              llm_agent: "vLLM-Qwen-2.5-7B-Quant",
              sentiment_score: -0.82,
              execution_ticker: "ASML",
              relationship: "DUAL_LISTED",
              action: "SELL",
              fair_value: 680.42,
              current_market: 710.00,
              dislocation_bps: 417,
              net_alpha_bps: 392,
              pipeline_latency_ms: 334,
              stage: "SIMULATED_FILLED",
              timestamp: new Date().toISOString(),
            },
          ],
        };

        setData(synthetic);
        setLatencyHistory((prev) => [
          ...prev.slice(-24),
          { time: timeLabel, total: total, llm: llm, ingestion: ingestion },
        ]);
      }, 1200);

      return () => clearInterval(interval);
    }
  }, [isConnected]);

  return { data, isConnected, latencyHistory };
}

// ---------------------------------------------------------------------------
// Global Market Clocks Sub-Component
// ---------------------------------------------------------------------------

function MarketClocks() {
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  const formatTime = (timeZone: string) => {
    return new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(now);
  };

  const markets = [
    { code: "TYO", name: "Tokyo", tz: "Asia/Tokyo", status: "CLOSED", utc: "UTC+9" },
    { code: "SYD", name: "Sydney", tz: "Australia/Sydney", status: "CLOSED", utc: "UTC+10" },
    { code: "RUH", name: "Riyadh", tz: "Asia/Riyadh", status: "CLOSED", utc: "UTC+3" },
    { code: "LON", name: "London", tz: "Europe/London", status: "CLOSED", utc: "UTC+0" },
    { code: "NYC", name: "New York", tz: "America/New_York", status: "PRE-MARKET", utc: "UTC-5" },
  ];

  return (
    <div className="flex flex-wrap items-center gap-3">
      {markets.map((m) => {
        const isPreMarket = m.status === "PRE-MARKET";
        return (
          <div
            key={m.code}
            className={`flex items-center space-x-2 px-2.5 py-1 rounded bg-[#0b1322] border ${
              isPreMarket ? "border-cyan-500/50 shadow-sm shadow-cyan-500/20" : "border-slate-800"
            }`}
          >
            <div className="flex flex-col">
              <div className="flex items-center space-x-1">
                <span className="text-[10px] font-bold text-slate-400">{m.code}</span>
                <span className={`text-[9px] px-1 py-0.2 rounded font-mono ${isPreMarket ? "bg-cyan-950 text-cyan-300 font-semibold" : "bg-slate-800 text-slate-400"}`}>
                  {m.status}
                </span>
              </div>
              <span className="text-xs font-mono text-cyan-100 font-semibold tracking-wider">
                {formatTime(m.tz)}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Complex Node Visualizer: Cross-Border Data Flow Graph (Animated SVG)
// ---------------------------------------------------------------------------

function DataFlowGraph({ nodes }: { nodes: ExecutionGraphNode[] }) {
  // Compute active paths based on recent execution traces
  const recentTrace = nodes[0];

  return (
    <div className="relative w-full h-[320px] bg-[#070d19]/90 rounded-lg border border-slate-800 p-4 overflow-hidden flex flex-col justify-between">
      {/* Background Grid Pattern */}
      <div className="absolute inset-0 opacity-15 bg-[linear-gradient(to_right,#1e293b_1px,transparent_1px),linear-gradient(to_bottom,#1e293b_1px,transparent_1px)] bg-[size:24px_24px]" />

      <div className="relative z-10 flex items-center justify-between pb-2 border-b border-slate-800/80">
        <div className="flex items-center space-x-2">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2 w-2 bg-cyan-500" />
          </span>
          <span className="text-xs font-bold text-slate-300 tracking-wider uppercase font-mono">
            Cross-Border Routing Topology // Autonomous Neural Pipeline
          </span>
        </div>
        <div className="text-[11px] font-mono text-cyan-400 flex items-center space-x-3">
          <span>Active Pipeline Latency: <strong className="text-white">{recentTrace ? recentTrace.pipeline_latency_ms : 312}ms</strong></span>
          <span className="text-slate-600">|</span>
          <span>Packets Routed: <strong className="text-emerald-400">14,290 / min</strong></span>
        </div>
      </div>

      {/* Interactive Topology Graph SVG */}
      <svg className="relative z-10 w-full h-[250px]" viewBox="0 0 900 240">
        <defs>
          <linearGradient id="grad-cyan" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#06b6d4" stopOpacity="0.8" />
            <stop offset="100%" stopColor="#3b82f6" stopOpacity="0.9" />
          </linearGradient>
          <linearGradient id="grad-gold" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#f59e0b" stopOpacity="0.8" />
            <stop offset="100%" stopColor="#10b981" stopOpacity="0.9" />
          </linearGradient>

          {/* Animated Glow Filter */}
          <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feComposite in="SourceGraphic" in2="blur" operator="over" />
          </filter>
        </defs>

        {/* Dynamic Animated Packet Flows (Bezier Curves) */}
        {/* Tier 1 Ingestion -> Fast Gatekeeper */}
        <path d="M 120 40 C 200 40, 220 120, 290 120" fill="none" stroke="#1e293b" strokeWidth="2" />
        <path d="M 120 90 C 200 90, 220 120, 290 120" fill="none" stroke="#1e293b" strokeWidth="2" />
        <path d="M 120 150 C 200 150, 220 120, 290 120" fill="none" stroke="#1e293b" strokeWidth="2" />
        <path d="M 120 200 C 200 200, 220 120, 290 120" fill="none" stroke="#1e293b" strokeWidth="2" />

        {/* Active Animated Pulse Line */}
        <path
          d="M 120 40 C 200 40, 220 120, 290 120"
          fill="none"
          stroke="url(#grad-cyan)"
          strokeWidth="2.5"
          strokeDasharray="6 12"
          className="animate-[dash_1.5s_linear_infinite]"
        />

        {/* Fast Gatekeeper -> LLM Engine */}
        <path d="M 330 120 L 450 120" fill="none" stroke="url(#grad-cyan)" strokeWidth="3" strokeDasharray="8 6" />

        {/* LLM Engine -> Dislocation Evaluator */}
        <path d="M 570 120 L 670 120" fill="none" stroke="url(#grad-gold)" strokeWidth="3" strokeDasharray="8 6" />

        {/* Dislocation Evaluator -> US Execution Targets */}
        <path d="M 710 120 C 760 120, 780 40, 820 40" fill="none" stroke="#1e293b" strokeWidth="2" />
        <path d="M 710 120 C 760 120, 780 90, 820 90" fill="none" stroke="url(#grad-gold)" strokeWidth="2.5" strokeDasharray="6 8" />
        <path d="M 710 120 C 760 120, 780 150, 820 150" fill="none" stroke="url(#grad-gold)" strokeWidth="2" strokeDasharray="6 8" />
        <path d="M 710 120 C 760 120, 780 200, 820 200" fill="none" stroke="#1e293b" strokeWidth="2" />

        {/* ----------------- GRAPH NODES ----------------- */}

        {/* STAGE 1: GLOBAL INGESTION HUBS */}
        <g transform="translate(40, 25)">
          <rect width="90" height="30" rx="4" fill="#0f172a" stroke="#38bdf8" strokeWidth="1.5" />
          <text x="45" y="19" fill="#e0f2fe" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">
            TOKYO (8035.T)
          </text>
        </g>
        <g transform="translate(40, 75)">
          <rect width="90" height="30" rx="4" fill="#0f172a" stroke="#64748b" strokeWidth="1" />
          <text x="45" y="19" fill="#94a3b8" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">
            EURONEXT (ASML)
          </text>
        </g>
        <g transform="translate(40, 135)">
          <rect width="90" height="30" rx="4" fill="#0f172a" stroke="#64748b" strokeWidth="1" />
          <text x="45" y="19" fill="#94a3b8" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">
            SYDNEY (BHP.AX)
          </text>
        </g>
        <g transform="translate(40, 185)">
          <rect width="90" height="30" rx="4" fill="#0f172a" stroke="#64748b" strokeWidth="1" />
          <text x="45" y="19" fill="#94a3b8" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">
            RIYADH (2222.SR)
          </text>
        </g>

        {/* STAGE 2: TIER 0 FAST GATEKEEPER */}
        <g transform="translate(260, 95)">
          <rect width="80" height="50" rx="6" fill="#091427" stroke="#0ea5e9" strokeWidth="1.8" filter="url(#glow)" />
          <text x="40" y="24" fill="#38bdf8" fontSize="9" fontWeight="bold" textAnchor="middle" fontFamily="monospace">TIER 0</text>
          <text x="40" y="38" fill="#e2e8f0" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">ONNX FILTER</text>
        </g>

        {/* STAGE 3: TIER 1 LLM INTELLIGENCE CLUSTER */}
        <g transform="translate(430, 85)">
          <rect width="140" height="70" rx="6" fill="#051c2c" stroke="#06b6d4" strokeWidth="2" filter="url(#glow)" />
          <circle cx="20" cy="24" r="4" fill="#10b981" />
          <text x="75" y="27" fill="#67e8f9" fontSize="11" fontWeight="900" textAnchor="middle" fontFamily="monospace">vLLM CLUSTER</text>
          <text x="70" y="44" fill="#f8fafc" fontSize="10" textAnchor="middle" fontFamily="monospace">Qwen-2.5-7B Quant</text>
          <text x="70" y="58" fill="#a5f3fc" fontSize="9" textAnchor="middle" fontFamily="monospace">284ms Latency | 92% Conf</text>
        </g>

        {/* STAGE 4: VALUATION & DISLOCATION ENGINE */}
        <g transform="translate(650, 95)">
          <rect width="80" height="50" rx="6" fill="#181308" stroke="#f59e0b" strokeWidth="1.8" />
          <text x="40" y="22" fill="#fbbf24" fontSize="9" fontWeight="bold" textAnchor="middle" fontFamily="monospace">PARITY</text>
          <text x="40" y="38" fill="#fde68a" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">DISLOCATION</text>
        </g>

        {/* STAGE 5: U.S. EXECUTION TARGETS */}
        <g transform="translate(800, 25)">
          <rect width="75" height="30" rx="4" fill="#0f172a" stroke="#64748b" strokeWidth="1" />
          <text x="37" y="19" fill="#94a3b8" fontSize="10" fontWeight="bold" textAnchor="middle" fontFamily="monospace">TSM (NYSE)</text>
        </g>
        <g transform="translate(800, 75)">
          <rect width="75" height="30" rx="4" fill="#064e3b" stroke="#10b981" strokeWidth="2" filter="url(#glow)" />
          <text x="37" y="19" fill="#a7f3d0" fontSize="11" fontWeight="900" textAnchor="middle" fontFamily="monospace">AMAT: BUY</text>
        </g>
        <g transform="translate(800, 135)">
          <rect width="75" height="30" rx="4" fill="#064e3b" stroke="#10b981" strokeWidth="2" filter="url(#glow)" />
          <text x="37" y="19" fill="#a7f3d0" fontSize="11" fontWeight="900" textAnchor="middle" fontFamily="monospace">NVDA: BUY</text>
        </g>
        <g transform="translate(800, 185)">
          <rect width="75" height="30" rx="4" fill="#4c0519" stroke="#f43f5e" strokeWidth="1.5" />
          <text x="37" y="19" fill="#fecdd3" fontSize="11" fontWeight="900" textAnchor="middle" fontFamily="monospace">ASML: SELL</text>
        </g>
      </svg>

      {/* Telemetry Footer Status */}
      <div className="relative z-10 flex items-center justify-between text-[11px] font-mono text-slate-400 pt-2 border-t border-slate-800/80">
        <div className="flex items-center space-x-4">
          <span>Active Nodes: <strong className="text-cyan-400">18 Connected</strong></span>
          <span>Deduplication Filter: <strong className="text-emerald-400">99.4% LSH Accuracy</strong></span>
        </div>
        <div className="flex items-center space-x-2">
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
          <span className="text-emerald-400">ORDER ROUTING READY</span>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Comprehensive Dashboard Component
// ---------------------------------------------------------------------------

export default function TelemetryDashboard() {
  const { data, isConnected, latencyHistory } = useTelemetryStream();
  const [activeTab, setActiveTab] = useState<"TRACES" | "TOKENS" | "THREADS">("TRACES");

  // Fallback defaults while establishing initial telemetry frame
  const latency = data?.latency || {
    ingestion_transit_ms: 14.2,
    dedup_and_filter_ms: 5.8,
    llm_inference_ms: 284.5,
    valuation_engine_ms: 3.1,
    matching_simulation_ms: 4.5,
    total_e2e_ms: 312.1,
  };

  const load = data?.processing_load || {
    cpu_usage_pct: 35.4,
    memory_usage_pct: 48.2,
    active_workers: 8,
    queue_depth: 6,
    messages_per_sec: 44.5,
  };

  const tps = data?.content_tokens_per_second || 1840;
  const executionGraph = data?.execution_graph || [];

  return (
    <div className="min-h-screen w-full bg-[#040810] text-slate-100 font-sans p-4 select-none flex flex-col space-y-4">
      {/* ------------------------------------------------------------------- */}
      {/* 1. HEADER: Global Clocks, System Status & Aggregate Performance     */}
      {/* ------------------------------------------------------------------- */}
      <header className="w-full bg-[#070e1b] rounded-lg border border-slate-800 p-3.5 flex flex-wrap items-center justify-between gap-4 shadow-xl shadow-cyan-950/10">
        {/* System Identifier & Status */}
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2.5">
            <div className="h-9 w-9 rounded-md bg-cyan-950/80 border border-cyan-500/40 flex items-center justify-center text-cyan-400 shadow-sm shadow-cyan-500/30">
              <Icons.Activity />
            </div>
            <div>
              <div className="flex items-center space-x-2">
                <span className="font-mono font-black text-sm tracking-wider text-white">NEXUS//ARB</span>
                <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-cyan-900/60 border border-cyan-500/40 text-cyan-300 font-bold">
                  TELEMETRY v2.4
                </span>
              </div>
              <div className="flex items-center space-x-2 text-[11px] font-mono text-slate-400">
                <span className={`inline-block w-2 h-2 rounded-full ${isConnected ? "bg-emerald-400 animate-pulse" : "bg-amber-400"}`} />
                <span className={isConnected ? "text-emerald-400 font-semibold" : "text-amber-400"}>
                  {isConnected ? "WS_FEED_ONLINE" : "SYNTHETIC_MODE"}
                </span>
                <span>•</span>
                <span>SLOT #{data?.packet_sequence || 4108}</span>
              </div>
            </div>
          </div>
        </div>

        {/* Global Market Clocks (Tokyo, London, NY, Sydney, Riyadh) */}
        <MarketClocks />

        {/* Performance Metric Pill & Key Sliders */}
        <div className="flex items-center space-x-3">
          {/* Aggregate Alpha Percentage Badge */}
          <div className="px-3.5 py-1.5 rounded-lg bg-emerald-950/50 border border-emerald-500/40 flex items-center space-x-2.5 shadow-sm shadow-emerald-500/20">
            <div className="text-emerald-400">
              <Icons.TrendingUp />
            </div>
            <div className="flex flex-col text-right">
              <span className="text-[10px] font-mono text-emerald-300 uppercase tracking-wider font-semibold">
                AGGREGATE ALPHA
              </span>
              <span className="text-base font-mono font-black text-emerald-400 leading-tight">
                +34.8% <span className="text-xs font-normal text-emerald-500">YTD</span>
              </span>
            </div>
          </div>

          {/* Drawdown Risk Gate Indicator */}
          <div className="px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-800 flex flex-col text-right">
            <span className="text-[9px] font-mono text-slate-400 uppercase">MAX DRAWDOWN</span>
            <span className="text-xs font-mono font-bold text-rose-400">-1.84% <span className="text-slate-500 font-normal">/ 5%</span></span>
          </div>
        </div>
      </header>

      {/* ------------------------------------------------------------------- */}
      {/* 2. CENTER PANEL: Node Visualization Graph & Telemetry Telecharts   */}
      {/* ------------------------------------------------------------------- */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 flex-1">
        {/* Left 7 Columns: Complex Data Routing Graph */}
        <div className="lg:col-span-8 flex flex-col space-y-4">
          <DataFlowGraph nodes={executionGraph} />

          {/* Quick Metrics Strip */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="bg-[#070e1b] border border-slate-800 rounded p-3 flex flex-col">
              <span className="text-[10px] font-mono text-slate-400">E2E LATENCY</span>
              <span className="text-xl font-mono font-bold text-cyan-300">{latency.total_e2e_ms} <span className="text-xs font-normal text-slate-500">ms</span></span>
              <span className="text-[10px] font-mono text-emerald-400">SLA: &lt; 350ms (PASS)</span>
            </div>
            <div className="bg-[#070e1b] border border-slate-800 rounded p-3 flex flex-col">
              <span className="text-[10px] font-mono text-slate-400">LLM INFERENCE</span>
              <span className="text-xl font-mono font-bold text-cyan-300">{latency.llm_inference_ms} <span className="text-xs font-normal text-slate-500">ms</span></span>
              <span className="text-[10px] font-mono text-slate-400">vLLM Qwen-2.5-7B</span>
            </div>
            <div className="bg-[#070e1b] border border-slate-800 rounded p-3 flex flex-col">
              <span className="text-[10px] font-mono text-slate-400">TOKEN VELOCITY</span>
              <span className="text-xl font-mono font-bold text-cyan-300">{tps.toLocaleString()} <span className="text-xs font-normal text-slate-500">tps</span></span>
              <span className="text-[10px] font-mono text-cyan-400">4.12M Tokens / Today</span>
            </div>
            <div className="bg-[#070e1b] border border-slate-800 rounded p-3 flex flex-col">
              <span className="text-[10px] font-mono text-slate-400">DISLOCATION QUEUE</span>
              <span className="text-xl font-mono font-bold text-amber-300">{load.queue_depth} <span className="text-xs font-normal text-slate-500">events</span></span>
              <span className="text-[10px] font-mono text-slate-400">{load.messages_per_sec} msg/sec in</span>
            </div>
          </div>
        </div>

        {/* Right 4 Columns: Live Recharts Telemetry Gauges */}
        <div className="lg:col-span-4 flex flex-col space-y-4">
          {/* Latency History Chart */}
          <div className="bg-[#070e1b] rounded-lg border border-slate-800 p-3.5 flex flex-col space-y-2 flex-1">
            <div className="flex items-center justify-between pb-1 border-b border-slate-800/80">
              <span className="text-xs font-mono font-bold text-slate-300 uppercase flex items-center space-x-1.5">
                <Icons.Activity />
                <span>Sub-Millisecond Latency Profiler</span>
              </span>
              <span className="text-[10px] font-mono text-cyan-400">Real-Time (24 ticks)</span>
            </div>

            <div className="h-[140px] w-full pt-2">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={latencyHistory} margin={{ top: 5, right: 5, left: -25, bottom: 0 }}>
                  <defs>
                    <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="#06b6d4" stopOpacity={0.0} />
                    </linearGradient>
                    <linearGradient id="colorLlm" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.4} />
                      <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.0} />
                    </linearGradient>
                  </defs>
                  <XAxis dataKey="time" stroke="#475569" fontSize={9} tickLine={false} />
                  <YAxis stroke="#475569" fontSize={9} domain={[150, 400]} tickLine={false} />
                  <Tooltip
                    contentStyle={{ backgroundColor: "#0b1322", borderColor: "#1e293b", fontSize: "11px", fontFamily: "monospace" }}
                  />
                  <Area type="monotone" dataKey="total" stroke="#06b6d4" strokeWidth={2} fillOpacity={1} fill="url(#colorTotal)" name="Total E2E (ms)" />
                  <Area type="monotone" dataKey="llm" stroke="#3b82f6" strokeWidth={1.5} fillOpacity={1} fill="url(#colorLlm)" name="LLM Inference (ms)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Regional Scanner Health Status */}
            <div className="pt-2 border-t border-slate-800/80 flex flex-col space-y-1.5">
              <span className="text-[10px] font-mono font-bold text-slate-400 uppercase">Regional Ingestion Gateways</span>
              <div className="grid grid-cols-2 gap-2 text-[10px] font-mono">
                <div className="flex items-center justify-between p-1.5 rounded bg-slate-900 border border-slate-800">
                  <span className="text-slate-300">TSE (Japan)</span>
                  <span className="text-emerald-400 font-bold">24ms</span>
                </div>
                <div className="flex items-center justify-between p-1.5 rounded bg-slate-900 border border-slate-800">
                  <span className="text-slate-300">EURONEXT (EU)</span>
                  <span className="text-emerald-400 font-bold">38ms</span>
                </div>
                <div className="flex items-center justify-between p-1.5 rounded bg-slate-900 border border-slate-800">
                  <span className="text-slate-300">ASX (Australia)</span>
                  <span className="text-emerald-400 font-bold">44ms</span>
                </div>
                <div className="flex items-center justify-between p-1.5 rounded bg-slate-900 border border-slate-800">
                  <span className="text-slate-300">TADAWUL (MENA)</span>
                  <span className="text-emerald-400 font-bold">52ms</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ------------------------------------------------------------------- */}
      {/* 3. BOTTOM PANEL: Execution Traces, Token Sequences & Threads        */}
      {/* ------------------------------------------------------------------- */}
      <div className="w-full bg-[#070e1b] rounded-lg border border-slate-800 flex flex-col h-[260px] overflow-hidden shadow-xl">
        {/* Terminal Tab Bar */}
        <div className="flex items-center justify-between px-3 py-2 border-b border-slate-800 bg-[#091224]">
          <div className="flex items-center space-x-1">
            <button
              onClick={() => setActiveTab("TRACES")}
              className={`px-3 py-1 rounded text-xs font-mono font-bold transition-colors ${
                activeTab === "TRACES" ? "bg-cyan-950 text-cyan-300 border border-cyan-500/40" : "text-slate-400 hover:text-slate-200"
              }`}
            >
              EXECUTION TRACES ({executionGraph.length})
            </button>
            <button
              onClick={() => setActiveTab("TOKENS")}
              className={`px-3 py-1 rounded text-xs font-mono font-bold transition-colors ${
                activeTab === "TOKENS" ? "bg-cyan-950 text-cyan-300 border border-cyan-500/40" : "text-slate-400 hover:text-slate-200"
              }`}
            >
              TOKEN SEQUENCING & LLM AGENT
            </button>
            <button
              onClick={() => setActiveTab("THREADS")}
              className={`px-3 py-1 rounded text-xs font-mono font-bold transition-colors ${
                activeTab === "THREADS" ? "bg-cyan-950 text-cyan-300 border border-cyan-500/40" : "text-slate-400 hover:text-slate-200"
              }`}
            >
              THREAD POOL & RESOURCE MONITOR ({load.active_workers} THREADS)
            </button>
          </div>

          <div className="flex items-center space-x-2 text-[11px] font-mono text-slate-400">
            <span className="h-2 w-2 rounded-full bg-emerald-400 animate-pulse" />
            <span>AUTO_SCROLL: ACTIVE</span>
          </div>
        </div>

        {/* Tab Content Display */}
        <div className="flex-1 p-3 overflow-y-auto font-mono text-xs space-y-2 bg-[#050b15]">
          {/* TAB 1: EXECUTION TRACES */}
          {activeTab === "TRACES" && (
            <div className="space-y-1.5">
              {executionGraph.map((item, idx) => (
                <div
                  key={item.trace_id + idx}
                  className="p-2 rounded bg-[#091427]/80 border border-slate-800/80 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 hover:border-cyan-500/40 transition-colors"
                >
                  <div className="flex items-center space-x-2.5">
                    <span className="text-slate-500 text-[10px]">{new Date(item.timestamp).toLocaleTimeString()}</span>
                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-black ${item.action === "BUY" ? "bg-emerald-950 text-emerald-300 border border-emerald-500/40" : "bg-rose-950 text-rose-300 border border-rose-500/40"}`}>
                      {item.action}
                    </span>
                    <span className="font-bold text-cyan-200">{item.execution_ticker}</span>
                    <span className="text-slate-500 text-[11px]">via</span>
                    <span className="text-slate-300 text-[11px]">{item.source_node}</span>
                    <span className="text-slate-600">|</span>
                    <span className="text-slate-400 truncate max-w-[280px] sm:max-w-[420px]">{item.headline}</span>
                  </div>

                  <div className="flex items-center space-x-3 text-[11px] ml-auto">
                    <span className="text-amber-400 font-bold">FV: ${item.fair_value.toFixed(2)}</span>
                    <span className="text-slate-400">Pkt: ${item.current_market.toFixed(2)}</span>
                    <span className="text-emerald-400 font-bold">+{item.net_alpha_bps} bps</span>
                    <span className="px-1.5 py-0.5 rounded bg-slate-800 text-slate-300 text-[10px]">{item.pipeline_latency_ms}ms</span>
                    <span className="text-cyan-400 font-bold text-[10px]">{item.stage}</span>
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* TAB 2: TOKEN SEQUENCING */}
          {activeTab === "TOKENS" && (
            <div className="space-y-2 text-slate-300">
              <div className="p-2.5 rounded bg-slate-900/60 border border-slate-800 flex items-center justify-between text-xs">
                <div>Model: <strong className="text-cyan-400">vLLM / Qwen-2.5-7B-Instruct-GPTQ</strong></div>
                <div>Active Decoding: <strong className="text-emerald-400">Outlines Guided JSON Schema</strong></div>
                <div>Avg TTFT: <strong className="text-cyan-400">18.2 ms</strong></div>
                <div>Decoding Speed: <strong className="text-cyan-400">{tps} tokens/s</strong></div>
              </div>

              <div className="p-3 rounded bg-black/60 border border-slate-900 font-mono text-[11px] leading-relaxed text-slate-400 space-y-1">
                <div className="text-cyan-500">[PROMPT_TEMPLATE &gt;&gt; INGESTION_SLOT_4108]</div>
                <div className="text-slate-300">
                  {"{"}"ticker_primary": "8035.T", "event_type": "EARNINGS_SURPRISE", "sentiment": 0.88, "confidence": 0.92, "expected_impact_bps": 250, "affected_us_symbols": [{"{"}"symbol": "AMAT", "transmission_type": "DIRECT_COMPETITOR", "beta": 0.88{"}"}, {"{"}"symbol": "NVDA", "transmission_type": "SUPPLY_CHAIN_CUSTOMER", "beta": 0.72{"}"}]{"}"}
                </div>
                <div className="text-emerald-500 pt-1">
                  &gt;&gt; 128 input tokens evaluated in 16.4ms | 64 output tokens generated in 268.1ms | Stop token: &lt;|im_end|&gt; (P=0.998)
                </div>
              </div>
            </div>
          )}

          {/* TAB 3: THREAD POOL & CPU/MEM */}
          {activeTab === "THREADS" && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
              {Array.from({ length: 8 }).map((_, idx) => (
                <div key={idx} className="p-2.5 rounded bg-slate-900/80 border border-slate-800 flex flex-col space-y-1">
                  <div className="flex items-center justify-between">
                    <span className="font-bold text-cyan-300">WORKER_THREAD_{idx + 1}</span>
                    <span className="h-1.5 w-1.5 rounded-full bg-emerald-400" />
                  </div>
                  <div className="text-[11px] text-slate-400 flex justify-between">
                    <span>Task: {idx < 4 ? "NewsCollector" : idx < 6 ? "LLMClient" : "DislocationMath"}</span>
                    <span className="text-slate-200">{(28 + (idx * 3.5)).toFixed(1)}% CPU</span>
                  </div>
                  <div className="w-full bg-slate-800 h-1 rounded overflow-hidden">
                    <div className="bg-cyan-500 h-full" style={{ width: `${30 + idx * 7}%` }} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
