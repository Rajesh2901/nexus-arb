# Nexus-Arb: Cross-Border News Arbitrage Trading Engine

## Quick Start

### 1. Backend – News Ingestion Microservice
```bash
cd services/ingestion
uv venv --python 3.11 .venv
uv pip install -r requirements.txt
cp .env.example .env
python main.py        # Starts on :8001
```

### 2. Backend – Telemetry Gateway API
```bash
cd services/api
# (uses same .venv from ingestion)
python server.py      # Starts on :8002
```

### 3. Frontend – Institutional React Dashboard
```bash
cd web
npm install
npm run dev           # Starts on http://localhost:3000
```

---

## Architecture

```
nexus-arb/
├── services/
│   ├── ingestion/          # News wire collectors, Redis Streams, deduplication
│   │   ├── main.py
│   │   ├── config.py
│   │   ├── models.py
│   │   ├── collectors.py   # Japan (TSE), Europe (EQS/RNS), Australia (ASX), MENA (Tadawul)
│   │   ├── publisher.py    # Redis XADD + PUBLISH + SHA-256 dedup
│   │   └── rate_limiter.py # Async token bucket per provider
│   ├── strategy/           # Cross-border arbitrage signal engine
│   │   ├── arbitrage_engine.py  # ADR parity + spillover beta + time-decay formulas
│   │   ├── knowledge_graph.py   # Foreign → US ADR/peer mapping
│   │   └── models.py
│   └── api/                # FastAPI WebSocket telemetry gateway
│       ├── server.py       # /ws/telemetry + REST snapshot endpoints
│       ├── telemetry_hub.py
│       └── telemetry_models.py
└── web/                    # React 18 + TypeScript + Tailwind CSS + Recharts
    └── src/
        └── TelemetryDashboard.tsx
```

## Tech Stack
- **Backend**: Python 3.11, FastAPI, Redis 7 (Streams + Pub/Sub), PostgreSQL + TimescaleDB
- **AI/LLM**: vLLM (Qwen-2.5-7B), Gemini Flash / Claude fallback
- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, Recharts
