# Agent Viz: Visual Analytics for Agent Trajectories & Failure Modes

Agent Viz is an interactive debugging/observability tool for agent traces.

It is built for **human sensemaking**:
- inspect run trajectories step-by-step
- compare successful vs failed runs
- surface failure overlays and empty-space signals
- analyze fleet-level failure patterns
- create case studies and reviewer notes

Status: implemented through **Week 15** of the Project 1 roadmap.

## Stack
- Backend: FastAPI + DuckDB
- Frontend: Next.js + React + TypeScript
- Data pipeline: Python ingestion from canonical event-level traces

## Repository Structure
```text
agent-viz/
  Dataset/               # real OpenClaw session traces (source of truth)
  apps/
    api/                 # FastAPI app
    web/                 # Next.js app
  data/
    raw/project1_event_runs/generated/   # optional synthetic fallback
    manifests/all_runs_manifest.jsonl    # optional synthetic manifest
    legacy/
    normalized/
  db/
    traces.duckdb
  scripts/
    ingest_assetops.py
  docs/
```

## Prerequisites
- Python 3.11+
- Node.js 18+
- npm

## Quick Start (Copy/Paste)
From `agent-viz/`:

```bash
python -m pip install -r requirements.txt
cd apps/web && npm install && cd ../..
python scripts/ingest_assetops.py
```

Notes:
- Default ingest source is `dataset/` (or `Dataset/` fallback) for real OpenClaw session traces.
- To ingest synthetic traces instead:
```bash
python data/generate_traces.py --out-dir data/raw/project1_event_runs/generated --manifest-file data/manifests/all_runs_manifest.jsonl --seed 7 --overwrite
python scripts/ingest_assetops.py --raw-dir data/raw/project1_event_runs/generated
```

Run API (choose a free port; `8001` used below):
```bash
uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001
```

Run web:
```bash
cd apps/web
NEXT_PUBLIC_API_BASE_URL=http://localhost:8001 npm run dev
```

Open:
- `http://localhost:3000/` Run Explorer
- `http://localhost:3000/compare`
- `http://localhost:3000/analytics`
- `http://localhost:3000/case-studies`

## Core Features
- Run Explorer with rich filtering and run stats
- Trace Viewer with virtualized step timeline
- Failure overlays (`provided`, `taxonomy`, `heuristic`, `manual`)
- Gap signals (`missing_*`, `absent_*`, `never_*`)
- Compare view (raw + alignment-aware)
- Fleet analytics (labels, gaps, tool usage, prevalence metrics)
- Case-study mode and run-level reports (`json` + `md`)
- Reviewer notes and benchmark subset assignment

## API Endpoints (Key)
- `GET /health`
- `GET /runs`
- `GET /runs/{run_id}`
- `GET /runs/{run_id}/steps`
- `GET /runs/{run_id}/annotations`
- `POST /runs/{run_id}/annotations/manual`
- `GET /runs/{run_id}/failure-summary`
- `GET /runs/{run_id}/gaps`
- `GET /runs/{run_id}/report?format=json|md`
- `GET /compare?left_run_id=&right_run_id=&mode=raw|aligned`
- `GET /analytics/overview`
- `GET /analytics/failure-labels`
- `GET /analytics/tool-usage`
- `GET /analytics/gaps`
- `POST /runs/{run_id}/case-study`
- `GET /case-studies`
- `GET /runs/{run_id}/review-notes`
- `POST /runs/{run_id}/review-notes`
- `GET /evaluation/benchmark-subsets`
- `POST /runs/{run_id}/benchmark-subsets`
- `POST /evaluation/benchmark-subsets/auto-build`
- `POST /admin/recompute-heuristics`
- `POST /admin/rebuild-derived-data`

## Testing
```bash
python -m unittest tests/test_week9_pipeline.py
```

## Common Troubleshooting
1. `Failed to fetch runs (404)`
- Usually wrong API base URL or wrong server on that port.
- Verify:
```bash
curl "http://localhost:8001/health"
curl "http://localhost:8001/runs?limit=1"
```

2. Next.js error: `Cannot find module './425.js'`
- Clear stale cache and restart:
```bash
cd apps/web
rm -rf .next node_modules/.cache
NEXT_PUBLIC_API_BASE_URL=http://localhost:8001 npm run dev
```
- If still broken:
```bash
rm -rf .next node_modules package-lock.json
npm install
NEXT_PUBLIC_API_BASE_URL=http://localhost:8001 npm run dev
```

## Data Note
Current default dataset is real OpenClaw session JSONL traces from `Dataset/`.
Synthetic traces remain available as a fallback/dev dataset.

## Documentation
- `docs/current_status.md`
- `docs/project1_roadmap.md`
- `docs/schema_upgrade.md`
- `docs/heuristics.md`
- `docs/datasets.md`
- `shared/schema/trace_schema.md`
