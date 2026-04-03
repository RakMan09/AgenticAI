# Week 1 + Week 2 Status (updated February 13, 2026)

## Week 1: Foundation (Data + Schema + Ingestion)
### Work completed
- Built project skeleton and Week 1 data pipeline components.
- Implemented `scripts/fetch_assetopsbench_subset.py` with two paths:
  - real trajectory discovery (when available)
  - fallback to IBM AssetOpsBench dummy benchmark/sample JSON files (for immediate progress).
- Implemented schema + docs in `/Users/raksh/Documents/New project/agent-viz/shared/schema/trace_schema.md` (`runs`, `steps`, `annotations`).
- Implemented `scripts/ingest_assetops.py` to:
  - normalize raw files
  - create/write `db/traces.duckdb`
  - export `data/normalized/traces.jsonl`
  - print run/step/tool/outcome summaries.
- Added dummy-record normalization so IBM benchmark/sample rows become timeline steps (including tool-call-like entries and error flags).
- Current ingest state from IBM dummy files:
  - `6073` runs
  - `12444` steps
  - `298` fail runs (error-highlight demo now works).

### Challenges
- Public AssetOpsBench repo/dataset does not provide ready-made trajectory JSON outputs for Track 1 in a directly ingestible form.
- True Track 1 trajectory generation is still blocked by missing Watsonx credentials:
  - `WATSONX_APIKEY`
  - `WATSONX_PROJECT_ID`
  - `WATSONX_URL`

## Week 2: MVP Visualization (API + UI)
### Work completed
- FastAPI backend implemented in `/Users/raksh/Documents/New project/agent-viz/apps/api`:
  - `GET /health`
  - `GET /runs` (all required filters)
  - `GET /runs/{run_id}`
  - `GET /runs/{run_id}/steps`
- Next.js UI implemented in `/Users/raksh/Documents/New project/agent-viz/apps/web`:
  - Run Explorer (`/`)
  - Trace Viewer (`/runs/[run_id]`)
  - step expand/collapse, tool payload display, error highlighting, jump-to-first-error, virtualized list.
- UI is now populated using IBM dummy data (not synthetic demo_task records), and build/type checks pass.

### Challenges
- Week 2 MVP is functional for demonstration, but it currently reflects transformed IBM dummy benchmark/sample data rather than genuine Track 1 execution traces.
- Final quality objective (real trajectory browsing) depends on unblocking Week 1 Watsonx credentials and generating true Track 1 outputs.

## Remaining blocker to close Week 1 + Week 2 with real traces
- Set Watsonx credentials in `/tmp/AssetOpsBench/benchmark/cods_track1/.env.local`.
- Run local Track 1 pipeline to generate `Q_*_trajectory.json`.
- Copy generated files into `/Users/raksh/Documents/New project/agent-viz/data/raw`.
- Re-run ingestion:
  - `python /Users/raksh/Documents/New\ project/agent-viz/scripts/ingest_assetops.py`
