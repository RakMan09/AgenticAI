# AgenticAI Repository

This repository contains the Project 1 implementation in:

- `agent-viz/` -> Visual Analytics for Agent Trajectories & Failure Modes

## Quick Start

```bash
cd agent-viz
python -m pip install -r requirements.txt
cd apps/web && npm install && cd ../..
python scripts/ingest_assetops.py
uvicorn apps.api.app.main:app --reload --host 0.0.0.0 --port 8001
```

In a second terminal:

```bash
cd agent-viz/apps/web
npm run dev
```

Open `http://localhost:3000`.

The frontend defaults to `http://127.0.0.1:8001` for the API.

## Current Highlights

- trajectory-centric run explorer with failure-family grouping
- compact run detail diagnosis with a click-to-inspect trace ribbon, selected-step context, emergence, workflow, gap, and intervention views
- compare mode with multi-run selection, cumulative error growth charting, divergence summaries, and semantic alignment
- analytics with compact graph-based summaries, emergence heatmaps, and run scatterplots
- case-study workflow with reviewer notes and baseline benchmark subset building

For full setup details, see:

- `agent-viz/README.md`
