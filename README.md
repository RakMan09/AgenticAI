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
NEXT_PUBLIC_API_BASE_URL=http://localhost:8001 npm run dev
```

Open `http://localhost:3000`.

For full setup details, see:

- `agent-viz/README.md`
