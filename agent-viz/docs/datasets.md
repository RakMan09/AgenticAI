# Dataset Organization

## Canonical Dataset (Project 1, current)
- Directory: `Dataset/` (real OpenClaw session traces)
- Files:
  - `openclaw_session_trace_1.jsonl`
  - `openclaw_session_trace_2.jsonl`
  - `readme.txt`
  - `dataset_summary.txt`

These are raw session logs where:
- tool calls are frequently embedded inside `message.role="assistant"` content items with `type="toolCall"`
- tool outputs appear as `message.role="toolResult"`
- runs are derived during ingestion as user-turn trajectories

## Secondary Dataset (Synthetic Fallback)
- Directory: `data/raw/project1_event_runs/generated/`
- Manifest: `data/manifests/all_runs_manifest.jsonl`
- Generator: `data/generate_traces.py`

Synthetic traces are still supported for compatibility and controlled demos.

## Legacy / Compatibility
- `data/raw/legacy/`
  - Archived older flat run files and prior manifest.
- `data/smoke_tests/`
  - Demo fixtures used for quick compatibility checks.

## Real-Trace Ingestion
```bash
python scripts/ingest_assetops.py --raw-dir Dataset
```

## Synthetic Regeneration
Example:
```bash
python data/generate_traces.py \
  --out-dir data/raw/project1_event_runs/generated \
  --manifest-file data/manifests/all_runs_manifest.jsonl \
  --seed 7 \
  --overwrite
```

Optional controls:
- `--profile small`
- `--counts-file /path/to/counts.json`
