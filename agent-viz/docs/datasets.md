# Dataset Organization

## Canonical Dataset (Project 1)
- Directory: `data/raw/project1_event_runs/generated/`
- Manifest: `data/manifests/all_runs_manifest.jsonl`
- Generator: `data/generate_traces.py`

Canonical traces are event-level JSONL files (`run_*.jsonl`) with stable schema metadata:
- `schema_version`
- `generator_version`
- `dataset_name`
- `generated_at`

## Legacy / Compatibility Data
- `data/raw/legacy/`
  - Archived older flat run files and prior manifest.
- `data/smoke_tests/`
  - Demo fixtures used for quick compatibility checks.

## Regeneration
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
