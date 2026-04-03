# Current Implementation Status (Project 1)

Updated: 2026-04-02

## Summary
The codebase preserves Week 1-6 behavior and now extends through Week 15 with case-study workflow, reviewer notes, benchmark subsets, and final packaging docs on top of event-level ingestion, overlays, heuristics, gaps, and analytics.

## Week-by-Week Mapping
- Week 1: Implemented. Ingestion + local DB pipeline exists.
- Week 2: Implemented. Run explorer + trace viewer work end-to-end.
- Week 3: Implemented. Better trace player UX and metadata panel are present.
- Week 4: Implemented. Richer filters and indexing/run stats are present.
- Week 5: Implemented. Side-by-side compare view exists.
- Week 6: Implemented. Alignment-aware compare exists.
- Week 7: Implemented. Failure overlay labels are persisted and exposed.
- Week 8: Implemented. Taxonomy layer introduced via structured labels.
- Week 9: Implemented. Rule-based heuristic detectors produce transparent labels with reason payloads.
- Week 10: Implemented. Gap/empty-space labels are exposed per-run and in analytics.
- Week 11: Implemented. Fleet analytics endpoints and UI page.
- Week 12: Implemented. Case-study run marking, enriched report payload/markdown, and UI page.
- Week 13: Implemented. Additional indexes, cached analytics queries, migration-safe support tables.
- Week 14: Implemented. Reviewer notes and benchmark subset assignment + auto-build helpers.
- Week 15: Implemented. Docs/readme/demo flow aligned to complete roadmap.

## What Exists Now
- Canonical dataset direction:
  - `data/raw/project1_event_runs/generated/`
  - `data/manifests/all_runs_manifest.jsonl`
- Legacy preserved:
  - `data/raw/legacy/`
  - `data/smoke_tests/`
- Generator:
  - `data/generate_traces.py` now supports CLI, dataset metadata, and monotonic loop step indexing.
- Ingestion:
  - `scripts/ingest_assetops.py` now ingests canonical event runs first, with optional legacy inclusion.
  - Creates/refreshes `raw_events`, `derived_runs`, `derived_steps`, compatibility `runs` and `steps`, `annotations`, and `run_stats`.
- API:
  - Existing endpoints preserved.
  - Added:
    - `GET /runs/{run_id}/annotations`
    - `POST /runs/{run_id}/annotations/manual`
    - `GET /runs/{run_id}/failure-summary`
    - `GET /runs/{run_id}/gaps`
    - `GET /runs/{run_id}/report?format=json|md`
    - `POST /runs/{run_id}/case-study`
    - `GET /case-studies`
    - `GET /runs/{run_id}/review-notes`
    - `POST /runs/{run_id}/review-notes`
    - `GET /evaluation/benchmark-subsets`
    - `POST /runs/{run_id}/benchmark-subsets`
    - `POST /evaluation/benchmark-subsets/auto-build`
    - `POST /admin/recompute-heuristics`
    - `POST /admin/rebuild-derived-data`
- UI:
  - Run Explorer includes source/type/failure-aware filtering and display.
  - Run Detail shows failure summary + overlays + gap signals + manual labels + reviewer/case-study controls.
  - Compare View surfaces overlay labels and first divergence marker.
  - Fleet Analytics page exists.
  - Case Studies page exists for curation and benchmark subset operations.
