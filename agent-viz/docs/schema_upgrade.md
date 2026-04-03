# Schema Upgrade Notes (Week 7-9)

## Goal
Preserve compatibility with existing `runs` / `steps` API while introducing canonical event-level storage for Project 1 analysis.

## Core Tables
- `raw_events`
  - Canonical event ingestion table.
  - Stores event graph fields (`event_id`, `parent_event_id`, `causal_id`) and event payload JSON.
- `derived_runs`
  - App-facing run summary table.
  - Includes `source`, `dataset_name`, `run_type`, `failure_category`, `first_failure_event_id`, `root_cause_event_id`, and workflow metadata.
- `derived_steps`
  - App-facing event/step view model.
  - Includes tool/error/intent/evidence proxy fields.
- `annotations`
  - Unified label table for `provided`, `taxonomy`, `heuristic`, and `manual` labels.
  - Includes `reason_payload` JSON for explainability.

## Compatibility Tables
- `runs`
  - Backward-compatible surface used by existing endpoints/UI.
  - Extended with source and failure metadata.
- `steps`
  - Backward-compatible timeline surface.
  - Extended with event metadata fields.
- `run_stats`
  - Query-performance helper for run-level filtering.

## Ingestion Strategy
1. Canonical event-level runs are ingested first.
2. Optional legacy ingestion can be enabled with `--include-legacy`.
3. Derived tables and compatibility tables are rebuilt atomically per ingest run.

## Why This Upgrade
- Enables Week 7-9 overlays and heuristics without breaking the existing Week 1-6 app flow.
- Keeps event-level provenance available for later weeks (empty-space and analytics work).
