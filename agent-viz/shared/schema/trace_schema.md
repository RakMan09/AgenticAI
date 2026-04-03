# Trace Schema (Project 1, Through Week 9)

This project uses an event-first schema with compatibility layers for existing Week 1-6 UI/API behavior.

## Canonical Tables

### `raw_events`
One row per event from canonical synthetic run files.

Core fields:
- `run_id`
- `event_id`
- `parent_event_id`
- `causal_id`
- `timestamp`
- `step_index`
- `agent_id`
- `event_type`
- `status`
- `tags` (JSON)
- `payload` (JSON)
- `run_type`
- `scenario`
- `dataset_name`
- `schema_version`
- `generator_version`
- `generated_at`
- `expected_workflow` (JSON)
- `root_cause_event_id`
- `first_failure_event_id`
- `propagated_to_event_ids` (JSON)
- `loop_iteration`
- `loop_group`
- `repeated_pattern_id`
- `source_file`

### `derived_runs`
App-facing run model derived from `raw_events` (or compatibility adapters).

Core fields:
- `run_id`
- `source`
- `dataset_name`
- `run_type`
- `task_id`
- `scenario`
- `outcome` (`success` / `fail` / `unknown`)
- `failure_category`
- `num_steps`
- `first_error_step`
- `first_failure_event_id`
- `root_cause_event_id`
- `expected_workflow` (JSON)
- `started_at`
- `ended_at`
- `metadata` (JSON)

### `derived_steps`
Event-level step view model for UI and compare flows.

Core fields:
- `run_id`
- `event_id`
- `step_idx`
- `event_type`
- `step_type`
- `agent_id`
- `text`
- `tool_name`
- `tool_input` (JSON)
- `tool_output` (JSON)
- `error_flag`
- `error_type`
- `latency_ms`
- `retry_count`
- `timestamp`
- `parent_event_id`
- `causal_id`
- `tags` (JSON)
- `status`
- `inferred_intent`
- `intended_next_action`
- `evidence_summary`
- `source_file`

### `annotations`
Unified labels table for overlays/taxonomy/heuristics/manual annotations.

Fields:
- `run_id`
- `step_idx` (nullable)
- `event_id` (nullable)
- `label_type` (`provided` / `taxonomy` / `heuristic` / `manual`)
- `label`
- `confidence`
- `reason_payload` (JSON)
- `source`

## Compatibility Tables

### `runs`
Backward-compatible surface used by existing endpoints.

### `steps`
Backward-compatible step timeline surface used by existing endpoints.

### `run_stats`
Derived helper table for filtering/performance:
- `error_steps`
- `tool_call_steps`
- `distinct_tools`
- `first_error_step`

## Labeling (Week 7-9)

The ingestion pipeline emits:
- provided labels from canonical payloads (when available)
- taxonomy labels from `run_type`
- heuristic labels with explicit reason payloads

## JSONL Normalized Output

`data/normalized/traces.jsonl` contains one run per line:
- `run` (derived run payload)
- `steps` (derived step list)
