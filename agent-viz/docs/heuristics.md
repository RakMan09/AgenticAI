# Heuristic Labeling (Week 9 + Week 10 Gaps)

## Overview
Heuristics are computed during ingestion and stored in `annotations` with:
- `label_type = "heuristic"`
- `label`
- `confidence`
- `reason_payload` (JSON)
- `source = "heuristics_v1"`

## Current Detectors
1. `loop`
2. `tool_misuse`
3. `early_quit`
4. `timeout_failure`
5. `repeated_retry`
6. `conflict_ignored`
7. `missing_verification_step`
8. `wrong_tool_chosen`
9. `stale_belief_used`
10. `partial_recovery`
11. `absent_reasoning_step` (optional)
12. `missing_retry_after_failure`
13. `conflict_detected_but_unresolved`
14. `never_reached_expected_stage`
15. `finalization_without_evidence_consolidation`
16. `unknown_failure_pattern` fallback for unresolved failures

## Taxonomy Layer (Week 8)
`run_type` to taxonomy labels are also emitted with:
- `label_type = "taxonomy"`
- `source = "run_type_mapping"`

## Provided Labels
If canonical traces include `failure_category` in `final_outcome`, ingestion emits:
- `label_type = "provided"`
- `source = "canonical_payload"`

## Threshold Config
Ingestion uses centralized thresholds:
- `loop_min_repetitions`
- `retry_min_count`
- `low_event_count_early_quit`
- `expected_stage_min_coverage`

These are defined in `scripts/ingest_assetops.py` under `HEURISTIC_THRESHOLD_CONFIG`.

## Recompute
Use either:
- `python scripts/ingest_assetops.py`
- API: `POST /admin/recompute-heuristics`
