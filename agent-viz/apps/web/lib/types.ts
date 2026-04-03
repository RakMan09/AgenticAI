export type Outcome = "success" | "fail" | "unknown";

export interface RunRow {
  run_id: string;
  task_id: string | null;
  scenario: string | null;
  outcome: Outcome;
  num_steps: number;
  started_at: string | null;
  ended_at: string | null;
  error_steps: number;
  tool_call_steps: number;
  distinct_tools: number;
  first_error_step: number | null;
  source: string | null;
  dataset_name: string | null;
  run_type: string | null;
  failure_category: string | null;
  first_failure_event_id: string | null;
  root_cause_event_id: string | null;
}

export interface RunsResponse {
  total: number;
  items: RunRow[];
}

export interface RunDetail extends RunRow {
  metadata?: Record<string, unknown> | string | null;
}

export interface StepRow {
  run_id: string;
  step_idx: number;
  step_type: "thought" | "action" | "observation" | "tool_call" | "unknown";
  display_step_type: "thought" | "action" | "observation" | "tool_call" | "unknown";
  text: string | null;
  tool_name: string | null;
  tool_input: string | Record<string, unknown> | null;
  tool_output: string | Record<string, unknown> | null;
  error_flag: boolean;
  latency_ms: number | null;
  event_id: string | null;
  event_type: string | null;
  agent_id: string | null;
  timestamp: string | null;
  parent_event_id: string | null;
  causal_id: string | null;
  tags: string[];
  status: string | null;
  error_type: string | null;
  retry_count: number | null;
  inferred_intent: string | null;
  intended_next_action: string | null;
  evidence_summary: string | null;
}

export interface StepsResponse {
  run_id: string;
  total: number;
  items: StepRow[];
}

export interface AnnotationRow {
  run_id: string;
  step_idx: number | null;
  event_id: string | null;
  label_type: string;
  label: string;
  confidence: number | null;
  reason_payload: Record<string, unknown> | string | null;
  source: string | null;
}

export interface AnnotationsResponse {
  run_id: string;
  total: number;
  items: AnnotationRow[];
}

export interface RunGapsResponse {
  run_id: string;
  total: number;
  items: AnnotationRow[];
}

export interface FailureSummary {
  run_id: string;
  outcome: string;
  failure_category: string | null;
  first_error_step: number | null;
  first_failure_event_id: string | null;
  root_cause_event_id: string | null;
  labels: AnnotationRow[];
  key_error_events: StepRow[];
  summary_text: string | null;
}

export interface RunReportJson {
  run: RunDetail;
  failure_summary: FailureSummary;
  top_tools: Array<{ tool_name: string; count: number }>;
  gap_signals: AnnotationRow[];
  review_notes: ReviewNoteRow[];
  case_study: CaseStudyRow | null;
  notes: string[];
}

export interface ComparePair {
  pair_idx: number;
  status: "match" | "mismatch" | "left_only" | "right_only";
  note: string | null;
  left_step: StepRow | null;
  right_step: StepRow | null;
}

export interface CompareStats {
  match: number;
  mismatch: number;
  left_only: number;
  right_only: number;
}

export interface CompareResponse {
  mode: "raw" | "aligned";
  left_run: RunDetail;
  right_run: RunDetail;
  total_pairs: number;
  stats: CompareStats;
  items: ComparePair[];
}

export interface AnalyticsOverviewResponse {
  total_runs: number;
  total_steps: number;
  outcome_counts: Record<string, number>;
  top_failure_labels: Array<{ label: string; run_count: number }>;
  first_failure_step_histogram: Array<{ bucket: string; run_count: number }>;
  retry_prevalence: number;
  loop_prevalence: number;
  timeout_prevalence: number;
}

export interface AnalyticsLabelsResponse {
  total: number;
  items: Array<{
    label: string;
    label_type: string;
    annotation_count: number;
    run_count: number;
  }>;
}

export interface AnalyticsToolUsageResponse {
  total: number;
  items: Array<{
    tool_name: string;
    call_count: number;
    error_count: number;
    avg_latency_ms: number | null;
    success_calls: number;
    fail_calls: number;
  }>;
}

export interface AnalyticsGapsResponse {
  total: number;
  items: Array<{
    label: string;
    annotation_count: number;
    run_count: number;
    fail_runs: number;
    success_runs: number;
  }>;
}

export interface CaseStudyRow {
  run_id: string;
  title: string | null;
  focus: string | null;
  status: string;
  created_at: string;
  updated_at: string;
}

export interface CaseStudiesResponse {
  total: number;
  items: CaseStudyRow[];
}

export interface ReviewNoteRow {
  id: number;
  run_id: string;
  reviewer: string;
  label: string | null;
  note: string;
  created_at: string;
}

export interface ReviewNotesResponse {
  total: number;
  items: ReviewNoteRow[];
}

export interface BenchmarkSubsetRow {
  subset_name: string;
  run_id: string;
  rationale: string | null;
  created_at: string;
}

export interface BenchmarkSubsetsResponse {
  total: number;
  items: BenchmarkSubsetRow[];
}
