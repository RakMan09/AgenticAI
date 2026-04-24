import type { AnnotationRow, ComparePair, RunRow, StepRow } from "./types";

export interface StepPhase {
  name: string;
  start: number;
  end: number;
  count: number;
  errorCount: number;
  tools: string[];
}

export interface FingerprintCluster {
  key: string;
  count: number;
  outcome: string;
  failureCategory: string;
  avgSteps: number;
  sampleRunId: string;
}

export interface CategorySummary {
  label: string;
  count: number;
  outcome: string;
}

export interface EmergenceMatrixCell {
  outcome: string;
  bucket: string;
  count: number;
}

export interface ScatterPoint {
  runId: string;
  x: number;
  y: number;
  steps: number;
  errors: number;
  outcome: string;
  radius: number;
  label: string;
}

function uniqueSorted(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))].sort((left, right) => left.localeCompare(right));
}

export function clampPercent(value: number, max: number): number {
  if (max <= 0) return 0;
  return Math.max(0, Math.min(100, (value / max) * 100));
}

export function summarizeToolMix(steps: StepRow[], limit = 4): string[] {
  const counts = new Map<string, number>();
  for (const step of steps) {
    if (!step.tool_name) continue;
    counts.set(step.tool_name, (counts.get(step.tool_name) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, limit)
    .map(([tool]) => tool);
}

export function buildStepPhases(steps: StepRow[]): StepPhase[] {
  if (steps.length === 0) return [];
  const bucketSize = Math.max(1, Math.ceil(steps.length / 4));
  const phases: StepPhase[] = [];
  for (let index = 0; index < steps.length; index += bucketSize) {
    const slice = steps.slice(index, index + bucketSize);
    const phaseNumber = phases.length + 1;
    phases.push({
      name: `Phase ${phaseNumber}`,
      start: slice[0]?.step_idx ?? index,
      end: slice[slice.length - 1]?.step_idx ?? index,
      count: slice.length,
      errorCount: slice.filter((step) => step.error_flag).length,
      tools: summarizeToolMix(slice, 3),
    });
  }
  return phases;
}

export function buildExpectedWorkflow(run: RunRow | { run_type: string | null; failure_category: string | null }, steps: StepRow[], labels: AnnotationRow[]) {
  const presentStepTypes = new Set(steps.map((step) => step.display_step_type));
  const presentTools = new Set(steps.map((step) => step.tool_name).filter(Boolean) as string[]);
  const labelNames = new Set(labels.map((label) => label.label.toLowerCase()));
  const expectations = [
    {
      key: "reasoning",
      label: "Reasoning steps",
      status: presentStepTypes.has("thought") ? "observed" : labelNames.has("absent_reasoning_step") ? "missing" : "weak",
      detail: presentStepTypes.has("thought") ? "Thought steps appear in the trajectory." : "Little or no explicit reasoning is visible.",
    },
    {
      key: "verification",
      label: "Verification checkpoint",
      status: labelNames.has("missing_verification_step") ? "missing" : steps.some((step) => /verify|check|confirm/i.test(step.text ?? "")) ? "observed" : "weak",
      detail: labelNames.has("missing_verification_step")
        ? "The annotation layer explicitly flags missing verification."
        : "Searches for verification language in the run trace.",
    },
    {
      key: "tooling",
      label: "Tool usage breadth",
      status: presentTools.size >= 2 ? "observed" : presentTools.size === 1 ? "weak" : "missing",
      detail: presentTools.size >= 2 ? `${presentTools.size} distinct tools were used.` : "Tool coverage is narrow or absent.",
    },
    {
      key: "recovery",
      label: "Recovery / retry behavior",
      status: steps.some((step) => (step.retry_count ?? 0) > 0) ? "observed" : labelNames.has("early_quit") ? "missing" : "weak",
      detail: steps.some((step) => (step.retry_count ?? 0) > 0) ? "The agent attempted recovery after difficulty." : "Limited evidence of recovery after difficulty.",
    },
    {
      key: "termination",
      label: "Explicit finalization",
      status: steps.some((step) => /final|done|summary|answer/i.test(step.text ?? "")) ? "observed" : "weak",
      detail: "Looks for an explicit wrap-up or answer-delivery step.",
    },
  ];
  return expectations.map((expectation) => ({
    ...expectation,
    priority:
      expectation.status === "missing" ? 3 :
      expectation.status === "weak" ? 2 :
      1,
  }));
}

export function buildInterventions(steps: StepRow[], labels: AnnotationRow[]) {
  const labelNames = new Set(labels.map((item) => item.label.toLowerCase()));
  const firstError = steps.find((step) => step.error_flag);
  const interventions: string[] = [];
  if (labelNames.has("missing_verification_step")) {
    interventions.push("Insert a verification checkpoint immediately before the final answer or state transition.");
  }
  if (labelNames.has("early_quit")) {
    interventions.push("Add an explicit recovery branch after the first blocked tool call instead of terminating the run.");
  }
  if (labelNames.has("tool_misuse") || labelNames.has("wrong_tool_chosen")) {
    interventions.push("Constrain tool selection by task intent and re-check the tool choice before execution.");
  }
  if (labelNames.has("repeated_retry") || labelNames.has("loop")) {
    interventions.push("Escalate after repeated retries: switch strategy, summarize failure, or request alternate evidence.");
  }
  if (firstError?.tool_name) {
    interventions.push(`Inspect the first failing tool step around ${firstError.tool_name} and verify preconditions before reuse.`);
  }
  if (interventions.length === 0) {
    interventions.push("Use the failure-emergence view to inspect the first divergence and identify the next best missing step.");
  }
  return uniqueSorted(interventions);
}

export function buildOutcomeFingerprint(run: RunRow): string {
  return [
    run.outcome,
    run.failure_category ?? run.run_type ?? "untyped",
    run.error_steps > 0 ? "errors" : "clean",
    run.distinct_tools >= 2 ? "multi_tool" : run.distinct_tools === 1 ? "single_tool" : "no_tool",
  ].join("|");
}

export function clusterRunsByFingerprint(runs: RunRow[]): FingerprintCluster[] {
  const clusters = new Map<string, { runs: RunRow[]; totalSteps: number }>();
  for (const run of runs) {
    const key = buildOutcomeFingerprint(run);
    const existing = clusters.get(key);
    if (existing) {
      existing.runs.push(run);
      existing.totalSteps += run.num_steps;
      continue;
    }
    clusters.set(key, { runs: [run], totalSteps: run.num_steps });
  }
  return [...clusters.entries()]
    .map(([key, value]) => ({
      key,
      count: value.runs.length,
      outcome: value.runs[0]?.outcome ?? "unknown",
      failureCategory: value.runs[0]?.failure_category ?? value.runs[0]?.run_type ?? "untyped",
      avgSteps: Math.round(value.totalSteps / value.runs.length),
      sampleRunId: value.runs[0]?.run_id ?? "",
    }))
    .sort((left, right) => right.count - left.count);
}

export function inferStepIntent(step: StepRow): string {
  if (step.intended_next_action) return step.intended_next_action;
  if (step.inferred_intent) return step.inferred_intent;
  if (step.tool_name) return `Use ${step.tool_name}`;
  if (step.error_flag) return "Recover from failure";
  if (step.display_step_type === "thought") return "Reason about next move";
  if (step.display_step_type === "observation") return "Interpret evidence";
  if (step.display_step_type === "action") return "Commit to next action";
  return "Continue run";
}

export function compareSemanticSignature(step: StepRow | null): string {
  if (!step) return "none";
  return [step.display_step_type, step.tool_name ?? "no_tool", inferStepIntent(step).toLowerCase()].join("|");
}

export function summarizeCompareDivergence(pairs: ComparePair[]) {
  const mismatchPairs = pairs.filter((pair) => pair.status !== "match");
  const semanticGaps = mismatchPairs.filter(
    (pair) => compareSemanticSignature(pair.left_step) !== compareSemanticSignature(pair.right_step),
  );
  const leftOnlyCount = mismatchPairs.filter((pair) => pair.status === "left_only").length;
  const rightOnlyCount = mismatchPairs.filter((pair) => pair.status === "right_only").length;
  return {
    mismatchPairs,
    semanticGaps,
    leftOnlyCount,
    rightOnlyCount,
  };
}

export function normalizeLabel(label: string): string {
  return label.replaceAll("_", " ");
}

export function summarizeFailureCategories(runs: RunRow[], limit = 8): CategorySummary[] {
  const counts = new Map<string, CategorySummary>();
  for (const run of runs) {
    const label = run.failure_category ?? run.run_type ?? "untyped";
    const key = `${run.outcome}|${label}`;
    const existing = counts.get(key);
    if (existing) {
      existing.count += 1;
      continue;
    }
    counts.set(key, { label, count: 1, outcome: run.outcome });
  }
  return [...counts.values()]
    .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label))
    .slice(0, limit);
}

export function bucketFirstErrorStep(step: number | null): string {
  if (step === null || step < 0) return "none";
  if (step <= 1) return "0-1";
  if (step <= 3) return "2-3";
  if (step <= 6) return "4-6";
  return "7+";
}

export function buildEmergenceMatrix(runs: RunRow[]): EmergenceMatrixCell[] {
  const outcomes = ["success", "fail", "unknown"];
  const buckets = ["none", "0-1", "2-3", "4-6", "7+"];
  const counts = new Map<string, number>();
  for (const run of runs) {
    const bucket = bucketFirstErrorStep(run.first_error_step);
    const key = `${run.outcome}|${bucket}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return outcomes.flatMap((outcome) =>
    buckets.map((bucket) => ({
      outcome,
      bucket,
      count: counts.get(`${outcome}|${bucket}`) ?? 0,
    })),
  );
}

export function buildRunScatter(runs: RunRow[]): ScatterPoint[] {
  const maxSteps = Math.max(...runs.map((run) => run.num_steps), 1);
  const maxErrors = Math.max(...runs.map((run) => run.error_steps), 1);
  return runs.map((run) => ({
    runId: run.run_id,
    x: (run.num_steps / maxSteps) * 100,
    y: 100 - (run.error_steps / maxErrors) * 100,
    steps: run.num_steps,
    errors: run.error_steps,
    outcome: run.outcome,
    radius: 5 + Math.min(run.distinct_tools, 4),
    label: run.failure_category ?? run.run_type ?? "untyped",
  }));
}
