import type {
  AnalyticsGapsResponse,
  AnalyticsLabelsResponse,
  AnalyticsOverviewResponse,
  AnalyticsToolUsageResponse,
  AnnotationsResponse,
  BenchmarkSubsetsResponse,
  CaseStudiesResponse,
  CompareResponse,
  FailureSummary,
  ReviewNotesResponse,
  RunDetail,
  RunGapsResponse,
  RunReportJson,
  RunsResponse,
  StepsResponse,
} from "./types";

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

function withQuery(path: string, params: Record<string, string | number | boolean | undefined>) {
  const url = new URL(path, API_BASE_URL);
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === "") continue;
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

export async function fetchRuns(params: {
  outcome?: string;
  q?: string;
  min_steps?: number;
  max_steps?: number;
  has_errors?: string;
  min_error_steps?: number;
  max_error_steps?: number;
  step_type?: string;
  label?: string;
  source?: string;
  started_after?: string;
  started_before?: string;
  sort_by?: string;
  sort_dir?: "asc" | "desc";
  limit?: number;
  offset?: number;
  tool?: string;
}): Promise<RunsResponse> {
  const res = await fetch(withQuery("/runs", params), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch runs (${res.status})`);
  }
  return (await res.json()) as RunsResponse;
}

export async function fetchRun(runId: string): Promise<RunDetail> {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Run ${runId} not found (${res.status})`);
  }
  return (await res.json()) as RunDetail;
}

export async function fetchSteps(
  runId: string,
  params: {
    q?: string;
    step_type?: string;
    error_only?: boolean;
    limit?: number;
    offset?: number;
  } = {},
): Promise<StepsResponse> {
  const res = await fetch(withQuery(`/runs/${runId}/steps`, params), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch steps for ${runId} (${res.status})`);
  }
  return (await res.json()) as StepsResponse;
}

export async function fetchAnnotations(runId: string, labelType: string = "all"): Promise<AnnotationsResponse> {
  const res = await fetch(withQuery(`/runs/${runId}/annotations`, { label_type: labelType }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch annotations for ${runId} (${res.status})`);
  }
  return (await res.json()) as AnnotationsResponse;
}

export async function createManualAnnotation(
  runId: string,
  payload: {
    step_idx?: number;
    event_id?: string;
    label: string;
    confidence?: number;
    reason_payload?: Record<string, unknown> | string;
    source?: string;
  },
): Promise<AnnotationsResponse["items"][number]> {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/annotations/manual`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Failed to create annotation (${res.status})`);
  }
  return (await res.json()) as AnnotationsResponse["items"][number];
}

export async function fetchFailureSummary(runId: string): Promise<FailureSummary> {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/failure-summary`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch failure summary (${res.status})`);
  }
  return (await res.json()) as FailureSummary;
}

export async function fetchRunGaps(runId: string): Promise<RunGapsResponse> {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/gaps`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch gaps (${res.status})`);
  }
  return (await res.json()) as RunGapsResponse;
}

export async function fetchReport(runId: string): Promise<RunReportJson> {
  const res = await fetch(withQuery(`/runs/${runId}/report`, { format: "json" }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch report (${res.status})`);
  }
  return (await res.json()) as RunReportJson;
}

export async function fetchCompare(params: {
  left_run_id: string;
  right_run_id: string;
  mode: "raw" | "aligned";
}): Promise<CompareResponse> {
  const res = await fetch(withQuery("/compare", params), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to compare runs (${res.status})`);
  }
  return (await res.json()) as CompareResponse;
}

export async function fetchAnalyticsOverview(): Promise<AnalyticsOverviewResponse> {
  const res = await fetch(`${API_BASE_URL}/analytics/overview`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch analytics overview (${res.status})`);
  }
  return (await res.json()) as AnalyticsOverviewResponse;
}

export async function fetchAnalyticsFailureLabels(limit = 50): Promise<AnalyticsLabelsResponse> {
  const res = await fetch(withQuery("/analytics/failure-labels", { limit }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch analytics failure labels (${res.status})`);
  }
  return (await res.json()) as AnalyticsLabelsResponse;
}

export async function fetchAnalyticsToolUsage(limit = 40): Promise<AnalyticsToolUsageResponse> {
  const res = await fetch(withQuery("/analytics/tool-usage", { limit }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch analytics tool usage (${res.status})`);
  }
  return (await res.json()) as AnalyticsToolUsageResponse;
}

export async function fetchAnalyticsGaps(limit = 50): Promise<AnalyticsGapsResponse> {
  const res = await fetch(withQuery("/analytics/gaps", { limit }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch analytics gaps (${res.status})`);
  }
  return (await res.json()) as AnalyticsGapsResponse;
}

export async function fetchCaseStudies(params: { status?: string; limit?: number; offset?: number } = {}): Promise<CaseStudiesResponse> {
  const res = await fetch(withQuery("/case-studies", params), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch case studies (${res.status})`);
  }
  return (await res.json()) as CaseStudiesResponse;
}

export async function upsertCaseStudy(
  runId: string,
  payload: { title?: string; focus?: string; status?: string },
) {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/case-study`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Failed to mark case study (${res.status})`);
  }
  return await res.json();
}

export async function fetchReviewNotes(runId: string, limit = 200, offset = 0): Promise<ReviewNotesResponse> {
  const res = await fetch(withQuery(`/runs/${runId}/review-notes`, { limit, offset }), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch review notes (${res.status})`);
  }
  return (await res.json()) as ReviewNotesResponse;
}

export async function createReviewNote(
  runId: string,
  payload: { reviewer: string; label?: string; note: string },
) {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/review-notes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Failed to create review note (${res.status})`);
  }
  return await res.json();
}

export async function fetchBenchmarkSubsets(params: { subset_name?: string; limit?: number; offset?: number } = {}): Promise<BenchmarkSubsetsResponse> {
  const res = await fetch(withQuery("/evaluation/benchmark-subsets", params), { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`Failed to fetch benchmark subsets (${res.status})`);
  }
  return (await res.json()) as BenchmarkSubsetsResponse;
}

export async function assignBenchmarkSubset(
  runId: string,
  payload: { subset_name: string; rationale?: string },
) {
  const res = await fetch(`${API_BASE_URL}/runs/${runId}/benchmark-subsets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(`Failed to assign benchmark subset (${res.status})`);
  }
  return await res.json();
}

export async function autoBuildBenchmarkSubsets() {
  const res = await fetch(`${API_BASE_URL}/evaluation/benchmark-subsets/auto-build`, {
    method: "POST",
  });
  if (!res.ok) {
    throw new Error(`Failed to auto build benchmark subsets (${res.status})`);
  }
  return await res.json();
}
