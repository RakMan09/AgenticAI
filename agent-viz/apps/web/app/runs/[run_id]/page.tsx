"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";

import {
  assignBenchmarkSubset,
  createManualAnnotation,
  createReviewNote,
  fetchAnnotations,
  fetchFailureSummary,
  fetchReviewNotes,
  fetchRun,
  fetchRunGaps,
  fetchSteps,
  upsertCaseStudy,
} from "../../../lib/api";
import type { AnnotationRow, FailureSummary, ReviewNoteRow, RunDetail, StepRow } from "../../../lib/types";

type ExpandState = Record<number, boolean>;

type StepTypeFilter = "all" | "thought" | "action" | "observation" | "tool_call" | "unknown";

function parseMaybeJson(raw: string | Record<string, unknown> | null): string {
  if (!raw) return "";
  if (typeof raw !== "string") {
    return JSON.stringify(raw, null, 2);
  }
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function formatDateTime(value: string | null): string {
  if (!value) return "-";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString();
}

function outcomeStyle(outcome: string): { background: string; color: string } {
  if (outcome === "success") return { background: "#e6fffa", color: "#0f766e" };
  if (outcome === "fail") return { background: "#ffe9e9", color: "#b91c1c" };
  return { background: "#f1e8ff", color: "#6d28d9" };
}

function stepTypeStyle(stepType: string): { background: string; color: string } {
  switch (stepType) {
    case "thought":
      return { background: "#e8f0ff", color: "#1d4ed8" };
    case "action":
      return { background: "#ecfeff", color: "#0e7490" };
    case "observation":
      return { background: "#fef9c3", color: "#854d0e" };
    case "tool_call":
      return { background: "#dcfce7", color: "#166534" };
    default:
      return { background: "#e2e8f0", color: "#334155" };
  }
}

function stringifyMetadata(metadata: RunDetail["metadata"]): string {
  if (!metadata) return "{}";
  if (typeof metadata === "string") return metadata;
  return JSON.stringify(metadata, null, 2);
}

function labelTypeStyle(labelType: string): { background: string; color: string } {
  if (labelType === "heuristic") return { background: "#fef9c3", color: "#854d0e" };
  if (labelType === "provided") return { background: "#dcfce7", color: "#166534" };
  if (labelType === "taxonomy") return { background: "#dbeafe", color: "#1d4ed8" };
  if (labelType === "manual") return { background: "#ede9fe", color: "#6d28d9" };
  return { background: "#e2e8f0", color: "#334155" };
}

function dedupeAnnotations(items: AnnotationRow[]): AnnotationRow[] {
  const byLabel = new Map<string, AnnotationRow>();
  for (const item of items) {
    const existing = byLabel.get(item.label);
    if (!existing) {
      byLabel.set(item.label, item);
      continue;
    }
    if ((existing.confidence ?? 0) < (item.confidence ?? 0)) {
      byLabel.set(item.label, item);
    }
  }
  return [...byLabel.values()].sort((a, b) => a.label.localeCompare(b.label));
}

export default function RunDetailPage({ params }: { params: { run_id: string } }) {
  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";
  const [run, setRun] = useState<RunDetail | null>(null);
  const [steps, setSteps] = useState<StepRow[]>([]);
  const [stepsTotal, setStepsTotal] = useState(0);
  const [annotations, setAnnotations] = useState<AnnotationRow[]>([]);
  const [gapSignals, setGapSignals] = useState<AnnotationRow[]>([]);
  const [failureSummary, setFailureSummary] = useState<FailureSummary | null>(null);
  const [manualLabel, setManualLabel] = useState("");
  const [savingLabel, setSavingLabel] = useState(false);
  const [reviewNotes, setReviewNotes] = useState<ReviewNoteRow[]>([]);
  const [reviewer, setReviewer] = useState("professor");
  const [reviewLabel, setReviewLabel] = useState("");
  const [reviewNoteText, setReviewNoteText] = useState("");
  const [caseStudyTitle, setCaseStudyTitle] = useState("");
  const [caseStudyFocus, setCaseStudyFocus] = useState("");
  const [subsetName, setSubsetName] = useState("demo_subset");
  const [actionNotice, setActionNotice] = useState("");

  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [stepTypeFilter, setStepTypeFilter] = useState<StepTypeFilter>("all");
  const [errorOnly, setErrorOnly] = useState(false);

  const [expanded, setExpanded] = useState<ExpandState>({});
  const [loadingRun, setLoadingRun] = useState(true);
  const [loadingSteps, setLoadingSteps] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const parentRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const timeout = setTimeout(() => setDebouncedQuery(query), 200);
    return () => clearTimeout(timeout);
  }, [query]);

  useEffect(() => {
    let mounted = true;
    setLoadingRun(true);
    setError(null);

    fetchRun(params.run_id)
      .then((runData) => {
        if (!mounted) return;
        setRun(runData);
        const existingTitle = typeof runData.metadata === "object" && runData.metadata && "case_study_title" in runData.metadata
          ? String((runData.metadata as Record<string, unknown>)["case_study_title"] ?? "")
          : "";
        if (existingTitle) setCaseStudyTitle(existingTitle);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Unknown error");
      })
      .finally(() => {
        if (mounted) setLoadingRun(false);
      });

    return () => {
      mounted = false;
    };
  }, [params.run_id]);

  useEffect(() => {
    let mounted = true;
    setLoadingSteps(true);

    fetchSteps(params.run_id, {
      q: debouncedQuery,
      step_type: stepTypeFilter,
      error_only: errorOnly,
      limit: 20000,
      offset: 0,
    })
      .then((stepsData) => {
        if (!mounted) return;
        setSteps(stepsData.items);
        setStepsTotal(stepsData.total);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Unknown error");
      })
      .finally(() => {
        if (mounted) setLoadingSteps(false);
      });

    return () => {
      mounted = false;
    };
  }, [params.run_id, debouncedQuery, stepTypeFilter, errorOnly]);

  useEffect(() => {
    let mounted = true;
    fetchAnnotations(params.run_id)
      .then((data) => {
        if (!mounted) return;
        setAnnotations(data.items);
      })
      .catch(() => {
        if (!mounted) return;
        setAnnotations([]);
      });
    fetchFailureSummary(params.run_id)
      .then((data) => {
        if (!mounted) return;
        setFailureSummary(data);
      })
      .catch(() => {
        if (!mounted) return;
        setFailureSummary(null);
      });
    fetchRunGaps(params.run_id)
      .then((data) => {
        if (!mounted) return;
        setGapSignals(data.items);
      })
      .catch(() => {
        if (!mounted) return;
        setGapSignals([]);
      });
    fetchReviewNotes(params.run_id)
      .then((data) => {
        if (!mounted) return;
        setReviewNotes(data.items);
      })
      .catch(() => {
        if (!mounted) return;
        setReviewNotes([]);
      });
    return () => {
      mounted = false;
    };
  }, [params.run_id]);

  const firstErrorIndex = useMemo(() => steps.findIndex((step) => step.error_flag), [steps]);

  const stepTypeCounts = useMemo(() => {
    const counts: Record<string, number> = { thought: 0, action: 0, observation: 0, tool_call: 0, unknown: 0 };
    for (const step of steps) {
      counts[step.display_step_type] = (counts[step.display_step_type] ?? 0) + 1;
    }
    return counts;
  }, [steps]);

  const toolCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const step of steps) {
      if (!step.tool_name) continue;
      counts.set(step.tool_name, (counts.get(step.tool_name) ?? 0) + 1);
    }
    return [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
  }, [steps]);

  const errorCount = useMemo(() => steps.filter((step) => step.error_flag).length, [steps]);
  const uniqueOverlays = useMemo(() => dedupeAnnotations(annotations), [annotations]);
  const uniqueGapSignals = useMemo(() => dedupeAnnotations(gapSignals), [gapSignals]);
  const rowVirtualizer = useVirtualizer({
    count: steps.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 170,
    overscan: 8,
  });

  const toggleExpanded = (stepIdx: number) => {
    setExpanded((prev) => ({ ...prev, [stepIdx]: !prev[stepIdx] }));
  };

  const jumpToFirstError = () => {
    if (firstErrorIndex >= 0) {
      rowVirtualizer.scrollToIndex(firstErrorIndex, { align: "center" });
      const targetStep = steps[firstErrorIndex];
      if (targetStep) {
        setExpanded((prev) => ({ ...prev, [targetStep.step_idx]: true }));
      }
    }
  };

  const resetFilters = () => {
    setQuery("");
    setDebouncedQuery("");
    setStepTypeFilter("all");
    setErrorOnly(false);
  };

  const submitManualLabel = async () => {
    const trimmed = manualLabel.trim();
    if (!trimmed) return;
    setSavingLabel(true);
    try {
      await createManualAnnotation(params.run_id, { label: trimmed, step_idx: firstErrorIndex >= 0 ? steps[firstErrorIndex]?.step_idx : undefined });
      const refreshed = await fetchAnnotations(params.run_id);
      setAnnotations(refreshed.items);
      setManualLabel("");
      setActionNotice(`Added manual label: ${trimmed}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save annotation");
    } finally {
      setSavingLabel(false);
    }
  };

  const submitReviewNote = async () => {
    const note = reviewNoteText.trim();
    const rev = reviewer.trim();
    if (!note || !rev) return;
    try {
      await createReviewNote(params.run_id, { reviewer: rev, label: reviewLabel.trim() || undefined, note });
      const refreshed = await fetchReviewNotes(params.run_id);
      setReviewNotes(refreshed.items);
      setReviewNoteText("");
      setReviewLabel("");
      setActionNotice("Added reviewer note");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save review note");
    }
  };

  const markCaseStudy = async () => {
    try {
      await upsertCaseStudy(params.run_id, {
        title: caseStudyTitle.trim() || undefined,
        focus: caseStudyFocus.trim() || undefined,
        status: "active",
      });
      setActionNotice("Run marked as case study");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to mark case study");
    }
  };

  const addToBenchmarkSubset = async () => {
    const name = subsetName.trim();
    if (!name) return;
    try {
      await assignBenchmarkSubset(params.run_id, { subset_name: name, rationale: "Added from run detail view" });
      setActionNotice(`Added to benchmark subset: ${name}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to assign benchmark subset");
    }
  };

  if (loadingRun) {
    return <main className="container">Loading run...</main>;
  }

  if (error || !run) {
    return (
      <main className="container">
        <p style={{ color: "#b91c1c" }}>{error ?? "Run not found"}</p>
        <Link href="/">Back to Run Explorer</Link>
      </main>
    );
  }

  return (
    <main className="container">
      <div style={{ marginBottom: 12 }}>
        <Link href="/" style={{ textDecoration: "underline", color: "#334155" }}>
          Back to Run Explorer
        </Link>
        <span style={{ margin: "0 8px", color: "#94a3b8" }}>|</span>
        <Link
          href={`/compare?left_run_id=${encodeURIComponent(run.run_id)}&mode=aligned`}
          style={{ textDecoration: "underline", color: "#334155" }}
        >
          Open in Compare
        </Link>
        <span style={{ margin: "0 8px", color: "#94a3b8" }}>|</span>
        <a
          href={`${apiBaseUrl}/runs/${encodeURIComponent(params.run_id)}/report?format=md`}
          target="_blank"
          rel="noreferrer"
          style={{ textDecoration: "underline", color: "#334155" }}
        >
          Report (md)
        </a>
        <span style={{ margin: "0 8px", color: "#94a3b8" }}>|</span>
        <a
          href={`${apiBaseUrl}/runs/${encodeURIComponent(params.run_id)}/report?format=json`}
          target="_blank"
          rel="noreferrer"
          style={{ textDecoration: "underline", color: "#334155" }}
        >
          Report (json)
        </a>
      </div>
      {actionNotice ? (
        <p style={{ marginTop: 0, marginBottom: 12, color: "#166534", fontSize: 13 }}>{actionNotice}</p>
      ) : null}

      <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
        <h1 style={{ marginTop: 0, marginBottom: 8 }}>Trace Viewer</h1>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 8 }}>
          <span
            style={{
              ...outcomeStyle(run.outcome),
              borderRadius: 999,
              padding: "2px 10px",
              fontSize: 12,
              fontWeight: 600,
            }}
          >
            {run.outcome}
          </span>
          <span style={{ color: "#475569" }}>run: {run.run_id}</span>
          <span style={{ color: "#475569" }}>steps: {run.num_steps}</span>
        </div>

        <div style={{ color: "#334155", marginBottom: 10 }}>
          <strong>Task/Scenario:</strong> {run.task_id ?? run.scenario ?? "-"}
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 10 }}>
          <label>
            Search within run
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="text / tool / payload"
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Step type
            <select
              value={stepTypeFilter}
              onChange={(e) => setStepTypeFilter(e.target.value as StepTypeFilter)}
              style={{ width: "100%" }}
            >
              <option value="all">all</option>
              <option value="thought">thought</option>
              <option value="action">action</option>
              <option value="observation">observation</option>
              <option value="tool_call">tool_call</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
          <label style={{ display: "flex", alignItems: "end", gap: 8 }}>
            <input
              type="checkbox"
              checked={errorOnly}
              onChange={(e) => setErrorOnly(e.target.checked)}
            />
            error steps only
          </label>
          <div style={{ display: "flex", alignItems: "end", gap: 8 }}>
            <button
              onClick={resetFilters}
              style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "6px 10px", background: "white" }}
            >
              Clear filters
            </button>
            {firstErrorIndex >= 0 ? (
              <button
                onClick={jumpToFirstError}
                style={{ border: "1px solid #ef4444", borderRadius: 8, padding: "6px 10px", background: "white" }}
              >
                Jump to first error
              </button>
            ) : null}
          </div>
        </div>
      </section>

      <div className="viewer-layout">
        <aside className="panel" style={{ padding: 12, alignSelf: "start" }}>
          <h2 style={{ marginTop: 0, marginBottom: 8, fontSize: 18 }}>Metadata Panel</h2>
          <div style={{ fontSize: 14, color: "#334155", marginBottom: 10 }}>
            <div><strong>Started:</strong> {formatDateTime(run.started_at)}</div>
            <div><strong>Ended:</strong> {formatDateTime(run.ended_at)}</div>
            <div><strong>Filtered steps:</strong> {stepsTotal}</div>
            <div><strong>Error steps:</strong> {errorCount}</div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Step Type Counts</strong>
            <div style={{ display: "grid", gap: 4, marginTop: 6, fontSize: 13 }}>
              {Object.entries(stepTypeCounts).map(([key, count]) => (
                <div key={key} style={{ display: "flex", justifyContent: "space-between" }}>
                  <span>{key}</span>
                  <span>{count}</span>
                </div>
              ))}
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Top Tools (filtered)</strong>
            <div style={{ marginTop: 6, fontSize: 13, color: "#334155" }}>
              {toolCounts.length === 0 ? (
                <div>-</div>
              ) : (
                toolCounts.map(([tool, count]) => (
                  <div key={tool} style={{ display: "flex", justifyContent: "space-between" }}>
                    <span>{tool}</span>
                    <span>{count}</span>
                  </div>
                ))
              )}
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Failure Summary</strong>
            <div style={{ marginTop: 6, fontSize: 13, color: "#334155" }}>
              <div><strong>Category:</strong> {failureSummary?.failure_category ?? run.failure_category ?? "-"}</div>
              <div><strong>First failure event:</strong> {failureSummary?.first_failure_event_id ?? run.first_failure_event_id ?? "-"}</div>
              <div><strong>Root cause event:</strong> {failureSummary?.root_cause_event_id ?? run.root_cause_event_id ?? "-"}</div>
            </div>
            {failureSummary?.summary_text ? (
              <p style={{ fontSize: 13, marginTop: 6, color: "#475569" }}>{failureSummary.summary_text}</p>
            ) : null}
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Failure Overlays</strong>
            <div style={{ marginTop: 4, fontSize: 11, color: "#64748b" }}>
              showing unique labels (highest-confidence source kept)
            </div>
            <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 6 }}>
              {uniqueOverlays.length === 0 ? (
                <span style={{ fontSize: 13, color: "#64748b" }}>no labels</span>
              ) : (
                uniqueOverlays.map((annotation, idx) => (
                  <span
                    key={`${annotation.label}-${idx}`}
                    style={{
                      border: "1px solid #cbd5e1",
                      borderRadius: 999,
                      padding: "1px 8px",
                      fontSize: 12,
                      ...labelTypeStyle(annotation.label_type),
                    }}
                  >
                    {annotation.label} <span style={{ opacity: 0.8 }}>[{annotation.label_type}]</span>
                  </span>
                ))
              )}
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Gap Signals</strong>
            <div style={{ marginTop: 6, fontSize: 13, color: "#334155" }}>
              {uniqueGapSignals.length === 0 ? (
                <div>none detected</div>
              ) : (
                uniqueGapSignals.map((annotation, idx) => (
                  <div key={`${annotation.label}-gap-${idx}`} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                    <span>{annotation.label}</span>
                    <span style={{ color: "#64748b" }}>
                      {annotation.confidence !== null && annotation.confidence !== undefined
                        ? `${Math.round(annotation.confidence * 100)}%`
                        : annotation.label_type}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Add Manual Label</strong>
            <div style={{ marginTop: 6, display: "flex", gap: 6 }}>
              <input
                value={manualLabel}
                onChange={(event) => setManualLabel(event.target.value)}
                placeholder="e.g., tool_misuse"
                style={{ width: "100%" }}
              />
              <button
                onClick={submitManualLabel}
                disabled={savingLabel}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "4px 10px", background: "white" }}
              >
                {savingLabel ? "Saving..." : "Add"}
              </button>
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Case Study Controls</strong>
            <div style={{ marginTop: 6, display: "grid", gap: 6 }}>
              <input
                value={caseStudyTitle}
                onChange={(event) => setCaseStudyTitle(event.target.value)}
                placeholder="Case study title"
              />
              <input
                value={caseStudyFocus}
                onChange={(event) => setCaseStudyFocus(event.target.value)}
                placeholder="Focus area (e.g., missing verification)"
              />
              <button
                onClick={markCaseStudy}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "4px 10px", background: "white" }}
              >
                Mark as Case Study
              </button>
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Reviewer Notes</strong>
            <div style={{ marginTop: 6, display: "grid", gap: 6 }}>
              <input value={reviewer} onChange={(event) => setReviewer(event.target.value)} placeholder="reviewer" />
              <input value={reviewLabel} onChange={(event) => setReviewLabel(event.target.value)} placeholder="label (optional)" />
              <textarea
                value={reviewNoteText}
                onChange={(event) => setReviewNoteText(event.target.value)}
                placeholder="Write evaluation/debug note"
                rows={3}
              />
              <button
                onClick={submitReviewNote}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "4px 10px", background: "white" }}
              >
                Add Review Note
              </button>
              {reviewNotes.length > 0 ? (
                <div style={{ maxHeight: 140, overflow: "auto", border: "1px solid #e2e8f0", borderRadius: 8, padding: 6 }}>
                  {reviewNotes.slice(0, 8).map((item) => (
                    <div key={item.id} style={{ marginBottom: 6, fontSize: 12 }}>
                      <strong>{item.reviewer}</strong> {item.label ? `(${item.label})` : ""}: {item.note}
                    </div>
                  ))}
                </div>
              ) : (
                <span style={{ color: "#64748b", fontSize: 12 }}>No review notes yet</span>
              )}
            </div>
          </div>

          <div style={{ marginBottom: 10 }}>
            <strong style={{ fontSize: 13 }}>Benchmark Subset</strong>
            <div style={{ marginTop: 6, display: "flex", gap: 6 }}>
              <input value={subsetName} onChange={(event) => setSubsetName(event.target.value)} placeholder="subset name" style={{ width: "100%" }} />
              <button
                onClick={addToBenchmarkSubset}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "4px 10px", background: "white" }}
              >
                Add
              </button>
            </div>
          </div>

          <details>
            <summary style={{ cursor: "pointer", fontWeight: 600 }}>Raw run metadata</summary>
            <pre
              style={{
                margin: "8px 0 0",
                background: "#f8fafc",
                border: "1px solid #e2e8f0",
                borderRadius: 8,
                padding: 8,
                overflowX: "auto",
                fontSize: 12,
                whiteSpace: "pre-wrap",
              }}
            >
              {stringifyMetadata(run.metadata)}
            </pre>
          </details>
        </aside>

        <section className="panel" style={{ padding: 8 }}>
          <div style={{ padding: "4px 6px 8px", color: "#475569", fontSize: 14 }}>
            {loadingSteps ? "Loading steps..." : `Showing ${steps.length} step(s) (${stepsTotal} matched).`}
          </div>

          <div
            ref={parentRef}
            style={{
              height: "70vh",
              overflow: "auto",
              position: "relative",
            }}
          >
            <div
              style={{
                height: `${rowVirtualizer.getTotalSize()}px`,
                width: "100%",
                position: "relative",
              }}
            >
              {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                const step = steps[virtualRow.index];
                if (!step) {
                  return null;
                }
                const isExpanded = expanded[step.step_idx] ?? false;
                const hasToolPayload = Boolean(step.tool_input || step.tool_output);

                return (
                  <article
                    key={virtualRow.key}
                    data-index={virtualRow.index}
                    ref={rowVirtualizer.measureElement}
                    style={{
                      position: "absolute",
                      top: 0,
                      left: 0,
                      width: "100%",
                      transform: `translateY(${virtualRow.start}px)`,
                      border: "1px solid #e2e8f0",
                      borderRadius: 10,
                      marginBottom: 8,
                      background: step.error_flag ? "#fff1f2" : "#ffffff",
                      padding: 10,
                    }}
                  >
                    <header style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <strong>#{step.step_idx}</strong>
                        <span
                          style={{
                            ...stepTypeStyle(step.display_step_type),
                            borderRadius: 999,
                            padding: "2px 10px",
                            fontSize: 12,
                            fontWeight: 600,
                          }}
                        >
                          {step.display_step_type}
                        </span>
                        {step.step_type !== step.display_step_type ? (
                          <span style={{ color: "#64748b", fontSize: 12 }}>raw: {step.step_type}</span>
                        ) : null}
                        {step.tool_name ? (
                          <span style={{ color: "#0f172a", fontSize: 12 }}>tool: {step.tool_name}</span>
                        ) : null}
                      </div>
                      <button
                        onClick={() => toggleExpanded(step.step_idx)}
                        style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "2px 8px", height: 28 }}
                      >
                        {isExpanded ? "Collapse" : "Expand"}
                      </button>
                    </header>

                    <p
                      style={{
                        marginBottom: 4,
                        whiteSpace: "pre-wrap",
                        display: "-webkit-box",
                        WebkitLineClamp: isExpanded ? "unset" : 4,
                        WebkitBoxOrient: "vertical",
                        overflow: "hidden",
                      }}
                    >
                      {step.text || "(no text)"}
                    </p>

                    {step.error_flag ? <p style={{ color: "#b91c1c", margin: "4px 0" }}>Error flagged</p> : null}

                    {isExpanded && hasToolPayload ? (
                      <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 8, marginTop: 6 }}>
                        {step.tool_input ? (
                          <details open>
                            <summary>Tool input</summary>
                            <pre
                              style={{
                                margin: 0,
                                background: "#f8fafc",
                                border: "1px solid #e2e8f0",
                                borderRadius: 8,
                                padding: 8,
                                overflowX: "auto",
                              }}
                            >
                              {parseMaybeJson(step.tool_input)}
                            </pre>
                          </details>
                        ) : null}
                        {step.tool_output ? (
                          <details open>
                            <summary>Tool output</summary>
                            <pre
                              style={{
                                margin: 0,
                                background: "#f8fafc",
                                border: "1px solid #e2e8f0",
                                borderRadius: 8,
                                padding: 8,
                                overflowX: "auto",
                              }}
                            >
                              {parseMaybeJson(step.tool_output)}
                            </pre>
                          </details>
                        ) : null}
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
