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
import {
  buildExpectedWorkflow,
  buildInterventions,
  buildStepPhases,
  clampPercent,
  inferStepIntent,
  normalizeLabel,
  summarizeToolMix,
} from "../../../lib/analysis";
import type { AnnotationRow, FailureSummary, ReviewNoteRow, RunDetail, StepRow } from "../../../lib/types";

type ExpandState = Record<number, boolean>;
type StepTypeFilter = "all" | "thought" | "action" | "observation" | "tool_call" | "unknown";

function parseMaybeJson(raw: string | Record<string, unknown> | null): string {
  if (!raw) return "";
  if (typeof raw !== "string") return JSON.stringify(raw, null, 2);
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
  if (outcome === "success") return { background: "#dcfce7", color: "#166534" };
  if (outcome === "fail") return { background: "#fee2e2", color: "#b91c1c" };
  return { background: "#ede9fe", color: "#6d28d9" };
}

function stepTypeStyle(stepType: string): { background: string; color: string } {
  switch (stepType) {
    case "thought":
      return { background: "#dbeafe", color: "#1d4ed8" };
    case "action":
      return { background: "#cffafe", color: "#0f766e" };
    case "observation":
      return { background: "#fef3c7", color: "#92400e" };
    case "tool_call":
      return { background: "#dcfce7", color: "#166534" };
    default:
      return { background: "#e2e8f0", color: "#334155" };
  }
}

function labelTypeStyle(labelType: string): { background: string; color: string } {
  if (labelType === "heuristic") return { background: "#fef3c7", color: "#92400e" };
  if (labelType === "provided") return { background: "#dcfce7", color: "#166534" };
  if (labelType === "taxonomy") return { background: "#dbeafe", color: "#1d4ed8" };
  if (labelType === "manual") return { background: "#ede9fe", color: "#6d28d9" };
  return { background: "#e2e8f0", color: "#334155" };
}

function dedupeAnnotations(items: AnnotationRow[]): AnnotationRow[] {
  const byLabel = new Map<string, AnnotationRow>();
  for (const item of items) {
    const existing = byLabel.get(item.label);
    if (!existing || (existing.confidence ?? 0) < (item.confidence ?? 0)) {
      byLabel.set(item.label, item);
    }
  }
  return [...byLabel.values()].sort((left, right) => left.label.localeCompare(right.label));
}

function annotationLabel(annotation: AnnotationRow) {
  return normalizeLabel(annotation.label);
}

function copyText(value: string) {
  if (typeof window === "undefined") return;
  void navigator.clipboard.writeText(value);
}

export default function RunDetailPage({ params }: { params: { run_id: string } }) {
  const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8001";
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
    const timeout = window.setTimeout(() => setDebouncedQuery(query), 180);
    return () => window.clearTimeout(timeout);
  }, [query]);

  useEffect(() => {
    if (!actionNotice) return;
    const timeout = window.setTimeout(() => setActionNotice(""), 2200);
    return () => window.clearTimeout(timeout);
  }, [actionNotice]);

  useEffect(() => {
    let mounted = true;
    setLoadingRun(true);
    setError(null);
    fetchRun(params.run_id)
      .then((runData) => {
        if (!mounted) return;
        setRun(runData);
        const metadata = typeof runData.metadata === "object" && runData.metadata ? runData.metadata : {};
        setCaseStudyTitle(String((metadata as Record<string, unknown>).case_study_title ?? ""));
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
  }, [debouncedQuery, errorOnly, params.run_id, stepTypeFilter]);

  useEffect(() => {
    let mounted = true;
    Promise.all([
      fetchAnnotations(params.run_id).catch(() => ({ items: [] })),
      fetchFailureSummary(params.run_id).catch(() => null),
      fetchRunGaps(params.run_id).catch(() => ({ items: [] })),
      fetchReviewNotes(params.run_id).catch(() => ({ items: [] })),
    ]).then(([annotationData, failureData, gapData, noteData]) => {
      if (!mounted) return;
      setAnnotations(annotationData.items);
      setFailureSummary(failureData);
      setGapSignals(gapData.items);
      setReviewNotes(noteData.items);
    });
    return () => {
      mounted = false;
    };
  }, [params.run_id]);

  const firstErrorIndex = useMemo(() => steps.findIndex((step) => step.error_flag), [steps]);
  const rowVirtualizer = useVirtualizer({
    count: steps.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 170,
    overscan: 8,
  });

  const errorCount = useMemo(() => steps.filter((step) => step.error_flag).length, [steps]);
  const uniqueOverlays = useMemo(() => dedupeAnnotations(annotations), [annotations]);
  const uniqueGapSignals = useMemo(() => dedupeAnnotations(gapSignals), [gapSignals]);
  const stepTypeCounts = useMemo(() => {
    const counts: Record<string, number> = { thought: 0, action: 0, observation: 0, tool_call: 0, unknown: 0 };
    for (const step of steps) counts[step.display_step_type] = (counts[step.display_step_type] ?? 0) + 1;
    return counts;
  }, [steps]);
  const topTools = useMemo(() => summarizeToolMix(steps, 8), [steps]);
  const phases = useMemo(() => buildStepPhases(steps), [steps]);
  const workflowChecks = useMemo(() => buildExpectedWorkflow(run ?? { run_type: null, failure_category: null }, steps, annotations), [annotations, run, steps]);
  const interventions = useMemo(() => buildInterventions(steps, annotations), [annotations, steps]);
  const firstErrorStep = useMemo(() => (firstErrorIndex >= 0 ? steps[firstErrorIndex] : null), [firstErrorIndex, steps]);
  const keyMoments = useMemo(() => {
    const items = new Set<number>();
    if (steps[0]) items.add(steps[0].step_idx);
    if (firstErrorStep) items.add(firstErrorStep.step_idx);
    if (failureSummary?.key_error_events) {
      for (const event of failureSummary.key_error_events.slice(0, 3)) items.add(event.step_idx);
    }
    if (steps[steps.length - 1]) items.add(steps[steps.length - 1].step_idx);
    return steps.filter((step) => items.has(step.step_idx));
  }, [failureSummary, firstErrorStep, steps]);

  const toggleExpanded = (stepIdx: number) => {
    setExpanded((prev) => ({ ...prev, [stepIdx]: !prev[stepIdx] }));
  };

  const jumpToStep = (targetIdx: number) => {
    const index = steps.findIndex((step) => step.step_idx === targetIdx);
    if (index >= 0) {
      rowVirtualizer.scrollToIndex(index, { align: "center" });
      setExpanded((prev) => ({ ...prev, [targetIdx]: true }));
    }
  };

  const jumpToFirstError = () => {
    if (firstErrorStep) jumpToStep(firstErrorStep.step_idx);
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
      await createManualAnnotation(params.run_id, {
        label: trimmed,
        step_idx: firstErrorStep?.step_idx,
        reason_payload: { note: "Added from run diagnosis view" },
      });
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
      setCaseStudyFocus("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to mark case study");
    }
  };

  const addToBenchmarkSubset = async () => {
    const name = subsetName.trim();
    if (!name) return;
    try {
      await assignBenchmarkSubset(params.run_id, { subset_name: name, rationale: "Added from diagnosis view" });
      setActionNotice(`Added to benchmark subset: ${name}`);
      setSubsetName("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to assign benchmark subset");
    }
  };

  const copyShareLink = () => {
    if (typeof window === "undefined") return;
    copyText(window.location.href);
    setActionNotice("Copied run-analysis link");
  };

  const stepTypeChart = useMemo(() => {
    const total = Math.max(steps.length, 1);
    return Object.entries(stepTypeCounts).map(([label, value]) => ({
      label,
      value,
      width: `${(value / total) * 100}%`,
    }));
  }, [stepTypeCounts, steps.length]);

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
        <Link href="/" style={{ textDecoration: "underline", color: "#334155" }}>Back to Run Explorer</Link>
        <span style={{ margin: "0 8px", color: "#94a3b8" }}>|</span>
        <a href={`/compare?left_run_id=${encodeURIComponent(run.run_id)}&mode=aligned`} style={{ textDecoration: "underline", color: "#334155" }}>
          Open in Compare
        </a>
        <span style={{ margin: "0 8px", color: "#94a3b8" }}>|</span>
        <button onClick={copyShareLink}>Copy share link</button>
      </div>

      {actionNotice ? <p className="subtle" style={{ color: "#166534" }}>{actionNotice}</p> : null}

      <section className="hero-panel" style={{ marginBottom: 12 }}>
        <div>
          <p className="eyebrow">Run Diagnosis</p>
          <h1 className="hero-title" style={{ fontSize: 30 }}>Trace Viewer</h1>
          <p className="hero-copy">
            Surface where failure emerges, what the agent skipped, and what a reviewer should inspect next.
          </p>
        </div>
        <div className="hero-actions">
          <span className="pill" style={outcomeStyle(run.outcome)}>{run.outcome}</span>
          <span className="kpi-chip">steps: <strong>{run.num_steps}</strong></span>
          <span className="kpi-chip">filtered: <strong>{stepsTotal}</strong></span>
          <span className="kpi-chip">errors: <strong>{errorCount}</strong></span>
        </div>
      </section>

      <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
        <div style={{ color: "#334155", marginBottom: 10 }}>
          <strong>Task/Scenario:</strong> {run.task_id ?? run.scenario ?? "-"}
        </div>
        <div className="filter-grid">
          <label>
            Search within run
            <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="text / tool / payload" style={{ width: "100%" }} />
          </label>
          <label>
            Step type
            <select value={stepTypeFilter} onChange={(e) => setStepTypeFilter(e.target.value as StepTypeFilter)} style={{ width: "100%" }}>
              <option value="all">all</option>
              <option value="thought">thought</option>
              <option value="action">action</option>
              <option value="observation">observation</option>
              <option value="tool_call">tool_call</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
          <label style={{ display: "flex", alignItems: "end", gap: 8 }}>
            <input type="checkbox" checked={errorOnly} onChange={(e) => setErrorOnly(e.target.checked)} />
            error steps only
          </label>
        </div>
        <div className="header-actions" style={{ marginTop: 12 }}>
          <button onClick={resetFilters}>Clear filters</button>
          {firstErrorStep ? <button onClick={jumpToFirstError}>Jump to first error</button> : null}
          {loadingSteps ? <span className="subtle">Loading steps...</span> : null}
        </div>
      </section>

      <div className="viewer-layout">
        <aside className="panel" style={{ padding: 12, alignSelf: "start" }}>
          <h2 style={{ marginTop: 0, fontSize: 18 }}>Run Snapshot</h2>
          <div className="subtle" style={{ display: "grid", gap: 6, marginBottom: 12 }}>
            <div><strong>Started:</strong> {formatDateTime(run.started_at)}</div>
            <div><strong>Ended:</strong> {formatDateTime(run.ended_at)}</div>
            <div><strong>Source:</strong> {run.source ?? "-"}</div>
            <div><strong>Dataset:</strong> {run.dataset_name ?? "-"}</div>
            <div><strong>First failure event:</strong> {failureSummary?.first_failure_event_id ?? run.first_failure_event_id ?? "-"}</div>
            <div><strong>Root cause event:</strong> {failureSummary?.root_cause_event_id ?? run.root_cause_event_id ?? "-"}</div>
          </div>

          <div style={{ marginBottom: 12 }}>
            <strong style={{ fontSize: 13 }}>Observed Step Types</strong>
            <div style={{ display: "grid", gap: 8, marginTop: 6 }}>
              {stepTypeChart.map((item) => (
                <div key={item.label}>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span>{item.label}</span>
                    <span>{item.value}</span>
                  </div>
                  <div className="micro-bar-track">
                    <div className="micro-bar-fill" style={{ width: item.width }} />
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div style={{ marginBottom: 12 }}>
            <strong style={{ fontSize: 13 }}>Top Tools</strong>
            <div className="subtle" style={{ marginTop: 6 }}>
              {topTools.length ? topTools.join(", ") : "No tool calls in filtered trace."}
            </div>
          </div>

          <div style={{ marginBottom: 12 }}>
            <strong style={{ fontSize: 13 }}>Failure Overlays</strong>
            <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 6 }}>
              {uniqueOverlays.length === 0 ? (
                <span className="subtle">no labels</span>
              ) : (
                uniqueOverlays.map((annotation) => (
                  <span key={`${annotation.label}-${annotation.label_type}`} className="pill" style={labelTypeStyle(annotation.label_type)}>
                    {annotationLabel(annotation)}
                  </span>
                ))
              )}
            </div>
          </div>

          <div>
            <strong style={{ fontSize: 13 }}>Gap Signals</strong>
            <div style={{ marginTop: 6, display: "grid", gap: 6 }}>
              {uniqueGapSignals.length === 0 ? (
                <span className="subtle">No explicit gap labels.</span>
              ) : (
                uniqueGapSignals.map((annotation) => (
                  <button key={annotation.label} onClick={() => annotation.step_idx !== null && jumpToStep(annotation.step_idx)} className="gap-callout">
                    <strong>{annotationLabel(annotation)}</strong>
                    <span className="subtle">step {annotation.step_idx ?? "-"}</span>
                  </button>
                ))
              )}
            </div>
          </div>
        </aside>

        <section style={{ display: "grid", gap: 12 }}>
          <section className="analysis-grid">
            <article className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Failure Emergence</h2>
              <div className="subtle" style={{ marginBottom: 10 }}>
                Highlights the transition from setup into failure, anchored by the first error step.
              </div>
              <div className="timeline-strip" style={{ marginBottom: 12 }}>
                <div className="timeline-fill" style={{ width: "100%" }} />
                {firstErrorStep ? (
                  <div className="timeline-marker" style={{ left: `${clampPercent(firstErrorStep.step_idx, Math.max(run.num_steps, 1))}%` }} />
                ) : null}
              </div>
              <div style={{ display: "grid", gap: 8 }}>
                {keyMoments.map((step) => (
                  <button key={step.step_idx} className="moment-card" onClick={() => jumpToStep(step.step_idx)}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>Step {step.step_idx}</strong>
                      <span className="pill" style={stepTypeStyle(step.display_step_type)}>{step.display_step_type}</span>
                    </div>
                    <div className="subtle">{inferStepIntent(step)}</div>
                    <div style={{ color: "#334155", whiteSpace: "pre-wrap" }}>{step.text ?? "(no text)"}</div>
                  </button>
                ))}
              </div>
            </article>

            <article className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Expected vs Observed Workflow</h2>
              <div style={{ display: "grid", gap: 10 }}>
                {workflowChecks.map((item) => (
                  <div key={item.key} className="workflow-row">
                    <div>
                      <strong>{item.label}</strong>
                      <div className="subtle">{item.detail}</div>
                    </div>
                    <span
                      className="pill"
                      style={{
                        background: item.status === "observed" ? "#dcfce7" : item.status === "missing" ? "#fee2e2" : "#fef3c7",
                        color: item.status === "observed" ? "#166534" : item.status === "missing" ? "#b91c1c" : "#92400e",
                      }}
                    >
                      {item.status}
                    </span>
                  </div>
                ))}
              </div>
            </article>
          </section>

          <section className="analysis-grid">
            <article className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Phase Map</h2>
              <div className="subtle" style={{ marginBottom: 10 }}>
                Semantic chunks help reviewers reason about where the run spent time and where error density spiked.
              </div>
              <div style={{ display: "grid", gap: 10 }}>
                {phases.map((phase) => (
                  <div key={phase.name} className="phase-card">
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{phase.name}</strong>
                      <span className="subtle">steps {phase.start}-{phase.end}</span>
                    </div>
                    <div className="timeline-strip" style={{ margin: "8px 0" }}>
                      <div className="timeline-fill" style={{ width: `${clampPercent(phase.end, Math.max(run.num_steps, 1))}%` }} />
                    </div>
                    <div className="subtle">
                      step count: {phase.count} | errors: {phase.errorCount} | tools: {phase.tools.join(", ") || "none"}
                    </div>
                  </div>
                ))}
              </div>
            </article>

            <article className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Likely Interventions</h2>
              <div style={{ display: "grid", gap: 8 }}>
                {interventions.map((item) => (
                  <div key={item} className="intervention-card">{item}</div>
                ))}
              </div>
              {failureSummary?.summary_text ? (
                <div style={{ marginTop: 12 }}>
                  <strong>Failure summary</strong>
                  <p className="subtle" style={{ marginBottom: 0 }}>{failureSummary.summary_text}</p>
                </div>
              ) : null}
            </article>
          </section>

          <section className="panel" style={{ padding: 14 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Evidence Bundle</h2>
            <div className="header-actions" style={{ marginBottom: 10 }}>
              <a href={`${apiBaseUrl}/runs/${encodeURIComponent(params.run_id)}/report?format=md`} target="_blank" rel="noreferrer" className="kpi-chip">
                Report (md)
              </a>
              <a href={`${apiBaseUrl}/runs/${encodeURIComponent(params.run_id)}/report?format=json`} target="_blank" rel="noreferrer" className="kpi-chip">
                Report (json)
              </a>
              <a href={`/compare?left_run_id=${encodeURIComponent(params.run_id)}&mode=aligned`} className="kpi-chip">
                Compare from this run
              </a>
            </div>
            <div className="analysis-grid">
              <div>
                <strong>Case-study curation</strong>
                <div className="subtle" style={{ margin: "6px 0" }}>Capture a reusable analysis artifact for review or demo flows.</div>
                <input value={caseStudyTitle} onChange={(e) => setCaseStudyTitle(e.target.value)} placeholder="Case study title" style={{ width: "100%", marginBottom: 8 }} />
                <textarea value={caseStudyFocus} onChange={(e) => setCaseStudyFocus(e.target.value)} placeholder="Focus: what this run teaches" style={{ width: "100%", minHeight: 96, marginBottom: 8 }} />
                <div className="header-actions">
                  <button disabled={!caseStudyTitle.trim() && !caseStudyFocus.trim()} onClick={() => void markCaseStudy()}>Mark case study</button>
                  <input value={subsetName} onChange={(e) => setSubsetName(e.target.value)} placeholder="benchmark subset" />
                  <button disabled={!subsetName.trim()} onClick={() => void addToBenchmarkSubset()}>Add subset</button>
                </div>
              </div>
              <div>
                <strong>Review notes</strong>
                <div className="subtle" style={{ margin: "6px 0" }}>Notes below become part of the reviewer workflow.</div>
                <input value={reviewer} onChange={(e) => setReviewer(e.target.value)} placeholder="reviewer" style={{ width: "100%", marginBottom: 8 }} />
                <input value={reviewLabel} onChange={(e) => setReviewLabel(e.target.value)} placeholder="label (optional)" style={{ width: "100%", marginBottom: 8 }} />
                <textarea value={reviewNoteText} onChange={(e) => setReviewNoteText(e.target.value)} placeholder="What should another engineer notice?" style={{ width: "100%", minHeight: 96, marginBottom: 8 }} />
                <button disabled={!reviewer.trim() || !reviewNoteText.trim()} onClick={() => void submitReviewNote()}>Add reviewer note</button>
                <div style={{ display: "grid", gap: 8, marginTop: 12 }}>
                  {reviewNotes.map((note) => (
                    <div key={note.id} className="note-card">
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <strong>{note.reviewer}</strong>
                        <span className="subtle">{formatDateTime(note.created_at)}</span>
                      </div>
                      {note.label ? <div className="subtle">{note.label}</div> : null}
                      <div>{note.note}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="panel" style={{ padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", marginBottom: 12 }}>
              <h2 style={{ margin: 0, fontSize: 18 }}>Trace Timeline</h2>
              <div className="header-actions">
                <input value={manualLabel} onChange={(e) => setManualLabel(e.target.value)} placeholder="Add manual label" />
                <button onClick={() => void submitManualLabel()} disabled={savingLabel || !manualLabel.trim()}>{savingLabel ? "Saving..." : "Save label"}</button>
              </div>
            </div>

            <div ref={parentRef} style={{ maxHeight: "72vh", overflow: "auto", paddingRight: 4 }}>
              <div style={{ height: `${rowVirtualizer.getTotalSize()}px`, position: "relative" }}>
                {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                  const step = steps[virtualRow.index];
                  const isExpanded = expanded[step.step_idx] ?? step.error_flag;
                  const relatedLabels = annotations.filter((annotation) => annotation.step_idx === step.step_idx);
                  return (
                    <article
                      key={step.step_idx}
                      ref={rowVirtualizer.measureElement}
                      data-index={virtualRow.index}
                      className="trace-card"
                      style={{
                        position: "absolute",
                        top: 0,
                        left: 0,
                        width: "100%",
                        transform: `translateY(${virtualRow.start}px)`,
                        background: step.error_flag ? "#fff7f7" : "white",
                      }}
                    >
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, flexWrap: "wrap", marginBottom: 8 }}>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                          <strong>Step {step.step_idx}</strong>
                          <span className="pill" style={stepTypeStyle(step.display_step_type)}>{step.display_step_type}</span>
                          {step.tool_name ? <span className="kpi-chip">tool: {step.tool_name}</span> : null}
                          {step.error_flag ? <span className="pill" style={{ background: "#fee2e2", color: "#b91c1c" }}>error</span> : null}
                        </div>
                        <button onClick={() => toggleExpanded(step.step_idx)}>{isExpanded ? "Collapse" : "Expand"}</button>
                      </div>

                      <div className="subtle" style={{ marginBottom: 8 }}>
                        {formatDateTime(step.timestamp)} | intent: {inferStepIntent(step)}
                      </div>
                      <p style={{ marginTop: 0, whiteSpace: "pre-wrap" }}>{step.text || "(no text)"}</p>

                      {relatedLabels.length ? (
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 8 }}>
                          {relatedLabels.map((annotation) => (
                            <span key={`${annotation.label}-${annotation.label_type}`} className="pill" style={labelTypeStyle(annotation.label_type)}>
                              {annotationLabel(annotation)}
                            </span>
                          ))}
                        </div>
                      ) : null}

                      {isExpanded ? (
                        <div className="analysis-grid">
                          <div>
                            <strong>Structured fields</strong>
                            <div className="subtle">agent: {step.agent_id ?? "-"} | status: {step.status ?? "-"}</div>
                            <div className="subtle">error type: {step.error_type ?? "-"}</div>
                            <div className="subtle">evidence: {step.evidence_summary ?? "-"}</div>
                            <div className="subtle">next action: {step.intended_next_action ?? "-"}</div>
                          </div>
                          <div>
                            {step.tool_input ? (
                              <>
                                <strong>Tool input</strong>
                                <pre className="code-panel">{parseMaybeJson(step.tool_input)}</pre>
                              </>
                            ) : null}
                            {step.tool_output ? (
                              <>
                                <strong>Tool output</strong>
                                <pre className="code-panel">{parseMaybeJson(step.tool_output)}</pre>
                              </>
                            ) : null}
                          </div>
                        </div>
                      ) : null}
                    </article>
                  );
                })}
              </div>
            </div>
          </section>

          <section className="panel" style={{ padding: 14 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Metadata</h2>
            <pre className="code-panel">{JSON.stringify(run.metadata ?? {}, null, 2)}</pre>
          </section>
        </section>
      </div>
    </main>
  );
}
