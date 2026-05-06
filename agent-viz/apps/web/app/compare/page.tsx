"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { fetchFailureSummary, fetchRun, fetchRuns, fetchSteps } from "../../lib/api";
import { inferStepIntent, normalizeLabel } from "../../lib/analysis";
import type { FailureSummary, RunDetail, RunRow, StepRow } from "../../lib/types";

interface TraceBundle {
  run: RunDetail;
  steps: StepRow[];
  summary: FailureSummary | null;
}

type ExpandedMap = Record<string, number | null>;
type StepTypeKey = StepRow["display_step_type"];

const RUN_COLORS = ["#dc2626", "#0ea5c6", "#7c3aed", "#0f766e", "#f59e0b", "#64748b", "#be123c", "#2563eb"];

function outcomeStyle(outcome: string): { background: string; color: string } {
  if (outcome === "success") return { background: "#dcfce7", color: "#166534" };
  if (outcome === "fail") return { background: "#fee2e2", color: "#b91c1c" };
  return { background: "#ede9fe", color: "#6d28d9" };
}

function compactStepLabel(step: StepRow): string {
  if (step.tool_name) return step.tool_name;
  if (step.event_type === "user_message") return "user";
  if (step.event_type === "assistant_message") return "answer";
  if (step.event_type === "assistant_thinking") return "think";
  if (step.event_type === "tool_result") return step.error_flag ? "tool error" : "result";
  return step.display_step_type.replace("_", " ");
}

function stepTitle(step: StepRow): string {
  const text = step.text ? ` | ${step.text.slice(0, 140)}` : "";
  const status = step.status ? ` | ${step.status}` : "";
  return `Step ${step.step_idx}: ${compactStepLabel(step)}${status}${text}`;
}

function parseMaybeJson(raw: string | Record<string, unknown> | null): string {
  if (!raw) return "";
  if (typeof raw !== "string") return JSON.stringify(raw, null, 2);
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

function summarizePayload(raw: string | Record<string, unknown> | null): string {
  const parsed = parseMaybeJson(raw);
  if (!parsed) return "";
  return parsed.length > 2400 ? `${parsed.slice(0, 2400)}\n...` : parsed;
}

function PayloadBlock({ title, value }: { title: string; value: string | Record<string, unknown> | null }) {
  if (!value) return <span className="subtle">No {title.toLowerCase()}</span>;
  const parsed = parseMaybeJson(value);
  const isLarge = parsed.length > 2400;
  return (
    <details className="payload-details">
      <summary>
        <strong>{title}</strong>
        <span className="subtle">{isLarge ? "large payload" : "payload"}</span>
      </summary>
      <pre className="code-panel code-panel-compact">{isLarge ? summarizePayload(value) : parsed}</pre>
      {isLarge ? (
        <details className="payload-details payload-details-full">
          <summary>Show full payload</summary>
          <pre className="code-panel code-panel-full">{parsed}</pre>
        </details>
      ) : null}
    </details>
  );
}

function runOptionLabel(run: RunRow): string {
  const shortScenario = (run.scenario ?? run.task_id ?? "").slice(0, 72);
  return `${run.run_id.slice(0, 18)} | ${run.outcome} | ${run.num_steps} steps | ${shortScenario}`;
}

function summarizeLabels(summary: FailureSummary | null): string[] {
  if (!summary?.labels) return [];
  return [...new Set(summary.labels.map((item) => normalizeLabel(item.label)))].sort((a, b) => a.localeCompare(b));
}

function stepTypeCounts(steps: StepRow[]): Record<StepTypeKey, number> {
  return steps.reduce<Record<StepTypeKey, number>>(
    (counts, step) => {
      counts[step.display_step_type] += 1;
      return counts;
    },
    { thought: 0, action: 0, observation: 0, tool_call: 0, unknown: 0 },
  );
}

function cumulativeErrorPoints(steps: StepRow[], maxErrors: number, width = 344, height = 104, x0 = 28, y0 = 130): string {
  if (!steps.length) return `${x0},${y0} ${x0 + width},${y0}`;
  let cumulative = 0;
  return steps
    .map((step, index) => {
      if (step.error_flag) cumulative += 1;
      const x = x0 + (index / Math.max(steps.length - 1, 1)) * width;
      const y = y0 - (cumulative / Math.max(maxErrors, 1)) * height;
      return `${x},${y}`;
    })
    .join(" ");
}

function runDisplayLabel(runId: string, index: number): string {
  return `Run ${index + 1}: ${runId.replace(/^run_/, "").slice(0, 22)}`;
}

function finalErrorY(steps: StepRow[], maxErrors: number, height = 184, y0 = 218): number {
  const errors = steps.filter((step) => step.error_flag).length;
  return y0 - (errors / Math.max(maxErrors, 1)) * height;
}

function errorPointPositions(steps: StepRow[], maxErrors: number): Array<{ x: number; y: number; stepIdx: number; count: number }> {
  let cumulative = 0;
  return steps.flatMap((step, index) => {
    if (!step.error_flag) return [];
    cumulative += 1;
    return [{
      x: 54 + (index / Math.max(steps.length - 1, 1)) * 556,
      y: 218 - (cumulative / Math.max(maxErrors, 1)) * 184,
      stepIdx: step.step_idx,
      count: cumulative,
    }];
  });
}

function StepDetail({ step }: { step: StepRow }) {
  return (
    <div className="stacked-step-detail">
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center", marginBottom: 8 }}>
        <strong>Step {step.step_idx}</strong>
        <span className={`trace-mini-type trace-mini-type-${step.display_step_type}`}>{step.display_step_type}</span>
        {step.tool_name ? <span className="kpi-chip">tool: {step.tool_name}</span> : null}
        {step.error_flag ? <span className="pill" style={{ background: "#fee2e2", color: "#b91c1c" }}>error</span> : null}
        {step.retry_count !== null ? <span className="kpi-chip">retry: {step.retry_count}</span> : null}
        {step.status ? <span className="kpi-chip">status: {step.status}</span> : null}
      </div>
      <div className="subtle" style={{ marginBottom: 8 }}>intent: {inferStepIntent(step)}</div>
      <p className="step-text-compact">{step.text || "(no text)"}</p>
      {(step.tool_input || step.tool_output) ? (
        <div className="analysis-grid" style={{ marginTop: 10 }}>
          <PayloadBlock title="Tool input" value={step.tool_input} />
          <PayloadBlock title="Tool output" value={step.tool_output} />
        </div>
      ) : null}
    </div>
  );
}

export default function ComparePage() {
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [selectedRunIds, setSelectedRunIds] = useState<string[]>([]);
  const [candidateRunId, setCandidateRunId] = useState("");
  const [bundles, setBundles] = useState<TraceBundle[]>([]);
  const [expanded, setExpanded] = useState<ExpandedMap>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [shareNotice, setShareNotice] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const runIdsParam = params.get("run_ids");
    const selectedFromQuery = runIdsParam
      ? runIdsParam.split(",").map((item) => item.trim()).filter(Boolean)
      : [params.get("left_run_id"), params.get("right_run_id")].filter(Boolean) as string[];
    if (selectedFromQuery.length) {
      setSelectedRunIds([...new Set(selectedFromQuery)].slice(0, 8));
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    fetchRuns({ limit: 500, offset: 0, sort_by: "started_at", sort_dir: "desc" })
      .then((res) => {
        if (!mounted) return;
        setRuns(res.items);
        setCandidateRunId(res.items[0]?.run_id ?? "");
      })
      .catch((err) => setError(err instanceof Error ? err.message : "Failed to load runs"));
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams();
    if (selectedRunIds.length) params.set("run_ids", selectedRunIds.join(","));
    window.history.replaceState(null, "", `/compare?${params.toString()}`);
  }, [selectedRunIds]);

  useEffect(() => {
    if (!selectedRunIds.length) {
      setBundles([]);
      return;
    }
    let mounted = true;
    setLoading(true);
    setError(null);
    Promise.all(
      selectedRunIds.map(async (runId) => {
        const [run, stepsResponse, summary] = await Promise.all([
          fetchRun(runId),
          fetchSteps(runId, { limit: 20000, offset: 0 }),
          fetchFailureSummary(runId).catch(() => null),
        ]);
        return { run, steps: stepsResponse.items, summary };
      }),
    )
      .then((items) => {
        if (!mounted) return;
        setBundles(items);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to load selected traces");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, [selectedRunIds]);

  const totalSteps = useMemo(() => bundles.reduce((sum, item) => sum + item.steps.length, 0), [bundles]);
  const totalErrors = useMemo(() => bundles.reduce((sum, item) => sum + item.steps.filter((step) => step.error_flag).length, 0), [bundles]);
  const maxErrorsPerRun = useMemo(() => Math.max(...bundles.map((bundle) => bundle.steps.filter((step) => step.error_flag).length), 1), [bundles]);
  const sharedLabels = useMemo(() => {
    if (bundles.length < 2) return [];
    const labelSets = bundles.map((bundle) => new Set(summarizeLabels(bundle.summary)));
    return [...labelSets[0]].filter((label) => labelSets.every((set) => set.has(label))).sort((a, b) => a.localeCompare(b));
  }, [bundles]);

  const addRun = () => {
    if (!candidateRunId || selectedRunIds.includes(candidateRunId)) return;
    setSelectedRunIds((current) => [...current, candidateRunId].slice(0, 8));
  };

  const removeRun = (runId: string) => {
    setSelectedRunIds((current) => current.filter((item) => item !== runId));
    setExpanded((current) => {
      const copy = { ...current };
      delete copy[runId];
      return copy;
    });
  };

  const clearSelection = () => {
    setSelectedRunIds([]);
    setBundles([]);
    setExpanded({});
  };

  const copyShareLink = () => {
    if (typeof window === "undefined") return;
    void navigator.clipboard.writeText(window.location.href);
    setShareNotice("Copied compare view URL.");
    window.setTimeout(() => setShareNotice(""), 1800);
  };

  const toggleStep = (runId: string, stepIdx: number) => {
    setExpanded((current) => ({ ...current, [runId]: current[runId] === stepIdx ? null : stepIdx }));
  };

  return (
    <main className="container">
      <div style={{ marginBottom: 12 }}>
        <Link href="/" style={{ textDecoration: "underline", color: "#334155" }}>Back to Run Explorer</Link>
      </div>

      <section className="hero-panel" style={{ marginBottom: 12 }}>
        <div>
          <p className="eyebrow">Compare Runs</p>
          <h1 className="hero-title" style={{ fontSize: 30 }}>Stacked Trace Comparison</h1>
          <p className="hero-copy">
            Add several runs and compare them as compact execution ribbons. Expand only the steps you need,
            so long traces stay visible without scrolling through every card.
          </p>
          <div className="hero-stat-grid" style={{ marginTop: 18 }}>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Selected runs</span>
              <strong className="hero-stat-value">{bundles.length}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Trace steps</span>
              <strong className="hero-stat-value">{totalSteps}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Error steps</span>
              <strong className="hero-stat-value">{totalErrors}</strong>
            </div>
          </div>
        </div>
        <div className="hero-actions">
          <button className="btn-primary" onClick={copyShareLink}>Copy share link</button>
          {shareNotice ? <span className="subtle" style={{ color: "#166534" }}>{shareNotice}</span> : null}
        </div>
      </section>

      <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
        <div className="section-caption">
          <div>
            <span className="section-kicker">Comparison Setup</span>
            <h2 className="section-title">Add Multiple Runs</h2>
          </div>
          <span className="subtle">Up to 8 traces can be stacked line by line</span>
        </div>
        <div className="compare-add-row">
          <select value={candidateRunId} onChange={(event) => setCandidateRunId(event.target.value)}>
            {runs.map((run) => (
              <option key={run.run_id} value={run.run_id}>{runOptionLabel(run)}</option>
            ))}
          </select>
          <button onClick={addRun} disabled={!candidateRunId || selectedRunIds.includes(candidateRunId)}>Add run</button>
          <button onClick={clearSelection} disabled={!selectedRunIds.length}>Clear</button>
        </div>
        <div className="timeline-legend" style={{ marginTop: 12 }}>
          <span><i className="legend-dot timeline-dot-thought" />thought</span>
          <span><i className="legend-dot timeline-dot-tool_call" />tool call</span>
          <span><i className="legend-dot timeline-dot-observation" />result</span>
          <span><i className="legend-dot timeline-dot-action" />answer</span>
          <span><i className="legend-dot timeline-dot-error" />error</span>
        </div>
        {error ? <p style={{ color: "#b91c1c", marginTop: 12 }}>{error}</p> : null}
      </section>

      {bundles.length ? (
        <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
          <div className="section-caption">
            <div>
              <span className="section-kicker">Comparison Graphs</span>
              <h2 className="section-title">Step Mix & Error Growth</h2>
            </div>
            <span className="subtle">Bars compare step types; lines show cumulative errors over the run</span>
          </div>
          <div className="compare-single-chart">
            <svg viewBox="0 0 760 270" className="compare-line-chart" aria-label="Combined cumulative error graph">
              <rect x="54" y="34" width="556" height="184" className="compare-plot-bg" />
              {[0, 25, 50, 75, 100].map((tick) => {
                const x = 54 + (tick / 100) * 556;
                return (
                  <g key={`x-${tick}`}>
                    <line x1={x} x2={x} y1="34" y2="218" className="grid-line" />
                    <text x={x} y="237" textAnchor="middle" className="axis-tick-label">{tick}%</text>
                  </g>
                );
              })}
              {Array.from({ length: maxErrorsPerRun + 1 }, (_, tick) => {
                const y = 218 - (tick / Math.max(maxErrorsPerRun, 1)) * 184;
                return (
                  <g key={`y-${tick}`}>
                    <line x1="54" x2="610" y1={y} y2={y} className="grid-line" />
                    <text x="42" y={y + 4} textAnchor="end" className="axis-tick-label">{tick}</text>
                  </g>
                );
              })}
              <line x1="54" y1="218" x2="610" y2="218" className="axis-line" />
              <line x1="54" y1="34" x2="54" y2="218" className="axis-line" />
              {bundles.map((bundle, index) => {
                const color = RUN_COLORS[index % RUN_COLORS.length];
                const errorPoints = errorPointPositions(bundle.steps, maxErrorsPerRun);
                return errorPoints.map((point) => (
                  <circle
                    key={`error-point-${bundle.run.run_id}-${point.stepIdx}`}
                    cx={point.x}
                    cy={point.y}
                    r="4.5"
                    fill={color}
                    stroke="#ffffff"
                    strokeWidth="2"
                  >
                    <title>{`Run ${index + 1}, step ${point.stepIdx}: cumulative errors ${point.count}`}</title>
                  </circle>
                ));
              })}
              {bundles.map((bundle, index) => {
                const color = RUN_COLORS[index % RUN_COLORS.length];
                const errorCount = bundle.steps.filter((step) => step.error_flag).length;
                const y = finalErrorY(bundle.steps, maxErrorsPerRun);
                return (
                  <g key={`combined-line-${bundle.run.run_id}`}>
                    <polyline
                      points={cumulativeErrorPoints(bundle.steps, maxErrorsPerRun, 556, 184, 54, 218)}
                      fill="none"
                      stroke={color}
                      strokeWidth="4"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                    <circle cx="610" cy={y} r="5" fill={color} />
                    <text x="624" y={y + 4} className="compare-direct-label">
                      {`Run ${index + 1}: ${errorCount} errors`}
                    </text>
                  </g>
                );
              })}
              <text x="332" y="260" textAnchor="middle" className="axis-label">progress through run</text>
              <text x="16" y="126" textAnchor="middle" className="axis-label" transform="rotate(-90 16 126)">cumulative errors</text>
            </svg>
            <div className="compare-run-summary">
              {bundles.map((bundle, index) => {
                const color = RUN_COLORS[index % RUN_COLORS.length];
                const counts = stepTypeCounts(bundle.steps);
                return (
                  <div key={`summary-${bundle.run.run_id}`} className="compare-run-summary-row" style={{ borderLeftColor: color }}>
                    <strong><span className="run-color-dot" style={{ background: color }} />{runDisplayLabel(bundle.run.run_id, index)}</strong>
                    <span>{bundle.run.outcome}</span>
                    <span>steps {bundle.steps.length}</span>
                    <span>errors {bundle.steps.filter((step) => step.error_flag).length}</span>
                    <span>tools {counts.tool_call}</span>
                    <span>answers {counts.action}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </section>
      ) : null}

      <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
        <div className="section-caption">
          <div>
            <span className="section-kicker">Trace Stack</span>
            <h2 className="section-title">Line-by-Line Trajectories</h2>
          </div>
          <span className="subtle">{loading ? "Loading traces..." : "Click any segment to expand details"}</span>
        </div>

        {bundles.length === 0 && !loading ? (
          <div className="subtle">Add one or more runs to compare their traces.</div>
        ) : null}

        <div className="stacked-trace-list">
          {bundles.map((bundle) => {
            const expandedStepIdx = expanded[bundle.run.run_id];
            const expandedStep = bundle.steps.find((step) => step.step_idx === expandedStepIdx) ?? null;
            const labels = summarizeLabels(bundle.summary);
            const firstErrorStep = bundle.steps.find((step) => step.error_flag);
            return (
              <article key={bundle.run.run_id} className="stacked-trace-row">
                <div className="stacked-trace-meta">
                  <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                    <span className="pill" style={outcomeStyle(bundle.run.outcome)}>{bundle.run.outcome}</span>
                    {bundle.run.failure_category ? <span className="kpi-chip">{bundle.run.failure_category}</span> : null}
                  </div>
                  <a href={`/runs/${encodeURIComponent(bundle.run.run_id)}`} style={{ textDecoration: "underline", color: "#0f172a", fontWeight: 700 }}>
                    {bundle.run.run_id}
                  </a>
                  <div className="subtle">{bundle.run.scenario ?? bundle.run.task_id ?? "-"}</div>
                  <div className="subtle">steps {bundle.steps.length} | errors {bundle.steps.filter((step) => step.error_flag).length} | first error {firstErrorStep?.step_idx ?? "-"}</div>
                  <button onClick={() => removeRun(bundle.run.run_id)}>Remove</button>
                </div>

                <div className="stacked-trace-main">
                  <div className="trace-ribbon trace-ribbon-stacked">
                    {bundle.steps.map((step) => (
                      <button
                        key={`${bundle.run.run_id}-${step.step_idx}`}
                        className={`trace-node trace-node-${step.display_step_type}${step.error_flag ? " trace-node-error" : ""}${expandedStepIdx === step.step_idx ? " trace-node-selected" : ""}`}
                        title={stepTitle(step)}
                        onClick={() => toggleStep(bundle.run.run_id, step.step_idx)}
                      >
                        <span className="trace-node-index">{step.step_idx}</span>
                        <span className="trace-node-label">{compactStepLabel(step)}</span>
                      </button>
                    ))}
                  </div>
                  <div className="compare-label-wrap" style={{ marginTop: 10 }}>
                    {labels.length ? labels.map((label) => <span key={`${bundle.run.run_id}-${label}`} className="kpi-chip">{label}</span>) : <span className="subtle">no labels</span>}
                  </div>
                  {expandedStep ? <StepDetail step={expandedStep} /> : null}
                </div>
              </article>
            );
          })}
        </div>
      </section>

      {bundles.length >= 2 ? (
        <section className="panel" style={{ padding: 16 }}>
          <div className="section-caption">
            <div>
              <span className="section-kicker">Cross-Run Signals</span>
              <h2 className="section-title">Shared Failure Evidence</h2>
            </div>
            <span className="subtle">Labels that appear across every selected trace</span>
          </div>
          <div className="compare-label-wrap">
            {sharedLabels.length ? sharedLabels.map((label) => <span key={label} className="kpi-chip">{label}</span>) : <span className="subtle">No shared labels across all selected traces.</span>}
          </div>
        </section>
      ) : null}
    </main>
  );
}
