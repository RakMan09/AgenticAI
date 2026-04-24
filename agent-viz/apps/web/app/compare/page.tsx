"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  buildStepPhases,
  compareSemanticSignature,
  inferStepIntent,
  normalizeLabel,
  summarizeCompareDivergence,
} from "../../lib/analysis";
import { fetchCompare, fetchFailureSummary, fetchRuns } from "../../lib/api";
import type { ComparePair, CompareResponse, FailureSummary, RunRow, StepRow } from "../../lib/types";

type CompareMode = "raw" | "aligned";

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

function statusStyle(status: ComparePair["status"]): { background: string; color: string; border: string } {
  switch (status) {
    case "match":
      return { background: "#ecfdf5", color: "#065f46", border: "#6ee7b7" };
    case "mismatch":
      return { background: "#fff7ed", color: "#9a3412", border: "#fdba74" };
    case "left_only":
      return { background: "#fef2f2", color: "#991b1b", border: "#fca5a5" };
    case "right_only":
      return { background: "#eff6ff", color: "#1d4ed8", border: "#93c5fd" };
  }
}

function uniqueLabelNames(summary: FailureSummary | null): string[] {
  if (!summary?.labels) return [];
  return [...new Set(summary.labels.map((item) => normalizeLabel(item.label)))].sort((a, b) => a.localeCompare(b));
}

function StepCard({ step, emptyLabel }: { step: StepRow | null; emptyLabel: string }) {
  if (!step) {
    return <div className="trace-card" style={{ borderStyle: "dashed", color: "#64748b" }}>{emptyLabel}</div>;
  }
  return (
    <article className="trace-card" style={{ background: step.error_flag ? "#fff7f7" : "white" }}>
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 6 }}>
        <strong>#{step.step_idx}</strong>
        <span className="pill" style={stepTypeStyle(step.display_step_type)}>{step.display_step_type}</span>
        {step.tool_name ? <span className="kpi-chip">tool: {step.tool_name}</span> : null}
      </div>
      <div className="subtle" style={{ marginBottom: 6 }}>{inferStepIntent(step)}</div>
      <p style={{ margin: 0, whiteSpace: "pre-wrap", color: "#0f172a" }}>{step.text || "(no text)"}</p>
    </article>
  );
}

export default function ComparePage() {
  const [runs, setRuns] = useState<RunRow[]>([]);
  const [leftRunId, setLeftRunId] = useState("");
  const [rightRunId, setRightRunId] = useState("");
  const [mode, setMode] = useState<CompareMode>("aligned");
  const [data, setData] = useState<CompareResponse | null>(null);
  const [leftSummary, setLeftSummary] = useState<FailureSummary | null>(null);
  const [rightSummary, setRightSummary] = useState<FailureSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [shareNotice, setShareNotice] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const left = params.get("left_run_id") ?? "";
    const right = params.get("right_run_id") ?? "";
    const modeParam = params.get("mode");
    if (left) setLeftRunId(left);
    if (right) setRightRunId(right);
    if (modeParam === "raw" || modeParam === "aligned") setMode(modeParam);
  }, []);

  useEffect(() => {
    let mounted = true;
    fetchRuns({ limit: 500, offset: 0, sort_by: "started_at", sort_dir: "desc" })
      .then((res) => {
        if (!mounted) return;
        setRuns(res.items);
        const failRun = res.items.find((run) => run.outcome === "fail");
        const successRun = res.items.find((run) => run.outcome === "success");
        if (failRun) setLeftRunId((current) => current || failRun.run_id);
        if (successRun) setRightRunId((current) => current || successRun.run_id);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to load runs");
      });
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams();
    if (leftRunId) params.set("left_run_id", leftRunId);
    if (rightRunId) params.set("right_run_id", rightRunId);
    params.set("mode", mode);
    window.history.replaceState(null, "", `/compare?${params.toString()}`);
  }, [leftRunId, mode, rightRunId]);

  useEffect(() => {
    if (!leftRunId || !rightRunId) {
      setData(null);
      return;
    }
    let mounted = true;
    setLoading(true);
    setError(null);
    fetchCompare({ left_run_id: leftRunId, right_run_id: rightRunId, mode })
      .then((res) => {
        if (!mounted) return;
        setData(res);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Failed to compare runs");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });
    return () => {
      mounted = false;
    };
  }, [leftRunId, mode, rightRunId]);

  useEffect(() => {
    if (!leftRunId) {
      setLeftSummary(null);
      return;
    }
    fetchFailureSummary(leftRunId).then(setLeftSummary).catch(() => setLeftSummary(null));
  }, [leftRunId]);

  useEffect(() => {
    if (!rightRunId) {
      setRightSummary(null);
      return;
    }
    fetchFailureSummary(rightRunId).then(setRightSummary).catch(() => setRightSummary(null));
  }, [rightRunId]);

  const filteredRuns = useMemo(() => runs.slice(0, 500), [runs]);
  const leftLabelNames = useMemo(() => uniqueLabelNames(leftSummary), [leftSummary]);
  const rightLabelNames = useMemo(() => uniqueLabelNames(rightSummary), [rightSummary]);
  const sharedLabels = useMemo(
    () => leftLabelNames.filter((label) => rightLabelNames.includes(label)),
    [leftLabelNames, rightLabelNames],
  );
  const leftOnlyLabels = useMemo(
    () => leftLabelNames.filter((label) => !rightLabelNames.includes(label)),
    [leftLabelNames, rightLabelNames],
  );
  const rightOnlyLabels = useMemo(
    () => rightLabelNames.filter((label) => !leftLabelNames.includes(label)),
    [leftLabelNames, rightLabelNames],
  );
  const divergence = useMemo(() => summarizeCompareDivergence(data?.items ?? []), [data]);
  const firstDivergence = useMemo(
    () => data?.items.find((pair) => pair.status !== "match")?.pair_idx ?? null,
    [data],
  );
  const leftPhases = useMemo(
    () => buildStepPhases(((data?.items ?? []).map((pair) => pair.left_step).filter(Boolean)) as StepRow[]),
    [data],
  );
  const rightPhases = useMemo(
    () => buildStepPhases(((data?.items ?? []).map((pair) => pair.right_step).filter(Boolean)) as StepRow[]),
    [data],
  );
  const representativeDiffs = useMemo(() => divergence.semanticGaps.slice(0, 6), [divergence.semanticGaps]);

  const copyShareLink = () => {
    if (typeof window === "undefined") return;
    void navigator.clipboard.writeText(window.location.href);
    setShareNotice("Copied compare view URL.");
    window.setTimeout(() => setShareNotice(""), 1800);
  };

  const clearSelection = () => {
    setLeftRunId("");
    setRightRunId("");
    setData(null);
  };

  const swapSelection = () => {
    setLeftRunId(rightRunId);
    setRightRunId(leftRunId);
  };

  const divergenceChart = useMemo(() => {
    if (!data) return [];
    const total = Math.max(data.total_pairs, 1);
    return [
      { label: "match", value: data.stats.match, tone: "#16a34a" },
      { label: "mismatch", value: data.stats.mismatch, tone: "#ea580c" },
      { label: "left only", value: data.stats.left_only, tone: "#dc2626" },
      { label: "right only", value: data.stats.right_only, tone: "#2563eb" },
    ].map((item) => ({ ...item, width: `${(item.value / total) * 100}%` }));
  }, [data]);

  return (
    <main className="container">
      <div style={{ marginBottom: 12 }}>
        <Link href="/" style={{ textDecoration: "underline", color: "#334155" }}>Back to Run Explorer</Link>
      </div>

      <section className="hero-panel" style={{ marginBottom: 12 }}>
        <div>
          <p className="eyebrow">Compare Runs</p>
          <h1 className="hero-title" style={{ fontSize: 30 }}>Diagnostic Comparison</h1>
          <p className="hero-copy">
            Compare trajectories semantically, not just line by line. Surface divergence families, missing branches,
            and the labels unique to each run.
          </p>
          <div className="hero-stat-grid" style={{ marginTop: 18 }}>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Semantic gaps</span>
              <strong className="hero-stat-value">{divergence.semanticGaps.length}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Shared labels</span>
              <strong className="hero-stat-value">{sharedLabels.length}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">First divergence</span>
              <strong className="hero-stat-value">{firstDivergence ?? "-"}</strong>
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
            <h2 className="section-title">Choose Trajectories</h2>
          </div>
          <span className="subtle">Select two runs and switch between raw and aligned comparison</span>
        </div>
        <div className="filter-grid">
          <label>
            Left run
            <select value={leftRunId} onChange={(e) => setLeftRunId(e.target.value)} style={{ width: "100%" }}>
              <option value="">select run</option>
              {filteredRuns.map((run) => (
                <option key={`left-${run.run_id}`} value={run.run_id}>
                  {run.run_id.slice(0, 14)} | {run.outcome} | steps {run.num_steps}
                </option>
              ))}
            </select>
          </label>
          <label>
            Right run
            <select value={rightRunId} onChange={(e) => setRightRunId(e.target.value)} style={{ width: "100%" }}>
              <option value="">select run</option>
              {filteredRuns.map((run) => (
                <option key={`right-${run.run_id}`} value={run.run_id}>
                  {run.run_id.slice(0, 14)} | {run.outcome} | steps {run.num_steps}
                </option>
              ))}
            </select>
          </label>
          <label>
            Compare mode
            <select value={mode} onChange={(e) => setMode(e.target.value as CompareMode)} style={{ width: "100%" }}>
              <option value="raw">raw (index aligned)</option>
              <option value="aligned">aligned (retry / missing aware)</option>
            </select>
          </label>
        </div>
        <div className="header-actions" style={{ marginTop: 12 }}>
          <button disabled={!leftRunId && !rightRunId} onClick={swapSelection}>Swap sides</button>
          <button disabled={!leftRunId && !rightRunId} onClick={clearSelection}>Clear selection</button>
        </div>
        {error ? <p style={{ color: "#b91c1c", marginTop: 12 }}>{error}</p> : null}
      </section>

      {data ? (
        <>
          <div className="dashboard-feature-grid" style={{ marginBottom: 12 }}>
            <section className="panel" style={{ padding: 16 }}>
              <div className="section-caption">
                <div>
                  <span className="section-kicker">Summary</span>
                  <h2 className="section-title">Divergence Summary</h2>
                </div>
                <span className="subtle">How far apart the two traces drift</span>
              </div>
              <div className="kpi-row">
                <span className="kpi-chip">pairs: <strong>{data.total_pairs}</strong></span>
                <span className="kpi-chip">mismatch: <strong>{data.stats.mismatch}</strong></span>
                <span className="kpi-chip">first divergence: <strong>{firstDivergence ?? "-"}</strong></span>
                <span className="kpi-chip">semantic gaps: <strong>{divergence.semanticGaps.length}</strong></span>
              </div>
              <div className="subtle" style={{ display: "grid", gap: 4 }}>
                <div>left-only steps: <strong>{divergence.leftOnlyCount}</strong></div>
                <div>right-only steps: <strong>{divergence.rightOnlyCount}</strong></div>
                <div>shared labels: <strong>{sharedLabels.length}</strong></div>
              </div>
              <div style={{ display: "grid", gap: 8, marginTop: 12 }}>
                {divergenceChart.map((item) => (
                  <div key={item.label}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{item.label}</strong>
                      <span className="subtle">{item.value}</span>
                    </div>
                    <div className="micro-bar-track">
                      <div className="micro-bar-fill" style={{ width: item.width, background: item.tone }} />
                    </div>
                  </div>
                ))}
              </div>
            </section>

            <section className="panel" style={{ padding: 16 }}>
              <div className="section-caption">
                <div>
                  <span className="section-kicker">Annotations</span>
                  <h2 className="section-title">Label Overlap</h2>
                </div>
                <span className="subtle">Common and unique explanations across both runs</span>
              </div>
              <div className="compare-label-board">
                <div className="compare-label-column">
                  <strong>shared</strong>
                  <div className="compare-label-wrap">
                    {sharedLabels.length ? sharedLabels.map((label) => <span key={`shared-${label}`} className="kpi-chip">{label}</span>) : <span className="subtle">-</span>}
                  </div>
                </div>
                <div className="compare-label-column">
                  <strong>left only</strong>
                  <div className="compare-label-wrap">
                    {leftOnlyLabels.length ? leftOnlyLabels.map((label) => <span key={`left-${label}`} className="kpi-chip">{label}</span>) : <span className="subtle">-</span>}
                  </div>
                </div>
                <div className="compare-label-column">
                  <strong>right only</strong>
                  <div className="compare-label-wrap">
                    {rightOnlyLabels.length ? rightOnlyLabels.map((label) => <span key={`right-${label}`} className="kpi-chip">{label}</span>) : <span className="subtle">-</span>}
                  </div>
                </div>
              </div>
            </section>
          </div>

          <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
            <div className="section-caption">
              <div>
                <span className="section-kicker">Sequence Structure</span>
                <h2 className="section-title">Phase Alignment</h2>
              </div>
              <span className="subtle">High-level rhythm of the two trajectories</span>
            </div>
            <div className="compare-grid">
              <div>
                <strong>Left run phases</strong>
                <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                  {leftPhases.map((phase) => (
                    <div key={`left-${phase.name}`} className="phase-card">
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <strong>{phase.name}</strong>
                        <span className="subtle">{phase.start}-{phase.end}</span>
                      </div>
                      <div className="subtle">errors: {phase.errorCount} | tools: {phase.tools.join(", ") || "none"}</div>
                    </div>
                  ))}
                </div>
              </div>
              <div>
                <strong>Right run phases</strong>
                <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                  {rightPhases.map((phase) => (
                    <div key={`right-${phase.name}`} className="phase-card">
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <strong>{phase.name}</strong>
                        <span className="subtle">{phase.start}-{phase.end}</span>
                      </div>
                      <div className="subtle">errors: {phase.errorCount} | tools: {phase.tools.join(", ") || "none"}</div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
            <div className="section-caption">
              <div>
                <span className="section-kicker">Key Moments</span>
                <h2 className="section-title">Representative Divergences</h2>
              </div>
              <span className="subtle">The mismatches most likely to explain outcome differences</span>
            </div>
            <div style={{ display: "grid", gap: 10 }}>
              {representativeDiffs.length === 0 ? (
                <div className="subtle">The selected runs are currently aligned with no semantic divergence to highlight.</div>
              ) : representativeDiffs.map((pair) => (
                <div key={pair.pair_idx} className="moment-card">
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 8 }}>
                    <strong>Pair {pair.pair_idx}</strong>
                    <span className="subtle">{pair.note ?? "semantic mismatch"}</span>
                  </div>
                  <div className="compare-grid">
                    <div>
                      <strong>Left signature</strong>
                      <div className="subtle">{compareSemanticSignature(pair.left_step)}</div>
                    </div>
                    <div>
                      <strong>Right signature</strong>
                      <div className="subtle">{compareSemanticSignature(pair.right_step)}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="panel" style={{ padding: 12 }}>
            <div className="section-caption" style={{ padding: "4px 8px 8px" }}>
              <div>
                <span className="section-kicker">Trace Evidence</span>
                <h2 className="section-title">Pair-by-Pair Comparison</h2>
              </div>
              <span className="subtle">Exact evidence backing the higher-level divergence summaries</span>
            </div>
            <div className="compare-grid-header">
              <div>
                <strong>Left:</strong>{" "}
                <a href={`/runs/${data.left_run.run_id}`} style={{ textDecoration: "underline" }}>
                  {data.left_run.run_id}
                </a>{" "}
                ({data.left_run.outcome}, steps {data.left_run.num_steps})
              </div>
              <div>
                <strong>Right:</strong>{" "}
                <a href={`/runs/${data.right_run.run_id}`} style={{ textDecoration: "underline" }}>
                  {data.right_run.run_id}
                </a>{" "}
                ({data.right_run.outcome}, steps {data.right_run.num_steps})
              </div>
            </div>
            <div style={{ maxHeight: "72vh", overflow: "auto", paddingRight: 4 }}>
              {data.items.map((pair) => {
                const style = statusStyle(pair.status);
                return (
                  <article
                    key={pair.pair_idx}
                    className="compare-pair"
                    style={{ borderColor: style.border, background: style.background }}
                  >
                    <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
                      <strong>Pair {pair.pair_idx}</strong>
                      <span className="pill" style={{ background: "white", color: style.color }}>{pair.status}</span>
                      {pair.note ? <span className="subtle" style={{ color: style.color }}>{pair.note.replaceAll("_", " ")}</span> : null}
                    </div>
                    <div className="compare-grid">
                      <StepCard step={pair.left_step} emptyLabel="No left step" />
                      <StepCard step={pair.right_step} emptyLabel="No right step" />
                    </div>
                  </article>
                );
              })}
            </div>
          </section>
        </>
      ) : null}

      {loading ? <section className="panel" style={{ padding: 16 }}>Loading comparison...</section> : null}
    </main>
  );
}
