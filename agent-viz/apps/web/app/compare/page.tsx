"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { fetchCompare, fetchFailureSummary, fetchRuns } from "../../lib/api";
import type { ComparePair, CompareResponse, FailureSummary, RunRow, StepRow } from "../../lib/types";

type CompareMode = "raw" | "aligned";

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

function formatNote(note: string | null): string {
  if (!note) return "";
  return note.replaceAll("_", " ");
}

function uniqueLabelNames(summary: FailureSummary | null): string[] {
  if (!summary?.labels) return [];
  return [...new Set(summary.labels.map((item) => item.label))].sort((a, b) => a.localeCompare(b));
}

function StepCard({ step, emptyLabel }: { step: StepRow | null; emptyLabel: string }) {
  if (!step) {
    return (
      <div
        style={{
          border: "1px dashed #cbd5e1",
          borderRadius: 10,
          padding: 10,
          color: "#64748b",
          minHeight: 96,
          display: "flex",
          alignItems: "center",
        }}
      >
        {emptyLabel}
      </div>
    );
  }

  return (
    <article
      style={{
        border: "1px solid #e2e8f0",
        borderRadius: 10,
        padding: 10,
        background: step.error_flag ? "#fff1f2" : "white",
      }}
    >
      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 6 }}>
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
        {step.tool_name ? <span style={{ color: "#334155", fontSize: 12 }}>tool: {step.tool_name}</span> : null}
      </div>
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

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const left = params.get("left_run_id") ?? "";
    const right = params.get("right_run_id") ?? "";
    const modeParam = params.get("mode");

    if (left) setLeftRunId(left);
    if (right) setRightRunId(right);
    if (modeParam === "raw" || modeParam === "aligned") {
      setMode(modeParam);
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    fetchRuns({ limit: 500, offset: 0, sort_by: "started_at", sort_dir: "desc" })
      .then((res) => {
        if (!mounted) return;
        setRuns(res.items);
        const failRun = res.items.find((run) => run.outcome === "fail");
        if (failRun) {
          setLeftRunId((current) => current || failRun.run_id);
        }
        const successRun = res.items.find((run) => run.outcome === "success");
        if (successRun) {
          setRightRunId((current) => current || successRun.run_id);
        }
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
  }, [leftRunId, rightRunId, mode]);

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
  const firstDivergence = useMemo(
    () => data?.items.find((pair) => pair.status !== "match")?.pair_idx ?? null,
    [data],
  );
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

  return (
    <main className="container">
      <div style={{ marginBottom: 12 }}>
        <Link href="/" style={{ textDecoration: "underline", color: "#334155" }}>
          Back to Run Explorer
        </Link>
      </div>

      <section className="panel" style={{ padding: 16, marginBottom: 12 }}>
        <h1 className="section-title">Compare View</h1>
        <p className="subtle" style={{ marginTop: 0 }}>
          Side-by-side trajectory comparison with alignment mode that tolerates retries and highlights missing-step gaps.
        </p>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
            gap: 10,
            marginBottom: 10,
          }}
        >
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
              <option value="raw">raw (index-aligned)</option>
              <option value="aligned">aligned (retry/missing aware)</option>
            </select>
          </label>
        </div>

        {error ? <p style={{ color: "#b91c1c", margin: 0 }}>{error}</p> : null}

        {data ? (
          <div className="kpi-row" style={{ marginBottom: 8 }}>
            <span>
              pairs: <strong>{data.total_pairs}</strong>
            </span>
            <span>
              match: <strong>{data.stats.match}</strong>
            </span>
            <span>
              mismatch: <strong>{data.stats.mismatch}</strong>
            </span>
            <span>
              left_only: <strong>{data.stats.left_only}</strong>
            </span>
            <span>
              right_only: <strong>{data.stats.right_only}</strong>
            </span>
            <span>
              first divergence: <strong>{firstDivergence ?? "-"}</strong>
            </span>
          </div>
        ) : null}
        {data ? (
          <div className="subtle" style={{ display: "grid", gap: 4 }}>
            <div>
              shared labels: <strong>{sharedLabels.length}</strong>
            </div>
            <div>
              left-only labels: <strong>{leftOnlyLabels.length}</strong> | right-only labels: <strong>{rightOnlyLabels.length}</strong>
            </div>
          </div>
        ) : null}
      </section>

      {loading ? (
        <section className="panel" style={{ padding: 16 }}>
          Loading comparison...
        </section>
      ) : null}

      {data ? (
        <section className="panel" style={{ padding: 12 }}>
          <div className="compare-grid-header">
            <div>
              <strong>Left:</strong>{" "}
              <Link href={`/runs/${data.left_run.run_id}`} style={{ textDecoration: "underline" }}>
                {data.left_run.run_id}
              </Link>{" "}
              ({data.left_run.outcome}, steps {data.left_run.num_steps})
              {leftLabelNames.length ? (
                <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 4 }}>
                  {leftLabelNames.slice(0, 6).map((label, idx) => (
                    <span
                      key={`left-label-${idx}`}
                      style={{
                        border: "1px solid #cbd5e1",
                        borderRadius: 999,
                        padding: "1px 8px",
                        fontSize: 12,
                        background: "#fef9c3",
                      }}
                    >
                      {label}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
            <div>
              <strong>Right:</strong>{" "}
              <Link href={`/runs/${data.right_run.run_id}`} style={{ textDecoration: "underline" }}>
                {data.right_run.run_id}
              </Link>{" "}
              ({data.right_run.outcome}, steps {data.right_run.num_steps})
              {rightLabelNames.length ? (
                <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 4 }}>
                  {rightLabelNames.slice(0, 6).map((label, idx) => (
                    <span
                      key={`right-label-${idx}`}
                      style={{
                        border: "1px solid #cbd5e1",
                        borderRadius: 999,
                        padding: "1px 8px",
                        fontSize: 12,
                        background: "#e0f2fe",
                      }}
                    >
                      {label}
                    </span>
                  ))}
                </div>
              ) : null}
            </div>
          </div>

          <div style={{ marginBottom: 10, border: "1px solid #e2e8f0", borderRadius: 10, padding: 10, background: "#f8fafc" }}>
            <div style={{ fontSize: 13, color: "#334155", marginBottom: 6 }}>
              <strong>Label overlap snapshot</strong>
            </div>
            <div style={{ display: "grid", gap: 4, fontSize: 13, color: "#475569" }}>
              <div>shared: {sharedLabels.length ? sharedLabels.join(", ") : "-"}</div>
              <div>left-only: {leftOnlyLabels.length ? leftOnlyLabels.join(", ") : "-"}</div>
              <div>right-only: {rightOnlyLabels.length ? rightOnlyLabels.join(", ") : "-"}</div>
            </div>
          </div>

          <div style={{ maxHeight: "72vh", overflow: "auto", paddingRight: 4 }}>
            {data.items.map((pair) => {
              const style = statusStyle(pair.status);
              return (
                <article
                  key={pair.pair_idx}
                  style={{
                    border: `1px solid ${style.border}`,
                    borderRadius: 12,
                    marginBottom: 10,
                    background: style.background,
                    padding: 10,
                  }}
                >
                  <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 8, flexWrap: "wrap" }}>
                    <strong style={{ color: "#0f172a" }}>Pair {pair.pair_idx}</strong>
                    <span
                      style={{
                        border: `1px solid ${style.border}`,
                        borderRadius: 999,
                        padding: "1px 10px",
                        fontSize: 12,
                        color: style.color,
                        background: "white",
                        fontWeight: 600,
                      }}
                    >
                      {pair.status}
                    </span>
                    {pair.note ? <span style={{ color: style.color, fontSize: 13 }}>{formatNote(pair.note)}</span> : null}
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
      ) : null}
    </main>
  );
}
