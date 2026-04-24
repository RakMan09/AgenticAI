"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  buildEmergenceMatrix,
  buildRunScatter,
  clampPercent,
  clusterRunsByFingerprint,
  normalizeLabel,
  summarizeFailureCategories,
} from "../../lib/analysis";
import {
  fetchAnalyticsFailureLabels,
  fetchAnalyticsGaps,
  fetchAnalyticsOverview,
  fetchAnalyticsToolUsage,
  fetchRuns,
} from "../../lib/api";
import type {
  AnalyticsGapsResponse,
  AnalyticsLabelsResponse,
  AnalyticsOverviewResponse,
  AnalyticsToolUsageResponse,
  RunRow,
} from "../../lib/types";

type CohortFilter =
  | { kind: "label"; value: string }
  | { kind: "gap"; value: string }
  | { kind: "tool"; value: string }
  | { kind: "outcome"; value: string }
  | null;

export default function AnalyticsPage() {
  const [overview, setOverview] = useState<AnalyticsOverviewResponse | null>(null);
  const [labels, setLabels] = useState<AnalyticsLabelsResponse["items"]>([]);
  const [tools, setTools] = useState<AnalyticsToolUsageResponse["items"]>([]);
  const [gaps, setGaps] = useState<AnalyticsGapsResponse["items"]>([]);
  const [allRuns, setAllRuns] = useState<RunRow[]>([]);
  const [cohortRuns, setCohortRuns] = useState<RunRow[]>([]);
  const [cohortFilter, setCohortFilter] = useState<CohortFilter>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [overviewData, labelsData, toolsData, gapsData, runsData] = await Promise.all([
        fetchAnalyticsOverview(),
        fetchAnalyticsFailureLabels(30),
        fetchAnalyticsToolUsage(30),
        fetchAnalyticsGaps(30),
        fetchRuns({ limit: 500, offset: 0, sort_by: "started_at", sort_dir: "desc" }),
      ]);
      setOverview(overviewData);
      setLabels(labelsData.items);
      setTools(toolsData.items);
      setGaps(gapsData.items);
      setAllRuns(runsData.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load analytics");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  useEffect(() => {
    if (!cohortFilter) {
      setCohortRuns([]);
      return;
    }
    const params: Parameters<typeof fetchRuns>[0] = { limit: 50, offset: 0, sort_by: "started_at", sort_dir: "desc" };
    if (cohortFilter.kind === "label" || cohortFilter.kind === "gap") params.label = cohortFilter.value;
    if (cohortFilter.kind === "tool") params.tool = cohortFilter.value;
    if (cohortFilter.kind === "outcome") params.outcome = cohortFilter.value;
    fetchRuns(params)
      .then((res) => setCohortRuns(res.items))
      .catch(() => setCohortRuns([]));
  }, [cohortFilter]);

  const cohortFamilies = useMemo(() => clusterRunsByFingerprint(cohortRuns).slice(0, 5), [cohortRuns]);
  const topLabelMax = useMemo(() => Math.max(...labels.map((item) => item.run_count), 1), [labels]);
  const topToolMax = useMemo(() => Math.max(...tools.map((item) => item.call_count), 1), [tools]);
  const topGapMax = useMemo(() => Math.max(...gaps.map((item) => item.run_count), 1), [gaps]);
  const categorySummary = useMemo(() => summarizeFailureCategories(allRuns, 8), [allRuns]);
  const categoryMax = useMemo(() => Math.max(...categorySummary.map((item) => item.count), 1), [categorySummary]);
  const emergenceMatrix = useMemo(() => buildEmergenceMatrix(allRuns), [allRuns]);
  const emergenceMax = useMemo(() => Math.max(...emergenceMatrix.map((item) => item.count), 1), [emergenceMatrix]);
  const scatterPoints = useMemo(() => buildRunScatter(allRuns), [allRuns]);

  const graphOutcomeTone = (outcome: string) =>
    outcome === "fail" ? "#dc2626" : outcome === "success" ? "#0f766e" : "#64748b";

  const outcomeSummary = useMemo(
    () => [
      { label: "success", count: overview?.outcome_counts.success ?? 0, tone: graphOutcomeTone("success") },
      { label: "fail", count: overview?.outcome_counts.fail ?? 0, tone: graphOutcomeTone("fail") },
      { label: "unknown", count: overview?.outcome_counts.unknown ?? 0, tone: graphOutcomeTone("unknown") },
    ],
    [overview],
  );
  const totalVisibleRuns = Math.max(outcomeSummary.reduce((sum, item) => sum + item.count, 0), 1);

  const toggleCohortFilter = (nextFilter: Exclude<CohortFilter, null>) => {
    setCohortFilter((current) => {
      if (current && current.kind === nextFilter.kind && current.value === nextFilter.value) {
        return null;
      }
      return nextFilter;
    });
  };

  return (
    <main className="container">
      <section className="hero-panel" style={{ marginBottom: 12 }}>
        <div>
          <p className="eyebrow">Fleet View</p>
          <h1 className="hero-title" style={{ fontSize: 30 }}>Fleet Analytics</h1>
          <p className="hero-copy">
            Move from descriptive counts to exploratory analysis. Click a label, tool, gap, or outcome to build a cohort and inspect representative runs.
          </p>
        </div>
        <div className="hero-actions">
          <button onClick={() => void load()}>Refresh</button>
          <Link href="/" className="kpi-chip">Back to runs</Link>
        </div>
      </section>

      {loading ? <p>Loading analytics...</p> : null}
      {error ? <p style={{ color: "#b91c1c" }}>{error}</p> : null}

      {!loading && !error && overview ? (
        <>
          <section className="panel" style={{ padding: 14, marginBottom: 12 }}>
            <div className="section-caption">
              <div>
                <span className="section-kicker">Graph Summary</span>
                <h2 className="section-title">All Runs At A Glance</h2>
              </div>
              <span className="subtle">Each graph summarizes the entire dataset, not just the active cohort.</span>
            </div>
            <div className="analytics-grid">
              <section className="graph-panel">
                <div className="graph-header">
                  <div>
                    <strong>Outcome Mix</strong>
                    <div className="subtle">Distribution across all ingested runs</div>
                  </div>
                </div>
                <div className="stacked-band">
                  {outcomeSummary.map((item) => (
                    <button
                      key={item.label}
                      className={`stacked-segment ${cohortFilter?.kind === "outcome" && cohortFilter.value === item.label ? "button-selected" : ""}`}
                      style={{ width: `${clampPercent(item.count, totalVisibleRuns)}%`, background: item.tone }}
                      onClick={() => toggleCohortFilter({ kind: "outcome", value: item.label })}
                      title={`${item.label}: ${item.count}`}
                    >
                      <span>{item.label}</span>
                      <strong>{item.count}</strong>
                    </button>
                  ))}
                </div>
                <div className="graph-legend">
                  <span className="legend-chip"><span className="legend-dot" style={{ background: "#0f766e" }} />success</span>
                  <span className="legend-chip"><span className="legend-dot" style={{ background: "#dc2626" }} />fail</span>
                  <span className="legend-chip"><span className="legend-dot" style={{ background: "#64748b" }} />unknown</span>
                </div>
              </section>

              <section className="graph-panel">
                <div className="graph-header">
                  <div>
                    <strong>Failure / Run-Type Mix</strong>
                    <div className="subtle">Most common categories across all runs</div>
                  </div>
                </div>
                <div style={{ display: "grid", gap: 9 }}>
                  {categorySummary.map((item) => (
                    <div key={`${item.outcome}-${item.label}`} className="graph-row">
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <strong>{normalizeLabel(item.label)}</strong>
                        <span className="subtle">{item.count}</span>
                      </div>
                      <div className="micro-bar-track" style={{ marginTop: 6 }}>
                        <div
                          className="micro-bar-fill"
                          style={{ width: `${clampPercent(item.count, categoryMax)}%`, background: graphOutcomeTone(item.outcome) }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </section>
            </div>
          </section>

          <div className="analytics-grid" style={{ marginBottom: 12 }}>
            <section className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Overview</h2>
              <div className="kpi-row">
                <span className="kpi-chip">total runs: <strong>{overview.total_runs}</strong></span>
                <span className="kpi-chip">total steps: <strong>{overview.total_steps}</strong></span>
                <button className={`kpi-chip ${cohortFilter?.kind === "outcome" && cohortFilter.value === "fail" ? "button-selected" : ""}`} onClick={() => toggleCohortFilter({ kind: "outcome", value: "fail" })}>
                  fail cohort: <strong>{overview.outcome_counts.fail ?? 0}</strong>
                </button>
                <button className={`kpi-chip ${cohortFilter?.kind === "outcome" && cohortFilter.value === "success" ? "button-selected" : ""}`} onClick={() => toggleCohortFilter({ kind: "outcome", value: "success" })}>
                  success cohort: <strong>{overview.outcome_counts.success ?? 0}</strong>
                </button>
              </div>
              <div className="analytics-grid" style={{ gridTemplateColumns: "repeat(3, minmax(0, 1fr))" }}>
                <div className="metric-card">
                  <strong>Retry prevalence</strong>
                  <div className="metric-value">{overview.retry_prevalence}%</div>
                </div>
                <div className="metric-card">
                  <strong>Loop prevalence</strong>
                  <div className="metric-value">{overview.loop_prevalence}%</div>
                </div>
                <div className="metric-card">
                  <strong>Timeout prevalence</strong>
                  <div className="metric-value">{overview.timeout_prevalence}%</div>
                </div>
              </div>
            </section>

            <section className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Failure Timing</h2>
              <div style={{ display: "grid", gap: 10 }}>
                {overview.first_failure_step_histogram.map((row) => {
                  const maxCount = Math.max(...overview.first_failure_step_histogram.map((item) => item.run_count), 1);
                  return (
                    <div key={row.bucket}>
                      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                        <strong>{row.bucket}</strong>
                        <span className="subtle">{row.run_count} runs</span>
                      </div>
                      <div className="timeline-strip">
                        <div className="timeline-fill" style={{ width: `${clampPercent(row.run_count, maxCount)}%` }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </section>
          </div>

          <div className="analytics-grid" style={{ marginBottom: 12 }}>
            <section className="graph-panel">
              <div className="graph-header">
                <div>
                  <span className="section-kicker">Emergence Matrix</span>
                  <h2 className="section-title">Where Issues First Appear</h2>
                </div>
                <span className="subtle">Darker cells indicate denser clusters of runs.</span>
              </div>
              <div className="heatmap">
                <div className="heatmap-corner" />
                {["none", "0-1", "2-3", "4-6", "7+"].map((bucket) => (
                  <div key={bucket} className="heatmap-axis-label">{bucket}</div>
                ))}
                {["success", "fail", "unknown"].map((outcome) => (
                  <div key={outcome} style={{ display: "contents" }}>
                    <div key={`${outcome}-label`} className="heatmap-axis-label heatmap-axis-label-row">{outcome}</div>
                    {emergenceMatrix
                      .filter((cell) => cell.outcome === outcome)
                      .map((cell) => (
                        <button
                          key={`${cell.outcome}-${cell.bucket}`}
                          className={`heatmap-cell ${cohortFilter?.kind === "outcome" && cohortFilter.value === outcome ? "button-selected" : ""}`}
                          style={{
                            background: graphOutcomeTone(outcome),
                            opacity: 0.18 + clampPercent(cell.count, emergenceMax) / 120,
                          }}
                          onClick={() => toggleCohortFilter({ kind: "outcome", value: outcome })}
                          title={`${outcome} / ${cell.bucket}: ${cell.count}`}
                        >
                          <strong>{cell.count}</strong>
                        </button>
                      ))}
                  </div>
                ))}
              </div>
            </section>

            <section className="graph-panel">
              <div className="graph-header">
                <div>
                  <span className="section-kicker">Run Scatterplot</span>
                  <h2 className="section-title">Run Complexity vs Error Load</h2>
                </div>
                <span className="subtle">X = steps, Y = error steps, size = tool breadth.</span>
              </div>
              <svg viewBox="0 0 420 240" className="scatter-plot" aria-label="Run complexity scatter plot">
                <line x1="44" y1="196" x2="392" y2="196" className="axis-line" />
                <line x1="44" y1="24" x2="44" y2="196" className="axis-line" />
                {[0, 25, 50, 75, 100].map((tick) => (
                  <g key={tick}>
                    <line x1="44" y1={24 + ((100 - tick) / 100) * 172} x2="392" y2={24 + ((100 - tick) / 100) * 172} className="grid-line" />
                    <line x1={44 + (tick / 100) * 348} y1="24" x2={44 + (tick / 100) * 348} y2="196" className="grid-line" />
                  </g>
                ))}
                {scatterPoints.map((point) => (
                  <a key={point.runId} href={`/runs/${encodeURIComponent(point.runId)}`}>
                    <circle
                      cx={44 + (point.x / 100) * 348}
                      cy={24 + (point.y / 100) * 172}
                      r={point.radius}
                      fill={graphOutcomeTone(point.outcome)}
                      fillOpacity="0.78"
                      stroke="#ffffff"
                      strokeWidth="2"
                    >
                      <title>{`${point.runId}: ${point.steps} steps, ${point.errors} errors, ${point.label}`}</title>
                    </circle>
                  </a>
                ))}
                <text x="218" y="228" textAnchor="middle" className="axis-label">More steps</text>
                <text x="18" y="110" textAnchor="middle" className="axis-label" transform="rotate(-90 18 110)">More errors</text>
              </svg>
              <div className="graph-legend">
                <span className="legend-chip"><span className="legend-dot" style={{ background: "#0f766e" }} />success</span>
                <span className="legend-chip"><span className="legend-dot" style={{ background: "#dc2626" }} />fail</span>
                <span className="legend-chip"><span className="legend-dot" style={{ background: "#64748b" }} />unknown</span>
              </div>
            </section>
          </div>

          <div className="analytics-grid" style={{ marginBottom: 12 }}>
            <section className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Failure Labels</h2>
              <div style={{ display: "grid", gap: 8 }}>
                {labels.map((row) => (
                  <button
                    key={`${row.label}-${row.label_type}`}
                    className={`metric-card metric-button ${cohortFilter?.kind === "label" && cohortFilter.value === row.label ? "button-selected" : ""}`}
                    onClick={() => toggleCohortFilter({ kind: "label", value: row.label })}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{normalizeLabel(row.label)}</strong>
                      <span className="subtle">{row.label_type}</span>
                    </div>
                    <div className="subtle">runs: {row.run_count} | annotations: {row.annotation_count}</div>
                    <div className="micro-bar-track" style={{ marginTop: 8 }}>
                      <div className="micro-bar-fill" style={{ width: `${clampPercent(row.run_count, topLabelMax)}%` }} />
                    </div>
                  </button>
                ))}
              </div>
            </section>

            <section className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Tool Usage</h2>
              <div style={{ display: "grid", gap: 8 }}>
                {tools.map((row) => (
                  <button
                    key={row.tool_name}
                    className={`metric-card metric-button ${cohortFilter?.kind === "tool" && cohortFilter.value === row.tool_name ? "button-selected" : ""}`}
                    onClick={() => toggleCohortFilter({ kind: "tool", value: row.tool_name })}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{row.tool_name}</strong>
                      <span className="subtle">calls: {row.call_count}</span>
                    </div>
                    <div className="subtle">
                      errors: {row.error_count} | avg latency: {row.avg_latency_ms ?? "-"} | success/fail: {row.success_calls}/{row.fail_calls}
                    </div>
                    <div className="micro-bar-track" style={{ marginTop: 8 }}>
                      <div className="micro-bar-fill" style={{ width: `${clampPercent(row.call_count, topToolMax)}%`, background: "#0f766e" }} />
                    </div>
                  </button>
                ))}
              </div>
            </section>

            <section className="panel" style={{ padding: 14 }}>
              <h2 style={{ marginTop: 0, fontSize: 18 }}>Gap Patterns</h2>
              <div style={{ display: "grid", gap: 8 }}>
                {gaps.map((row) => (
                  <button
                    key={row.label}
                    className={`metric-card metric-button ${cohortFilter?.kind === "gap" && cohortFilter.value === row.label ? "button-selected" : ""}`}
                    onClick={() => toggleCohortFilter({ kind: "gap", value: row.label })}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong>{normalizeLabel(row.label)}</strong>
                      <span className="subtle">{row.run_count} runs</span>
                    </div>
                    <div className="subtle">fail/success: {row.fail_runs}/{row.success_runs}</div>
                    <div className="micro-bar-track" style={{ marginTop: 8 }}>
                      <div className="micro-bar-fill" style={{ width: `${clampPercent(row.run_count, topGapMax)}%`, background: "#dc2626" }} />
                    </div>
                  </button>
                ))}
              </div>
            </section>
          </div>

          <section className="panel" style={{ padding: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center", marginBottom: 10 }}>
              <h2 style={{ margin: 0, fontSize: 18 }}>Cohort Explorer</h2>
              {cohortFilter ? (
                <button onClick={() => setCohortFilter(null)}>Clear cohort</button>
              ) : null}
            </div>
            {cohortFilter ? (
              <>
                <p className="subtle" style={{ marginTop: 0 }}>
                  Active cohort: <strong>{cohortFilter.kind}</strong> = <strong>{normalizeLabel(cohortFilter.value)}</strong>
                </p>
                <div className="analysis-grid">
                  <div>
                    <strong>Representative families</strong>
                    <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                      {cohortFamilies.map((family) => (
                        <a key={family.key} href={`/runs/${encodeURIComponent(family.sampleRunId)}`} className="family-card">
                          <strong>{family.failureCategory}</strong>
                          <div className="subtle">runs: {family.count} | avg steps: {family.avgSteps}</div>
                        </a>
                      ))}
                    </div>
                  </div>
                  <div>
                    <strong>Runs in cohort</strong>
                    <div style={{ display: "grid", gap: 8, marginTop: 8 }}>
                      {cohortRuns.map((run) => (
                        <a key={run.run_id} href={`/runs/${encodeURIComponent(run.run_id)}`} className="moment-card">
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                            <strong>{run.run_id.slice(0, 16)}</strong>
                            <span className="pill" style={{ background: run.outcome === "fail" ? "#fee2e2" : "#dcfce7" }}>{run.outcome}</span>
                          </div>
                          <div className="subtle">{run.failure_category ?? run.run_type ?? "untyped"} | steps {run.num_steps}</div>
                        </a>
                      ))}
                    </div>
                  </div>
                </div>
                <div className="header-actions" style={{ marginTop: 12 }}>
                  <Link
                    href={
                      cohortFilter.kind === "tool"
                        ? `/?tool=${encodeURIComponent(cohortFilter.value)}`
                        : cohortFilter.kind === "outcome"
                          ? `/?outcome=${encodeURIComponent(cohortFilter.value)}`
                          : `/?label=${encodeURIComponent(cohortFilter.value)}`
                    }
                    className="kpi-chip"
                  >
                    Open this cohort in run explorer
                  </Link>
                </div>
              </>
            ) : (
              <p className="subtle" style={{ marginBottom: 0 }}>Click a metric above to build a cohort and drill into representative runs.</p>
            )}
          </section>
        </>
      ) : null}
    </main>
  );
}
