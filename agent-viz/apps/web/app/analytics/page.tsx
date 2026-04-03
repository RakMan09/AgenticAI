"use client";

import { useEffect, useState } from "react";

import {
  fetchAnalyticsFailureLabels,
  fetchAnalyticsGaps,
  fetchAnalyticsOverview,
  fetchAnalyticsToolUsage,
} from "../../lib/api";
import type {
  AnalyticsGapsResponse,
  AnalyticsLabelsResponse,
  AnalyticsOverviewResponse,
  AnalyticsToolUsageResponse,
} from "../../lib/types";

export default function AnalyticsPage() {
  const [overview, setOverview] = useState<AnalyticsOverviewResponse | null>(null);
  const [labels, setLabels] = useState<AnalyticsLabelsResponse["items"]>([]);
  const [tools, setTools] = useState<AnalyticsToolUsageResponse["items"]>([]);
  const [gaps, setGaps] = useState<AnalyticsGapsResponse["items"]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [overviewData, labelsData, toolsData, gapsData] = await Promise.all([
        fetchAnalyticsOverview(),
        fetchAnalyticsFailureLabels(30),
        fetchAnalyticsToolUsage(30),
        fetchAnalyticsGaps(30),
      ]);
      setOverview(overviewData);
      setLabels(labelsData.items);
      setTools(toolsData.items);
      setGaps(gapsData.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load analytics");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  return (
    <main className="container">
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
        <h1 className="section-title" style={{ marginBottom: 8 }}>Fleet Analytics</h1>
        <button
          onClick={() => void load()}
          style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "6px 10px", background: "white" }}
        >
          Refresh
        </button>
      </div>
      {loading ? <p>Loading analytics...</p> : null}
      {error ? <p style={{ color: "#b91c1c" }}>{error}</p> : null}

      {!loading && !error && overview ? (
        <>
          <section className="panel" style={{ padding: 14, marginBottom: 12 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Overview</h2>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(170px,1fr))", gap: 10 }}>
              <div>
                <strong>Total runs</strong>
                <div>{overview.total_runs}</div>
              </div>
              <div>
                <strong>Total steps</strong>
                <div>{overview.total_steps}</div>
              </div>
              <div>
                <strong>Retry prevalence</strong>
                <div>{overview.retry_prevalence}%</div>
              </div>
              <div>
                <strong>Loop prevalence</strong>
                <div>{overview.loop_prevalence}%</div>
              </div>
              <div>
                <strong>Timeout prevalence</strong>
                <div>{overview.timeout_prevalence}%</div>
              </div>
            </div>

            <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 10 }}>
              <div>
                <strong>Outcome counts</strong>
                <div style={{ marginTop: 6 }}>
                  {Object.entries(overview.outcome_counts).map(([k, v]) => (
                    <div key={k} style={{ display: "flex", justifyContent: "space-between" }}>
                      <span>{k}</span>
                      <span>{v}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div>
                <strong>First failure step histogram</strong>
                <div style={{ marginTop: 6 }}>
                  {overview.first_failure_step_histogram.map((row) => (
                    <div key={row.bucket} style={{ display: "flex", justifyContent: "space-between" }}>
                      <span>{row.bucket}</span>
                      <span>{row.run_count}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="panel" style={{ padding: 14, marginBottom: 12 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Failure Labels</h2>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ textAlign: "left", borderBottom: "1px solid #e2e8f0" }}>
                  <th style={{ padding: "6px 4px" }}>Label</th>
                  <th style={{ padding: "6px 4px" }}>Type</th>
                  <th style={{ padding: "6px 4px" }}>Runs</th>
                  <th style={{ padding: "6px 4px" }}>Annotations</th>
                </tr>
              </thead>
              <tbody>
                {labels.map((row) => (
                  <tr key={`${row.label}-${row.label_type}`} style={{ borderBottom: "1px solid #f1f5f9" }}>
                    <td style={{ padding: "6px 4px" }}>{row.label}</td>
                    <td style={{ padding: "6px 4px" }}>{row.label_type}</td>
                    <td style={{ padding: "6px 4px" }}>{row.run_count}</td>
                    <td style={{ padding: "6px 4px" }}>{row.annotation_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="panel" style={{ padding: 14, marginBottom: 12 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Tool Usage</h2>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ textAlign: "left", borderBottom: "1px solid #e2e8f0" }}>
                  <th style={{ padding: "6px 4px" }}>Tool</th>
                  <th style={{ padding: "6px 4px" }}>Calls</th>
                  <th style={{ padding: "6px 4px" }}>Errors</th>
                  <th style={{ padding: "6px 4px" }}>Avg latency (ms)</th>
                  <th style={{ padding: "6px 4px" }}>Success calls</th>
                  <th style={{ padding: "6px 4px" }}>Fail calls</th>
                </tr>
              </thead>
              <tbody>
                {tools.map((row) => (
                  <tr key={row.tool_name} style={{ borderBottom: "1px solid #f1f5f9" }}>
                    <td style={{ padding: "6px 4px" }}>{row.tool_name}</td>
                    <td style={{ padding: "6px 4px" }}>{row.call_count}</td>
                    <td style={{ padding: "6px 4px" }}>{row.error_count}</td>
                    <td style={{ padding: "6px 4px" }}>{row.avg_latency_ms ?? "-"}</td>
                    <td style={{ padding: "6px 4px" }}>{row.success_calls}</td>
                    <td style={{ padding: "6px 4px" }}>{row.fail_calls}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="panel" style={{ padding: 14 }}>
            <h2 style={{ marginTop: 0, fontSize: 18 }}>Gap Patterns</h2>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr style={{ textAlign: "left", borderBottom: "1px solid #e2e8f0" }}>
                  <th style={{ padding: "6px 4px" }}>Gap label</th>
                  <th style={{ padding: "6px 4px" }}>Runs</th>
                  <th style={{ padding: "6px 4px" }}>Annotations</th>
                  <th style={{ padding: "6px 4px" }}>Fail runs</th>
                  <th style={{ padding: "6px 4px" }}>Success runs</th>
                </tr>
              </thead>
              <tbody>
                {gaps.map((row) => (
                  <tr key={row.label} style={{ borderBottom: "1px solid #f1f5f9" }}>
                    <td style={{ padding: "6px 4px" }}>{row.label}</td>
                    <td style={{ padding: "6px 4px" }}>{row.run_count}</td>
                    <td style={{ padding: "6px 4px" }}>{row.annotation_count}</td>
                    <td style={{ padding: "6px 4px" }}>{row.fail_runs}</td>
                    <td style={{ padding: "6px 4px" }}>{row.success_runs}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      ) : null}
    </main>
  );
}
