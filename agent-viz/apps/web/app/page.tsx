"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";

import { clampPercent, clusterRunsByFingerprint } from "../lib/analysis";
import { fetchRuns } from "../lib/api";
import type { RunRow } from "../lib/types";

const columnHelper = createColumnHelper<RunRow>();

function parseOptionalNumber(value: string): number | undefined {
  if (!value.trim()) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function copyText(value: string) {
  if (typeof window === "undefined") return;
  void navigator.clipboard.writeText(value);
}

function setCompareSlot(
  runId: string,
  slot: "left" | "right",
  currentLeft: string,
  currentRight: string,
  setLeft: (value: string) => void,
  setRight: (value: string) => void,
) {
  if (slot === "left") {
    setLeft(runId);
    if (currentRight === runId) setRight("");
    return;
  }
  setRight(runId);
  if (currentLeft === runId) setLeft("");
}

export default function HomePage() {
  const [outcome, setOutcome] = useState("all");
  const [q, setQ] = useState("");
  const [tool, setTool] = useState("");
  const [source, setSource] = useState("all");
  const [stepType, setStepType] = useState("all");
  const [hasErrors, setHasErrors] = useState("all");
  const [label, setLabel] = useState("");
  const [startedAfter, setStartedAfter] = useState("");
  const [startedBefore, setStartedBefore] = useState("");
  const [sortBy, setSortBy] = useState("started_at");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [minSteps, setMinSteps] = useState("");
  const [maxSteps, setMaxSteps] = useState("");
  const [minErrorSteps, setMinErrorSteps] = useState("");
  const [maxErrorSteps, setMaxErrorSteps] = useState("");
  const [compareLeft, setCompareLeft] = useState("");
  const [compareRight, setCompareRight] = useState("");
  const [shareNotice, setShareNotice] = useState("");

  const [rows, setRows] = useState<RunRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const columns = useMemo(
    () => [
      columnHelper.accessor("run_id", {
        header: "Run ID",
        cell: (info) => info.getValue().slice(0, 14),
      }),
      columnHelper.accessor((row) => row.task_id ?? row.scenario ?? "-", {
        id: "task_or_scenario",
        header: "Task/Scenario",
      }),
      columnHelper.accessor("outcome", {
        header: "Outcome",
      }),
      columnHelper.accessor("source", {
        header: "Source",
        cell: (info) => info.getValue() ?? "-",
      }),
      columnHelper.accessor((row) => row.failure_category ?? row.run_type ?? "-", {
        id: "run_type_or_failure",
        header: "Type/Failure",
      }),
      columnHelper.accessor("num_steps", {
        header: "Steps",
      }),
      columnHelper.accessor("error_steps", {
        header: "Errors",
      }),
      columnHelper.accessor("distinct_tools", {
        header: "Tools",
      }),
      columnHelper.display({
        id: "actions",
        header: "Actions",
        cell: (info) => {
          const runId = info.row.original.run_id;
          const selectedLeft = compareLeft === runId;
          const selectedRight = compareRight === runId;
          return (
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              <button
                className={selectedLeft ? "button-selected" : undefined}
                onClick={() => setCompareSlot(runId, "left", compareLeft, compareRight, setCompareLeft, setCompareRight)}
              >
                {selectedLeft ? "Left selected" : "Set left"}
              </button>
              <button
                className={selectedRight ? "button-selected" : undefined}
                onClick={() => setCompareSlot(runId, "right", compareLeft, compareRight, setCompareLeft, setCompareRight)}
              >
                {selectedRight ? "Right selected" : "Set right"}
              </button>
              <a href={`/runs/${encodeURIComponent(runId)}`} className="kpi-chip">
                Inspect
              </a>
            </div>
          );
        },
      }),
    ],
    [compareLeft, compareRight],
  );

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    setOutcome(params.get("outcome") ?? "all");
    setQ(params.get("q") ?? "");
    setTool(params.get("tool") ?? "");
    setSource(params.get("source") ?? "all");
    setStepType(params.get("step_type") ?? "all");
    setHasErrors(params.get("has_errors") ?? "all");
    setLabel(params.get("label") ?? "");
    setStartedAfter(params.get("started_after") ?? "");
    setStartedBefore(params.get("started_before") ?? "");
    setSortBy(params.get("sort_by") ?? "started_at");
    setSortDir((params.get("sort_dir") as "asc" | "desc") ?? "desc");
    setMinSteps(params.get("min_steps") ?? "");
    setMaxSteps(params.get("max_steps") ?? "");
    setMinErrorSteps(params.get("min_error_steps") ?? "");
    setMaxErrorSteps(params.get("max_error_steps") ?? "");
    setCompareLeft(params.get("compare_left") ?? "");
    setCompareRight(params.get("compare_right") ?? "");
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams();
    const entries: Record<string, string> = {
      outcome,
      q,
      tool,
      source,
      step_type: stepType,
      has_errors: hasErrors,
      label,
      started_after: startedAfter,
      started_before: startedBefore,
      sort_by: sortBy,
      sort_dir: sortDir,
      min_steps: minSteps,
      max_steps: maxSteps,
      min_error_steps: minErrorSteps,
      max_error_steps: maxErrorSteps,
      compare_left: compareLeft,
      compare_right: compareRight,
    };
    for (const [key, value] of Object.entries(entries)) {
      if (!value || value === "all") continue;
      params.set(key, value);
    }
    const nextUrl = params.toString() ? `/?${params.toString()}` : "/";
    window.history.replaceState(null, "", nextUrl);
  }, [
    compareLeft,
    compareRight,
    hasErrors,
    label,
    maxErrorSteps,
    maxSteps,
    minErrorSteps,
    minSteps,
    outcome,
    q,
    sortBy,
    sortDir,
    source,
    startedAfter,
    startedBefore,
    stepType,
    tool,
  ]);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    fetchRuns({
      outcome,
      q,
      tool,
      source,
      step_type: stepType,
      has_errors: hasErrors,
      label,
      started_after: startedAfter || undefined,
      started_before: startedBefore || undefined,
      sort_by: sortBy,
      sort_dir: sortDir,
      min_steps: parseOptionalNumber(minSteps),
      max_steps: parseOptionalNumber(maxSteps),
      min_error_steps: parseOptionalNumber(minErrorSteps),
      max_error_steps: parseOptionalNumber(maxErrorSteps),
      limit: 300,
      offset: 0,
    })
      .then((data) => {
        if (!mounted) return;
        setRows(data.items);
        setTotal(data.total);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(err instanceof Error ? err.message : "Unknown error");
      })
      .finally(() => {
        if (mounted) setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, [
    hasErrors,
    label,
    maxErrorSteps,
    maxSteps,
    minErrorSteps,
    minSteps,
    outcome,
    q,
    source,
    sortBy,
    sortDir,
    startedAfter,
    startedBefore,
    stepType,
    tool,
  ]);

  const table = useReactTable({
    data: rows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  const compareHref =
    compareLeft && compareRight
      ? `/compare?left_run_id=${encodeURIComponent(compareLeft)}&right_run_id=${encodeURIComponent(compareRight)}&mode=aligned`
      : null;

  const runSummary = useMemo(() => {
    let success = 0;
    let fail = 0;
    let unknown = 0;
    for (const row of rows) {
      if (row.outcome === "success") success += 1;
      else if (row.outcome === "fail") fail += 1;
      else unknown += 1;
    }
    return { success, fail, unknown };
  }, [rows]);

  const failureFamilies = useMemo(() => clusterRunsByFingerprint(rows).slice(0, 6), [rows]);

  const trajectoryMoments = useMemo(() => {
    const maxStepsValue = Math.max(...rows.map((row) => row.num_steps), 1);
    return rows
      .slice()
      .sort((left, right) => {
        const leftMoment = left.first_error_step ?? left.num_steps + 5;
        const rightMoment = right.first_error_step ?? right.num_steps + 5;
        return leftMoment - rightMoment;
      })
      .slice(0, 8)
      .map((row) => ({
        ...row,
        moment: row.first_error_step ?? row.num_steps,
        barWidth: clampPercent(row.num_steps, maxStepsValue),
      }));
  }, [rows]);

  const drilldownLinks = useMemo(
    () => [
      { label: "All failures with errors", href: "/?outcome=fail&has_errors=true" },
      { label: "Runs with missing verification", href: "/?label=missing_verification" },
      { label: "Retry-heavy failures", href: "/?outcome=fail&label=repeated_retry" },
      { label: "Tool misuse cohort", href: "/?label=tool_misuse" },
    ],
    [],
  );

  const clearFilters = () => {
    setOutcome("all");
    setQ("");
    setTool("");
    setSource("all");
    setStepType("all");
    setHasErrors("all");
    setLabel("");
    setStartedAfter("");
    setStartedBefore("");
    setSortBy("started_at");
    setSortDir("desc");
    setMinSteps("");
    setMaxSteps("");
    setMinErrorSteps("");
    setMaxErrorSteps("");
  };

  const clearCompareSelection = () => {
    setCompareLeft("");
    setCompareRight("");
  };

  const swapCompareSelection = () => {
    if (!compareLeft && !compareRight) return;
    setCompareLeft(compareRight);
    setCompareRight(compareLeft);
  };

  const outcomeChart = useMemo(() => {
    const totalCount = Math.max(rows.length, 1);
    return [
      { label: "success", value: runSummary.success, tone: "#16a34a", width: clampPercent(runSummary.success, totalCount) },
      { label: "fail", value: runSummary.fail, tone: "#dc2626", width: clampPercent(runSummary.fail, totalCount) },
      { label: "unknown", value: runSummary.unknown, tone: "#64748b", width: clampPercent(runSummary.unknown, totalCount) },
    ];
  }, [rows.length, runSummary.fail, runSummary.success, runSummary.unknown]);

  const shareCurrentView = () => {
    if (typeof window === "undefined") return;
    copyText(window.location.href);
    setShareNotice("Copied a shareable filtered run-explorer URL.");
    window.setTimeout(() => setShareNotice(""), 1800);
  };

  return (
    <main className="container">
      <section className="hero-panel">
        <div>
          <p className="eyebrow">Project 1</p>
          <h1 className="hero-title">Visual Analytics for Agent Trajectories & Failure Modes</h1>
          <p className="hero-copy">
            Explore trajectories as cohorts, not just rows. The dashboard now emphasizes failure families,
            emergence timing, and direct drill-down into runs that exemplify a pattern.
          </p>
          <div className="hero-stat-grid" style={{ marginTop: 18 }}>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Visible runs</span>
              <strong className="hero-stat-value">{rows.length}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Failure families</span>
              <strong className="hero-stat-value">{failureFamilies.length}</strong>
            </div>
            <div className="hero-stat-card">
              <span className="hero-stat-label">Earliest first error</span>
              <strong className="hero-stat-value">{trajectoryMoments[0]?.first_error_step ?? "none"}</strong>
            </div>
          </div>
        </div>
        <div className="hero-actions">
          <button className="btn-primary" onClick={shareCurrentView}>Share current view</button>
          <Link href="/analytics" className="kpi-chip">Open fleet analytics</Link>
          <Link href="/case-studies" className="kpi-chip">Open evidence bundles</Link>
        </div>
      </section>

      {shareNotice ? <p className="subtle" style={{ color: "#166534" }}>{shareNotice}</p> : null}

      <div className="kpi-row">
        <span className="kpi-chip">loaded: <strong>{rows.length}</strong> / matches: <strong>{total}</strong></span>
        <span className="kpi-chip">success: <strong>{runSummary.success}</strong></span>
        <span className="kpi-chip">fail: <strong>{runSummary.fail}</strong></span>
        <span className="kpi-chip">unknown: <strong>{runSummary.unknown}</strong></span>
      </div>

      <div className="dashboard-feature-grid" style={{ marginBottom: 16 }}>
        <section className="panel" style={{ padding: 16 }}>
          <div className="section-caption">
            <div>
              <span className="section-kicker">Cluster View</span>
              <h2 className="section-title">Failure Families</h2>
            </div>
            <span className="subtle">Grouped by outcome, category, and footprint</span>
          </div>
          <div style={{ display: "grid", gap: 10 }}>
            {failureFamilies.map((family) => (
              <a key={family.key} href={`/runs/${encodeURIComponent(family.sampleRunId)}`} className="family-card family-card-featured">
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong>{family.failureCategory}</strong>
                  <span className="pill" style={{ background: family.outcome === "fail" ? "#fee2e2" : "#dcfce7" }}>
                    {family.outcome}
                  </span>
                </div>
                <div className="subtle">runs: {family.count} | avg steps: {family.avgSteps}</div>
                <div className="subtle">sample run: {family.sampleRunId.slice(0, 18)}</div>
                <div className="micro-bar-track" style={{ marginTop: 8 }}>
                  <div className="micro-bar-fill" style={{ width: `${clampPercent(family.count, Math.max(failureFamilies[0]?.count ?? 1, 1))}%` }} />
                </div>
              </a>
            ))}
          </div>
        </section>

        <section className="panel" style={{ padding: 16 }}>
          <div className="section-caption">
            <div>
              <span className="section-kicker">Distribution View</span>
              <h2 className="section-title">Fleet Snapshot</h2>
            </div>
            <span className="subtle">Micro-graphs from the active cohort</span>
          </div>
          <div style={{ display: "grid", gap: 12, marginBottom: 12 }}>
            {outcomeChart.map((item) => (
              <div key={item.label} className="mini-chart-card">
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong>{item.label}</strong>
                  <span className="subtle">{item.value}</span>
                </div>
                <div className="micro-bar-track">
                  <div className="micro-bar-fill" style={{ width: `${item.width}%`, background: item.tone }} />
                </div>
              </div>
            ))}
          </div>
          <h3 style={{ marginTop: 0, marginBottom: 8, fontSize: 16 }}>Failure Emergence Snapshot</h3>
          <p className="subtle">Where failure first shows up relative to trajectory length.</p>
          <div style={{ display: "grid", gap: 10 }}>
            {trajectoryMoments.map((row) => (
              <a key={row.run_id} href={`/runs/${encodeURIComponent(row.run_id)}`} className="trajectory-row trajectory-row-featured">
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong>{row.run_id.slice(0, 16)}</strong>
                  <span className="subtle">
                    first issue: {row.first_error_step ?? "none"} / {row.num_steps}
                  </span>
                </div>
                <div className="timeline-strip">
                  <div className="timeline-fill" style={{ width: `${row.barWidth}%` }} />
                  {row.first_error_step !== null ? (
                    <div
                      className="timeline-marker"
                      style={{ left: `${clampPercent(row.first_error_step, row.num_steps)}%` }}
                    />
                  ) : null}
                </div>
                <div className="subtle">{row.failure_category ?? row.run_type ?? "untyped"}</div>
              </a>
            ))}
          </div>
        </section>
      </div>

      <section className="panel" style={{ padding: 16, marginBottom: 16 }}>
        <div className="section-caption">
          <div>
            <span className="section-kicker">Analysis Workspace</span>
            <h2 className="section-title">Run Explorer</h2>
          </div>
          <div className="header-actions">
            <span className="subtle">
              compare left: <strong>{compareLeft ? compareLeft.slice(0, 14) : "-"}</strong>
            </span>
            <span className="subtle">
              compare right: <strong>{compareRight ? compareRight.slice(0, 14) : "-"}</strong>
            </span>
          </div>
        </div>
        <div className="header-actions" style={{ marginBottom: 10 }}>
          {compareHref ? <a href={compareHref} className="btn-primary">Open Compare</a> : <span className="subtle">Pick both runs to compare</span>}
          <button disabled={!compareLeft && !compareRight} onClick={swapCompareSelection}>Swap</button>
          <button disabled={!compareLeft && !compareRight} onClick={clearCompareSelection}>Clear compare</button>
        </div>

        <div className="filter-grid">
          <label>
            Outcome
            <select value={outcome} onChange={(e) => setOutcome(e.target.value)} style={{ width: "100%" }}>
              <option value="all">all</option>
              <option value="success">success</option>
              <option value="fail">fail</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
          <label>
            Search
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="run_id / task / scenario" style={{ width: "100%" }} />
          </label>
          <label>
            Tool
            <input value={tool} onChange={(e) => setTool(e.target.value)} placeholder="tool name" style={{ width: "100%" }} />
          </label>
          <label>
            Source
            <select value={source} onChange={(e) => setSource(e.target.value)} style={{ width: "100%" }}>
              <option value="all">all</option>
              <option value="openclaw_real">openclaw_real</option>
              <option value="canonical_synthetic">canonical_synthetic</option>
              <option value="legacy">legacy</option>
            </select>
          </label>
          <label>
            Contains step type
            <select value={stepType} onChange={(e) => setStepType(e.target.value)} style={{ width: "100%" }}>
              <option value="all">all</option>
              <option value="thought">thought</option>
              <option value="action">action</option>
              <option value="observation">observation</option>
              <option value="tool_call">tool_call</option>
              <option value="unknown">unknown</option>
            </select>
          </label>
          <label>
            Has errors
            <select value={hasErrors} onChange={(e) => setHasErrors(e.target.value)} style={{ width: "100%" }}>
              <option value="all">all</option>
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          </label>
          <label>
            Label contains
            <input value={label} onChange={(e) => setLabel(e.target.value)} placeholder="annotation label" style={{ width: "100%" }} />
          </label>
          <label>
            Min steps
            <input type="number" min={0} value={minSteps} onChange={(e) => setMinSteps(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Max steps
            <input type="number" min={0} value={maxSteps} onChange={(e) => setMaxSteps(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Min error steps
            <input type="number" min={0} value={minErrorSteps} onChange={(e) => setMinErrorSteps(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Max error steps
            <input type="number" min={0} value={maxErrorSteps} onChange={(e) => setMaxErrorSteps(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Started after
            <input type="datetime-local" value={startedAfter} onChange={(e) => setStartedAfter(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Started before
            <input type="datetime-local" value={startedBefore} onChange={(e) => setStartedBefore(e.target.value)} style={{ width: "100%" }} />
          </label>
          <label>
            Sort by
            <select value={sortBy} onChange={(e) => setSortBy(e.target.value)} style={{ width: "100%" }}>
              <option value="started_at">started_at</option>
              <option value="num_steps">num_steps</option>
              <option value="error_steps">error_steps</option>
              <option value="tool_call_steps">tool_call_steps</option>
              <option value="run_id">run_id</option>
              <option value="outcome">outcome</option>
            </select>
          </label>
          <label>
            Sort direction
            <select value={sortDir} onChange={(e) => setSortDir(e.target.value as "asc" | "desc")} style={{ width: "100%" }}>
              <option value="desc">desc</option>
              <option value="asc">asc</option>
            </select>
          </label>
        </div>
        <div className="header-actions" style={{ marginTop: 12 }}>
          <button onClick={clearFilters}>Clear filters</button>
          <button onClick={shareCurrentView}>Copy share link</button>
          {loading ? <span className="subtle">Loading runs...</span> : null}
          {error ? <span style={{ color: "#b91c1c" }}>{error}</span> : null}
        </div>
      </section>

      <section className="panel" style={{ padding: 16, marginBottom: 16 }}>
        <div className="section-caption">
          <div>
            <span className="section-kicker">Shortcuts</span>
            <h2 className="section-title">Suggested Drill-Downs</h2>
          </div>
          <span className="subtle">Jump into common cohorts without rebuilding filters</span>
        </div>
        <div className="kpi-row" style={{ marginBottom: 0 }}>
          {drilldownLinks.map((item) => (
            <a key={item.label} href={item.href} className="kpi-chip">
              {item.label}
            </a>
          ))}
        </div>
      </section>

      <section className="panel" style={{ padding: 12 }}>
        <div className="section-caption" style={{ padding: "4px 8px 8px" }}>
          <div>
            <span className="section-kicker">Evidence Table</span>
            <h2 className="section-title">Runs</h2>
          </div>
          <span className="subtle">Every row links back to a trajectory-level explanation</span>
        </div>
        <div className="table-wrap">
          <table className="modern-table">
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <th key={header.id}>
                      {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => {
                const run = row.original;
                return (
                  <tr key={row.id}>
                    {row.getVisibleCells().map((cell) => (
                      <td key={cell.id}>
                        {cell.column.id === "outcome" ? (
                          <span
                            className="pill"
                            style={{
                              background: run.outcome === "success" ? "#dcfce7" : run.outcome === "fail" ? "#fee2e2" : "#ede9fe",
                              color: run.outcome === "success" ? "#166534" : run.outcome === "fail" ? "#b91c1c" : "#6d28d9",
                            }}
                          >
                            {run.outcome}
                          </span>
                        ) : cell.column.id === "task_or_scenario" ? (
                          <div style={{ display: "grid", gap: 4 }}>
                            <span>{flexRender(cell.column.columnDef.cell, cell.getContext())}</span>
                            <div className="subtle">
                              {run.failure_category ?? run.run_type ?? "untyped"} | first error: {run.first_error_step ?? "none"}
                            </div>
                          </div>
                        ) : (
                          flexRender(cell.column.columnDef.cell, cell.getContext())
                        )}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
