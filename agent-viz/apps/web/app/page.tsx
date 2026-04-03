"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";

import { fetchRuns } from "../lib/api";
import type { RunRow } from "../lib/types";

const columnHelper = createColumnHelper<RunRow>();

function parseOptionalNumber(value: string): number | undefined {
  if (!value.trim()) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
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
        id: "compare",
        header: "Compare",
        cell: (info) => {
          const runId = info.row.original.run_id;
          return (
            <div style={{ display: "flex", gap: 6 }}>
              <button
                onClick={() => setCompareLeft(runId)}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, background: "white", padding: "3px 8px" }}
              >
                Left
              </button>
              <button
                onClick={() => setCompareRight(runId)}
                style={{ border: "1px solid #cbd5e1", borderRadius: 8, background: "white", padding: "3px 8px" }}
              >
                Right
              </button>
            </div>
          );
        },
      }),
    ],
    [],
  );

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

  const outcomeClass = useMemo(
    () =>
      ({
        success: { background: "#e6fffa", color: "#0f766e" },
        fail: { background: "#ffe9e9", color: "#b91c1c" },
        unknown: { background: "#f1e8ff", color: "#6d28d9" },
      }) as const,
    [],
  );

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

  return (
    <main className="container">
      <h1 className="section-title">Run Explorer</h1>
      <div className="kpi-row">
        <span className="kpi-chip">loaded: <strong>{rows.length}</strong> / matches: <strong>{total}</strong></span>
        <span className="kpi-chip">success: <strong>{runSummary.success}</strong></span>
        <span className="kpi-chip">fail: <strong>{runSummary.fail}</strong></span>
        <span className="kpi-chip">unknown: <strong>{runSummary.unknown}</strong></span>
        <Link href="/analytics" className="kpi-chip">Analytics</Link>
        <Link href="/case-studies" className="kpi-chip">Case Studies</Link>
      </div>
      <section className="panel" style={{ padding: 16, marginBottom: 16 }}>
        <div className="header-actions" style={{ marginBottom: 10 }}>
          <span className="subtle">
            compare left: <strong>{compareLeft ? compareLeft.slice(0, 14) : "-"}</strong>
          </span>
          <span className="subtle">
            compare right: <strong>{compareRight ? compareRight.slice(0, 14) : "-"}</strong>
          </span>
          {compareHref ? (
            <a
              href={compareHref}
              className="btn-primary"
            >
              Open Compare
            </a>
          ) : (
            <span className="subtle">Pick both runs to compare</span>
          )}
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))",
            gap: 10,
          }}
        >
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
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="run_id / task / scenario"
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Tool
            <input
              value={tool}
              onChange={(e) => setTool(e.target.value)}
              placeholder="tool name"
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Source
            <select value={source} onChange={(e) => setSource(e.target.value)} style={{ width: "100%" }}>
              <option value="all">all</option>
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
            <input
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="annotation label"
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Min steps
            <input
              type="number"
              min={0}
              value={minSteps}
              onChange={(e) => setMinSteps(e.target.value)}
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Max steps
            <input
              type="number"
              min={0}
              value={maxSteps}
              onChange={(e) => setMaxSteps(e.target.value)}
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Min error steps
            <input
              type="number"
              min={0}
              value={minErrorSteps}
              onChange={(e) => setMinErrorSteps(e.target.value)}
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Max error steps
            <input
              type="number"
              min={0}
              value={maxErrorSteps}
              onChange={(e) => setMaxErrorSteps(e.target.value)}
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Started after
            <input
              type="datetime-local"
              value={startedAfter}
              onChange={(e) => setStartedAfter(e.target.value)}
              style={{ width: "100%" }}
            />
          </label>
          <label>
            Started before
            <input
              type="datetime-local"
              value={startedBefore}
              onChange={(e) => setStartedBefore(e.target.value)}
              style={{ width: "100%" }}
            />
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
            <select
              value={sortDir}
              onChange={(e) => setSortDir(e.target.value as "asc" | "desc")}
              style={{ width: "100%" }}
            >
              <option value="desc">desc</option>
              <option value="asc">asc</option>
            </select>
          </label>
        </div>

        <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
          <button
            onClick={clearFilters}
            style={{ border: "1px solid #cbd5e1", borderRadius: 8, background: "white", padding: "6px 12px" }}
          >
            Clear filters
          </button>
        </div>
      </section>

      <section className="panel" style={{ overflow: "hidden" }}>
        <div style={{ padding: "12px 16px", borderBottom: "1px solid #d8dee8" }}>
          <strong>{loading ? "Loading..." : `${rows.length} rows`}</strong>
          <span className="muted" style={{ marginLeft: 8 }}>total matches: {total}</span>
          {error ? <span style={{ color: "#b91c1c", marginLeft: 12 }}>{error}</span> : null}
        </div>

        <div className="table-wrap">
          <table className="modern-table">
            <thead>
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <th key={header.id}>
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
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
                        {cell.column.id === "run_id" ? (
                          <Link href={`/runs/${run.run_id}`} style={{ textDecoration: "underline", color: "#0f172a" }}>
                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                          </Link>
                        ) : cell.column.id === "outcome" ? (
                          <span
                            style={{
                              ...outcomeClass[run.outcome],
                              borderRadius: 999,
                              padding: "2px 10px",
                              display: "inline-block",
                              fontSize: 12,
                            }}
                          >
                            {run.outcome}
                          </span>
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
