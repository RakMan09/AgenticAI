"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { autoBuildBenchmarkSubsets, fetchBenchmarkSubsets, fetchCaseStudies } from "../../lib/api";
import type { BenchmarkSubsetRow, CaseStudyRow } from "../../lib/types";

export default function CaseStudiesPage() {
  const [items, setItems] = useState<CaseStudyRow[]>([]);
  const [benchmarks, setBenchmarks] = useState<BenchmarkSubsetRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoBuildResult, setAutoBuildResult] = useState<string>("");

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [caseStudies, subsets] = await Promise.all([
        fetchCaseStudies({ status: "all", limit: 500, offset: 0 }),
        fetchBenchmarkSubsets({ limit: 500, offset: 0 }),
      ]);
      setItems(caseStudies.items);
      setBenchmarks(subsets.items);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const handleAutoBuild = async () => {
    try {
      const result = await autoBuildBenchmarkSubsets();
      setAutoBuildResult(JSON.stringify(result.counts ?? {}, null, 2));
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to build subsets");
    }
  };

  return (
    <main className="container">
      <h1 className="section-title">Case Study Mode</h1>
      <p className="subtle">
        Curated runs for presentation and reviewer evaluation artifacts.
      </p>
      {loading ? <p>Loading...</p> : null}
      {error ? <p style={{ color: "#b91c1c" }}>{error}</p> : null}

      <section className="panel" style={{ padding: 14, marginBottom: 12 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
          <h2 style={{ margin: 0, fontSize: 18 }}>Case Study Runs</h2>
          <button
            onClick={() => void load()}
            style={{ border: "1px solid #cbd5e1", borderRadius: 8, padding: "6px 10px", background: "white" }}
          >
            Refresh
          </button>
        </div>
        <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
          {items.length === 0 ? (
            <div style={{ color: "#64748b" }}>No case study runs marked yet.</div>
          ) : (
            items.map((item) => (
              <div key={item.run_id} style={{ border: "1px solid #e2e8f0", borderRadius: 10, padding: 10 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                  <div>
                    <strong>{item.title || item.run_id}</strong>
                    <div style={{ color: "#475569", fontSize: 13 }}>
                      status: {item.status} | focus: {item.focus || "n/a"}
                    </div>
                  </div>
                  <Link href={`/runs/${encodeURIComponent(item.run_id)}`} style={{ textDecoration: "underline" }}>
                    Open run
                  </Link>
                </div>
              </div>
            ))
          )}
        </div>
      </section>

      <section className="panel" style={{ padding: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
          <h2 style={{ margin: 0, fontSize: 18 }}>Evaluation Benchmark Subsets</h2>
          <button
            onClick={() => void handleAutoBuild()}
            style={{ border: "1px solid #334155", borderRadius: 8, padding: "6px 10px", background: "white" }}
          >
            Auto-build baseline subsets
          </button>
        </div>
        {autoBuildResult ? (
          <pre
            style={{
              marginTop: 8,
              padding: 8,
              background: "#f8fafc",
              border: "1px solid #e2e8f0",
              borderRadius: 8,
              overflowX: "auto",
              fontSize: 12,
            }}
          >
            {autoBuildResult}
          </pre>
        ) : null}
        <table style={{ width: "100%", borderCollapse: "collapse", marginTop: 8 }}>
          <thead>
            <tr style={{ textAlign: "left", borderBottom: "1px solid #e2e8f0" }}>
              <th style={{ padding: "6px 4px" }}>Subset</th>
              <th style={{ padding: "6px 4px" }}>Run</th>
              <th style={{ padding: "6px 4px" }}>Rationale</th>
            </tr>
          </thead>
          <tbody>
            {benchmarks.map((row) => (
              <tr key={`${row.subset_name}-${row.run_id}`} style={{ borderBottom: "1px solid #f1f5f9" }}>
                <td style={{ padding: "6px 4px" }}>{row.subset_name}</td>
                <td style={{ padding: "6px 4px" }}>
                  <Link href={`/runs/${encodeURIComponent(row.run_id)}`} style={{ textDecoration: "underline" }}>
                    {row.run_id.slice(0, 16)}
                  </Link>
                </td>
                <td style={{ padding: "6px 4px" }}>{row.rationale || "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}
