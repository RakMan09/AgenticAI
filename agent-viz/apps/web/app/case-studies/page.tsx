"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { fetchBenchmarkSubsets, fetchCaseStudies, autoBuildBenchmarkSubsets } from "../../lib/api";
import type { BenchmarkSubsetRow, CaseStudyRow } from "../../lib/types";

function copyText(value: string) {
  if (typeof window === "undefined") return;
  void navigator.clipboard.writeText(value);
}

export default function CaseStudiesPage() {
  const [items, setItems] = useState<CaseStudyRow[]>([]);
  const [benchmarks, setBenchmarks] = useState<BenchmarkSubsetRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoBuildResult, setAutoBuildResult] = useState<string>("");
  const [notice, setNotice] = useState("");

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

  useEffect(() => {
    if (!notice) return;
    const timeout = window.setTimeout(() => setNotice(""), 1800);
    return () => window.clearTimeout(timeout);
  }, [notice]);

  const handleAutoBuild = async () => {
    try {
      const result = await autoBuildBenchmarkSubsets();
      setAutoBuildResult(JSON.stringify(result.counts ?? {}, null, 2));
      await load();
      setNotice("Benchmark subsets rebuilt.");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to build subsets");
    }
  };

  return (
    <main className="container">
      <section className="hero-panel" style={{ marginBottom: 12 }}>
        <div>
          <p className="eyebrow">Evidence Workflow</p>
          <h1 className="hero-title" style={{ fontSize: 30 }}>Case Study Mode</h1>
          <p className="hero-copy">
            Curate reproducible analysis bundles: saved runs, benchmark subsets, and shareable evidence links for reviewers and demos.
          </p>
        </div>
        <div className="hero-actions">
          <button onClick={() => void load()}>Refresh</button>
          <button onClick={() => void handleAutoBuild()}>Auto-build baseline subsets</button>
        </div>
      </section>

      {notice ? <p className="subtle" style={{ color: "#166534" }}>{notice}</p> : null}
      {loading ? <p>Loading...</p> : null}
      {error ? <p style={{ color: "#b91c1c" }}>{error}</p> : null}

      <div className="analytics-grid">
        <section className="panel" style={{ padding: 14 }}>
          <h2 style={{ marginTop: 0, fontSize: 18 }}>Saved Case Studies</h2>
          <div style={{ display: "grid", gap: 10 }}>
            {items.length === 0 ? (
              <div className="subtle">No case study runs marked yet.</div>
            ) : (
              items.map((item) => (
                <div key={item.run_id} className="family-card">
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                    <div>
                      <strong>{item.title || item.run_id}</strong>
                      <div className="subtle">status: {item.status} | focus: {item.focus || "n/a"}</div>
                    </div>
                    <a href={`/runs/${encodeURIComponent(item.run_id)}`}>Open run</a>
                  </div>
                  <div className="header-actions" style={{ marginTop: 8 }}>
                    <a href={`/compare?left_run_id=${encodeURIComponent(item.run_id)}&mode=aligned`} className="kpi-chip">
                      Compare from run
                    </a>
                    <button onClick={() => {
                      copyText(`${window.location.origin}/runs/${encodeURIComponent(item.run_id)}`);
                      setNotice("Copied run link.");
                    }}>
                      Copy run link
                    </button>
                  </div>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="panel" style={{ padding: 14 }}>
          <h2 style={{ marginTop: 0, fontSize: 18 }}>Benchmark Subsets</h2>
          {autoBuildResult ? <pre className="code-panel">{autoBuildResult}</pre> : null}
          <div style={{ display: "grid", gap: 8 }}>
            {benchmarks.map((row) => (
              <div key={`${row.subset_name}-${row.run_id}`} className="metric-card">
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong>{row.subset_name}</strong>
                  <a href={`/runs/${encodeURIComponent(row.run_id)}`}>{row.run_id.slice(0, 16)}</a>
                </div>
                <div className="subtle">{row.rationale || "-"}</div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </main>
  );
}
