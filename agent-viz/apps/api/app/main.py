from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

from .db import (
    assign_benchmark_subset,
    auto_build_benchmark_subsets,
    clear_cached_queries,
    fetch_analytics_failure_labels,
    fetch_analytics_gaps,
    fetch_analytics_overview,
    fetch_analytics_tool_usage,
    fetch_annotations,
    fetch_benchmark_subsets,
    fetch_case_studies,
    fetch_compare,
    fetch_failure_summary,
    fetch_gap_signals,
    fetch_review_notes,
    fetch_run_detail,
    fetch_run_report,
    fetch_runs,
    fetch_steps,
    get_db_path,
    insert_review_note,
    insert_manual_annotation,
    upsert_case_study,
)
from .models import (
    AnalyticsGapsResponse,
    AnalyticsLabelsResponse,
    AnalyticsOverviewResponse,
    AnalyticsToolUsageResponse,
    AnnotationRow,
    AnnotationsResponse,
    BenchmarkSubsetAssignRequest,
    BenchmarkSubsetsResponse,
    BenchmarkSubsetRow,
    CaseStudiesResponse,
    CaseStudyRow,
    CaseStudyUpsertRequest,
    CompareResponse,
    FailureSummary,
    ManualAnnotationCreate,
    ReviewNoteCreate,
    ReviewNoteRow,
    ReviewNotesResponse,
    RunDetail,
    RunReportJson,
    RunGapsResponse,
    RunsResponse,
    StepsResponse,
)

app = FastAPI(title="Agent Trace Viz API", version="0.3.0")
PROJECT_ROOT = Path(__file__).resolve().parents[3]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "db_path": get_db_path()}


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "agent-viz-api",
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
        "runs": "/runs?limit=5",
    }


@app.get("/runs", response_model=RunsResponse)
def list_runs(
    outcome: str | None = Query(default="all"),
    q: str | None = Query(default=None),
    tool: str | None = Query(default=None),
    source: str | None = Query(default="all"),
    min_steps: int | None = Query(default=None, ge=0),
    max_steps: int | None = Query(default=None, ge=0),
    has_errors: str | None = Query(default="all"),
    min_error_steps: int | None = Query(default=None, ge=0),
    max_error_steps: int | None = Query(default=None, ge=0),
    step_type: str | None = Query(default="all"),
    label: str | None = Query(default=None),
    started_after: str | None = Query(default=None),
    started_before: str | None = Query(default=None),
    sort_by: str = Query(default="started_at"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> RunsResponse:
    total, items = fetch_runs(
        outcome=outcome,
        q=q,
        tool=tool,
        source=source,
        min_steps=min_steps,
        max_steps=max_steps,
        has_errors=has_errors,
        min_error_steps=min_error_steps,
        max_error_steps=max_error_steps,
        step_type=step_type,
        label=label,
        started_after=started_after,
        started_before=started_before,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )
    return RunsResponse(total=total, items=items)


@app.get("/runs/{run_id}", response_model=RunDetail)
def get_run(run_id: str) -> RunDetail:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return RunDetail(**run)


@app.get("/runs/{run_id}/steps", response_model=StepsResponse)
def get_run_steps(
    run_id: str,
    q: str | None = Query(default=None),
    step_type: str | None = Query(default="all"),
    error_only: bool = Query(default=False),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
) -> StepsResponse:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    total, items = fetch_steps(run_id, q=q, step_type=step_type, error_only=error_only, limit=limit, offset=offset)
    return StepsResponse(run_id=run_id, total=total, items=items)


@app.get("/runs/{run_id}/annotations", response_model=AnnotationsResponse)
def get_run_annotations(
    run_id: str,
    label_type: str | None = Query(default="all"),
) -> AnnotationsResponse:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    items = fetch_annotations(run_id, label_type=label_type)
    return AnnotationsResponse(run_id=run_id, total=len(items), items=[AnnotationRow(**item) for item in items])


@app.post("/runs/{run_id}/annotations/manual", response_model=AnnotationRow)
def create_manual_annotation(run_id: str, payload: ManualAnnotationCreate) -> AnnotationRow:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    inserted = insert_manual_annotation(run_id, payload.model_dump())
    clear_cached_queries()
    return AnnotationRow(**inserted)


@app.get("/runs/{run_id}/failure-summary", response_model=FailureSummary)
def get_run_failure_summary(run_id: str) -> FailureSummary:
    summary = fetch_failure_summary(run_id)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return FailureSummary(**summary)


@app.get("/runs/{run_id}/gaps", response_model=RunGapsResponse)
def get_run_gaps(run_id: str) -> RunGapsResponse:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    items = fetch_gap_signals(run_id)
    return RunGapsResponse(run_id=run_id, total=len(items), items=[AnnotationRow(**item) for item in items])


@app.get("/runs/{run_id}/report")
def get_run_report(
    run_id: str,
    format: str = Query(default="json"),
) -> Any:
    report = fetch_run_report(run_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    if format == "json":
        return RunReportJson(**report)
    if format == "md":
        summary = report["failure_summary"]
        lines = [
            f"# Run Report: {report['run']['run_id']}",
            "",
            f"- Outcome: {report['run']['outcome']}",
            f"- Run Type: {report['run'].get('run_type') or 'unknown'}",
            f"- Failure Category: {report['run'].get('failure_category') or 'n/a'}",
            f"- Source: {report['run'].get('source') or 'unknown'}",
            "",
            "## Failure Summary",
            summary.get("summary_text") or "No summary available.",
            "",
            "## Labels",
        ]
        labels = summary.get("labels", [])
        if labels:
            lines.extend([f"- {item['label_type']}: {item['label']}" for item in labels])
        else:
            lines.append("- none")
        lines.extend(["", "## Top Tools"])
        tools = report.get("top_tools", [])
        if tools:
            lines.extend([f"- {item['tool_name']}: {item['count']}" for item in tools])
        else:
            lines.append("- none")
        lines.extend(["", "## Gap Signals"])
        gaps = report.get("gap_signals", [])
        if gaps:
            lines.extend([f"- {item['label']}" for item in gaps])
        else:
            lines.append("- none")
        lines.extend(["", "## Case Study"])
        case_study = report.get("case_study")
        if case_study:
            lines.append(f"- status: {case_study.get('status')}")
            lines.append(f"- title: {case_study.get('title') or 'n/a'}")
            lines.append(f"- focus: {case_study.get('focus') or 'n/a'}")
        else:
            lines.append("- not marked")
        lines.extend(["", "## Reviewer Notes"])
        review_notes = report.get("review_notes", [])
        if review_notes:
            lines.extend([f"- [{item.get('reviewer')}] {item.get('label') or 'note'}: {item.get('note')}" for item in review_notes[:10]])
        else:
            lines.append("- none")
        return PlainTextResponse("\n".join(lines))
    raise HTTPException(status_code=400, detail="format must be json or md")


@app.get("/compare", response_model=CompareResponse)
def compare_runs(
    left_run_id: str = Query(...),
    right_run_id: str = Query(...),
    mode: str = Query(default="aligned"),
) -> CompareResponse:
    if mode not in {"raw", "aligned"}:
        raise HTTPException(status_code=400, detail="mode must be 'raw' or 'aligned'")
    comparison = fetch_compare(left_run_id, right_run_id, mode=mode)
    if comparison is None:
        raise HTTPException(
            status_code=404,
            detail=f"Run not found (left_run_id={left_run_id}, right_run_id={right_run_id})",
        )
    return CompareResponse(**comparison)


@app.get("/analytics/overview", response_model=AnalyticsOverviewResponse)
def get_analytics_overview() -> AnalyticsOverviewResponse:
    return AnalyticsOverviewResponse(**fetch_analytics_overview())


@app.get("/analytics/failure-labels", response_model=AnalyticsLabelsResponse)
def get_analytics_failure_labels(limit: int = Query(default=50, ge=1, le=2000)) -> AnalyticsLabelsResponse:
    items = fetch_analytics_failure_labels(limit=limit)
    return AnalyticsLabelsResponse(total=len(items), items=items)


@app.get("/analytics/tool-usage", response_model=AnalyticsToolUsageResponse)
def get_analytics_tool_usage(limit: int = Query(default=40, ge=1, le=500)) -> AnalyticsToolUsageResponse:
    items = fetch_analytics_tool_usage(limit=limit)
    return AnalyticsToolUsageResponse(total=len(items), items=items)


@app.get("/analytics/gaps", response_model=AnalyticsGapsResponse)
def get_analytics_gaps(limit: int = Query(default=50, ge=1, le=500)) -> AnalyticsGapsResponse:
    items = fetch_analytics_gaps(limit=limit)
    return AnalyticsGapsResponse(total=len(items), items=items)


@app.get("/case-studies", response_model=CaseStudiesResponse)
def get_case_studies(
    status: str | None = Query(default="all"),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> CaseStudiesResponse:
    items = fetch_case_studies(status=status, limit=limit, offset=offset)
    return CaseStudiesResponse(total=len(items), items=[CaseStudyRow(**item) for item in items])


@app.post("/runs/{run_id}/case-study", response_model=CaseStudyRow)
def create_or_update_case_study(run_id: str, payload: CaseStudyUpsertRequest) -> CaseStudyRow:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    row = upsert_case_study(run_id, payload.model_dump())
    return CaseStudyRow(**row)


@app.get("/runs/{run_id}/review-notes", response_model=ReviewNotesResponse)
def get_review_notes(
    run_id: str,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> ReviewNotesResponse:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    items = fetch_review_notes(run_id, limit=limit, offset=offset)
    return ReviewNotesResponse(total=len(items), items=[ReviewNoteRow(**item) for item in items])


@app.post("/runs/{run_id}/review-notes", response_model=ReviewNoteRow)
def create_review_note(run_id: str, payload: ReviewNoteCreate) -> ReviewNoteRow:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    row = insert_review_note(run_id, payload.model_dump())
    return ReviewNoteRow(**row)


@app.get("/evaluation/benchmark-subsets", response_model=BenchmarkSubsetsResponse)
def get_benchmark_subsets(
    subset_name: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> BenchmarkSubsetsResponse:
    items = fetch_benchmark_subsets(subset_name=subset_name, limit=limit, offset=offset)
    return BenchmarkSubsetsResponse(total=len(items), items=[BenchmarkSubsetRow(**item) for item in items])


@app.post("/runs/{run_id}/benchmark-subsets", response_model=BenchmarkSubsetRow)
def assign_run_to_benchmark_subset(run_id: str, payload: BenchmarkSubsetAssignRequest) -> BenchmarkSubsetRow:
    run = fetch_run_detail(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    row = assign_benchmark_subset(run_id, payload.model_dump())
    return BenchmarkSubsetRow(**row)


@app.post("/evaluation/benchmark-subsets/auto-build")
def build_auto_benchmark_subsets() -> dict[str, Any]:
    return {"counts": auto_build_benchmark_subsets()}


def run_ingest(include_legacy: bool) -> dict[str, Any]:
    ingest_script = PROJECT_ROOT / "scripts" / "ingest_assetops.py"
    command = [sys.executable, str(ingest_script)]
    if include_legacy:
        command.append("--include-legacy")
    completed = subprocess.run(command, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip().splitlines()[-15:],
        "stderr": completed.stderr.strip().splitlines()[-15:],
    }


@app.post("/admin/recompute-heuristics")
def admin_recompute_heuristics(include_legacy: bool = Query(default=False)) -> dict[str, Any]:
    # Heuristics are recomputed during ingestion rebuild.
    result = run_ingest(include_legacy=include_legacy)
    if not result["ok"]:
        raise HTTPException(status_code=500, detail=result)
    clear_cached_queries()
    return result


@app.post("/admin/rebuild-derived-data")
def admin_rebuild_derived_data(include_legacy: bool = Query(default=False)) -> dict[str, Any]:
    result = run_ingest(include_legacy=include_legacy)
    if not result["ok"]:
        raise HTTPException(status_code=500, detail=result)
    clear_cached_queries()
    return result
