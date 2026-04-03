from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class RunRow(BaseModel):
    run_id: str
    task_id: str | None = None
    scenario: str | None = None
    outcome: str
    num_steps: int
    started_at: datetime | None = None
    ended_at: datetime | None = None
    error_steps: int = 0
    tool_call_steps: int = 0
    distinct_tools: int = 0
    first_error_step: int | None = None
    source: str | None = None
    dataset_name: str | None = None
    run_type: str | None = None
    failure_category: str | None = None
    first_failure_event_id: str | None = None
    root_cause_event_id: str | None = None


class RunDetail(RunRow):
    metadata: dict[str, Any] | str | None = None


class StepRow(BaseModel):
    run_id: str
    step_idx: int
    step_type: str
    display_step_type: str
    text: str | None = None
    tool_name: str | None = None
    tool_input: str | dict[str, Any] | None = None
    tool_output: str | dict[str, Any] | None = None
    error_flag: bool = False
    latency_ms: int | None = None
    event_id: str | None = None
    event_type: str | None = None
    agent_id: str | None = None
    timestamp: datetime | None = None
    parent_event_id: str | None = None
    causal_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    status: str | None = None
    error_type: str | None = None
    retry_count: int | None = None
    inferred_intent: str | None = None
    intended_next_action: str | None = None
    evidence_summary: str | None = None


class AnnotationRow(BaseModel):
    run_id: str
    step_idx: int | None = None
    event_id: str | None = None
    label_type: str
    label: str
    confidence: float | None = None
    reason_payload: dict[str, Any] | str | None = None
    source: str | None = None


class ManualAnnotationCreate(BaseModel):
    step_idx: int | None = None
    event_id: str | None = None
    label: str
    confidence: float | None = None
    reason_payload: dict[str, Any] | str | None = None
    source: str | None = "manual_ui"


class FailureSummary(BaseModel):
    run_id: str
    outcome: str
    failure_category: str | None = None
    first_error_step: int | None = None
    first_failure_event_id: str | None = None
    root_cause_event_id: str | None = None
    labels: list[AnnotationRow] = Field(default_factory=list)
    key_error_events: list[StepRow] = Field(default_factory=list)
    summary_text: str | None = None


class RunReportJson(BaseModel):
    run: RunDetail
    failure_summary: FailureSummary
    top_tools: list[dict[str, Any]] = Field(default_factory=list)
    gap_signals: list[AnnotationRow] = Field(default_factory=list)
    review_notes: list[ReviewNoteRow] = Field(default_factory=list)
    case_study: CaseStudyRow | None = None
    notes: list[str] = Field(default_factory=list)


class RunsResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[RunRow]


class StepsResponse(BaseModel):
    run_id: str
    total: int = Field(ge=0)
    items: list[StepRow]


class AnnotationsResponse(BaseModel):
    run_id: str
    total: int = Field(ge=0)
    items: list[AnnotationRow]


class ComparePair(BaseModel):
    pair_idx: int
    status: str
    note: str | None = None
    left_step: StepRow | None = None
    right_step: StepRow | None = None


class CompareStats(BaseModel):
    match: int = 0
    mismatch: int = 0
    left_only: int = 0
    right_only: int = 0


class CompareResponse(BaseModel):
    mode: str
    left_run: RunDetail
    right_run: RunDetail
    total_pairs: int = Field(ge=0)
    stats: CompareStats
    items: list[ComparePair]


class RunGapsResponse(BaseModel):
    run_id: str
    total: int = Field(ge=0)
    items: list[AnnotationRow]


class AnalyticsOverviewResponse(BaseModel):
    total_runs: int = 0
    total_steps: int = 0
    outcome_counts: dict[str, int] = Field(default_factory=dict)
    top_failure_labels: list[dict[str, Any]] = Field(default_factory=list)
    first_failure_step_histogram: list[dict[str, Any]] = Field(default_factory=list)
    retry_prevalence: float = 0.0
    loop_prevalence: float = 0.0
    timeout_prevalence: float = 0.0


class AnalyticsLabelsResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[dict[str, Any]] = Field(default_factory=list)


class AnalyticsToolUsageResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[dict[str, Any]] = Field(default_factory=list)


class AnalyticsGapsResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[dict[str, Any]] = Field(default_factory=list)


class CaseStudyRow(BaseModel):
    run_id: str
    title: str | None = None
    focus: str | None = None
    status: str = "active"
    created_at: datetime
    updated_at: datetime


class CaseStudiesResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[CaseStudyRow] = Field(default_factory=list)


class CaseStudyUpsertRequest(BaseModel):
    title: str | None = None
    focus: str | None = None
    status: str = "active"


class ReviewNoteRow(BaseModel):
    id: int
    run_id: str
    reviewer: str
    label: str | None = None
    note: str
    created_at: datetime


class ReviewNotesResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[ReviewNoteRow] = Field(default_factory=list)


class ReviewNoteCreate(BaseModel):
    reviewer: str
    label: str | None = None
    note: str


class BenchmarkSubsetRow(BaseModel):
    subset_name: str
    run_id: str
    rationale: str | None = None
    created_at: datetime


class BenchmarkSubsetsResponse(BaseModel):
    total: int = Field(ge=0)
    items: list[BenchmarkSubsetRow] = Field(default_factory=list)


class BenchmarkSubsetAssignRequest(BaseModel):
    subset_name: str
    rationale: str | None = None
