#!/usr/bin/env python3
"""Generate canonical Project 1 synthetic event-level traces."""

from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "2.0.0"
GENERATOR_VERSION = "2.0.0"
DEFAULT_DATASET_NAME = "project1_event_runs_v2"
DEFAULT_OUT_DIR = Path("data/raw/project1_event_runs/generated")
DEFAULT_MANIFEST_PATH = Path("data/manifests/all_runs_manifest.jsonl")

BASE_TIME = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)

# (run_type, first_run_number, default_count)
DEFAULT_RUN_SPECS: list[tuple[str, int, int]] = [
    ("success", 1, 10),
    ("timeout_failure", 11, 8),
    ("retry_success", 19, 8),
    ("conflict_ignored", 27, 8),
    ("conflict_resolved", 35, 8),
    ("missing_verification", 43, 6),
    ("early_quit", 49, 5),
    ("reasoning_loop", 54, 5),
    ("wrong_tool_chosen", 59, 6),
    ("missing_tool_use", 65, 6),
    ("stale_belief_used", 71, 6),
    ("partial_recovery", 77, 6),
]


@dataclass
class RunSpec:
    run_type: str
    start: int
    count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate canonical Project 1 event-level synthetic traces")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--manifest-file", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--profile", choices=["default", "small"], default="default")
    parser.add_argument(
        "--counts-file",
        type=Path,
        default=None,
        help="Optional JSON file mapping run_type -> count",
    )
    parser.add_argument("--dataset-name", type=str, default=DEFAULT_DATASET_NAME)
    return parser.parse_args()


def workflow_meta() -> dict[str, Any]:
    return {
        "expected_tools": ["monitoring_api", "log_analyzer"],
        "required_agents": ["planner_agent", "research_agent", "tool_agent", "verifier_agent"],
        "required_checkpoints": [
            "initial_assessment",
            "evidence_collection",
            "verification",
            "finalization",
        ],
        "expected_verification_step": True,
        "expected_finalization_condition": "verified_evidence_or_explicit_recovery",
    }


def belief_payload(
    claims: list[str],
    evidence: list[str],
    uncertainty: float,
    next_action: str,
    trigger_source: str,
    confidence_note: str = "",
) -> dict[str, Any]:
    return {
        "claims": claims,
        "supporting_evidence": evidence,
        "uncertainty": uncertainty,
        "intended_next_action": next_action,
        "trigger_source": trigger_source,
        "confidence_note": confidence_note,
    }


def tool_payload(
    tool_name: str,
    tool_input: dict[str, Any],
    output: dict[str, Any],
    latency_ms: int,
    retry_count: int,
    tool_status: str,
    backoff_ms: int = 0,
) -> dict[str, Any]:
    return {
        "tool_name": tool_name,
        "input": tool_input,
        "output": output,
        "latency_ms": latency_ms,
        "retry_count": retry_count,
        "tool_status": tool_status,
        "backoff_ms": backoff_ms,
    }


def message_payload(
    sender: str,
    receiver: str,
    message_type: str,
    content_summary: str,
    linked_to_event: str,
    requires_ack: bool = False,
) -> dict[str, Any]:
    return {
        "sender": sender,
        "receiver": receiver,
        "message_type": message_type,
        "content_summary": content_summary,
        "linked_to_event": linked_to_event,
        "requires_ack": requires_ack,
    }


def error_payload(
    error_type: str,
    source: str,
    severity: str,
    description: str,
    propagated_to: list[str],
    root_cause_candidate: bool,
    expected_but_missing: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "error_type": error_type,
        "source": source,
        "severity": severity,
        "description": description,
        "propagated_to": propagated_to,
        "root_cause_candidate": root_cause_candidate,
        "expected_but_missing": expected_but_missing or [],
    }


def verification_payload(
    verdict: str,
    checked_claim: str,
    evidence_used: list[str],
    confidence: float,
    missing_checks: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "verdict": verdict,
        "checked_claim": checked_claim,
        "evidence_used": evidence_used,
        "confidence": confidence,
        "missing_checks": missing_checks or [],
    }


def final_payload(
    outcome: str,
    failure_category: str | None,
    summary: str,
    final_uncertainty: float,
    expected_finalization_condition_met: bool,
) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "failure_category": failure_category,
        "summary": summary,
        "final_uncertainty": final_uncertainty,
        "expected_finalization_condition_met": expected_finalization_condition_met,
    }


class TraceBuilder:
    def __init__(
        self,
        *,
        run_id: str,
        run_type: str,
        start_time: datetime,
        scenario: str,
        expected_workflow: dict[str, Any],
        dataset_name: str,
        generated_at: str,
        rng: random.Random,
    ) -> None:
        self.run_id = run_id
        self.run_type = run_type
        self.scenario = scenario
        self.expected_workflow = expected_workflow
        self.dataset_name = dataset_name
        self.generated_at = generated_at
        self.rng = rng

        self.events: list[dict[str, Any]] = []
        self.event_num = 1
        self.causal_num = 1
        self.step_index = 1
        self.time = start_time
        self.first_failure_event_id: str | None = None
        self.root_cause_event_id: str | None = None

    def _ts(self) -> str:
        self.time += timedelta(seconds=self.rng.randint(1, 3))
        return self.time.isoformat().replace("+00:00", "Z")

    def _eid(self) -> str:
        value = f"evt_{self.event_num:04d}"
        self.event_num += 1
        return value

    def _cid(self) -> str:
        value = f"cause_{self.causal_num:04d}"
        self.causal_num += 1
        return value

    def add_event(
        self,
        *,
        agent_id: str,
        event_type: str,
        payload: dict[str, Any],
        status: str = "success",
        parent_event_id: str | None = None,
        causal_id: str | None = None,
        root_cause_event_id: str | None = None,
        propagated_to_event_ids: list[str] | None = None,
        tags: list[str] | None = None,
        loop_iteration: int | None = None,
        loop_group: str | None = None,
        repeated_pattern_id: str | None = None,
    ) -> tuple[str, str]:
        if causal_id is None:
            causal_id = self._cid()

        event_id = self._eid()
        step_index = self.step_index
        self.step_index += 1

        if status in {"failure", "warning"} and self.first_failure_event_id is None:
            self.first_failure_event_id = event_id
        if payload.get("root_cause_candidate") and self.root_cause_event_id is None:
            self.root_cause_event_id = event_id

        event: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "generator_version": GENERATOR_VERSION,
            "dataset_name": self.dataset_name,
            "generated_at": self.generated_at,
            "run_id": self.run_id,
            "run_type": self.run_type,
            "scenario": self.scenario,
            "expected_workflow": self.expected_workflow,
            "event_id": event_id,
            "parent_event_id": parent_event_id,
            "causal_id": causal_id,
            "timestamp": self._ts(),
            "step_index": step_index,
            "agent_id": agent_id,
            "event_type": event_type,
            "status": status,
            "root_cause_event_id": root_cause_event_id,
            "first_failure_event_id": self.first_failure_event_id,
            "propagated_to_event_ids": propagated_to_event_ids or [],
            "tags": tags or [],
            "payload": payload,
        }
        if loop_iteration is not None:
            event["loop_iteration"] = loop_iteration
        if loop_group is not None:
            event["loop_group"] = loop_group
        if repeated_pattern_id is not None:
            event["repeated_pattern_id"] = repeated_pattern_id

        self.events.append(event)
        return event_id, causal_id

    def finalize_failure_links(self) -> None:
        for event in self.events:
            if event["first_failure_event_id"] is None:
                event["first_failure_event_id"] = self.first_failure_event_id
            if event["root_cause_event_id"] is None:
                event["root_cause_event_id"] = self.root_cause_event_id

    def save(self, out_dir: Path) -> Path:
        self.finalize_failure_links()
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{self.run_id}.jsonl"
        with file_path.open("w", encoding="utf-8") as handle:
            for event in self.events:
                handle.write(json.dumps(event, ensure_ascii=True) + "\n")
        return file_path


def add_common_start(tb: TraceBuilder, topic_text: str) -> tuple[str, str, str]:
    event1, _ = tb.add_event(
        agent_id="planner_agent",
        event_type="belief_update",
        payload=belief_payload(
            [f"Need to diagnose {topic_text}"],
            [],
            round(tb.rng.uniform(0.45, 0.65), 2),
            "delegate_research",
            "initial_prompt",
            "Initial task framing before evidence collection",
        ),
        tags=["start"],
    )

    event2, _ = tb.add_event(
        agent_id="planner_agent",
        event_type="message",
        payload=message_payload(
            "planner_agent",
            "research_agent",
            "task_assignment",
            f"Investigate {topic_text} using available tools",
            event1,
            True,
        ),
        parent_event_id=event1,
        tags=["handoff"],
    )

    event3, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            [f"Need evidence before concluding {topic_text}"],
            ["Planner requested diagnosis"],
            round(tb.rng.uniform(0.30, 0.50), 2),
            "collect_tool_evidence",
            "planner_message",
            "Researcher acknowledges need for evidence",
        ),
        parent_event_id=event2,
        tags=["research_start"],
    )
    return event1, event2, event3


def build_success(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "inventory-db slowdown")
    call1, cause1 = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "inventory-db"},
            {
                "status": "degraded",
                "latency_ms": tb.rng.randint(800, 1100),
                "error_rate": round(tb.rng.uniform(0.08, 0.16), 2),
            },
            tb.rng.randint(220, 640),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "monitoring"],
    )
    call2, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "log_analyzer",
            {"service": "inventory-db"},
            {"error_pattern": "connection_pool_exhaustion", "sample_count": tb.rng.randint(16, 36)},
            tb.rng.randint(250, 700),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "logs"],
    )
    synthesis, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Inventory DB is degraded due to connection pool issues"],
            [
                "monitoring_api reported high latency and error rate",
                "log_analyzer reported connection_pool_exhaustion",
            ],
            round(tb.rng.uniform(0.08, 0.18), 2),
            "send_to_verifier",
            "consistent_tool_outputs",
            "Two independent sources are aligned",
        ),
        parent_event_id=call2,
        causal_id=cause1,
        tags=["synthesis"],
    )
    verification, _ = tb.add_event(
        agent_id="verifier_agent",
        event_type="verification",
        payload=verification_payload(
            "confirmed",
            "Inventory DB degradation",
            ["monitoring_api output", "log_analyzer output"],
            round(tb.rng.uniform(0.84, 0.95), 2),
        ),
        parent_event_id=synthesis,
        tags=["verification"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "success",
            None,
            "Evidence collected and verified; diagnosis is complete",
            round(tb.rng.uniform(0.05, 0.15), 2),
            True,
        ),
        parent_event_id=verification,
        status="success",
        tags=["final", "success"],
    )
    # keep causal context visible by linking key events
    tb.add_event(
        agent_id="planner_agent",
        event_type="message",
        payload=message_payload(
            "planner_agent",
            "research_agent",
            "close_case",
            "Verification complete, close incident triage.",
            verification,
            False,
        ),
        parent_event_id=verification,
        tags=["closure"],
    )


def build_timeout_failure(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "payments-api timeout spikes")
    call_id, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "payments-api"},
            {"error": "request_timeout", "status": "unavailable"},
            tb.rng.randint(1800, 3600),
            0,
            "timeout",
        ),
        status="warning",
        parent_event_id=research_start,
        tags=["tool", "timeout"],
    )
    error_id, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="error_event",
        payload=error_payload(
            "tool_timeout",
            "monitoring_api",
            "high",
            "Tool call timed out before telemetry response",
            ["research_agent", "planner_agent"],
            True,
            expected_but_missing=["monitoring_snapshot", "verification_step", "retry_with_backoff"],
        ),
        status="failure",
        parent_event_id=call_id,
        tags=["error", "timeout"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "timeout_failure",
            "Run terminated after monitoring_api timeout without adequate fallback",
            round(tb.rng.uniform(0.60, 0.80), 2),
            False,
        ),
        status="failure",
        parent_event_id=error_id,
        tags=["final", "failure", "timeout_failure"],
    )


def build_retry_success(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "checkout latency regression")
    first_call, cause = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "checkout-api"},
            {"error": "upstream_timeout"},
            tb.rng.randint(1200, 2600),
            0,
            "timeout",
        ),
        status="warning",
        parent_event_id=research_start,
        tags=["tool", "retry"],
    )
    retry_call, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "checkout-api"},
            {"status": "degraded", "latency_ms": tb.rng.randint(700, 980), "error_rate": 0.07},
            tb.rng.randint(260, 520),
            1,
            "success",
            backoff_ms=250,
        ),
        parent_event_id=first_call,
        causal_id=cause,
        tags=["tool", "retry"],
    )
    belief, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Checkout latency issue confirmed after retry"],
            ["Second attempt returned stable telemetry"],
            round(tb.rng.uniform(0.12, 0.25), 2),
            "verify_and_close",
            "retry_success",
            "Retry recovered missing telemetry",
        ),
        parent_event_id=retry_call,
        tags=["recovery"],
    )
    verification, _ = tb.add_event(
        agent_id="verifier_agent",
        event_type="verification",
        payload=verification_payload(
            "confirmed",
            "Checkout latency regression",
            ["retried monitoring_api output"],
            round(tb.rng.uniform(0.80, 0.90), 2),
            missing_checks=["log_analyzer optional check skipped due confidence"],
        ),
        parent_event_id=belief,
        tags=["verification"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "success",
            None,
            "Retry succeeded and diagnosis was verified",
            round(tb.rng.uniform(0.08, 0.20), 2),
            True,
        ),
        parent_event_id=verification,
        tags=["final", "success", "recovered"],
    )


def build_conflict_ignored(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "catalog API inconsistency")
    monitor_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "catalog-api"},
            {"status": "healthy", "latency_ms": tb.rng.randint(180, 280)},
            tb.rng.randint(180, 330),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "conflict"],
    )
    logs_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "log_analyzer",
            {"service": "catalog-api"},
            {"status": "degraded", "error_pattern": "deadlock_detected"},
            tb.rng.randint(300, 590),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "conflict"],
    )
    ignored_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Service is healthy"],
            ["Monitoring output only"],
            round(tb.rng.uniform(0.22, 0.38), 2),
            "finish_without_reconcile",
            "monitoring_bias",
            "Conflicting log evidence not reconciled",
        ),
        parent_event_id=logs_evt,
        tags=["conflict", "ignored"],
    )
    err_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="error_event",
        payload=error_payload(
            "conflict_ignored",
            "research_agent",
            "high",
            "Conflicting evidence detected but ignored",
            ["planner_agent"],
            True,
            expected_but_missing=["conflict_resolution_step", "verifier_handoff"],
        ),
        status="failure",
        parent_event_id=ignored_evt,
        propagated_to_event_ids=[monitor_evt],
        tags=["conflict", "error"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "conflict_ignored",
            "Run ended with unresolved evidence conflict",
            round(tb.rng.uniform(0.60, 0.75), 2),
            False,
        ),
        status="failure",
        parent_event_id=err_evt,
        tags=["final", "failure", "conflict_ignored"],
    )


def build_conflict_resolved(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "catalog API inconsistency")
    monitoring_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "catalog-api"},
            {"status": "healthy", "latency_ms": tb.rng.randint(180, 280)},
            tb.rng.randint(170, 340),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "conflict"],
    )
    logs_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "log_analyzer",
            {"service": "catalog-api"},
            {"status": "degraded", "error_pattern": "deadlock_detected"},
            tb.rng.randint(300, 620),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "conflict"],
    )
    resolve_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Transient lock contention likely"],
            ["monitoring healthy", "log spikes indicate intermittent deadlock"],
            round(tb.rng.uniform(0.18, 0.29), 2),
            "request_verification",
            "conflict_reconciliation",
            "Conflict resolved by considering time windows",
        ),
        parent_event_id=logs_evt,
        tags=["conflict", "resolved"],
        propagated_to_event_ids=[monitoring_evt],
    )
    verify_evt, _ = tb.add_event(
        agent_id="verifier_agent",
        event_type="verification",
        payload=verification_payload(
            "confirmed",
            "Intermittent lock contention",
            ["monitoring windowed trend", "log deadlock bursts"],
            round(tb.rng.uniform(0.79, 0.90), 2),
        ),
        parent_event_id=resolve_evt,
        tags=["verification"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "success",
            None,
            "Conflict was reconciled and diagnosis was validated",
            round(tb.rng.uniform(0.10, 0.18), 2),
            True,
        ),
        parent_event_id=verify_evt,
        tags=["final", "success"],
    )


def build_missing_verification(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "queue processor throughput drop")
    call_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "queue-processor"},
            {"status": "degraded", "throughput": "low"},
            tb.rng.randint(220, 440),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool"],
    )
    synthesis_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Queue processor slowdown likely due to downstream lock wait"],
            ["Monitoring indicates sustained throughput drop"],
            round(tb.rng.uniform(0.25, 0.40), 2),
            "finalize_without_verification",
            "single_source_confidence",
            "Skipped verification due time pressure",
        ),
        parent_event_id=call_evt,
        tags=["missing_verification"],
    )
    error_evt, _ = tb.add_event(
        agent_id="planner_agent",
        event_type="error_event",
        payload=error_payload(
            "missing_verification_step",
            "planner_agent",
            "high",
            "Finalization happened without a verification checkpoint",
            ["reporting_layer"],
            True,
            expected_but_missing=["verification_step", "cross_tool_consistency_check"],
        ),
        status="failure",
        parent_event_id=synthesis_evt,
        tags=["missing_verification", "error"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "missing_verification_step",
            "Diagnosis was submitted without verification",
            round(tb.rng.uniform(0.55, 0.72), 2),
            False,
        ),
        status="failure",
        parent_event_id=error_evt,
        tags=["final", "failure", "missing_verification"],
    )


def build_early_quit(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "auth token failure surge")
    quit_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Insufficient confidence but ending run"],
            ["Limited starting context only"],
            round(tb.rng.uniform(0.60, 0.78), 2),
            "stop_execution",
            "premature_termination",
            "Terminated before evidence collection",
        ),
        parent_event_id=research_start,
        tags=["early_quit"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "early_quit",
            "Run stopped before required evidence and verification steps",
            round(tb.rng.uniform(0.65, 0.82), 2),
            False,
        ),
        status="failure",
        parent_event_id=quit_evt,
        tags=["final", "failure", "early_quit"],
    )


def build_reasoning_loop(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "inventory-db slowdown")
    parent = research_start
    loop_group = "reasoning_loop_group_1"
    repeated_pattern_id = "monitoring_recheck_pattern"

    for loop_iteration in range(1, 6):
        belief_evt, _ = tb.add_event(
            agent_id="research_agent",
            event_type="belief_update",
            payload=belief_payload(
                ["Need more certainty before deciding"],
                ["Previous monitoring check did not resolve uncertainty"],
                round(tb.rng.uniform(0.58, 0.72), 2),
                "recheck_same_tool",
                "self_loop_reasoning",
                "Reasoning is repeating with no new evidence",
            ),
            parent_event_id=parent,
            tags=["loop"],
            loop_iteration=loop_iteration,
            loop_group=loop_group,
            repeated_pattern_id=repeated_pattern_id,
        )
        tool_evt, _ = tb.add_event(
            agent_id="tool_agent",
            event_type="tool_call",
            payload=tool_payload(
                "monitoring_api",
                {"service": "inventory-db"},
                {"status": "unknown", "latency_ms": tb.rng.randint(250, 430)},
                tb.rng.randint(250, 420),
                loop_iteration,
                "success",
            ),
            parent_event_id=belief_evt,
            tags=["loop", "repeated_tool"],
            loop_iteration=loop_iteration,
            loop_group=loop_group,
            repeated_pattern_id=repeated_pattern_id,
        )
        parent = tool_evt

    error_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="error_event",
        payload=error_payload(
            "reasoning_loop",
            "research_agent",
            "medium",
            "Repeated reasoning/tool cycle without state progress",
            ["planner_agent"],
            True,
            expected_but_missing=["state_progress", "new_evidence", "verification_handoff"],
        ),
        status="warning",
        parent_event_id=parent,
        tags=["loop", "warning"],
        loop_group=loop_group,
        repeated_pattern_id=repeated_pattern_id,
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "reasoning_loop",
            "Run looped through repeated checks without meaningful progress",
            round(tb.rng.uniform(0.70, 0.82), 2),
            False,
        ),
        status="failure",
        parent_event_id=error_evt,
        tags=["final", "failure", "reasoning_loop"],
    )


def build_wrong_tool_chosen(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "inventory-db latency regression")
    wrong_call, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "cache_debugger",
            {"service": "inventory-db"},
            {"status": "not_applicable", "details": "cache layer not in request path"},
            tb.rng.randint(220, 460),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool", "wrong_tool"],
    )
    err_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="error_event",
        payload=error_payload(
            "wrong_tool_chosen",
            "research_agent",
            "high",
            "Incorrect tool selected for DB latency investigation",
            ["planner_agent"],
            True,
            expected_but_missing=["monitoring_api_call", "log_analyzer_call"],
        ),
        status="failure",
        parent_event_id=wrong_call,
        tags=["wrong_tool", "error"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "wrong_tool_chosen",
            "Diagnosis failed due to selecting an irrelevant tool",
            round(tb.rng.uniform(0.62, 0.80), 2),
            False,
        ),
        status="failure",
        parent_event_id=err_evt,
        tags=["final", "failure", "wrong_tool"],
    )


def build_missing_tool_use(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "search-index delay")
    shortcut_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Search-index delay likely due infrastructure saturation"],
            ["No direct tool evidence"],
            round(tb.rng.uniform(0.55, 0.78), 2),
            "finalize_guess",
            "assumption_only",
            "Skipped all expected tool invocations",
        ),
        parent_event_id=research_start,
        tags=["missing_tool", "assumption"],
    )
    err_evt, _ = tb.add_event(
        agent_id="planner_agent",
        event_type="error_event",
        payload=error_payload(
            "missing_tool",
            "planner_agent",
            "high",
            "Expected tools were never invoked",
            ["reporting_layer"],
            True,
            expected_but_missing=["monitoring_api_call", "log_analyzer_call", "verification_step"],
        ),
        status="failure",
        parent_event_id=shortcut_evt,
        tags=["missing_tool", "error"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "missing_tool",
            "Run concluded without invoking required tools",
            round(tb.rng.uniform(0.64, 0.80), 2),
            False,
        ),
        status="failure",
        parent_event_id=err_evt,
        tags=["final", "failure", "missing_tool"],
    )


def build_stale_belief_used(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "billing queue backlog")
    tool_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "billing-queue"},
            {"status": "healthy", "queue_depth": "normal"},
            tb.rng.randint(220, 380),
            0,
            "success",
        ),
        parent_event_id=research_start,
        tags=["tool"],
    )
    stale_evt, _ = tb.add_event(
        agent_id="research_agent",
        event_type="belief_update",
        payload=belief_payload(
            ["Queue is still overloaded from last incident"],
            ["Old on-call note from yesterday"],
            round(tb.rng.uniform(0.50, 0.70), 2),
            "finalize_old_hypothesis",
            "stale_memory",
            "Used stale context over fresh telemetry",
        ),
        parent_event_id=tool_evt,
        tags=["stale_belief"],
    )
    err_evt, _ = tb.add_event(
        agent_id="verifier_agent",
        event_type="error_event",
        payload=error_payload(
            "stale_belief_used",
            "verifier_agent",
            "high",
            "Conclusion used stale belief despite contradictory fresh evidence",
            ["planner_agent"],
            True,
            expected_but_missing=["fresh_evidence_reconciliation"],
        ),
        status="failure",
        parent_event_id=stale_evt,
        tags=["stale_belief", "error"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "stale_belief_used",
            "Stale belief overrode current evidence",
            round(tb.rng.uniform(0.60, 0.78), 2),
            False,
        ),
        status="failure",
        parent_event_id=err_evt,
        tags=["final", "failure", "stale_belief"],
    )


def build_partial_recovery(tb: TraceBuilder) -> None:
    _, _, research_start = add_common_start(tb, "checkout write path instability")
    timeout_evt, cause = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "checkout-write"},
            {"error": "request_timeout"},
            tb.rng.randint(1300, 2800),
            0,
            "timeout",
        ),
        status="warning",
        parent_event_id=research_start,
        tags=["partial_recovery", "timeout"],
    )
    retry_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="tool_call",
        payload=tool_payload(
            "monitoring_api",
            {"service": "checkout-write"},
            {"status": "degraded", "latency_ms": tb.rng.randint(680, 980)},
            tb.rng.randint(260, 520),
            1,
            "success",
            backoff_ms=300,
        ),
        parent_event_id=timeout_evt,
        causal_id=cause,
        tags=["partial_recovery", "retry_success"],
    )
    new_err_evt, _ = tb.add_event(
        agent_id="tool_agent",
        event_type="error_event",
        payload=error_payload(
            "downstream_dependency_error",
            "log_analyzer",
            "high",
            "Secondary dependency failed after initial recovery",
            ["research_agent", "planner_agent"],
            True,
            expected_but_missing=["full_reverification", "second_recovery_plan"],
        ),
        status="failure",
        parent_event_id=retry_evt,
        tags=["partial_recovery", "new_failure"],
    )
    tb.add_event(
        agent_id="planner_agent",
        event_type="final_outcome",
        payload=final_payload(
            "failure",
            "partial_recovery",
            "Run recovered from first issue but failed on downstream dependency",
            round(tb.rng.uniform(0.58, 0.76), 2),
            False,
        ),
        status="failure",
        parent_event_id=new_err_evt,
        tags=["final", "failure", "partial_recovery"],
    )


RUN_BUILDERS = {
    "success": build_success,
    "timeout_failure": build_timeout_failure,
    "retry_success": build_retry_success,
    "conflict_ignored": build_conflict_ignored,
    "conflict_resolved": build_conflict_resolved,
    "missing_verification": build_missing_verification,
    "early_quit": build_early_quit,
    "reasoning_loop": build_reasoning_loop,
    "wrong_tool_chosen": build_wrong_tool_chosen,
    "missing_tool_use": build_missing_tool_use,
    "stale_belief_used": build_stale_belief_used,
    "partial_recovery": build_partial_recovery,
}


def default_specs_for_profile(profile: str) -> list[RunSpec]:
    if profile == "small":
        return [RunSpec(run_type, start, 1) for run_type, start, _ in DEFAULT_RUN_SPECS]
    return [RunSpec(run_type, start, count) for run_type, start, count in DEFAULT_RUN_SPECS]


def load_counts_override(path: Path) -> dict[str, int]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("--counts-file must be a JSON object {run_type: count}")
    parsed: dict[str, int] = {}
    for key, value in raw.items():
        if key not in RUN_BUILDERS:
            raise ValueError(f"Unknown run_type in counts file: {key}")
        if not isinstance(value, int) or value < 0:
            raise ValueError(f"Invalid count for {key}: {value}")
        parsed[key] = value
    return parsed


def apply_counts_override(specs: list[RunSpec], override: dict[str, int]) -> list[RunSpec]:
    by_type = {spec.run_type: spec for spec in specs}
    for run_type, count in override.items():
        if run_type in by_type:
            by_type[run_type].count = count
        else:
            start = max(spec.start + spec.count for spec in specs) + 1
            specs.append(RunSpec(run_type, start, count))
    return [spec for spec in specs if spec.count > 0]


def summarize_run(events: list[dict[str, Any]]) -> tuple[str, str | None, str | None, str | None]:
    outcome = "unknown"
    failure_category: str | None = None
    root_cause_event_id = None
    first_failure_event_id = None

    if events:
        root_cause_event_id = events[-1].get("root_cause_event_id")
        first_failure_event_id = events[-1].get("first_failure_event_id")

    for event in reversed(events):
        if event.get("event_type") == "final_outcome":
            payload = event.get("payload", {})
            if isinstance(payload, dict):
                outcome = str(payload.get("outcome", "unknown"))
                failure_category = payload.get("failure_category")
            break

    return outcome, failure_category, root_cause_event_id, first_failure_event_id


def validate_write_paths(out_dir: Path, manifest_file: Path, overwrite: bool) -> None:
    if overwrite:
        return
    existing_runs = list(out_dir.glob("run_*.jsonl"))
    if existing_runs:
        raise FileExistsError(
            f"Output directory already contains run files ({len(existing_runs)}). "
            "Use --overwrite to regenerate."
        )
    if manifest_file.exists():
        raise FileExistsError(f"Manifest already exists: {manifest_file}. Use --overwrite to regenerate.")


def generate_dataset(args: argparse.Namespace) -> int:
    rng = random.Random(args.seed)
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    specs = default_specs_for_profile(args.profile)
    if args.counts_file is not None:
        counts_override = load_counts_override(args.counts_file)
        specs = apply_counts_override(specs, counts_override)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    args.manifest_file.parent.mkdir(parents=True, exist_ok=True)

    validate_write_paths(args.out_dir, args.manifest_file, args.overwrite)

    if args.overwrite:
        for path in args.out_dir.glob("run_*.jsonl"):
            path.unlink()
        if args.manifest_file.exists():
            args.manifest_file.unlink()

    manifest_rows: list[dict[str, Any]] = []
    for run_type, start, count in [(spec.run_type, spec.start, spec.count) for spec in specs]:
        builder = RUN_BUILDERS[run_type]
        for index in range(count):
            run_number = start + index
            run_id = f"run_{run_type}_{run_number:03d}"
            trace = TraceBuilder(
                run_id=run_id,
                run_type=run_type,
                start_time=BASE_TIME + timedelta(minutes=run_number * 5),
                scenario=f"Synthetic scenario for {run_type}",
                expected_workflow=workflow_meta(),
                dataset_name=args.dataset_name,
                generated_at=generated_at,
                rng=rng,
            )
            builder(trace)
            file_path = trace.save(args.out_dir)

            outcome, failure_category, root_cause_event_id, first_failure_event_id = summarize_run(trace.events)
            manifest_rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "generator_version": GENERATOR_VERSION,
                    "dataset_name": args.dataset_name,
                    "generated_at": generated_at,
                    "run_id": run_id,
                    "run_type": run_type,
                    "scenario": trace.scenario,
                    "outcome": outcome,
                    "failure_category": failure_category,
                    "root_cause_event_id": root_cause_event_id,
                    "first_failure_event_id": first_failure_event_id,
                    "event_count": len(trace.events),
                    "expected_workflow": trace.expected_workflow,
                    "file": file_path.name,
                }
            )

    manifest_rows.sort(key=lambda row: str(row["run_id"]))
    with args.manifest_file.open("w", encoding="utf-8") as handle:
        for row in manifest_rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")

    print(f"[OK] Generated runs: {len(manifest_rows)}")
    print(f"[OK] Output dir: {args.out_dir}")
    print(f"[OK] Manifest: {args.manifest_file}")
    print(f"[OK] Dataset: {args.dataset_name}")
    print(f"[OK] Schema version: {SCHEMA_VERSION} | Generator version: {GENERATOR_VERSION}")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return generate_dataset(args)
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
