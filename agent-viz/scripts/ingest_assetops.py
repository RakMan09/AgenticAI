#!/usr/bin/env python3
"""Ingest OpenClaw session traces, canonical event-level traces, and legacy traces into DuckDB."""

from __future__ import annotations

import argparse
import hashlib
import re
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

TRACE_EXTENSIONS = {".json", ".jsonl"}
OUTCOME_VALUES = {"success", "fail", "unknown"}

HEURISTIC_THRESHOLD_CONFIG = {
    "loop_min_repetitions": 3,
    "retry_min_count": 1,
    "low_event_count_early_quit": 6,
    "expected_stage_min_coverage": 0.6,
}

RUN_TYPE_TO_LABELS: dict[str, list[str]] = {
    "reasoning_loop": ["loop"],
    "wrong_tool_chosen": ["wrong_tool_chosen", "tool_misuse"],
    "missing_tool_use": ["missing_tool"],
    "missing_verification": ["missing_verification_step"],
    "early_quit": ["early_quit"],
    "timeout_failure": ["timeout_failure"],
    "conflict_ignored": ["conflict_ignored"],
    "stale_belief_used": ["stale_belief_used"],
    "partial_recovery": ["partial_recovery"],
    "retry_success": ["repeated_retry"],
}


@dataclass
class RawEvent:
    schema_version: str | None
    generator_version: str | None
    dataset_name: str | None
    generated_at: str | None
    run_id: str
    run_type: str | None
    scenario: str | None
    expected_workflow: str | None
    event_id: str
    parent_event_id: str | None
    causal_id: str | None
    timestamp: str | None
    step_index: int
    agent_id: str | None
    event_type: str
    status: str | None
    root_cause_event_id: str | None
    first_failure_event_id: str | None
    propagated_to_event_ids: str | None
    tags: str | None
    payload: str | None
    loop_iteration: int | None
    loop_group: str | None
    repeated_pattern_id: str | None
    source_file: str


@dataclass
class DerivedRun:
    run_id: str
    source: str
    dataset_name: str | None
    run_type: str | None
    task_id: str | None
    scenario: str | None
    outcome: str
    failure_category: str | None
    num_steps: int
    first_error_step: int | None
    first_failure_event_id: str | None
    root_cause_event_id: str | None
    expected_workflow: str | None
    started_at: str | None
    ended_at: str | None
    metadata: str


@dataclass
class DerivedStep:
    run_id: str
    event_id: str | None
    step_idx: int
    event_type: str
    step_type: str
    agent_id: str | None
    text: str | None
    tool_name: str | None
    tool_input: str | None
    tool_output: str | None
    error_flag: bool
    error_type: str | None
    latency_ms: int | None
    retry_count: int | None
    timestamp: str | None
    parent_event_id: str | None
    causal_id: str | None
    tags: str | None
    status: str | None
    inferred_intent: str | None
    intended_next_action: str | None
    evidence_summary: str | None
    source_file: str


@dataclass
class Annotation:
    run_id: str
    step_idx: int | None
    event_id: str | None
    label_type: str
    label: str
    confidence: float | None
    reason_payload: str | None
    source: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Project 1 traces into DuckDB")
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("dataset"),
        help="Canonical raw run directory (OpenClaw session JSONL by default).",
    )
    parser.add_argument(
        "--manifest-file",
        type=Path,
        default=Path("data/manifests/all_runs_manifest.jsonl"),
        help="Optional manifest for canonical runs.",
    )
    parser.add_argument(
        "--legacy-dir",
        type=Path,
        default=Path("data/raw/legacy"),
        help="Legacy compatibility raw data directory.",
    )
    parser.add_argument("--db-path", type=Path, default=Path("db/traces.duckdb"))
    parser.add_argument("--normalized-out", type=Path, default=Path("data/normalized/traces.jsonl"))
    parser.add_argument("--include-legacy", action="store_true", help="Ingest legacy raw files in addition to canonical runs.")
    return parser.parse_args()


def safe_json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=True)
    except Exception:
        return json.dumps(str(value), ensure_ascii=True)


def shorten_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    if isinstance(value, (dict, list)):
        return safe_json_dumps(value)
    return str(value)


def parse_possible_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if value > 10_000_000_000:
                dt = datetime.fromtimestamp(value / 1000.0)
            else:
                dt = datetime.fromtimestamp(value)
            return dt.isoformat()
        except Exception:
            return None
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        try:
            if candidate.endswith("Z"):
                candidate = candidate[:-1] + "+00:00"
            return datetime.fromisoformat(candidate).isoformat()
        except Exception:
            return candidate
    return None


def hash_run_id(file_path: Path, idx: int) -> str:
    digest = hashlib.sha1(f"{file_path.as_posix()}::{idx}".encode("utf-8")).hexdigest()[:12]
    return f"run_{digest}"


def read_jsonl_dicts(path: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            results.append(obj)
    return results


def parse_manifest(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    by_run: dict[str, dict[str, Any]] = {}
    for row in read_jsonl_dicts(path):
        run_id = row.get("run_id")
        if isinstance(run_id, str):
            by_run[run_id] = row
    return by_run


def is_event_level_row(row: dict[str, Any]) -> bool:
    return all(key in row for key in ("run_id", "event_id", "event_type", "payload"))


def is_openclaw_session_rows(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    has_message_rows = any(
        row.get("type") == "message" and isinstance(row.get("message"), dict)
        for row in rows
    )
    has_session_rows = any(row.get("type") in {"session", "model_change", "thinking_level_change"} for row in rows)
    return has_message_rows and has_session_rows


def extract_text_from_content(content: Any) -> str | None:
    if isinstance(content, str):
        cleaned = content.strip()
        return cleaned or None
    if not isinstance(content, list):
        return None
    chunks: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type", "")).strip().lower()
        if item_type == "text":
            text_value = shorten_text(item.get("text"))
            if text_value:
                chunks.append(text_value)
        elif item_type == "thinking":
            thinking_value = shorten_text(item.get("thinking"))
            if thinking_value:
                chunks.append(thinking_value)
    if not chunks:
        return None
    return "\n".join(chunks).strip() or None


def cleaned_user_prompt(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = text
    cleaned = re.sub(
        r"Sender \(untrusted metadata\):\s*```json.*?```",
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    cleaned = re.sub(
        r"System \(untrusted\):.*?\n\n",
        "",
        cleaned,
        flags=re.DOTALL | re.IGNORECASE,
    )
    cleaned = cleaned.replace("```", "")
    cleaned = re.sub(r"^\[[^\]]+\]\s*", "", cleaned.strip())
    cleaned = cleaned.strip()
    return cleaned or text.strip()


def short_scenario_text(text: str | None, max_len: int = 180) -> str | None:
    cleaned = cleaned_user_prompt(text)
    if not cleaned:
        return None
    normalized = " ".join(cleaned.split())
    return normalized[:max_len]


def parse_tool_output_from_message(message_obj: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    details = message_obj.get("details")
    if isinstance(details, dict):
        summary = details.get("error") or details.get("status") or details.get("title")
        return shorten_text(summary), details
    text_payload = extract_text_from_content(message_obj.get("content"))
    if not text_payload:
        return None, None
    try:
        parsed = json.loads(text_payload)
        if isinstance(parsed, dict):
            summary = parsed.get("error") or parsed.get("status") or parsed.get("title")
            return shorten_text(summary), parsed
    except json.JSONDecodeError:
        pass
    return text_payload[:500], {"text": text_payload}


def infer_openclaw_outcome(
    has_assistant_stop: bool,
    has_assistant_error: bool,
    has_tool_error: bool,
    assistant_final_text: list[str],
) -> str:
    if has_assistant_error:
        return "fail"
    if has_assistant_stop:
        joined = " ".join(assistant_final_text).lower()
        if any(token in joined for token in ("failed", "cannot", "can't", "unable", "error")):
            return "fail"
        return "success"
    if has_tool_error:
        return "fail"
    return "unknown"


def infer_openclaw_failure_category(
    outcome: str,
    all_steps: list[DerivedStep],
    has_timeout: bool,
    has_missing_file: bool,
    has_auth_error: bool,
) -> str | None:
    if outcome != "fail":
        if any(step.error_flag for step in all_steps):
            return "partial_recovery"
        return None
    if has_auth_error:
        return "tool_misuse"
    if has_timeout:
        return "timeout_failure"
    if has_missing_file:
        return "missing_tool"
    retry_events = [step for step in all_steps if (step.retry_count or 0) > 0]
    if retry_events:
        return "repeated_retry"
    return "unknown_failure_pattern"


def derive_from_openclaw_session_rows(
    rows: list[dict[str, Any]],
    source_file: str,
    file_path: Path,
) -> list[tuple[DerivedRun, list[DerivedStep], list[RawEvent], list[Annotation]]]:
    message_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("type") == "message" and isinstance(row.get("message"), dict):
            message_rows.append(row)

    if not message_rows:
        return []

    user_turn_indices = [
        index
        for index, row in enumerate(message_rows)
        if str(row.get("message", {}).get("role", "")).lower() == "user"
    ]
    if not user_turn_indices:
        return []

    session_id = next((shorten_text(row.get("id")) for row in rows if row.get("type") == "session"), None)
    dataset_name = "openclaw_session_traces_v1"
    outputs: list[tuple[DerivedRun, list[DerivedStep], list[RawEvent], list[Annotation]]] = []

    for turn_idx, turn_start in enumerate(user_turn_indices):
        turn_end = user_turn_indices[turn_idx + 1] if turn_idx + 1 < len(user_turn_indices) else len(message_rows)
        turn_messages = message_rows[turn_start:turn_end]
        if not turn_messages:
            continue

        user_row = turn_messages[0]
        user_message_obj = user_row.get("message", {})
        user_text = extract_text_from_content(user_message_obj.get("content"))
        scenario = short_scenario_text(user_text)
        run_seed = f"{file_path.as_posix()}::{turn_idx}::{user_row.get('id', '')}"
        run_id = f"run_openclaw_{hashlib.sha1(run_seed.encode('utf-8')).hexdigest()[:12]}"

        derived_steps: list[DerivedStep] = []
        derived_raw_events: list[RawEvent] = []
        tool_retry_counter: Counter[str] = Counter()
        tool_call_event_by_id: dict[str, str] = {}
        has_assistant_stop = False
        has_assistant_error = False
        has_tool_error = False
        has_timeout = False
        has_missing_file = False
        has_auth_error = False
        assistant_final_text: list[str] = []
        step_idx = 0

        def append_step(
            *,
            row_id: str,
            timestamp: str | None,
            event_type: str,
            step_type: str,
            text: str | None,
            tool_name: str | None = None,
            tool_input: Any = None,
            tool_output: Any = None,
            error_flag: bool = False,
            error_type: str | None = None,
            latency_ms: int | None = None,
            retry_count: int | None = None,
            parent_event_id: str | None = None,
            status: str | None = None,
            inferred_intent: str | None = None,
            intended_next_action: str | None = None,
            tags: list[str] | None = None,
            payload_for_raw_event: dict[str, Any] | None = None,
        ) -> None:
            nonlocal step_idx
            event_id = f"evt_{step_idx:04d}"
            tag_list = tags if isinstance(tags, list) else []
            tags_json = safe_json_dumps(tag_list) or "[]"
            tool_input_json = safe_json_dumps(tool_input)
            tool_output_json = safe_json_dumps(tool_output)
            step = DerivedStep(
                run_id=run_id,
                event_id=event_id,
                step_idx=step_idx,
                event_type=event_type,
                step_type=step_type,
                agent_id=None,
                text=text,
                tool_name=tool_name,
                tool_input=tool_input_json,
                tool_output=tool_output_json,
                error_flag=error_flag,
                error_type=error_type,
                latency_ms=latency_ms,
                retry_count=retry_count,
                timestamp=timestamp,
                parent_event_id=parent_event_id,
                causal_id=row_id,
                tags=tags_json,
                status=status,
                inferred_intent=inferred_intent,
                intended_next_action=intended_next_action,
                evidence_summary=None,
                source_file=source_file,
            )
            derived_steps.append(step)
            raw_event_payload = payload_for_raw_event or {}
            if text:
                raw_event_payload.setdefault("summary", text[:1000])
            if tool_name:
                raw_event_payload.setdefault("tool_name", tool_name)
            if tool_input is not None:
                raw_event_payload.setdefault("input", tool_input)
            if tool_output is not None:
                raw_event_payload.setdefault("output", tool_output)
            if error_type:
                raw_event_payload.setdefault("error_type", error_type)
            if latency_ms is not None:
                raw_event_payload.setdefault("latency_ms", latency_ms)
            if retry_count is not None:
                raw_event_payload.setdefault("retry_count", retry_count)
            raw_event = RawEvent(
                schema_version="openclaw-session-v1",
                generator_version=None,
                dataset_name=dataset_name,
                generated_at=None,
                run_id=run_id,
                run_type="openclaw_turn",
                scenario=scenario,
                expected_workflow=None,
                event_id=event_id,
                parent_event_id=parent_event_id,
                causal_id=row_id,
                timestamp=timestamp,
                step_index=step_idx,
                agent_id=None,
                event_type=event_type,
                status=status,
                root_cause_event_id=None,
                first_failure_event_id=None,
                propagated_to_event_ids=safe_json_dumps([]),
                tags=tags_json,
                payload=safe_json_dumps(raw_event_payload),
                loop_iteration=None,
                loop_group=None,
                repeated_pattern_id=None,
                source_file=source_file,
            )
            derived_raw_events.append(raw_event)
            step_idx += 1

        for message_row in turn_messages:
            row_id = shorten_text(message_row.get("id")) or f"row_{step_idx}"
            timestamp = parse_possible_timestamp(message_row.get("timestamp"))
            message_obj = message_row.get("message", {})
            role = str(message_obj.get("role", "")).lower()

            if role == "user":
                prompt_text = cleaned_user_prompt(extract_text_from_content(message_obj.get("content")))
                append_step(
                    row_id=row_id,
                    timestamp=timestamp,
                    event_type="user_message",
                    step_type="thought",
                    text=prompt_text,
                    status="input",
                    tags=["openclaw", "user_turn"],
                    payload_for_raw_event={"role": "user"},
                )
                continue

            if role == "assistant":
                stop_reason = shorten_text(message_obj.get("stopReason"))
                error_message = shorten_text(message_obj.get("errorMessage"))
                content = message_obj.get("content")
                if stop_reason == "stop":
                    has_assistant_stop = True

                if isinstance(content, list):
                    for item_idx, item in enumerate(content):
                        if not isinstance(item, dict):
                            continue
                        item_type = str(item.get("type", "")).lower()
                        row_part_id = f"{row_id}#{item_idx}"
                        if item_type == "thinking":
                            append_step(
                                row_id=row_part_id,
                                timestamp=timestamp,
                                event_type="assistant_thinking",
                                step_type="thought",
                                text=shorten_text(item.get("thinking")),
                                status=stop_reason,
                                tags=["openclaw", "assistant", "thinking"],
                                payload_for_raw_event={"role": "assistant"},
                            )
                        elif item_type == "toolcall":
                            tool_name = shorten_text(item.get("name"))
                            retry_count = None
                            if tool_name:
                                retry_count = tool_retry_counter[tool_name]
                                tool_retry_counter[tool_name] += 1
                            append_step(
                                row_id=row_part_id,
                                timestamp=timestamp,
                                event_type="tool_call",
                                step_type="tool_call",
                                text=f"Tool call {tool_name or 'unknown'}",
                                tool_name=tool_name,
                                tool_input=item.get("arguments"),
                                status="requested",
                                retry_count=retry_count,
                                tags=["openclaw", "assistant", "tool_call"],
                                payload_for_raw_event={"role": "assistant", "tool_call_id": item.get("id")},
                            )
                            tool_call_id = shorten_text(item.get("id"))
                            if tool_call_id:
                                tool_call_event_by_id[tool_call_id] = f"evt_{step_idx - 1:04d}"
                        elif item_type == "text":
                            text_value = shorten_text(item.get("text"))
                            if text_value:
                                assistant_final_text.append(text_value)
                            append_step(
                                row_id=row_part_id,
                                timestamp=timestamp,
                                event_type="assistant_message",
                                step_type="action",
                                text=text_value,
                                status=stop_reason,
                                tags=["openclaw", "assistant", "text"],
                                payload_for_raw_event={"role": "assistant"},
                            )
                        else:
                            append_step(
                                row_id=row_part_id,
                                timestamp=timestamp,
                                event_type="assistant_message",
                                step_type="unknown",
                                text=shorten_text(item),
                                status=stop_reason,
                                tags=["openclaw", "assistant", "unknown_content_type"],
                                payload_for_raw_event={"role": "assistant", "content_type": item_type},
                            )

                if error_message:
                    has_assistant_error = True
                    if "api key" in error_message.lower() or "401" in error_message.lower():
                        has_auth_error = True
                    append_step(
                        row_id=row_id,
                        timestamp=timestamp,
                        event_type="assistant_error",
                        step_type="observation",
                        text=error_message,
                        error_flag=True,
                        error_type="assistant_error",
                        status="error",
                        tags=["openclaw", "assistant", "error"],
                        payload_for_raw_event={"role": "assistant", "stop_reason": stop_reason},
                    )
                continue

            if role == "toolresult":
                tool_name = shorten_text(message_obj.get("toolName"))
                summary, output_obj = parse_tool_output_from_message(message_obj)
                details = message_obj.get("details") if isinstance(message_obj.get("details"), dict) else {}
                tool_call_id = shorten_text(message_obj.get("toolCallId"))
                parent_event_id = tool_call_event_by_id.get(tool_call_id) if tool_call_id else None
                status = shorten_text(details.get("status")) or ("error" if bool(message_obj.get("isError")) else "success")
                latency_val = details.get("tookMs")
                latency_ms = int(latency_val) if isinstance(latency_val, int) else None
                error_type = shorten_text(details.get("error"))
                output_text = summary or f"Tool result {tool_name or 'unknown'} ({status or 'unknown'})"
                error_flag = (
                    status is not None
                    and status.lower() in {"error", "failed", "failure", "timeout"}
                ) or bool(error_type)
                if error_flag:
                    has_tool_error = True
                low_bundle = f"{output_text} {safe_json_dumps(output_obj) or ''}".lower()
                if "timeout" in low_bundle:
                    has_timeout = True
                if "enoent" in low_bundle or "no such file" in low_bundle:
                    has_missing_file = True
                if "api key" in low_bundle or "incorrect api key" in low_bundle or "401" in low_bundle:
                    has_auth_error = True
                append_step(
                    row_id=row_id,
                    timestamp=timestamp,
                    event_type="tool_result",
                    step_type="observation",
                    text=output_text,
                    tool_name=tool_name,
                    tool_output=output_obj,
                    error_flag=error_flag,
                    error_type=error_type,
                    latency_ms=latency_ms,
                    parent_event_id=parent_event_id,
                    status=status,
                    tags=["openclaw", "tool_result"],
                    payload_for_raw_event={"role": "toolResult", "tool_call_id": tool_call_id},
                )
                continue

            # Keep unknown message roles visible in trace timeline.
            append_step(
                row_id=row_id,
                timestamp=timestamp,
                event_type="message",
                step_type="unknown",
                text=extract_text_from_content(message_obj.get("content")) or shorten_text(message_obj),
                status=shorten_text(message_obj.get("status")),
                tags=["openclaw", "unknown_role"],
                payload_for_raw_event={"role": role},
            )

        if not derived_steps:
            continue

        outcome = infer_openclaw_outcome(
            has_assistant_stop=has_assistant_stop,
            has_assistant_error=has_assistant_error,
            has_tool_error=has_tool_error,
            assistant_final_text=assistant_final_text,
        )
        failure_category = infer_openclaw_failure_category(
            outcome=outcome,
            all_steps=derived_steps,
            has_timeout=has_timeout,
            has_missing_file=has_missing_file,
            has_auth_error=has_auth_error,
        )
        first_error_step = next((step.step_idx for step in derived_steps if step.error_flag), None)
        first_failure_event_id = next((step.event_id for step in derived_steps if step.error_flag), None)
        root_cause_event_id = first_failure_event_id
        started_at = derived_steps[0].timestamp
        ended_at = derived_steps[-1].timestamp
        run = DerivedRun(
            run_id=run_id,
            source="openclaw_real",
            dataset_name=dataset_name,
            run_type="openclaw_turn",
            task_id=f"openclaw_turn_{turn_idx + 1}",
            scenario=scenario,
            outcome=outcome,
            failure_category=failure_category,
            num_steps=len(derived_steps),
            first_error_step=first_error_step,
            first_failure_event_id=first_failure_event_id,
            root_cause_event_id=root_cause_event_id,
            expected_workflow=None,
            started_at=started_at,
            ended_at=ended_at,
            metadata=safe_json_dumps(
                {
                    "source_file": source_file,
                    "session_id": session_id,
                    "user_message_id": user_row.get("id"),
                    "user_message_text": scenario,
                    "openclaw_format": "session_jsonl_v3",
                }
            )
            or "{}",
        )
        annotations = build_labels_for_run(run, derived_raw_events, derived_steps, failure_category)
        outputs.append((run, derived_steps, derived_raw_events, annotations))

    return outputs


def extract_event_type_to_step_type(event_type: str, tool_name: str | None) -> str:
    low = (event_type or "").lower()
    if low == "belief_update":
        return "thought"
    if low == "tool_call" or tool_name:
        return "tool_call"
    if low in {"message", "action"}:
        return "action"
    if low in {"verification", "error_event", "final_outcome"}:
        return "observation"
    return "unknown"


def event_text_summary(event_type: str, payload: dict[str, Any] | None) -> str | None:
    payload = payload or {}
    if event_type == "belief_update":
        claims = payload.get("claims")
        if isinstance(claims, list) and claims:
            return "; ".join(str(item) for item in claims[:2])
        return shorten_text(payload)
    if event_type == "tool_call":
        tool_name = payload.get("tool_name")
        tool_status = payload.get("tool_status")
        return f"Tool call {tool_name or 'unknown'} ({tool_status or 'unknown'})"
    if event_type == "message":
        return shorten_text(payload.get("content_summary"))
    if event_type == "verification":
        verdict = payload.get("verdict")
        checked_claim = payload.get("checked_claim")
        return f"Verification: {verdict or 'unknown'} ({checked_claim or 'n/a'})"
    if event_type == "error_event":
        return shorten_text(payload.get("description") or payload.get("error_type"))
    if event_type == "final_outcome":
        return shorten_text(payload.get("summary"))
    return shorten_text(payload)


def to_raw_event(obj: dict[str, Any], source_file: str) -> RawEvent:
    payload = obj.get("payload") if isinstance(obj.get("payload"), dict) else {}
    propagated = obj.get("propagated_to_event_ids")
    tags = obj.get("tags")
    return RawEvent(
        schema_version=shorten_text(obj.get("schema_version")),
        generator_version=shorten_text(obj.get("generator_version")),
        dataset_name=shorten_text(obj.get("dataset_name")),
        generated_at=parse_possible_timestamp(obj.get("generated_at")),
        run_id=str(obj["run_id"]),
        run_type=shorten_text(obj.get("run_type")),
        scenario=shorten_text(obj.get("scenario")),
        expected_workflow=safe_json_dumps(obj.get("expected_workflow")),
        event_id=str(obj["event_id"]),
        parent_event_id=shorten_text(obj.get("parent_event_id")),
        causal_id=shorten_text(obj.get("causal_id")),
        timestamp=parse_possible_timestamp(obj.get("timestamp")),
        step_index=int(obj.get("step_index", 0)),
        agent_id=shorten_text(obj.get("agent_id")),
        event_type=str(obj.get("event_type", "unknown")),
        status=shorten_text(obj.get("status")),
        root_cause_event_id=shorten_text(obj.get("root_cause_event_id")),
        first_failure_event_id=shorten_text(obj.get("first_failure_event_id")),
        propagated_to_event_ids=safe_json_dumps(propagated if isinstance(propagated, list) else []),
        tags=safe_json_dumps(tags if isinstance(tags, list) else []),
        payload=safe_json_dumps(payload),
        loop_iteration=int(obj["loop_iteration"]) if isinstance(obj.get("loop_iteration"), int) else None,
        loop_group=shorten_text(obj.get("loop_group")),
        repeated_pattern_id=shorten_text(obj.get("repeated_pattern_id")),
        source_file=source_file,
    )


def derive_from_event_rows(
    rows: list[dict[str, Any]],
    source_file: str,
    manifest_row: dict[str, Any] | None = None,
) -> tuple[DerivedRun, list[DerivedStep], list[RawEvent], list[Annotation]]:
    raw_events = [to_raw_event(row, source_file) for row in rows]
    raw_events.sort(key=lambda event: (event.step_index, event.timestamp or "", event.event_id))

    if not raw_events:
        raise ValueError("empty event run")

    run_id = raw_events[0].run_id
    run_type = raw_events[0].run_type or (shorten_text(manifest_row.get("run_type")) if manifest_row else None)
    dataset_name = raw_events[0].dataset_name or (shorten_text(manifest_row.get("dataset_name")) if manifest_row else None)
    scenario = raw_events[0].scenario or (shorten_text(manifest_row.get("scenario")) if manifest_row else None)
    expected_workflow = raw_events[0].expected_workflow or (safe_json_dumps(manifest_row.get("expected_workflow")) if manifest_row else None)
    root_cause_event_id = raw_events[-1].root_cause_event_id or (shorten_text(manifest_row.get("root_cause_event_id")) if manifest_row else None)
    first_failure_event_id = raw_events[-1].first_failure_event_id or (
        shorten_text(manifest_row.get("first_failure_event_id")) if manifest_row else None
    )

    started_at = raw_events[0].timestamp
    ended_at = raw_events[-1].timestamp

    outcome = "unknown"
    failure_category: str | None = shorten_text(manifest_row.get("failure_category")) if manifest_row else None
    for event in reversed(raw_events):
        if event.event_type == "final_outcome":
            payload = json.loads(event.payload or "{}")
            if isinstance(payload, dict):
                raw_outcome = str(payload.get("outcome", "")).strip().lower()
                if raw_outcome in {"success", "fail", "failure"}:
                    outcome = "success" if raw_outcome == "success" else "fail"
                failure_category = shorten_text(payload.get("failure_category")) or failure_category
            break
    if outcome not in OUTCOME_VALUES:
        outcome = "unknown"

    steps: list[DerivedStep] = []
    for idx, event in enumerate(raw_events):
        payload_obj = json.loads(event.payload or "{}")
        tool_name = shorten_text(payload_obj.get("tool_name")) if isinstance(payload_obj, dict) else None
        tool_input = safe_json_dumps(payload_obj.get("input")) if isinstance(payload_obj, dict) and "input" in payload_obj else None
        tool_output = safe_json_dumps(payload_obj.get("output")) if isinstance(payload_obj, dict) and "output" in payload_obj else None
        error_type = shorten_text(payload_obj.get("error_type")) if isinstance(payload_obj, dict) else None
        latency_ms = int(payload_obj["latency_ms"]) if isinstance(payload_obj, dict) and isinstance(payload_obj.get("latency_ms"), (int, float)) else None
        retry_count = int(payload_obj["retry_count"]) if isinstance(payload_obj, dict) and isinstance(payload_obj.get("retry_count"), (int, float)) else None
        intended_next_action = shorten_text(payload_obj.get("intended_next_action")) if isinstance(payload_obj, dict) else None
        supporting_evidence = payload_obj.get("supporting_evidence") if isinstance(payload_obj, dict) else None
        evidence_summary = (
            "; ".join(str(item) for item in supporting_evidence[:2])
            if isinstance(supporting_evidence, list) and supporting_evidence
            else None
        )
        error_flag = (
            event.event_type in {"error_event"}
            or (event.status or "").lower() in {"failure", "warning", "error", "timeout"}
            or bool(error_type)
        )
        step_type = extract_event_type_to_step_type(event.event_type, tool_name)

        steps.append(
            DerivedStep(
                run_id=run_id,
                event_id=event.event_id,
                step_idx=idx,
                event_type=event.event_type,
                step_type=step_type,
                agent_id=event.agent_id,
                text=event_text_summary(event.event_type, payload_obj if isinstance(payload_obj, dict) else None),
                tool_name=tool_name,
                tool_input=tool_input,
                tool_output=tool_output,
                error_flag=error_flag,
                error_type=error_type,
                latency_ms=latency_ms,
                retry_count=retry_count,
                timestamp=event.timestamp,
                parent_event_id=event.parent_event_id,
                causal_id=event.causal_id,
                tags=event.tags,
                status=event.status,
                inferred_intent=shorten_text(payload_obj.get("trigger_source")) if isinstance(payload_obj, dict) else None,
                intended_next_action=intended_next_action,
                evidence_summary=evidence_summary,
                source_file=source_file,
            )
        )

    if outcome == "unknown":
        if any(step.error_flag for step in steps):
            outcome = "fail"
        elif steps and all((step.status or "").lower() in {"success", "ok", "completed"} for step in steps if step.status):
            outcome = "success"

    source = "canonical_synthetic"
    if any(
        "openclaw" in (event.source_file or "").lower()
        or "openclaw" in (event.dataset_name or "").lower()
        or "openclaw" in (event.tags or "").lower()
        for event in raw_events
    ):
        source = "openclaw_real"

    first_error_step = next((step.step_idx for step in steps if step.error_flag), None)
    run = DerivedRun(
        run_id=run_id,
        source=source,
        dataset_name=dataset_name,
        run_type=run_type,
        task_id=run_type,
        scenario=scenario,
        outcome=outcome,
        failure_category=failure_category,
        num_steps=len(steps),
        first_error_step=first_error_step,
        first_failure_event_id=first_failure_event_id,
        root_cause_event_id=root_cause_event_id,
        expected_workflow=expected_workflow,
        started_at=started_at,
        ended_at=ended_at,
        metadata=safe_json_dumps(
            {
                "source_file": source_file,
                "schema_version": raw_events[0].schema_version,
                "generator_version": raw_events[0].generator_version,
                "generated_at": raw_events[0].generated_at,
            }
        )
        or "{}",
    )

    annotations = build_labels_for_run(run, raw_events, steps, failure_category)
    return run, steps, raw_events, annotations


def build_labels_for_run(
    run: DerivedRun,
    raw_events: list[RawEvent],
    steps: list[DerivedStep],
    provided_failure_category: str | None,
) -> list[Annotation]:
    labels: list[Annotation] = []
    seen: set[tuple[str, str]] = set()

    def add_label(
        label: str,
        *,
        label_type: str,
        confidence: float,
        reason_payload: dict[str, Any],
        event_id: str | None = None,
        step_idx: int | None = None,
        source: str = "heuristics_v1",
    ) -> None:
        key = (label_type, label)
        if key in seen:
            return
        seen.add(key)
        labels.append(
            Annotation(
                run_id=run.run_id,
                step_idx=step_idx,
                event_id=event_id,
                label_type=label_type,
                label=label,
                confidence=confidence,
                reason_payload=safe_json_dumps(reason_payload),
                source=source,
            )
        )

    if provided_failure_category:
        add_label(
            provided_failure_category,
            label_type="provided",
            confidence=1.0,
            reason_payload={"source": "final_outcome_payload"},
            source="canonical_payload",
        )

    if run.run_type in RUN_TYPE_TO_LABELS:
        for label in RUN_TYPE_TO_LABELS[run.run_type]:
            add_label(
                label,
                label_type="taxonomy",
                confidence=0.98,
                reason_payload={"run_type": run.run_type},
                source="run_type_mapping",
            )

    # Loop detector
    loop_count = sum(1 for event in raw_events if event.loop_iteration is not None)
    repeated_pattern_ids = [event.repeated_pattern_id for event in raw_events if event.repeated_pattern_id]
    if loop_count >= HEURISTIC_THRESHOLD_CONFIG["loop_min_repetitions"] or repeated_pattern_ids:
        add_label(
            "loop",
            label_type="heuristic",
            confidence=0.92,
            reason_payload={
                "loop_events": loop_count,
                "repeated_pattern_ids": sorted(set(repeated_pattern_ids)),
            },
        )

    # Tool failure / misuse detector
    bad_tool_events = []
    wrong_tool_events = []
    expected_tools = set()
    if run.expected_workflow:
        workflow_obj = json.loads(run.expected_workflow)
        if isinstance(workflow_obj, dict) and isinstance(workflow_obj.get("expected_tools"), list):
            expected_tools = {str(tool) for tool in workflow_obj["expected_tools"]}
    for step in steps:
        if step.event_type != "tool_call":
            continue
        payload = json.loads(step.tool_output or "{}") if step.tool_output else {}
        status = (step.status or "").lower()
        if status in {"failure", "warning", "timeout"}:
            bad_tool_events.append(step.event_id)
        if step.tool_name and expected_tools and step.tool_name not in expected_tools:
            wrong_tool_events.append(step.event_id)
    if bad_tool_events:
        add_label(
            "tool_misuse",
            label_type="heuristic",
            confidence=0.88,
            reason_payload={"failing_tool_events": bad_tool_events},
        )
    if wrong_tool_events:
        add_label(
            "wrong_tool_chosen",
            label_type="heuristic",
            confidence=0.92,
            reason_payload={"unexpected_tool_events": wrong_tool_events, "expected_tools": sorted(expected_tools)},
        )

    # Early quit detector
    has_verification = any(step.event_type == "verification" for step in steps)
    if run.outcome == "fail" and run.num_steps <= HEURISTIC_THRESHOLD_CONFIG["low_event_count_early_quit"] and not has_verification:
        add_label(
            "early_quit",
            label_type="heuristic",
            confidence=0.85,
            reason_payload={"num_steps": run.num_steps, "has_verification": has_verification},
        )

    # Timeout / retry detector
    retry_steps = [step for step in steps if (step.retry_count or 0) >= HEURISTIC_THRESHOLD_CONFIG["retry_min_count"]]
    timeout_like = [step for step in steps if (step.status or "").lower() in {"timeout", "warning"} and step.event_type == "tool_call"]
    if retry_steps:
        add_label(
            "repeated_retry",
            label_type="heuristic",
            confidence=0.80,
            reason_payload={"retry_event_ids": [step.event_id for step in retry_steps]},
        )
    if timeout_like and run.outcome == "fail":
        add_label(
            "timeout_failure",
            label_type="heuristic",
            confidence=0.86,
            reason_payload={"timeout_event_ids": [step.event_id for step in timeout_like]},
        )
    if timeout_like and not retry_steps:
        add_label(
            "missing_retry_after_failure",
            label_type="heuristic",
            confidence=0.84,
            reason_payload={"timeout_event_ids": [step.event_id for step in timeout_like], "retry_events": 0},
        )

    # Conflict ignored detector
    conflict_errors = [step for step in steps if step.error_type == "conflict_ignored" or "conflict" in (step.text or "").lower()]
    if conflict_errors and run.outcome == "fail":
        add_label(
            "conflict_ignored",
            label_type="heuristic",
            confidence=0.90,
            reason_payload={"event_ids": [step.event_id for step in conflict_errors]},
        )
        has_resolution = any(
            token in (step.text or "").lower() for step in steps for token in ("resolved", "mitigated", "reconciled")
        )
        if not has_resolution:
            add_label(
                "conflict_detected_but_unresolved",
                label_type="heuristic",
                confidence=0.89,
                reason_payload={"conflict_event_ids": [step.event_id for step in conflict_errors], "resolution_events": 0},
            )

    # Missing verification detector
    expected_verification = False
    if run.expected_workflow:
        workflow_obj = json.loads(run.expected_workflow)
        if isinstance(workflow_obj, dict):
            expected_verification = bool(workflow_obj.get("expected_verification_step"))
    if expected_verification and not has_verification:
        add_label(
            "missing_verification_step",
            label_type="heuristic",
            confidence=0.93,
            reason_payload={"expected_verification_step": True, "verification_events": 0},
        )

    # Stale belief detector
    stale_belief_events = [
        step
        for step in steps
        if step.event_type == "belief_update"
        and any(token in (step.text or "").lower() for token in ("stale", "old", "yesterday"))
    ]
    if stale_belief_events:
        add_label(
            "stale_belief_used",
            label_type="heuristic",
            confidence=0.90,
            reason_payload={"event_ids": [step.event_id for step in stale_belief_events]},
        )

    # Partial recovery detector
    if retry_steps and run.outcome == "fail":
        add_label(
            "partial_recovery",
            label_type="heuristic",
            confidence=0.76,
            reason_payload={"retry_events": [step.event_id for step in retry_steps], "final_outcome": run.outcome},
        )

    # Optional absent reasoning detector
    has_reasoning = any(step.event_type == "belief_update" for step in steps)
    if run.outcome == "fail" and not has_reasoning:
        add_label(
            "absent_reasoning_step",
            label_type="heuristic",
            confidence=0.68,
            reason_payload={"belief_update_events": 0},
        )

    # Expected workflow coverage / empty-space detector
    expected_stage_hints: list[str] = []
    if run.expected_workflow:
        workflow_obj = json.loads(run.expected_workflow)
        if isinstance(workflow_obj, dict):
            for key in ("expected_stages", "stages", "workflow_stages"):
                raw_stages = workflow_obj.get(key)
                if isinstance(raw_stages, list):
                    expected_stage_hints = [str(item).strip().lower() for item in raw_stages if str(item).strip()]
                    break
    if expected_stage_hints:
        observed_stage_tokens: set[str] = set()
        for step in steps:
            if step.event_type:
                observed_stage_tokens.add(step.event_type.lower())
            if step.step_type:
                observed_stage_tokens.add(step.step_type.lower())
            if step.tags:
                try:
                    tag_values = json.loads(step.tags)
                except json.JSONDecodeError:
                    tag_values = []
                if isinstance(tag_values, list):
                    for tag in tag_values:
                        if isinstance(tag, str) and tag.strip():
                            observed_stage_tokens.add(tag.strip().lower())
        matched = [stage for stage in expected_stage_hints if stage in observed_stage_tokens]
        coverage = len(matched) / max(1, len(expected_stage_hints))
        if coverage < HEURISTIC_THRESHOLD_CONFIG["expected_stage_min_coverage"]:
            add_label(
                "never_reached_expected_stage",
                label_type="heuristic",
                confidence=0.82,
                reason_payload={
                    "expected_stages": expected_stage_hints,
                    "matched_stages": matched,
                    "coverage": round(coverage, 3),
                },
            )

    # Finalization without evidence consolidation
    finalization_events = [step for step in steps if (step.event_type or "").lower() in {"final", "finalization", "completion"}]
    has_evidence = any((step.evidence_summary or "").strip() for step in steps)
    if finalization_events and not has_evidence:
        add_label(
            "finalization_without_evidence_consolidation",
            label_type="heuristic",
            confidence=0.77,
            reason_payload={"finalization_events": [step.event_id for step in finalization_events], "evidence_summary_events": 0},
        )

    if run.outcome == "fail" and not labels:
        add_label(
            "unknown_failure_pattern",
            label_type="heuristic",
            confidence=0.40,
            reason_payload={"reason": "No detector matched"},
        )
    return labels


def extract_candidate_steps(obj: dict[str, Any]) -> list[Any]:
    for key in ("steps", "trajectory", "events", "messages", "trace", "history"):
        value = obj.get(key)
        if isinstance(value, list):
            return value
    if isinstance(obj.get("run"), dict):
        return extract_candidate_steps(obj["run"])
    return []


def derive_from_legacy_rows(rows: list[dict[str, Any]], source_file: str, file_path: Path) -> list[tuple[DerivedRun, list[DerivedStep], list[RawEvent], list[Annotation]]]:
    outputs: list[tuple[DerivedRun, list[DerivedStep], list[RawEvent], list[Annotation]]] = []
    for obj_idx, obj in enumerate(rows):
        if "run" in obj and isinstance(obj["run"], dict) and isinstance(obj.get("steps"), list):
            run_obj = obj["run"]
            run_id = shorten_text(run_obj.get("run_id")) or hash_run_id(file_path, obj_idx)
            outcome_raw = str(run_obj.get("outcome", "unknown")).lower()
            outcome = "success" if outcome_raw in {"success", "ok"} else "fail" if outcome_raw in {"fail", "failure"} else "unknown"
            step_rows = obj["steps"]
            steps: list[DerivedStep] = []
            for idx, step in enumerate(step_rows):
                if not isinstance(step, dict):
                    continue
                steps.append(
                    DerivedStep(
                        run_id=run_id,
                        event_id=None,
                        step_idx=idx,
                        event_type=str(step.get("step_type", "unknown")),
                        step_type=str(step.get("step_type", "unknown")),
                        agent_id=None,
                        text=shorten_text(step.get("text")),
                        tool_name=shorten_text(step.get("tool_name")),
                        tool_input=safe_json_dumps(step.get("tool_input")),
                        tool_output=safe_json_dumps(step.get("tool_output")),
                        error_flag=bool(step.get("error_flag", False)),
                        error_type=None,
                        latency_ms=int(step["latency_ms"]) if isinstance(step.get("latency_ms"), int) else None,
                        retry_count=None,
                        timestamp=None,
                        parent_event_id=None,
                        causal_id=None,
                        tags=safe_json_dumps([]),
                        status=None,
                        inferred_intent=None,
                        intended_next_action=None,
                        evidence_summary=None,
                        source_file=source_file,
                    )
                )
            run = DerivedRun(
                run_id=run_id,
                source="legacy",
                dataset_name="legacy",
                run_type=shorten_text(run_obj.get("task_id")) or "legacy",
                task_id=shorten_text(run_obj.get("task_id")),
                scenario=shorten_text(run_obj.get("scenario")),
                outcome=outcome,
                failure_category=None,
                num_steps=len(steps),
                first_error_step=next((s.step_idx for s in steps if s.error_flag), None),
                first_failure_event_id=None,
                root_cause_event_id=None,
                expected_workflow=None,
                started_at=parse_possible_timestamp(run_obj.get("started_at")),
                ended_at=parse_possible_timestamp(run_obj.get("ended_at")),
                metadata=safe_json_dumps({"source_file": source_file, "legacy_format": "normalized_jsonl"}) or "{}",
            )
            annotations: list[Annotation] = []
            outputs.append((run, steps, [], annotations))
            continue

        run_id = shorten_text(obj.get("run_id")) or hash_run_id(file_path, obj_idx)
        steps_payload = extract_candidate_steps(obj)
        steps: list[DerivedStep] = []
        for idx, raw_step in enumerate(steps_payload):
            if isinstance(raw_step, str):
                step_dict: dict[str, Any] = {"text": raw_step}
            elif isinstance(raw_step, dict):
                step_dict = raw_step
            else:
                step_dict = {"text": str(raw_step)}
            step_type = str(step_dict.get("step_type", step_dict.get("type", "unknown"))).lower()
            text = shorten_text(
                step_dict.get("text")
                or step_dict.get("content")
                or step_dict.get("message")
                or step_dict.get("thought")
                or step_dict.get("action")
                or step_dict.get("observation")
            )
            tool_name = shorten_text(step_dict.get("tool_name") or step_dict.get("tool") or step_dict.get("function"))
            error_flag = bool(step_dict.get("error")) or any(
                token in (text or "").lower() for token in ("error", "failed", "timeout", "exception")
            )
            steps.append(
                DerivedStep(
                    run_id=run_id,
                    event_id=None,
                    step_idx=idx,
                    event_type=step_type,
                    step_type=step_type if step_type in {"thought", "action", "observation", "tool_call"} else "unknown",
                    agent_id=shorten_text(step_dict.get("agent_id")),
                    text=text,
                    tool_name=tool_name,
                    tool_input=safe_json_dumps(step_dict.get("tool_input") or step_dict.get("input")),
                    tool_output=safe_json_dumps(step_dict.get("tool_output") or step_dict.get("output")),
                    error_flag=error_flag,
                    error_type=None,
                    latency_ms=int(step_dict["latency_ms"]) if isinstance(step_dict.get("latency_ms"), int) else None,
                    retry_count=None,
                    timestamp=parse_possible_timestamp(step_dict.get("timestamp")),
                    parent_event_id=None,
                    causal_id=None,
                    tags=safe_json_dumps(step_dict.get("tags") if isinstance(step_dict.get("tags"), list) else []),
                    status=shorten_text(step_dict.get("status")),
                    inferred_intent=None,
                    intended_next_action=None,
                    evidence_summary=None,
                    source_file=source_file,
                )
            )
        outcome = "unknown"
        status_value = shorten_text(obj.get("outcome") or obj.get("status") or obj.get("result"))
        if status_value:
            low = status_value.lower()
            if any(token in low for token in ("success", "ok", "complete")):
                outcome = "success"
            elif any(token in low for token in ("fail", "error", "timeout")):
                outcome = "fail"
        if outcome == "unknown" and any(step.error_flag for step in steps):
            outcome = "fail"
        run = DerivedRun(
            run_id=run_id,
            source="legacy",
            dataset_name="legacy",
            run_type=shorten_text(obj.get("run_type")) or "legacy",
            task_id=shorten_text(obj.get("task_id") or obj.get("task")),
            scenario=shorten_text(obj.get("scenario") or obj.get("prompt")),
            outcome=outcome,
            failure_category=None,
            num_steps=len(steps),
            first_error_step=next((s.step_idx for s in steps if s.error_flag), None),
            first_failure_event_id=None,
            root_cause_event_id=None,
            expected_workflow=None,
            started_at=parse_possible_timestamp(obj.get("started_at") or obj.get("start_time") or obj.get("timestamp")),
            ended_at=parse_possible_timestamp(obj.get("ended_at") or obj.get("end_time")),
            metadata=safe_json_dumps({"source_file": source_file, "legacy_format": "generic"}) or "{}",
        )
        outputs.append((run, steps, [], []))

    return outputs


def discover_trace_files(raw_dir: Path) -> list[Path]:
    if not raw_dir.exists() or not raw_dir.is_dir():
        return []
    files = [path for path in raw_dir.rglob("*") if path.is_file() and path.suffix.lower() in TRACE_EXTENSIONS]
    files.sort(key=lambda path: str(path))
    return files


def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    for table_name in [
        "raw_events",
        "derived_runs",
        "derived_steps",
        "annotations",
        "runs",
        "steps",
        "run_stats",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_events (
            schema_version TEXT,
            generator_version TEXT,
            dataset_name TEXT,
            generated_at TIMESTAMP,
            run_id TEXT,
            run_type TEXT,
            scenario TEXT,
            expected_workflow JSON,
            event_id TEXT,
            parent_event_id TEXT,
            causal_id TEXT,
            timestamp TIMESTAMP,
            step_index INTEGER,
            agent_id TEXT,
            event_type TEXT,
            status TEXT,
            root_cause_event_id TEXT,
            first_failure_event_id TEXT,
            propagated_to_event_ids JSON,
            tags JSON,
            payload JSON,
            loop_iteration INTEGER,
            loop_group TEXT,
            repeated_pattern_id TEXT,
            source_file TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS derived_runs (
            run_id TEXT PRIMARY KEY,
            source TEXT,
            dataset_name TEXT,
            run_type TEXT,
            task_id TEXT,
            scenario TEXT,
            outcome TEXT,
            failure_category TEXT,
            num_steps INTEGER,
            first_error_step INTEGER,
            first_failure_event_id TEXT,
            root_cause_event_id TEXT,
            expected_workflow JSON,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            metadata JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS derived_steps (
            run_id TEXT,
            event_id TEXT,
            step_idx INTEGER,
            event_type TEXT,
            step_type TEXT,
            agent_id TEXT,
            text TEXT,
            tool_name TEXT,
            tool_input JSON,
            tool_output JSON,
            error_flag BOOLEAN,
            error_type TEXT,
            latency_ms INTEGER,
            retry_count INTEGER,
            timestamp TIMESTAMP,
            parent_event_id TEXT,
            causal_id TEXT,
            tags JSON,
            status TEXT,
            inferred_intent TEXT,
            intended_next_action TEXT,
            evidence_summary TEXT,
            source_file TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS annotations (
            run_id TEXT,
            step_idx INTEGER,
            event_id TEXT,
            label_type TEXT,
            label TEXT,
            confidence DOUBLE,
            reason_payload JSON,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            task_id TEXT,
            scenario TEXT,
            outcome TEXT CHECK (outcome IN ('success','fail','unknown')),
            num_steps INTEGER,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            metadata JSON,
            source TEXT,
            dataset_name TEXT,
            run_type TEXT,
            failure_category TEXT,
            first_failure_event_id TEXT,
            root_cause_event_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS steps (
            run_id TEXT,
            step_idx INTEGER,
            step_type TEXT CHECK (step_type IN ('thought','action','observation','tool_call','unknown')),
            text TEXT,
            tool_name TEXT,
            tool_input JSON,
            tool_output JSON,
            error_flag BOOLEAN,
            latency_ms INTEGER,
            event_id TEXT,
            event_type TEXT,
            agent_id TEXT,
            timestamp TIMESTAMP,
            parent_event_id TEXT,
            causal_id TEXT,
            tags JSON,
            status TEXT,
            error_type TEXT,
            retry_count INTEGER,
            inferred_intent TEXT,
            intended_next_action TEXT,
            evidence_summary TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_stats (
            run_id TEXT PRIMARY KEY,
            error_steps INTEGER,
            tool_call_steps INTEGER,
            distinct_tools INTEGER,
            first_error_step INTEGER
        )
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_run_id ON raw_events(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_event_type ON raw_events(event_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_events_status ON raw_events(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_runs_outcome ON derived_runs(outcome)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_runs_failure_category ON derived_runs(failure_category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_steps_run_idx ON derived_steps(run_id, step_idx)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_steps_tool_name ON derived_steps(tool_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_derived_steps_error_flag ON derived_steps(error_flag)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_annotations_run_id ON annotations(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_annotations_label ON annotations(label)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_stats_error_steps ON run_stats(error_steps)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS case_studies (
            run_id TEXT PRIMARY KEY,
            title TEXT,
            focus TEXT,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS review_notes (
            id BIGINT PRIMARY KEY,
            run_id TEXT,
            reviewer TEXT,
            label TEXT,
            note TEXT,
            created_at TIMESTAMP
        )
        """
    )
    conn.execute("CREATE SEQUENCE IF NOT EXISTS review_notes_seq START 1")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_subsets (
            subset_name TEXT,
            run_id TEXT,
            rationale TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (subset_name, run_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_case_studies_status ON case_studies(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_review_notes_run_id ON review_notes(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_subsets_name ON benchmark_subsets(subset_name)")


def clear_tables(conn: duckdb.DuckDBPyConnection) -> None:
    for table_name in [
        "raw_events",
        "derived_runs",
        "derived_steps",
        "annotations",
        "runs",
        "steps",
        "run_stats",
    ]:
        conn.execute(f"DELETE FROM {table_name}")


def insert_all(
    conn: duckdb.DuckDBPyConnection,
    raw_events: list[RawEvent],
    runs: list[DerivedRun],
    steps: list[DerivedStep],
    annotations: list[Annotation],
) -> None:
    if raw_events:
        conn.executemany(
            """
            INSERT INTO raw_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    event.schema_version,
                    event.generator_version,
                    event.dataset_name,
                    event.generated_at,
                    event.run_id,
                    event.run_type,
                    event.scenario,
                    event.expected_workflow,
                    event.event_id,
                    event.parent_event_id,
                    event.causal_id,
                    event.timestamp,
                    event.step_index,
                    event.agent_id,
                    event.event_type,
                    event.status,
                    event.root_cause_event_id,
                    event.first_failure_event_id,
                    event.propagated_to_event_ids,
                    event.tags,
                    event.payload,
                    event.loop_iteration,
                    event.loop_group,
                    event.repeated_pattern_id,
                    event.source_file,
                )
                for event in raw_events
            ],
        )

    if runs:
        conn.executemany(
            """
            INSERT INTO derived_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run.run_id,
                    run.source,
                    run.dataset_name,
                    run.run_type,
                    run.task_id,
                    run.scenario,
                    run.outcome,
                    run.failure_category,
                    run.num_steps,
                    run.first_error_step,
                    run.first_failure_event_id,
                    run.root_cause_event_id,
                    run.expected_workflow,
                    run.started_at,
                    run.ended_at,
                    run.metadata,
                )
                for run in runs
            ],
        )
        conn.executemany(
            """
            INSERT INTO runs
            (run_id, task_id, scenario, outcome, num_steps, started_at, ended_at, metadata, source, dataset_name, run_type, failure_category, first_failure_event_id, root_cause_event_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run.run_id,
                    run.task_id,
                    run.scenario,
                    run.outcome,
                    run.num_steps,
                    run.started_at,
                    run.ended_at,
                    run.metadata,
                    run.source,
                    run.dataset_name,
                    run.run_type,
                    run.failure_category,
                    run.first_failure_event_id,
                    run.root_cause_event_id,
                )
                for run in runs
            ],
        )

    if steps:
        conn.executemany(
            """
            INSERT INTO derived_steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    step.run_id,
                    step.event_id,
                    step.step_idx,
                    step.event_type,
                    step.step_type,
                    step.agent_id,
                    step.text,
                    step.tool_name,
                    step.tool_input,
                    step.tool_output,
                    step.error_flag,
                    step.error_type,
                    step.latency_ms,
                    step.retry_count,
                    step.timestamp,
                    step.parent_event_id,
                    step.causal_id,
                    step.tags,
                    step.status,
                    step.inferred_intent,
                    step.intended_next_action,
                    step.evidence_summary,
                    step.source_file,
                )
                for step in steps
            ],
        )
        conn.executemany(
            """
            INSERT INTO steps
            (run_id, step_idx, step_type, text, tool_name, tool_input, tool_output, error_flag, latency_ms, event_id, event_type, agent_id, timestamp, parent_event_id, causal_id, tags, status, error_type, retry_count, inferred_intent, intended_next_action, evidence_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    step.run_id,
                    step.step_idx,
                    step.step_type,
                    step.text,
                    step.tool_name,
                    step.tool_input,
                    step.tool_output,
                    step.error_flag,
                    step.latency_ms,
                    step.event_id,
                    step.event_type,
                    step.agent_id,
                    step.timestamp,
                    step.parent_event_id,
                    step.causal_id,
                    step.tags,
                    step.status,
                    step.error_type,
                    step.retry_count,
                    step.inferred_intent,
                    step.intended_next_action,
                    step.evidence_summary,
                )
                for step in steps
            ],
        )

    if annotations:
        conn.executemany(
            """
            INSERT INTO annotations
            (run_id, step_idx, event_id, label_type, label, confidence, reason_payload, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    ann.run_id,
                    ann.step_idx,
                    ann.event_id,
                    ann.label_type,
                    ann.label,
                    ann.confidence,
                    ann.reason_payload,
                    ann.source,
                )
                for ann in annotations
            ],
        )

    conn.execute(
        """
        INSERT INTO run_stats
        SELECT
            run_id,
            sum(CASE WHEN error_flag THEN 1 ELSE 0 END) AS error_steps,
            sum(CASE WHEN step_type = 'tool_call' OR coalesce(tool_name, '') <> '' THEN 1 ELSE 0 END) AS tool_call_steps,
            count(DISTINCT nullif(tool_name, '')) AS distinct_tools,
            min(CASE WHEN error_flag THEN step_idx ELSE NULL END) AS first_error_step
        FROM steps
        GROUP BY run_id
        """
    )


def write_normalized_jsonl(path: Path, runs: list[DerivedRun], steps_by_run: dict[str, list[DerivedStep]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for run in runs:
            payload = {
                "run": {
                    "run_id": run.run_id,
                    "task_id": run.task_id,
                    "scenario": run.scenario,
                    "source": run.source,
                    "dataset_name": run.dataset_name,
                    "run_type": run.run_type,
                    "outcome": run.outcome,
                    "failure_category": run.failure_category,
                    "num_steps": run.num_steps,
                    "first_error_step": run.first_error_step,
                    "first_failure_event_id": run.first_failure_event_id,
                    "root_cause_event_id": run.root_cause_event_id,
                    "started_at": run.started_at,
                    "ended_at": run.ended_at,
                    "expected_workflow": json.loads(run.expected_workflow) if run.expected_workflow else None,
                    "metadata": json.loads(run.metadata) if run.metadata else {},
                },
                "steps": [
                    {
                        "event_id": step.event_id,
                        "step_idx": step.step_idx,
                        "event_type": step.event_type,
                        "step_type": step.step_type,
                        "agent_id": step.agent_id,
                        "text": step.text,
                        "tool_name": step.tool_name,
                        "tool_input": json.loads(step.tool_input) if step.tool_input else None,
                        "tool_output": json.loads(step.tool_output) if step.tool_output else None,
                        "error_flag": step.error_flag,
                        "error_type": step.error_type,
                        "latency_ms": step.latency_ms,
                        "retry_count": step.retry_count,
                        "timestamp": step.timestamp,
                        "parent_event_id": step.parent_event_id,
                        "causal_id": step.causal_id,
                        "tags": json.loads(step.tags) if step.tags else [],
                        "status": step.status,
                        "inferred_intent": step.inferred_intent,
                        "intended_next_action": step.intended_next_action,
                        "evidence_summary": step.evidence_summary,
                    }
                    for step in steps_by_run.get(run.run_id, [])
                ],
            }
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def ingest(args: argparse.Namespace) -> int:
    manifest_by_run = parse_manifest(args.manifest_file)

    raw_dir = args.raw_dir
    if not raw_dir.exists():
        fallback_dirs = [
            Path("Dataset"),
            Path("data/raw/project1_event_runs/generated"),
        ]
        for candidate in fallback_dirs:
            if candidate.exists():
                raw_dir = candidate
                break

    canonical_files = discover_trace_files(raw_dir)
    legacy_files = discover_trace_files(args.legacy_dir) if args.include_legacy else []

    if not canonical_files and not legacy_files:
        print("[ERROR] No raw trace files found.")
        print(f"  canonical dir: {raw_dir}")
        print(f"  legacy dir: {args.legacy_dir} (include with --include-legacy)")
        return 1

    runs: list[DerivedRun] = []
    steps: list[DerivedStep] = []
    raw_events: list[RawEvent] = []
    annotations: list[Annotation] = []
    steps_by_run: dict[str, list[DerivedStep]] = defaultdict(list)

    for file_path in canonical_files:
        if file_path.suffix.lower() == ".jsonl":
            rows = read_jsonl_dicts(file_path)
        elif file_path.suffix.lower() == ".json":
            try:
                payload = json.loads(file_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                payload = None
            if isinstance(payload, list):
                rows = [row for row in payload if isinstance(row, dict)]
            elif isinstance(payload, dict):
                rows = [payload]
            else:
                rows = []
        else:
            rows = []
        if not rows:
            continue
        if all(is_event_level_row(row) for row in rows):
            run_id = str(rows[0]["run_id"])
            manifest_row = manifest_by_run.get(run_id)
            run, derived_steps, derived_raw_events, derived_annotations = derive_from_event_rows(
                rows, str(file_path), manifest_row
            )
            runs.append(run)
            steps.extend(derived_steps)
            raw_events.extend(derived_raw_events)
            annotations.extend(derived_annotations)
            steps_by_run[run.run_id].extend(derived_steps)
            continue

        if is_openclaw_session_rows(rows):
            openclaw_outputs = derive_from_openclaw_session_rows(rows, str(file_path), file_path)
            for run, derived_steps, derived_raw_events, derived_annotations in openclaw_outputs:
                runs.append(run)
                steps.extend(derived_steps)
                raw_events.extend(derived_raw_events)
                annotations.extend(derived_annotations)
                steps_by_run[run.run_id].extend(derived_steps)

    if args.include_legacy:
        for file_path in legacy_files:
            if file_path.suffix.lower() == ".jsonl":
                legacy_rows = read_jsonl_dicts(file_path)
            elif file_path.suffix.lower() == ".json":
                try:
                    loaded = json.loads(file_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    continue
                if isinstance(loaded, list):
                    legacy_rows = [row for row in loaded if isinstance(row, dict)]
                elif isinstance(loaded, dict):
                    legacy_rows = [loaded]
                else:
                    legacy_rows = []
            else:
                legacy_rows = []

            if not legacy_rows:
                continue
            legacy_outputs = derive_from_legacy_rows(legacy_rows, str(file_path), file_path)
            for run, derived_steps, _, derived_annotations in legacy_outputs:
                runs.append(run)
                steps.extend(derived_steps)
                annotations.extend(derived_annotations)
                steps_by_run[run.run_id].extend(derived_steps)

    if not runs:
        print("[ERROR] Ingestion produced 0 runs. Check canonical raw files and manifest.")
        return 1

    # Deduplicate runs by run_id; keep canonical source over legacy if clash occurs.
    run_by_id: dict[str, DerivedRun] = {}
    for run in runs:
        if run.run_id not in run_by_id:
            run_by_id[run.run_id] = run
            continue
        if run_by_id[run.run_id].source == "legacy" and run.source != "legacy":
            run_by_id[run.run_id] = run
    runs = sorted(run_by_id.values(), key=lambda row: row.run_id)

    # Keep steps only for retained run IDs.
    valid_run_ids = {run.run_id for run in runs}
    steps = [step for step in steps if step.run_id in valid_run_ids]
    annotations = [ann for ann in annotations if ann.run_id in valid_run_ids]
    raw_events = [event for event in raw_events if event.run_id in valid_run_ids]

    write_normalized_jsonl(args.normalized_out, runs, steps_by_run)

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(args.db_path))
    try:
        init_db(conn)
        clear_tables(conn)
        insert_all(conn, raw_events, runs, steps, annotations)
    finally:
        conn.close()

    tool_counter = Counter(step.tool_name for step in steps if step.tool_name)
    outcome_counter = Counter(run.outcome for run in runs)
    label_counter = Counter(annotation.label for annotation in annotations)
    source_counter = Counter(run.source for run in runs)

    print(f"[OK] Runs ingested: {len(runs)}")
    print(f"[OK] Raw events ingested: {len(raw_events)}")
    print(f"[OK] Derived steps ingested: {len(steps)}")
    print(f"[OK] Annotations ingested: {len(annotations)}")
    print("[OK] Source counts:")
    for source, count in source_counter.items():
        print(f"  - {source}: {count}")

    print("[OK] Top tools:")
    for tool_name, count in tool_counter.most_common(10):
        print(f"  - {tool_name}: {count}")
    if not tool_counter:
        print("  - (none discovered)")

    print("[OK] Outcome counts:")
    for outcome in ["success", "fail", "unknown"]:
        print(f"  - {outcome}: {outcome_counter.get(outcome, 0)}")

    print("[OK] Top labels:")
    for label, count in label_counter.most_common(10):
        print(f"  - {label}: {count}")
    if not label_counter:
        print("  - (none)")

    print(f"[OK] Normalized JSONL written to: {args.normalized_out}")
    print(f"[OK] DuckDB path: {args.db_path}")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return ingest(args)
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"[ERROR] {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
