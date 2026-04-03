from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DB_PATH = PROJECT_ROOT / "db" / "traces.duckdb"
VALID_STEP_TYPES = {"thought", "action", "observation", "tool_call", "unknown"}
ACTION_TOOL_PATTERN = re.compile(r"\baction\s*:\s*([a-zA-Z_][a-zA-Z0-9_-]*)\s*\[", re.IGNORECASE)
WORD_SPLIT_PATTERN = re.compile(r"[^a-z0-9]+")


def get_db_path() -> str:
    return os.environ.get("TRACE_DB_PATH", str(DEFAULT_DB_PATH))


def clear_cached_queries() -> None:
    fetch_analytics_overview_cached.cache_clear()
    fetch_analytics_failure_labels_cached.cache_clear()
    fetch_analytics_tool_usage_cached.cache_clear()
    fetch_analytics_gaps_cached.cache_clear()


def table_exists(db: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM information_schema.tables WHERE lower(table_name) = lower(?) LIMIT 1",
        [table_name],
    ).fetchone()
    return row is not None


def get_table_columns(db: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    if not table_exists(db, table_name):
        return set()
    rows = db.execute(
        """
        SELECT lower(column_name)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchall()
    return {row[0] for row in rows}


def parse_json_value(value: Any) -> Any:
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return value
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def parse_json_list(value: Any) -> list[str]:
    parsed = parse_json_value(value)
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return []


def extract_tool_name_from_text(text: str | None) -> str | None:
    if not text:
        return None
    match = ACTION_TOOL_PATTERN.search(text)
    if not match:
        return None
    return match.group(1).strip().lower()


def infer_display_step_type(raw_step_type: str, text: str | None, tool_name: str | None) -> str:
    normalized_raw = (raw_step_type or "unknown").lower()
    if normalized_raw not in VALID_STEP_TYPES:
        normalized_raw = "unknown"

    if tool_name:
        return "tool_call"
    low = (text or "").lower()
    if "action:" in low and "[" in low and "]" in low:
        return "tool_call"
    if low.startswith("thought:") or "\nthought:" in low:
        return "thought"
    if low.startswith("observation:") or "\nobservation:" in low:
        return "observation"
    if low.startswith("action:") or "\naction:" in low:
        return "action"
    if any(token in low for token in ("search[", "click[", "call_tool", "function call")):
        return "tool_call"
    return normalized_raw


def get_run_stats_source_sql(db: duckdb.DuckDBPyConnection) -> str:
    if table_exists(db, "run_stats"):
        return """
            SELECT
                run_id,
                coalesce(error_steps, 0) AS error_steps,
                coalesce(tool_call_steps, 0) AS tool_call_steps,
                coalesce(distinct_tools, 0) AS distinct_tools,
                first_error_step
            FROM run_stats
        """
    return """
        SELECT
            run_id,
            sum(CASE WHEN error_flag THEN 1 ELSE 0 END) AS error_steps,
            sum(CASE WHEN step_type = 'tool_call' OR coalesce(tool_name, '') <> '' THEN 1 ELSE 0 END) AS tool_call_steps,
            count(DISTINCT nullif(tool_name, '')) AS distinct_tools,
            min(CASE WHEN error_flag THEN step_idx ELSE NULL END) AS first_error_step
        FROM steps
        GROUP BY run_id
    """


def fetch_runs(
    *,
    outcome: str | None,
    q: str | None,
    tool: str | None,
    source: str | None,
    min_steps: int | None,
    max_steps: int | None,
    has_errors: str | None,
    min_error_steps: int | None,
    max_error_steps: int | None,
    step_type: str | None,
    label: str | None,
    started_after: str | None,
    started_before: str | None,
    sort_by: str,
    sort_dir: str,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        stats_source_sql = get_run_stats_source_sql(db)
        runs_cols = get_table_columns(db, "runs")
        source_expr = "r.source" if "source" in runs_cols else "'unknown'"
        dataset_name_expr = "r.dataset_name" if "dataset_name" in runs_cols else "NULL"
        run_type_expr = "r.run_type" if "run_type" in runs_cols else "NULL"
        failure_category_expr = "r.failure_category" if "failure_category" in runs_cols else "NULL"
        first_failure_event_expr = "r.first_failure_event_id" if "first_failure_event_id" in runs_cols else "NULL"
        root_cause_event_expr = "r.root_cause_event_id" if "root_cause_event_id" in runs_cols else "NULL"

        where_parts: list[str] = []
        params: list[Any] = []

        if outcome and outcome != "all":
            where_parts.append("r.outcome = ?")
            params.append(outcome)
        if source and source != "all" and "source" in runs_cols:
            where_parts.append("lower(coalesce(r.source, '')) = lower(?)")
            params.append(source)
        if q:
            where_parts.append(
                "("
                "lower(r.run_id) LIKE lower(?) OR "
                "lower(coalesce(r.task_id, '')) LIKE lower(?) OR "
                "lower(coalesce(r.scenario, '')) LIKE lower(?) OR "
                "lower(coalesce(" + run_type_expr + ", '')) LIKE lower(?) OR "
                "lower(coalesce(" + failure_category_expr + ", '')) LIKE lower(?)"
                ")"
            )
            like_q = f"%{q}%"
            params.extend([like_q, like_q, like_q, like_q, like_q])
        if min_steps is not None:
            where_parts.append("r.num_steps >= ?")
            params.append(min_steps)
        if max_steps is not None:
            where_parts.append("r.num_steps <= ?")
            params.append(max_steps)
        if started_after:
            where_parts.append("r.started_at >= CAST(? AS TIMESTAMP)")
            params.append(started_after)
        if started_before:
            where_parts.append("r.started_at <= CAST(? AS TIMESTAMP)")
            params.append(started_before)

        normalized_has_errors = (has_errors or "all").strip().lower()
        if normalized_has_errors in {"true", "1", "yes"}:
            where_parts.append("coalesce(rs.error_steps, 0) > 0")
        elif normalized_has_errors in {"false", "0", "no"}:
            where_parts.append("coalesce(rs.error_steps, 0) = 0")
        if min_error_steps is not None:
            where_parts.append("coalesce(rs.error_steps, 0) >= ?")
            params.append(min_error_steps)
        if max_error_steps is not None:
            where_parts.append("coalesce(rs.error_steps, 0) <= ?")
            params.append(max_error_steps)

        if tool:
            where_parts.append(
                "EXISTS (SELECT 1 FROM steps s WHERE s.run_id = r.run_id AND lower(coalesce(s.tool_name, '')) LIKE lower(?))"
            )
            params.append(f"%{tool}%")

        if step_type and step_type.lower() != "all":
            normalized_step_type = step_type.lower()
            where_parts.append(
                "EXISTS ("
                "SELECT 1 FROM steps s "
                "WHERE s.run_id = r.run_id "
                "AND (lower(coalesce(s.step_type, '')) = ? "
                "OR (? = 'tool_call' AND coalesce(s.tool_name, '') <> ''))"
                ")"
            )
            params.extend([normalized_step_type, normalized_step_type])

        if label:
            where_parts.append(
                "EXISTS (SELECT 1 FROM annotations a WHERE a.run_id = r.run_id AND lower(coalesce(a.label, '')) LIKE lower(?))"
            )
            params.append(f"%{label}%")

        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
        sort_column_map = {
            "started_at": "r.started_at",
            "num_steps": "r.num_steps",
            "error_steps": "coalesce(rs.error_steps, 0)",
            "tool_call_steps": "coalesce(rs.tool_call_steps, 0)",
            "run_id": "r.run_id",
            "outcome": "r.outcome",
        }
        sort_expr = sort_column_map.get(sort_by, "r.started_at")
        normalized_sort_dir = "ASC" if (sort_dir or "").lower() == "asc" else "DESC"
        order_clause = f"{sort_expr} {normalized_sort_dir}, r.run_id ASC"

        from_clause = f"""
            FROM runs r
            LEFT JOIN ({stats_source_sql}) rs ON rs.run_id = r.run_id
            {where_clause}
        """
        total = db.execute(f"SELECT count(*) {from_clause}", params).fetchone()[0]

        rows = db.execute(
            f"""
            SELECT
                r.run_id,
                r.task_id,
                r.scenario,
                r.outcome,
                r.num_steps,
                r.started_at,
                r.ended_at,
                coalesce(rs.error_steps, 0),
                coalesce(rs.tool_call_steps, 0),
                coalesce(rs.distinct_tools, 0),
                rs.first_error_step,
                {source_expr} AS source,
                {dataset_name_expr} AS dataset_name,
                {run_type_expr} AS run_type,
                {failure_category_expr} AS failure_category,
                {first_failure_event_expr} AS first_failure_event_id,
                {root_cause_event_expr} AS root_cause_event_id
            {from_clause}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
    finally:
        db.close()

    items = [
        {
            "run_id": row[0],
            "task_id": row[1],
            "scenario": row[2],
            "outcome": row[3],
            "num_steps": row[4],
            "started_at": row[5],
            "ended_at": row[6],
            "error_steps": row[7],
            "tool_call_steps": row[8],
            "distinct_tools": row[9],
            "first_error_step": row[10],
            "source": row[11],
            "dataset_name": row[12],
            "run_type": row[13],
            "failure_category": row[14],
            "first_failure_event_id": row[15],
            "root_cause_event_id": row[16],
        }
        for row in rows
    ]
    return total, items


def fetch_run_detail(run_id: str) -> dict[str, Any] | None:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        stats_source_sql = get_run_stats_source_sql(db)
        runs_cols = get_table_columns(db, "runs")
        source_expr = "r.source" if "source" in runs_cols else "'unknown'"
        dataset_name_expr = "r.dataset_name" if "dataset_name" in runs_cols else "NULL"
        run_type_expr = "r.run_type" if "run_type" in runs_cols else "NULL"
        failure_category_expr = "r.failure_category" if "failure_category" in runs_cols else "NULL"
        first_failure_event_expr = "r.first_failure_event_id" if "first_failure_event_id" in runs_cols else "NULL"
        root_cause_event_expr = "r.root_cause_event_id" if "root_cause_event_id" in runs_cols else "NULL"
        row = db.execute(
            f"""
            SELECT
                r.run_id,
                r.task_id,
                r.scenario,
                r.outcome,
                r.num_steps,
                r.started_at,
                r.ended_at,
                r.metadata,
                coalesce(rs.error_steps, 0),
                coalesce(rs.tool_call_steps, 0),
                coalesce(rs.distinct_tools, 0),
                rs.first_error_step,
                {source_expr},
                {dataset_name_expr},
                {run_type_expr},
                {failure_category_expr},
                {first_failure_event_expr},
                {root_cause_event_expr}
            FROM runs r
            LEFT JOIN ({stats_source_sql}) rs ON rs.run_id = r.run_id
            WHERE r.run_id = ?
            """,
            [run_id],
        ).fetchone()
    finally:
        db.close()

    if row is None:
        return None

    return {
        "run_id": row[0],
        "task_id": row[1],
        "scenario": row[2],
        "outcome": row[3],
        "num_steps": row[4],
        "started_at": row[5],
        "ended_at": row[6],
        "metadata": parse_json_value(row[7]),
        "error_steps": row[8],
        "tool_call_steps": row[9],
        "distinct_tools": row[10],
        "first_error_step": row[11],
        "source": row[12],
        "dataset_name": row[13],
        "run_type": row[14],
        "failure_category": row[15],
        "first_failure_event_id": row[16],
        "root_cause_event_id": row[17],
    }


def fetch_steps(
    run_id: str,
    *,
    q: str | None = None,
    step_type: str | None = None,
    error_only: bool = False,
    limit: int = 5000,
    offset: int = 0,
) -> tuple[int, list[dict[str, Any]]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        step_cols = get_table_columns(db, "steps")
        event_id_expr = "event_id" if "event_id" in step_cols else "NULL"
        event_type_expr = "event_type" if "event_type" in step_cols else "NULL"
        agent_id_expr = "agent_id" if "agent_id" in step_cols else "NULL"
        timestamp_expr = "timestamp" if "timestamp" in step_cols else "NULL"
        parent_event_expr = "parent_event_id" if "parent_event_id" in step_cols else "NULL"
        causal_expr = "causal_id" if "causal_id" in step_cols else "NULL"
        tags_expr = "tags" if "tags" in step_cols else "NULL"
        status_expr = "status" if "status" in step_cols else "NULL"
        error_type_expr = "error_type" if "error_type" in step_cols else "NULL"
        retry_count_expr = "retry_count" if "retry_count" in step_cols else "NULL"
        inferred_intent_expr = "inferred_intent" if "inferred_intent" in step_cols else "NULL"
        intended_next_action_expr = "intended_next_action" if "intended_next_action" in step_cols else "NULL"
        evidence_summary_expr = "evidence_summary" if "evidence_summary" in step_cols else "NULL"

        where_parts = ["run_id = ?"]
        params: list[Any] = [run_id]
        if q:
            where_parts.append(
                "("
                "lower(coalesce(text, '')) LIKE lower(?) OR "
                "lower(coalesce(tool_name, '')) LIKE lower(?) OR "
                "lower(coalesce(cast(tool_input AS VARCHAR), '')) LIKE lower(?) OR "
                "lower(coalesce(cast(tool_output AS VARCHAR), '')) LIKE lower(?) OR "
                "lower(coalesce(cast(" + event_type_expr + " AS VARCHAR), '')) LIKE lower(?) OR "
                "lower(coalesce(cast(" + error_type_expr + " AS VARCHAR), '')) LIKE lower(?)"
                ")"
            )
            like_q = f"%{q}%"
            params.extend([like_q, like_q, like_q, like_q, like_q, like_q])
        if error_only:
            where_parts.append("error_flag = TRUE")

        rows = db.execute(
            f"""
            SELECT
                run_id, step_idx, step_type, text, tool_name, tool_input, tool_output, error_flag, latency_ms,
                {event_id_expr}, {event_type_expr}, {agent_id_expr}, {timestamp_expr},
                {parent_event_expr}, {causal_expr}, {tags_expr}, {status_expr}, {error_type_expr},
                {retry_count_expr}, {inferred_intent_expr}, {intended_next_action_expr}, {evidence_summary_expr}
            FROM steps
            WHERE {' AND '.join(where_parts)}
            ORDER BY step_idx ASC
            """,
            params,
        ).fetchall()
    finally:
        db.close()

    typed_rows: list[dict[str, Any]] = []
    for row in rows:
        raw_tool_name = row[4]
        inferred_tool_name = raw_tool_name or extract_tool_name_from_text(row[3])
        display_step_type = infer_display_step_type(row[2], row[3], inferred_tool_name)
        tags = parse_json_list(row[15])
        typed_rows.append(
            {
                "run_id": row[0],
                "step_idx": row[1],
                "step_type": row[2],
                "display_step_type": display_step_type,
                "text": row[3],
                "tool_name": inferred_tool_name,
                "tool_input": parse_json_value(row[5]),
                "tool_output": parse_json_value(row[6]),
                "error_flag": bool(row[7]),
                "latency_ms": row[8],
                "event_id": row[9],
                "event_type": row[10],
                "agent_id": row[11],
                "timestamp": row[12],
                "parent_event_id": row[13],
                "causal_id": row[14],
                "tags": tags,
                "status": row[16],
                "error_type": row[17],
                "retry_count": row[18],
                "inferred_intent": row[19],
                "intended_next_action": row[20],
                "evidence_summary": row[21],
            }
        )

    normalized_step_type = (step_type or "").lower().strip()
    if normalized_step_type and normalized_step_type != "all":
        typed_rows = [row for row in typed_rows if row["display_step_type"] == normalized_step_type]

    total = len(typed_rows)
    return total, typed_rows[offset : offset + limit]


def fetch_annotations(run_id: str, *, label_type: str | None = None) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        ann_cols = get_table_columns(db, "annotations")
        event_id_expr = "event_id" if "event_id" in ann_cols else "NULL"
        reason_expr = "reason_payload" if "reason_payload" in ann_cols else "NULL"
        where_parts = ["run_id = ?"]
        params: list[Any] = [run_id]
        if label_type and label_type != "all":
            where_parts.append("lower(coalesce(label_type, '')) = lower(?)")
            params.append(label_type)
        rows = db.execute(
            f"""
            SELECT run_id, step_idx, {event_id_expr}, label_type, label, confidence, {reason_expr}, source
            FROM annotations
            WHERE {' AND '.join(where_parts)}
            ORDER BY coalesce(step_idx, 999999), label
            """,
            params,
        ).fetchall()
    finally:
        db.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "run_id": row[0],
                "step_idx": row[1],
                "event_id": row[2],
                "label_type": row[3],
                "label": row[4],
                "confidence": row[5],
                "reason_payload": parse_json_value(row[6]),
                "source": row[7],
            }
        )
    return items


def gap_label_predicate_sql(label_expr: str = "label") -> str:
    normalized = f"lower(coalesce({label_expr}, ''))"
    return (
        f"substr({normalized}, 1, 8) = 'missing_' "
        f"OR substr({normalized}, 1, 7) = 'absent_' "
        f"OR substr({normalized}, 1, 6) = 'never_'"
    )


def fetch_gap_signals(run_id: str) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        ann_cols = get_table_columns(db, "annotations")
        event_id_expr = "event_id" if "event_id" in ann_cols else "NULL"
        reason_expr = "reason_payload" if "reason_payload" in ann_cols else "NULL"
        rows = db.execute(
            f"""
            SELECT run_id, step_idx, {event_id_expr}, label_type, label, confidence, {reason_expr}, source
            FROM annotations
            WHERE run_id = ?
              AND ({gap_label_predicate_sql('label')})
            ORDER BY coalesce(step_idx, 999999), label
            """,
            [run_id],
        ).fetchall()
    finally:
        db.close()

    return [
        {
            "run_id": row[0],
            "step_idx": row[1],
            "event_id": row[2],
            "label_type": row[3],
            "label": row[4],
            "confidence": row[5],
            "reason_payload": parse_json_value(row[6]),
            "source": row[7],
        }
        for row in rows
    ]


def ensure_annotations_extended_schema(db: duckdb.DuckDBPyConnection) -> None:
    ann_cols = get_table_columns(db, "annotations")
    if "event_id" not in ann_cols:
        db.execute("ALTER TABLE annotations ADD COLUMN event_id TEXT")
    if "reason_payload" not in ann_cols:
        db.execute("ALTER TABLE annotations ADD COLUMN reason_payload JSON")


def ensure_app_support_tables(db: duckdb.DuckDBPyConnection) -> None:
    db.execute(
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
    db.execute(
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
    db.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS review_notes_seq START 1
        """
    )
    db.execute(
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


def insert_manual_annotation(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_annotations_extended_schema(db)
        db.execute(
            """
            INSERT INTO annotations
            (run_id, step_idx, event_id, label_type, label, confidence, reason_payload, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                payload.get("step_idx"),
                payload.get("event_id"),
                "manual",
                payload.get("label"),
                payload.get("confidence"),
                json.dumps(payload.get("reason_payload"), ensure_ascii=True) if payload.get("reason_payload") is not None else None,
                payload.get("source") or "manual_ui",
            ],
        )
    finally:
        db.close()

    return {
        "run_id": run_id,
        "step_idx": payload.get("step_idx"),
        "event_id": payload.get("event_id"),
        "label_type": "manual",
        "label": payload.get("label"),
        "confidence": payload.get("confidence"),
        "reason_payload": payload.get("reason_payload"),
        "source": payload.get("source") or "manual_ui",
    }


def fetch_failure_summary(run_id: str) -> dict[str, Any] | None:
    run = fetch_run_detail(run_id)
    if run is None:
        return None
    annotations = fetch_annotations(run_id)
    _, error_steps = fetch_steps(run_id, error_only=True, limit=25, offset=0)
    if run["outcome"] == "success":
        summary = "Run completed successfully. No primary failure path detected."
    elif run.get("failure_category"):
        summary = f"Primary failure category: {run['failure_category']}."
    elif annotations:
        summary = f"Failure inferred from labels: {', '.join(sorted({item['label'] for item in annotations[:4]}))}."
    else:
        summary = "Failure detected but no structured category is available."
    return {
        "run_id": run_id,
        "outcome": run["outcome"],
        "failure_category": run.get("failure_category"),
        "first_error_step": run.get("first_error_step"),
        "first_failure_event_id": run.get("first_failure_event_id"),
        "root_cause_event_id": run.get("root_cause_event_id"),
        "labels": annotations,
        "key_error_events": error_steps[:10],
        "summary_text": summary,
    }


def fetch_run_report(run_id: str) -> dict[str, Any] | None:
    run = fetch_run_detail(run_id)
    if run is None:
        return None
    failure_summary = fetch_failure_summary(run_id)
    _, steps = fetch_steps(run_id, limit=50_000, offset=0)
    tool_counter: dict[str, int] = {}
    for step in steps:
        tool_name = step.get("tool_name")
        if not tool_name:
            continue
        tool_counter[tool_name] = tool_counter.get(tool_name, 0) + 1
    top_tools = [{"tool_name": name, "count": count} for name, count in sorted(tool_counter.items(), key=lambda item: item[1], reverse=True)[:8]]
    notes = [
        f"Source: {run.get('source') or 'unknown'}",
        f"Dataset: {run.get('dataset_name') or 'unknown'}",
        f"Run type: {run.get('run_type') or 'unknown'}",
    ]
    gaps = fetch_gap_signals(run_id)
    review_notes = fetch_review_notes(run_id, limit=10, offset=0)
    case_study = fetch_case_study(run_id)
    return {
        "run": run,
        "failure_summary": failure_summary,
        "top_tools": top_tools,
        "gap_signals": gaps,
        "review_notes": review_notes,
        "case_study": case_study,
        "notes": notes,
    }


def fetch_analytics_overview() -> dict[str, Any]:
    return fetch_analytics_overview_cached()


@lru_cache(maxsize=1)
def fetch_analytics_overview_cached() -> dict[str, Any]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        stats_source_sql = get_run_stats_source_sql(db)
        total_runs = db.execute("SELECT count(*) FROM runs").fetchone()[0]
        total_steps = db.execute("SELECT count(*) FROM steps").fetchone()[0]
        outcome_rows = db.execute(
            "SELECT outcome, count(*) FROM runs GROUP BY outcome ORDER BY count(*) DESC, outcome ASC"
        ).fetchall()
        outcome_counts = {str(row[0]): int(row[1]) for row in outcome_rows}

        label_rows = db.execute(
            """
            SELECT label, count(DISTINCT run_id) AS run_count
            FROM annotations
            GROUP BY label
            ORDER BY run_count DESC, label ASC
            LIMIT 12
            """
        ).fetchall()
        top_failure_labels = [{"label": row[0], "run_count": int(row[1])} for row in label_rows]

        first_failure_histogram_rows = db.execute(
            f"""
            SELECT
                CASE
                    WHEN rs.first_error_step IS NULL THEN 'none'
                    WHEN rs.first_error_step < 3 THEN '0-2'
                    WHEN rs.first_error_step < 6 THEN '3-5'
                    WHEN rs.first_error_step < 10 THEN '6-9'
                    ELSE '10+'
                END AS bucket,
                count(*) AS run_count
            FROM runs r
            LEFT JOIN ({stats_source_sql}) rs ON rs.run_id = r.run_id
            GROUP BY bucket
            ORDER BY
                CASE bucket
                    WHEN '0-2' THEN 1
                    WHEN '3-5' THEN 2
                    WHEN '6-9' THEN 3
                    WHEN '10+' THEN 4
                    ELSE 5
                END
            """
        ).fetchall()
        first_failure_step_histogram = [{"bucket": row[0], "run_count": int(row[1])} for row in first_failure_histogram_rows]

        def prevalence_for(label: str) -> float:
            if not total_runs:
                return 0.0
            row = db.execute(
                "SELECT count(DISTINCT run_id) FROM annotations WHERE lower(coalesce(label, '')) = lower(?)",
                [label],
            ).fetchone()
            count = int(row[0]) if row else 0
            return round((count / total_runs) * 100.0, 2)

        retry_prevalence = prevalence_for("repeated_retry")
        loop_prevalence = prevalence_for("loop")
        timeout_prevalence = prevalence_for("timeout_failure")
    finally:
        db.close()

    return {
        "total_runs": int(total_runs),
        "total_steps": int(total_steps),
        "outcome_counts": outcome_counts,
        "top_failure_labels": top_failure_labels,
        "first_failure_step_histogram": first_failure_step_histogram,
        "retry_prevalence": retry_prevalence,
        "loop_prevalence": loop_prevalence,
        "timeout_prevalence": timeout_prevalence,
    }


def fetch_analytics_failure_labels(limit: int = 50) -> list[dict[str, Any]]:
    return fetch_analytics_failure_labels_cached(limit)


@lru_cache(maxsize=32)
def fetch_analytics_failure_labels_cached(limit: int = 50) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        rows = db.execute(
            """
            SELECT
                a.label,
                a.label_type,
                count(*) AS annotation_count,
                count(DISTINCT a.run_id) AS run_count
            FROM annotations a
            GROUP BY a.label, a.label_type
            ORDER BY run_count DESC, annotation_count DESC, a.label ASC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "label": row[0],
            "label_type": row[1],
            "annotation_count": int(row[2]),
            "run_count": int(row[3]),
        }
        for row in rows
    ]


def fetch_analytics_tool_usage(limit: int = 40) -> list[dict[str, Any]]:
    return fetch_analytics_tool_usage_cached(limit)


@lru_cache(maxsize=32)
def fetch_analytics_tool_usage_cached(limit: int = 40) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        rows = db.execute(
            """
            SELECT
                coalesce(nullif(s.tool_name, ''), 'unknown') AS tool_name,
                count(*) AS call_count,
                sum(CASE WHEN s.error_flag THEN 1 ELSE 0 END) AS error_count,
                avg(CASE WHEN s.latency_ms IS NOT NULL THEN s.latency_ms ELSE NULL END) AS avg_latency_ms,
                sum(CASE WHEN r.outcome = 'success' THEN 1 ELSE 0 END) AS success_calls,
                sum(CASE WHEN r.outcome = 'fail' THEN 1 ELSE 0 END) AS fail_calls
            FROM steps s
            JOIN runs r ON r.run_id = s.run_id
            WHERE s.step_type = 'tool_call' OR coalesce(s.tool_name, '') <> ''
            GROUP BY tool_name
            ORDER BY call_count DESC, tool_name ASC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "tool_name": row[0],
            "call_count": int(row[1]),
            "error_count": int(row[2]),
            "avg_latency_ms": round(float(row[3]), 2) if row[3] is not None else None,
            "success_calls": int(row[4]),
            "fail_calls": int(row[5]),
        }
        for row in rows
    ]


def fetch_analytics_gaps(limit: int = 50) -> list[dict[str, Any]]:
    return fetch_analytics_gaps_cached(limit)


@lru_cache(maxsize=32)
def fetch_analytics_gaps_cached(limit: int = 50) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=True)
    try:
        rows = db.execute(
            f"""
            SELECT
                a.label,
                count(*) AS annotation_count,
                count(DISTINCT a.run_id) AS run_count,
                sum(CASE WHEN r.outcome = 'fail' THEN 1 ELSE 0 END) AS fail_runs,
                sum(CASE WHEN r.outcome = 'success' THEN 1 ELSE 0 END) AS success_runs
            FROM annotations a
            JOIN runs r ON r.run_id = a.run_id
            WHERE {gap_label_predicate_sql('a.label')}
            GROUP BY a.label
            ORDER BY run_count DESC, annotation_count DESC, a.label ASC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "label": row[0],
            "annotation_count": int(row[1]),
            "run_count": int(row[2]),
            "fail_runs": int(row[3]),
            "success_runs": int(row[4]),
        }
        for row in rows
    ]


def upsert_case_study(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        existing = db.execute("SELECT run_id FROM case_studies WHERE run_id = ?", [run_id]).fetchone()
        if existing:
            db.execute(
                """
                UPDATE case_studies
                SET title = ?, focus = ?, status = ?, updated_at = now()
                WHERE run_id = ?
                """,
                [payload.get("title"), payload.get("focus"), payload.get("status") or "active", run_id],
            )
        else:
            db.execute(
                """
                INSERT INTO case_studies (run_id, title, focus, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, now(), now())
                """,
                [run_id, payload.get("title"), payload.get("focus"), payload.get("status") or "active"],
            )
        row = db.execute(
            "SELECT run_id, title, focus, status, created_at, updated_at FROM case_studies WHERE run_id = ?",
            [run_id],
        ).fetchone()
    finally:
        db.close()
    return {
        "run_id": row[0],
        "title": row[1],
        "focus": row[2],
        "status": row[3],
        "created_at": row[4],
        "updated_at": row[5],
    }


def fetch_case_studies(status: str | None = None, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        where = ""
        params: list[Any] = []
        if status and status != "all":
            where = "WHERE lower(coalesce(status, '')) = lower(?)"
            params.append(status)
        rows = db.execute(
            f"""
            SELECT run_id, title, focus, status, created_at, updated_at
            FROM case_studies
            {where}
            ORDER BY updated_at DESC, run_id ASC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "run_id": row[0],
            "title": row[1],
            "focus": row[2],
            "status": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }
        for row in rows
    ]


def fetch_case_study(run_id: str) -> dict[str, Any] | None:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        row = db.execute(
            "SELECT run_id, title, focus, status, created_at, updated_at FROM case_studies WHERE run_id = ?",
            [run_id],
        ).fetchone()
    finally:
        db.close()
    if row is None:
        return None
    return {
        "run_id": row[0],
        "title": row[1],
        "focus": row[2],
        "status": row[3],
        "created_at": row[4],
        "updated_at": row[5],
    }


def insert_review_note(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        row = db.execute(
            """
            INSERT INTO review_notes (id, run_id, reviewer, label, note, created_at)
            VALUES (nextval('review_notes_seq'), ?, ?, ?, ?, now())
            RETURNING id, run_id, reviewer, label, note, created_at
            """,
            [run_id, payload.get("reviewer"), payload.get("label"), payload.get("note")],
        ).fetchone()
    finally:
        db.close()
    return {
        "id": int(row[0]),
        "run_id": row[1],
        "reviewer": row[2],
        "label": row[3],
        "note": row[4],
        "created_at": row[5],
    }


def fetch_review_notes(run_id: str, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        rows = db.execute(
            """
            SELECT id, run_id, reviewer, label, note, created_at
            FROM review_notes
            WHERE run_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [run_id, limit, offset],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "id": int(row[0]),
            "run_id": row[1],
            "reviewer": row[2],
            "label": row[3],
            "note": row[4],
            "created_at": row[5],
        }
        for row in rows
    ]


def assign_benchmark_subset(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        subset_name = str(payload.get("subset_name") or "").strip()
        rationale = payload.get("rationale")
        db.execute(
            """
            INSERT INTO benchmark_subsets (subset_name, run_id, rationale, created_at)
            VALUES (?, ?, ?, now())
            ON CONFLICT (subset_name, run_id) DO UPDATE SET rationale = excluded.rationale
            """,
            [subset_name, run_id, rationale],
        )
        row = db.execute(
            """
            SELECT subset_name, run_id, rationale, created_at
            FROM benchmark_subsets
            WHERE subset_name = ? AND run_id = ?
            """,
            [subset_name, run_id],
        ).fetchone()
    finally:
        db.close()
    return {
        "subset_name": row[0],
        "run_id": row[1],
        "rationale": row[2],
        "created_at": row[3],
    }


def fetch_benchmark_subsets(subset_name: str | None = None, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        where = ""
        params: list[Any] = []
        if subset_name:
            where = "WHERE lower(subset_name) = lower(?)"
            params.append(subset_name)
        rows = db.execute(
            f"""
            SELECT subset_name, run_id, rationale, created_at
            FROM benchmark_subsets
            {where}
            ORDER BY created_at DESC, subset_name ASC, run_id ASC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
    finally:
        db.close()
    return [
        {
            "subset_name": row[0],
            "run_id": row[1],
            "rationale": row[2],
            "created_at": row[3],
        }
        for row in rows
    ]


def auto_build_benchmark_subsets() -> dict[str, int]:
    db = duckdb.connect(get_db_path(), read_only=False)
    try:
        ensure_app_support_tables(db)
        db.execute("DELETE FROM benchmark_subsets WHERE lower(subset_name) LIKE 'auto_%'")
        db.execute(
            """
            INSERT INTO benchmark_subsets (subset_name, run_id, rationale, created_at)
            SELECT
                'auto_failure_hotspots' AS subset_name,
                run_id,
                'Auto-selected failed runs with structured failure labels' AS rationale,
                now() AS created_at
            FROM runs
            WHERE outcome = 'fail'
            ORDER BY num_steps DESC, run_id ASC
            LIMIT 30
            """
        )
        db.execute(
            """
            INSERT INTO benchmark_subsets (subset_name, run_id, rationale, created_at)
            SELECT
                'auto_success_reference' AS subset_name,
                run_id,
                'Auto-selected successful runs for baseline comparison' AS rationale,
                now() AS created_at
            FROM runs
            WHERE outcome = 'success'
            ORDER BY num_steps DESC, run_id ASC
            LIMIT 30
            """
        )
        counts = db.execute(
            """
            SELECT subset_name, count(*) FROM benchmark_subsets
            WHERE lower(subset_name) LIKE 'auto_%'
            GROUP BY subset_name
            """
        ).fetchall()
    finally:
        db.close()
    return {row[0]: int(row[1]) for row in counts}


def text_tokens(value: str | None) -> set[str]:
    if not value:
        return set()
    parts = [token for token in WORD_SPLIT_PATTERN.split(value.lower()) if len(token) >= 3]
    return set(parts[:12])


def step_similarity(left_step: dict[str, Any], right_step: dict[str, Any]) -> float:
    score = 0.0
    if left_step["display_step_type"] == right_step["display_step_type"]:
        score += 2.0
    left_tool = (left_step.get("tool_name") or "").lower()
    right_tool = (right_step.get("tool_name") or "").lower()
    if left_tool and right_tool:
        if left_tool == right_tool:
            score += 2.0
        elif left_tool in right_tool or right_tool in left_tool:
            score += 1.0
    union = text_tokens(left_step.get("text")) | text_tokens(right_step.get("text"))
    if union:
        overlap = len(text_tokens(left_step.get("text")) & text_tokens(right_step.get("text"))) / len(union)
        if overlap >= 0.5:
            score += 2.0
        elif overlap >= 0.25:
            score += 1.0
    if left_step.get("error_flag") and right_step.get("error_flag"):
        score += 0.5
    return score


def steps_match(left_step: dict[str, Any], right_step: dict[str, Any]) -> bool:
    return step_similarity(left_step, right_step) >= 2.0


def align_by_index(left_steps: list[dict[str, Any]], right_steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    max_len = max(len(left_steps), len(right_steps))
    pairs: list[dict[str, Any]] = []
    for pair_idx in range(max_len):
        left_step = left_steps[pair_idx] if pair_idx < len(left_steps) else None
        right_step = right_steps[pair_idx] if pair_idx < len(right_steps) else None
        if left_step is not None and right_step is not None:
            status = "match" if steps_match(left_step, right_step) else "mismatch"
            note = None if status == "match" else "different_step_content"
        elif left_step is not None:
            status = "left_only"
            note = "missing_on_right"
        else:
            status = "right_only"
            note = "missing_on_left"
        pairs.append(
            {
                "pair_idx": pair_idx,
                "status": status,
                "note": note,
                "left_step": left_step,
                "right_step": right_step,
            }
        )
    return pairs


def infer_left_only_note(left_steps: list[dict[str, Any]], index: int) -> str:
    previous = left_steps[index - 1] if index > 0 else None
    current = left_steps[index]
    if previous and current.get("tool_name") and previous.get("tool_name") and str(current["tool_name"]).lower() == str(previous["tool_name"]).lower():
        return "retry_candidate_left"
    return "missing_on_right"


def infer_right_only_note(right_steps: list[dict[str, Any]], index: int) -> str:
    previous = right_steps[index - 1] if index > 0 else None
    current = right_steps[index]
    if previous and current.get("tool_name") and previous.get("tool_name") and str(current["tool_name"]).lower() == str(previous["tool_name"]).lower():
        return "retry_candidate_right"
    return "missing_on_left"


def align_with_lookahead(left_steps: list[dict[str, Any]], right_steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    pair_idx = 0
    left_idx = 0
    right_idx = 0
    while left_idx < len(left_steps) and right_idx < len(right_steps):
        left_step = left_steps[left_idx]
        right_step = right_steps[right_idx]
        if steps_match(left_step, right_step):
            pairs.append({"pair_idx": pair_idx, "status": "match", "note": None, "left_step": left_step, "right_step": right_step})
            pair_idx += 1
            left_idx += 1
            right_idx += 1
            continue
        left_lookahead = left_idx + 1 < len(left_steps) and steps_match(left_steps[left_idx + 1], right_step)
        right_lookahead = right_idx + 1 < len(right_steps) and steps_match(left_step, right_steps[right_idx + 1])
        if left_lookahead and not right_lookahead:
            pairs.append(
                {
                    "pair_idx": pair_idx,
                    "status": "left_only",
                    "note": infer_left_only_note(left_steps, left_idx),
                    "left_step": left_step,
                    "right_step": None,
                }
            )
            pair_idx += 1
            left_idx += 1
            continue
        if right_lookahead and not left_lookahead:
            pairs.append(
                {
                    "pair_idx": pair_idx,
                    "status": "right_only",
                    "note": infer_right_only_note(right_steps, right_idx),
                    "left_step": None,
                    "right_step": right_step,
                }
            )
            pair_idx += 1
            right_idx += 1
            continue
        if left_lookahead and right_lookahead:
            score_skip_left = step_similarity(left_steps[left_idx + 1], right_step)
            score_skip_right = step_similarity(left_step, right_steps[right_idx + 1])
            if score_skip_left >= score_skip_right:
                pairs.append(
                    {
                        "pair_idx": pair_idx,
                        "status": "left_only",
                        "note": infer_left_only_note(left_steps, left_idx),
                        "left_step": left_step,
                        "right_step": None,
                    }
                )
                pair_idx += 1
                left_idx += 1
            else:
                pairs.append(
                    {
                        "pair_idx": pair_idx,
                        "status": "right_only",
                        "note": infer_right_only_note(right_steps, right_idx),
                        "left_step": None,
                        "right_step": right_step,
                    }
                )
                pair_idx += 1
                right_idx += 1
            continue
        pairs.append(
            {
                "pair_idx": pair_idx,
                "status": "mismatch",
                "note": "semantic_mismatch",
                "left_step": left_step,
                "right_step": right_step,
            }
        )
        pair_idx += 1
        left_idx += 1
        right_idx += 1

    while left_idx < len(left_steps):
        pairs.append(
            {
                "pair_idx": pair_idx,
                "status": "left_only",
                "note": infer_left_only_note(left_steps, left_idx),
                "left_step": left_steps[left_idx],
                "right_step": None,
            }
        )
        pair_idx += 1
        left_idx += 1
    while right_idx < len(right_steps):
        pairs.append(
            {
                "pair_idx": pair_idx,
                "status": "right_only",
                "note": infer_right_only_note(right_steps, right_idx),
                "left_step": None,
                "right_step": right_steps[right_idx],
            }
        )
        pair_idx += 1
        right_idx += 1
    return pairs


def fetch_compare(left_run_id: str, right_run_id: str, *, mode: str = "aligned") -> dict[str, Any] | None:
    left_run = fetch_run_detail(left_run_id)
    right_run = fetch_run_detail(right_run_id)
    if left_run is None or right_run is None:
        return None
    _, left_steps = fetch_steps(left_run_id, limit=50_000, offset=0)
    _, right_steps = fetch_steps(right_run_id, limit=50_000, offset=0)
    normalized_mode = "aligned" if mode == "aligned" else "raw"
    pairs = align_with_lookahead(left_steps, right_steps) if normalized_mode == "aligned" else align_by_index(left_steps, right_steps)
    stats = {"match": 0, "mismatch": 0, "left_only": 0, "right_only": 0}
    for pair in pairs:
        if pair["status"] in stats:
            stats[pair["status"]] += 1
    return {
        "mode": normalized_mode,
        "left_run": left_run,
        "right_run": right_run,
        "total_pairs": len(pairs),
        "stats": stats,
        "items": pairs,
    }
