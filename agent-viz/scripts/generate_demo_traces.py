#!/usr/bin/env python3
"""Generate small synthetic agent trace files for local UI/API verification."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate demo traces in data/raw")
    parser.add_argument("--out-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--num-runs", type=int, default=8)
    return parser.parse_args()


def make_run(i: int) -> dict:
    fail = i % 3 == 0
    steps = [
        {"step_type": "thought", "text": f"Analyze request for case {i}"},
        {
            "step_type": "tool_call",
            "tool_name": "search_inventory",
            "tool_input": {"sku": f"SKU-{1000+i}"},
            "tool_output": {"stock": 5 - (i % 4)},
        },
        {"step_type": "observation", "text": "Inventory lookup complete"},
        {
            "step_type": "action",
            "text": "Prepare response",
            "error": fail,
        },
    ]
    if fail:
        steps.append(
            {
                "step_type": "observation",
                "text": "Tool timeout while finalizing replacement",
                "error": True,
            }
        )
    else:
        steps.append({"step_type": "observation", "text": "Completed successfully"})

    return {
        "task_id": f"demo_task_{i}",
        "scenario": f"Demo scenario {i}",
        "status": "failed" if fail else "success",
        "steps": steps,
    }


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    out_file = args.out_dir / "demo_traces.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for i in range(args.num_runs):
            f.write(json.dumps(make_run(i), ensure_ascii=True) + "\n")

    print(f"[OK] Wrote {args.num_runs} demo runs to {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
