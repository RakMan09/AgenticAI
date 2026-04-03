#!/usr/bin/env python3
"""Fetch a small subset of public agent trace files into data/raw/."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

DEFAULT_REPO_URL = "https://github.com/IBM/AssetOpsBench.git"
TRACE_EXTENSIONS = {".json", ".jsonl"}
TRACE_KEYS = {"steps", "trajectory", "events", "messages", "trace", "logs", "history"}
TRACE_NAME_HINTS = ("trace", "trajectory", "run", "event", "history", "agentlog", "execution")
DUMMY_PATH_HINTS = ("sample_data", "scenarios", "utterance", "fact_sheet")
DUMMY_KEYS = {
    "question",
    "options",
    "correct",
    "text",
    "asset_id",
    "asset_name",
    "equipment_id",
    "equipment_name",
    "wo_id",
    "timestamp",
    "anomaly_score",
    "docs",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a subset of AssetOpsBench traces")
    parser.add_argument(
        "--repo-url",
        default=DEFAULT_REPO_URL,
        help=f"Git repo URL to clone (default: {DEFAULT_REPO_URL})",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="Optional local directory containing trace JSON/JSONL files.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory where subset files are copied.",
    )
    parser.add_argument(
        "--min-files",
        type=int,
        default=5,
        help="Minimum desired number of raw files.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=20,
        help="Maximum number of raw files to copy.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep temporary cloned repo for debugging.",
    )
    return parser.parse_args()


def discover_trace_files(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in TRACE_EXTENSIONS]
    files.sort(key=lambda p: str(p))
    return [p for p in files if is_trace_like_file(p)]


def discover_dummy_files(root: Path) -> list[Path]:
    files = [p for p in root.rglob("*") if p.is_file() and p.suffix.lower() in TRACE_EXTENSIONS]
    files.sort(key=lambda p: str(p))
    return [p for p in files if is_dummy_assetops_file(p)]


def _load_first_object(path: Path) -> dict | None:
    try:
        if path.suffix.lower() == ".jsonl":
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        return obj
                    return None
            return None
        obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return obj[0]
    except Exception:
        return None
    return None


def is_trace_like_file(path: Path) -> bool:
    name_low = path.name.lower()
    if any(hint in name_low for hint in TRACE_NAME_HINTS):
        return True

    obj = _load_first_object(path)
    if not isinstance(obj, dict):
        return False

    if TRACE_KEYS.intersection(obj.keys()):
        return True

    # Exclude known non-trace metadata/scenario/sensor files.
    non_trace_signals = {"characteristic_form", "deterministic", "asset_id", "@context", "distribution"}
    if non_trace_signals.intersection(obj.keys()):
        return False

    return False


def is_dummy_assetops_file(path: Path) -> bool:
    path_low = path.as_posix().lower()
    if any(hint in path_low for hint in DUMMY_PATH_HINTS):
        return True

    obj = _load_first_object(path)
    if not isinstance(obj, dict):
        return False
    return bool(DUMMY_KEYS.intersection(obj.keys()))


def copy_subset(files: Iterable[Path], source_root: Path, out_dir: Path, max_files: int) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    for old_file in out_dir.glob("*"):
        if old_file.is_file() and old_file.suffix.lower() in TRACE_EXTENSIONS:
            old_file.unlink()
    copied = 0
    for idx, src in enumerate(files):
        if copied >= max_files:
            break
        rel = src.relative_to(source_root)
        flat_name = "__".join(rel.parts)
        dest = out_dir / f"{idx:03d}_{flat_name}"
        shutil.copy2(src, dest)
        copied += 1
    return copied


def clone_repo(repo_url: str) -> Path | None:
    if shutil.which("git") is None:
        print("[ERROR] git is not installed or not on PATH.")
        return None

    temp_dir = Path(tempfile.mkdtemp(prefix="assetopsbench_"))
    cmd = ["git", "clone", "--depth", "1", repo_url, str(temp_dir / "repo")]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        print("[ERROR] Failed to clone repository.")
        print(f"        Command: {' '.join(cmd)}")
        if exc.stderr:
            print(f"        stderr: {exc.stderr.strip()}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None
    return temp_dir / "repo"


def print_manual_instructions(repo_url: str) -> None:
    print("\n[MANUAL ACTION REQUIRED]")
    print("No trace files were auto-discovered.")
    print("You can still proceed by pointing to a local trace folder:")
    print("  python scripts/fetch_assetopsbench_subset.py --source-dir /path/to/traces")
    print("\nSuggested steps:")
    print(f"  1) git clone {repo_url}")
    print("  2) locate JSON/JSONL trace files in that repo")
    print("  3) rerun this script with --source-dir on that folder")


def main() -> int:
    args = parse_args()

    if args.min_files < 1:
        print("[ERROR] --min-files must be >= 1")
        return 2
    if args.max_files < args.min_files:
        print("[ERROR] --max-files must be >= --min-files")
        return 2

    source_root: Path | None = None
    clone_root: Path | None = None

    if args.source_dir is not None:
        source_root = args.source_dir
        if not source_root.exists() or not source_root.is_dir():
            print(f"[ERROR] --source-dir does not exist or is not a directory: {source_root}")
            return 2
        print(f"[INFO] Using provided source directory: {source_root}")
    else:
        print(f"[INFO] Cloning repository: {args.repo_url}")
        clone_root = clone_repo(args.repo_url)
        if clone_root is None:
            print_manual_instructions(args.repo_url)
            return 1
        source_root = clone_root
        print(f"[INFO] Cloned to temporary directory: {clone_root}")

    assert source_root is not None
    trace_files = discover_trace_files(source_root)
    selected_files = trace_files
    selected_kind = "trace-like"
    if not trace_files:
        dummy_files = discover_dummy_files(source_root)
        if not dummy_files:
            print(f"[ERROR] No trace-like JSON/JSONL files found under: {source_root}")
            print("        Found JSON files, but they do not appear to contain run trajectories.")
            print_manual_instructions(args.repo_url)
            if clone_root and not args.keep_temp:
                shutil.rmtree(clone_root.parent, ignore_errors=True)
            return 1
        selected_files = dummy_files
        selected_kind = "IBM dummy benchmark/sample"
        print("[WARN] No true trajectory files found. Falling back to IBM dummy benchmark/sample JSON files.")

    copied = copy_subset(selected_files, source_root, args.out_dir, args.max_files)
    if copied < args.min_files:
        print(f"[ERROR] Only copied {copied} files, fewer than required minimum {args.min_files}.")
        print("        Try using --source-dir with a folder that has more trace files.")
        if clone_root and not args.keep_temp:
            shutil.rmtree(clone_root.parent, ignore_errors=True)
        return 1

    print(f"[OK] Copied {copied} {selected_kind} files into: {args.out_dir}")
    if clone_root and not args.keep_temp:
        shutil.rmtree(clone_root.parent, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
