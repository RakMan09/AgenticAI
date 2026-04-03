from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


class Week9PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repo_root = Path(__file__).resolve().parents[1]
        cls.tmp_dir_obj = tempfile.TemporaryDirectory()
        cls.tmp_dir = Path(cls.tmp_dir_obj.name)
        cls.generated_dir = cls.tmp_dir / "generated"
        cls.manifest_path = cls.tmp_dir / "manifest.jsonl"
        cls.db_path = cls.tmp_dir / "traces.duckdb"
        cls.normalized_path = cls.tmp_dir / "normalized.jsonl"

        generator_cmd = [
            sys.executable,
            str(cls.repo_root / "data" / "generate_traces.py"),
            "--out-dir",
            str(cls.generated_dir),
            "--manifest-file",
            str(cls.manifest_path),
            "--profile",
            "small",
            "--seed",
            "7",
            "--overwrite",
        ]
        ingest_cmd = [
            sys.executable,
            str(cls.repo_root / "scripts" / "ingest_assetops.py"),
            "--raw-dir",
            str(cls.generated_dir),
            "--manifest-file",
            str(cls.manifest_path),
            "--db-path",
            str(cls.db_path),
            "--normalized-out",
            str(cls.normalized_path),
        ]

        generator_run = subprocess.run(generator_cmd, capture_output=True, text=True)
        if generator_run.returncode != 0:
            raise RuntimeError(f"Generator failed:\n{generator_run.stdout}\n{generator_run.stderr}")

        ingest_run = subprocess.run(ingest_cmd, capture_output=True, text=True)
        if ingest_run.returncode != 0:
            raise RuntimeError(f"Ingestion failed:\n{ingest_run.stdout}\n{ingest_run.stderr}")

        os.environ["TRACE_DB_PATH"] = str(cls.db_path)
        sys.path.insert(0, str(cls.repo_root / "apps" / "api"))
        from app import db as api_db  # noqa: WPS433

        cls.api_db = api_db

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp_dir_obj.cleanup()

    def test_core_tables_and_counts_exist(self) -> None:
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            raw_events = con.execute("SELECT count(*) FROM raw_events").fetchone()[0]
            derived_runs = con.execute("SELECT count(*) FROM derived_runs").fetchone()[0]
            derived_steps = con.execute("SELECT count(*) FROM derived_steps").fetchone()[0]
            runs = con.execute("SELECT count(*) FROM runs").fetchone()[0]
            steps = con.execute("SELECT count(*) FROM steps").fetchone()[0]
        finally:
            con.close()

        self.assertGreater(raw_events, 0)
        self.assertGreater(derived_runs, 0)
        self.assertGreater(derived_steps, 0)
        self.assertEqual(derived_runs, runs)
        self.assertEqual(derived_steps, steps)

    def test_heuristic_labels_present(self) -> None:
        con = duckdb.connect(str(self.db_path), read_only=True)
        try:
            heuristic_count = con.execute(
                "SELECT count(*) FROM annotations WHERE label_type = 'heuristic'"
            ).fetchone()[0]
            taxonomy_count = con.execute(
                "SELECT count(*) FROM annotations WHERE label_type = 'taxonomy'"
            ).fetchone()[0]
        finally:
            con.close()

        self.assertGreater(heuristic_count, 0)
        self.assertGreater(taxonomy_count, 0)

    def test_compare_and_report_work(self) -> None:
        total, items = self.api_db.fetch_runs(
            outcome="all",
            q=None,
            tool=None,
            source="all",
            min_steps=None,
            max_steps=None,
            has_errors="all",
            min_error_steps=None,
            max_error_steps=None,
            step_type="all",
            label=None,
            started_after=None,
            started_before=None,
            sort_by="run_id",
            sort_dir="asc",
            limit=500,
            offset=0,
        )
        self.assertGreater(total, 0)

        fail_run = next((row["run_id"] for row in items if row["outcome"] == "fail"), None)
        success_run = next((row["run_id"] for row in items if row["outcome"] == "success"), None)
        self.assertIsNotNone(fail_run)
        self.assertIsNotNone(success_run)

        comparison = self.api_db.fetch_compare(fail_run, success_run, mode="aligned")
        self.assertIsNotNone(comparison)
        self.assertGreater(comparison["total_pairs"], 0)

        report = self.api_db.fetch_run_report(fail_run)
        self.assertIsNotNone(report)
        self.assertIn("failure_summary", report)
        self.assertIn("top_tools", report)

    def test_week12_week15_case_study_and_evaluation_features(self) -> None:
        total, items = self.api_db.fetch_runs(
            outcome="all",
            q=None,
            tool=None,
            source="all",
            min_steps=None,
            max_steps=None,
            has_errors="all",
            min_error_steps=None,
            max_error_steps=None,
            step_type="all",
            label=None,
            started_after=None,
            started_before=None,
            sort_by="run_id",
            sort_dir="asc",
            limit=20,
            offset=0,
        )
        self.assertGreater(total, 0)
        run_id = items[0]["run_id"]

        case_study = self.api_db.upsert_case_study(
            run_id,
            {"title": "Test Case", "focus": "verification", "status": "active"},
        )
        self.assertEqual(case_study["run_id"], run_id)
        studies = self.api_db.fetch_case_studies(status="all", limit=100, offset=0)
        self.assertTrue(any(item["run_id"] == run_id for item in studies))

        note = self.api_db.insert_review_note(
            run_id,
            {"reviewer": "qa", "label": "debug", "note": "Looks consistent with expected failure path."},
        )
        self.assertEqual(note["run_id"], run_id)
        notes = self.api_db.fetch_review_notes(run_id, limit=20, offset=0)
        self.assertGreaterEqual(len(notes), 1)

        assignment = self.api_db.assign_benchmark_subset(
            run_id,
            {"subset_name": "unit_test_subset", "rationale": "unit test assignment"},
        )
        self.assertEqual(assignment["run_id"], run_id)
        subsets = self.api_db.fetch_benchmark_subsets(subset_name="unit_test_subset", limit=20, offset=0)
        self.assertTrue(any(item["run_id"] == run_id for item in subsets))

        auto_counts = self.api_db.auto_build_benchmark_subsets()
        self.assertIsInstance(auto_counts, dict)


if __name__ == "__main__":
    unittest.main()
