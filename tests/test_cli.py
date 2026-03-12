from __future__ import annotations

from argparse import Namespace
from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from analysis_backend.cli import run_analysis_job


def build_test_db(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE analysis_tokens (
                paragraph_id INTEGER,
                sentence_id INTEGER,
                token_no INTEGER,
                normalized_form TEXT,
                surface TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE analysis_sentences (
                sentence_id INTEGER,
                paragraph_id INTEGER,
                sentence_no_in_paragraph INTEGER
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE analysis_paragraphs (
                paragraph_id INTEGER,
                document_id INTEGER
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE analysis_documents (
                document_id INTEGER,
                municipality_name TEXT,
                doc_type TEXT
            )
            """
        )

        cursor.executemany(
            "INSERT INTO analysis_tokens VALUES (?, ?, ?, ?, ?)",
            [
                (1, 11, 0, "抑制", "抑制"),
                (1, 11, 1, "区域", "区域"),
                (1, 11, 2, "指定", "指定"),
                (1, 11, 3, "する", "する"),
                (1, 11, 4, "。", "。"),
                (2, 21, 0, "その他", "その他"),
                (2, 21, 1, "規定", "規定"),
                (2, 21, 2, "。", "。"),
            ],
        )
        cursor.executemany(
            "INSERT INTO analysis_sentences VALUES (?, ?, ?)",
            [
                (11, 1, 1),
                (21, 2, 1),
            ],
        )
        cursor.executemany(
            "INSERT INTO analysis_paragraphs VALUES (?, ?)",
            [
                (1, 100),
                (2, 100),
            ],
        )
        cursor.execute(
            "INSERT INTO analysis_documents VALUES (?, ?, ?)",
            (100, "テスト市", "条例"),
        )
        connection.commit()
    finally:
        connection.close()


def build_filter_config(filter_config_path: Path) -> None:
    payload = {
        "condition_match_logic": "any",
        "max_reconstructed_paragraphs": 10,
        "cooccurrence_conditions": [
            {
                "condition_id": "suppress_area",
                "categories": ["概念:抑制区域"],
                "forms": ["抑制", "区域"],
                "form_match_logic": "all",
                "max_token_distance": 5,
                "search_scope": "sentence",
            }
        ],
    }
    filter_config_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class CliContractTests(unittest.TestCase):
    def test_run_analysis_job_writes_failure_meta_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "job"
            args = Namespace(
                job_id="failure-job",
                db_path=str(Path(temp_dir) / "missing.db"),
                filter_config_path=str(Path(temp_dir) / "missing.json"),
                output_dir=str(output_dir),
                output_csv_path=None,
                output_meta_json_path=None,
                limit_rows=None,
            )

            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                return_code = run_analysis_job(args)

            self.assertEqual(return_code, 1)
            meta_path = output_dir / "meta.json"
            self.assertTrue(meta_path.exists())
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "failed")
            self.assertEqual(meta["jobId"], "failure-job")
            self.assertEqual(meta["warningMessages"], [])
            self.assertIn("Filter config JSON not found", meta["errorSummary"])
            self.assertIn("Filter config JSON not found", stderr_buffer.getvalue())

    def test_run_analysis_job_writes_success_csv_and_meta_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_test_db(db_path)
            build_filter_config(filter_config_path)
            args = Namespace(
                job_id="success-job",
                db_path=str(db_path),
                filter_config_path=str(filter_config_path),
                output_dir=str(output_dir),
                output_csv_path=None,
                output_meta_json_path=None,
                limit_rows=None,
            )

            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                return_code = run_analysis_job(args)

            self.assertEqual(return_code, 0)

            meta_path = output_dir / "meta.json"
            csv_path = output_dir / "result.csv"
            self.assertTrue(meta_path.exists())
            self.assertTrue(csv_path.exists())

            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "succeeded")
            self.assertEqual(meta["jobId"], "success-job")
            self.assertEqual(meta["selectedParagraphCount"], 1)
            self.assertEqual(meta["outputCsvPath"], str(csv_path))
            self.assertEqual(meta["warningMessages"], [])
            self.assertEqual(stderr_buffer.getvalue(), "")
            self.assertIn('"status": "succeeded"', stdout_buffer.getvalue())

            with csv_path.open(encoding="utf-8", newline="") as csv_file:
                rows = list(csv.DictReader(csv_file))

            self.assertEqual(len(rows), 1)
            self.assertEqual(
                list(rows[0].keys()),
                [
                    "paragraph_id",
                    "document_id",
                    "municipality_name",
                    "ordinance_or_rule",
                    "doc_type",
                    "sentence_count",
                    "paragraph_text",
                    "paragraph_text_tagged",
                    "paragraph_text_highlight_html",
                    "matched_condition_ids_text",
                    "matched_categories_text",
                    "match_group_ids_text",
                    "match_group_count",
                    "annotated_token_count",
                ],
            )
            self.assertEqual(rows[0]["paragraph_id"], "1")
            self.assertEqual(rows[0]["municipality_name"], "テスト市")
            self.assertEqual(rows[0]["matched_condition_ids_text"], "suppress_area")
            self.assertEqual(rows[0]["matched_categories_text"], "概念:抑制区域")
            self.assertIn("[[HIT ", rows[0]["paragraph_text_tagged"])


if __name__ == "__main__":
    unittest.main()
