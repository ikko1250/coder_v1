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

import polars as pl

from analysis_backend.cli import _filter_sentences_for_tokens
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


def build_test_db_without_paragraph_metadata(db_path: Path) -> None:
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
        cursor.executemany(
            "INSERT INTO analysis_tokens VALUES (?, ?, ?, ?, ?)",
            [
                (1, 11, 0, "抑制", "抑制"),
                (1, 11, 1, "区域", "区域"),
                (1, 11, 2, "。", "。"),
            ],
        )
        cursor.execute(
            "INSERT INTO analysis_sentences VALUES (?, ?, ?)",
            (11, 1, 1),
        )
        connection.commit()
    finally:
        connection.close()


def build_large_distance_test_db(db_path: Path) -> None:
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

        rows: list[tuple[int, int, int, str, str]] = []
        token_no = 0
        for idx in range(4):
            rows.append((1, 11, token_no, "抑制", f"抑制{idx}"))
            token_no += 1
        for idx in range(3):
            rows.append((1, 11, token_no, "区域", f"区域{idx}"))
            token_no += 1
        rows.append((1, 11, token_no, "。", "。"))

        cursor.executemany(
            "INSERT INTO analysis_tokens VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        cursor.execute(
            "INSERT INTO analysis_sentences VALUES (?, ?, ?)",
            (11, 1, 1),
        )
        cursor.execute(
            "INSERT INTO analysis_paragraphs VALUES (?, ?)",
            (1, 100),
        )
        cursor.execute(
            "INSERT INTO analysis_documents VALUES (?, ?, ?)",
            (100, "テスト市", "条例"),
        )
        connection.commit()
    finally:
        connection.close()


def build_table_paragraph_test_db(db_path: Path) -> None:
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
                document_id INTEGER,
                is_table_paragraph INTEGER NOT NULL DEFAULT 0
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
                (1, 12, 0, "指定", "指定"),
                (1, 12, 1, "する", "する"),
                (1, 12, 2, "。", "。"),
            ],
        )
        cursor.executemany(
            "INSERT INTO analysis_sentences VALUES (?, ?, ?)",
            [
                (11, 1, 1),
                (12, 1, 2),
            ],
        )
        cursor.execute(
            "INSERT INTO analysis_paragraphs VALUES (?, ?, ?)",
            (1, 100, 1),
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


def build_warning_filter_config(filter_config_path: Path) -> None:
    payload = {
        "condition_match_logic": "unexpected",
        "max_reconstructed_paragraphs": 0,
        "distance_matching_mode": "unexpected",
        "distance_match_combination_cap": 0,
        "distance_match_strict_safety_limit": -1,
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


def build_fallback_filter_config(filter_config_path: Path) -> None:
    payload = {
        "condition_match_logic": "any",
        "max_reconstructed_paragraphs": 10,
        "distance_matching_mode": "auto-approx",
        "distance_match_combination_cap": 5,
        "cooccurrence_conditions": [
            {
                "condition_id": "suppress_area",
                "categories": ["概念:抑制区域"],
                "forms": ["抑制", "区域"],
                "form_match_logic": "all",
                "max_token_distance": 10,
                "search_scope": "sentence",
            }
        ],
    }
    filter_config_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_strict_limit_filter_config(filter_config_path: Path) -> None:
    payload = {
        "condition_match_logic": "any",
        "max_reconstructed_paragraphs": 10,
        "distance_matching_mode": "strict",
        "distance_match_strict_safety_limit": 5,
        "cooccurrence_conditions": [
            {
                "condition_id": "suppress_area",
                "categories": ["概念:抑制区域"],
                "forms": ["抑制", "区域"],
                "form_match_logic": "all",
                "max_token_distance": 10,
                "search_scope": "sentence",
            }
        ],
    }
    filter_config_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


class CliContractTests(unittest.TestCase):
    def test_filter_sentences_for_tokens_keeps_only_sentence_keys_present_in_limited_tokens(self) -> None:
        analysis_tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 2],
                "sentence_id": [11, 11, 21],
                "token_no": [0, 1, 0],
            }
        )
        analysis_sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12, 21, 31],
                "paragraph_id": [1, 1, 2, 3],
                "sentence_no_in_paragraph": [1, 2, 1, 1],
            }
        )

        filtered_df = _filter_sentences_for_tokens(
            analysis_tokens_df=analysis_tokens_df,
            analysis_sentences_df=analysis_sentences_df,
        )

        self.assertEqual(
            filtered_df.sort(["paragraph_id", "sentence_id"]).to_dict(as_series=False),
            {
                "sentence_id": [11, 21],
                "paragraph_id": [1, 2],
                "sentence_no_in_paragraph": [1, 1],
            },
        )

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

    def test_run_analysis_job_writes_failure_meta_json_for_data_access_result_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "empty.db"
            sqlite3.connect(db_path).close()
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_filter_config(filter_config_path)
            args = Namespace(
                job_id="data-access-failure-job",
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

            self.assertEqual(return_code, 1)
            self.assertEqual(stdout_buffer.getvalue(), "")

            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "failed")
            self.assertEqual(meta["jobId"], "data-access-failure-job")
            self.assertEqual(len(meta["warningMessages"]), 1)
            self.assertEqual(meta["warningMessages"][0]["code"], "sqlite_read_failed")
            self.assertEqual(meta["warningMessages"][0]["queryName"], "analysis_tokens")
            self.assertEqual(meta["warningMessages"][0]["severity"], "error")
            self.assertIn("SQLite read failed", meta["errorSummary"])
            self.assertIn("SQLite read failed", stderr_buffer.getvalue())

    def test_run_analysis_job_writes_failure_meta_json_for_metadata_result_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_test_db_without_paragraph_metadata(db_path)
            build_filter_config(filter_config_path)
            args = Namespace(
                job_id="metadata-failure-job",
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

            self.assertEqual(return_code, 1)
            self.assertEqual(stdout_buffer.getvalue(), "")

            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "failed")
            self.assertEqual(meta["jobId"], "metadata-failure-job")
            self.assertEqual(len(meta["warningMessages"]), 1)
            self.assertEqual(meta["warningMessages"][0]["code"], "sqlite_metadata_read_failed")
            self.assertEqual(meta["warningMessages"][0]["queryName"], "paragraph_document_metadata")
            self.assertIn("SQLite metadata read failed", meta["errorSummary"])
            self.assertIn("SQLite metadata read failed", stderr_buffer.getvalue())

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

    def test_run_analysis_job_inserts_newlines_for_table_paragraph_csv_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_table_paragraph_test_db(db_path)
            build_filter_config(filter_config_path)
            args = Namespace(
                job_id="table-paragraph-job",
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
            self.assertEqual(stderr_buffer.getvalue(), "")

            csv_path = output_dir / "result.csv"
            with csv_path.open(encoding="utf-8", newline="") as csv_file:
                rows = list(csv.DictReader(csv_file))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["paragraph_text"], "抑制区域\n指定する。")
            self.assertIn("\n", rows[0]["paragraph_text_tagged"])
            self.assertIn("[[HIT ", rows[0]["paragraph_text_tagged"])
            self.assertEqual(rows[0]["paragraph_text_highlight_html"].count("\n"), 1)

    def test_run_analysis_job_surfaces_matching_warnings_in_meta_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_large_distance_test_db(db_path)
            build_fallback_filter_config(filter_config_path)
            args = Namespace(
                job_id="warning-job",
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
            self.assertEqual(stderr_buffer.getvalue(), "")

            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "succeeded")
            self.assertEqual(len(meta["warningMessages"]), 1)
            warning = meta["warningMessages"][0]
            self.assertEqual(warning["code"], "distance_match_fallback")
            self.assertEqual(warning["conditionId"], "suppress_area")
            self.assertEqual(warning["requestedMode"], "auto-approx")
            self.assertEqual(warning["usedMode"], "approx")
            self.assertEqual(warning["combinationCap"], 5)
            self.assertGreaterEqual(warning["combinationCount"], 6)
            self.assertIn('"warningMessages"', stdout_buffer.getvalue())

    def test_run_analysis_job_surfaces_filter_config_warnings_in_meta_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_test_db(db_path)
            build_warning_filter_config(filter_config_path)
            args = Namespace(
                job_id="config-warning-job",
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
            self.assertEqual(stderr_buffer.getvalue(), "")

            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "succeeded")
            self.assertEqual(len(meta["warningMessages"]), 5)
            first_warning = meta["warningMessages"][0]
            self.assertEqual(first_warning["code"], "condition_match_logic_defaulted")
            self.assertEqual(first_warning["scope"], "filter_config")
            self.assertEqual(first_warning["severity"], "warning")
            self.assertEqual(first_warning["fieldName"], "condition_match_logic")
            self.assertIsNone(first_warning["conditionId"])
            self.assertIn('"warningMessages"', stdout_buffer.getvalue())

    def test_run_analysis_job_writes_failure_meta_json_for_strict_limit_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "analysis.db"
            filter_config_path = temp_path / "conditions.json"
            output_dir = temp_path / "job"
            build_large_distance_test_db(db_path)
            build_strict_limit_filter_config(filter_config_path)
            args = Namespace(
                job_id="strict-failure-job",
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

            self.assertEqual(return_code, 1)
            self.assertEqual(stdout_buffer.getvalue(), "")

            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.assertEqual(meta["status"], "failed")
            self.assertEqual(meta["jobId"], "strict-failure-job")
            self.assertEqual(meta["warningMessages"], [])
            self.assertIn("distance_match_strict_limit_exceeded", meta["errorSummary"])
            self.assertIn("distance_match_strict_limit_exceeded", stderr_buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
