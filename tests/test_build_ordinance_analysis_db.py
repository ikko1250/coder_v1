from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


def _load_builder_module():
    module_path = Path(__file__).resolve().parents[1] / "docs" / "build_ordinance_analysis_db.py"
    spec = importlib.util.spec_from_file_location("build_ordinance_analysis_db", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder_module()


class FolderInputBuildTest(unittest.TestCase):
    def test_folder_input_builds_analysis_db_with_category_columns(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "札幌市_条例.txt").write_text("第1条 本文です。", encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            self.assertTrue(analysis_db_path.exists())
            with sqlite3.connect(analysis_db_path) as conn:
                row = conn.execute(
                    """
                    SELECT file_name, category1, category2, raw_text
                    FROM analysis_documents
                    """
                ).fetchone()
            self.assertEqual(
                row,
                ("札幌市_条例.txt", "札幌市", "条例", "第1条 本文です。"),
            )

    def test_numeric_id_prefix_maps_to_category1_category2(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "100_かすみがうら市_施行規則.md").write_text(
                "第1条 本文です。", encoding="utf-8"
            )
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                row = conn.execute(
                    """
                    SELECT file_name, category1, category2, raw_text
                    FROM analysis_documents
                    """
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "100_かすみがうら市_施行規則.md",
                    "かすみがうら市",
                    "施行規則",
                    "第1条 本文です。",
                ),
            )

    def test_invalid_file_name_fails_before_output_db_update(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "invalid-name.txt").write_text("本文", encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 1)
            self.assertFalse(analysis_db_path.exists())
            report_path = analysis_db_path.with_name(f"{analysis_db_path.name}.report.json")
            self.assertTrue(report_path.exists())
            self.assertIn("invalid_file_name", report_path.read_text(encoding="utf-8"))

    def test_bom_prefixed_utf8_is_read_without_bom_character(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "カテゴリ1_カテゴリ2.txt").write_bytes(b"\xef\xbb\xbfBOM text")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                raw_text = conn.execute(
                    "SELECT raw_text FROM analysis_documents"
                ).fetchone()[0]
            self.assertEqual(raw_text, "BOM text")


class ForbiddenDirTest(unittest.TestCase):
    def test_exact_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            (forbidden / "cat1_cat2.txt").write_text("test", encoding="utf-8")

            rows, issues = builder.load_source_rows_from_dir(forbidden, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_parent_of_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            parent = temp_path / "parent"
            parent.mkdir()
            forbidden = parent / "forbidden"
            forbidden.mkdir()

            rows, issues = builder.load_source_rows_from_dir(parent, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_child_of_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            child = forbidden / "child"
            child.mkdir()

            rows, issues = builder.load_source_rows_from_dir(child, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_prune_excludes_forbidden_subtree(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "valid1_cat2.txt").write_text("test", encoding="utf-8")

            forbidden = input_dir / "forbidden"
            forbidden.mkdir()
            (forbidden / "valid2_cat2.txt").write_text("test", encoding="utf-8")

            # input_dir is parent of forbidden, so preflight blocks it
            rows, issues = builder.load_source_rows_from_dir(input_dir, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

            # Verify the prune logic itself (same / child dirs are dropped)
            dirs = ["forbidden", "allowed"]
            pruned = []
            for d in dirs:
                dir_path = (input_dir / d).resolve()
                is_forbidden = False
                for fdir in [forbidden]:
                    rel = builder.classify_forbidden_input_relation(dir_path, fdir)
                    if rel in ("same", "child"):
                        is_forbidden = True
                        break
                if not is_forbidden:
                    pruned.append(d)
            self.assertEqual(pruned, ["allowed"])

    def test_limit_counts_only_valid_candidates(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "a_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "b_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "c_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "invalid.txt").write_text("test", encoding="utf-8")
            (input_dir / "bad-name.md").write_text("test", encoding="utf-8")

            # Forbidden dir as a sibling; walk never reaches it, but limit should
            # still apply only to valid rows.
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            (forbidden / "d_cat2.txt").write_text("test", encoding="utf-8")

            rows, issues = builder.load_source_rows_from_dir(input_dir, 2, [forbidden])
            self.assertEqual(len(rows), 2)
            self.assertEqual(len([i for i in issues if i.code == "invalid_file_name"]), 2)


if __name__ == "__main__":
    unittest.main()
