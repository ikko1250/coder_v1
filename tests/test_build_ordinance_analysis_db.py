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


if __name__ == "__main__":
    unittest.main()
