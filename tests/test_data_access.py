from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from analysis_backend.data_access import read_analysis_sentences_result
from analysis_backend.data_access import read_paragraph_document_metadata_result
from analysis_backend.data_access import read_sentence_document_metadata_result
from analysis_backend.frame_schema import ANALYSIS_SENTENCES_READ_SCHEMA
from analysis_backend.frame_schema import PARAGRAPH_METADATA_SCHEMA
from analysis_backend.frame_schema import SENTENCE_METADATA_SCHEMA


class ReadAnalysisSentencesResultTest(unittest.TestCase):
    def _create_minimal_db(
        self,
        db_path: Path,
        *,
        include_sentence_text_column: bool,
        sentence_text_value: str | None,
    ) -> None:
        connection = sqlite3.connect(db_path)
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE analysis_paragraphs (
                    paragraph_id INTEGER,
                    document_id INTEGER,
                    is_table_paragraph INTEGER
                )
                """
            )
            cols = "sentence_id INTEGER, paragraph_id INTEGER, sentence_no_in_paragraph INTEGER"
            if include_sentence_text_column:
                cols += ", sentence_text TEXT"
            cursor.execute(f"CREATE TABLE analysis_sentences ({cols})")
            cursor.execute("INSERT INTO analysis_paragraphs VALUES (1, 100, 0)")
            if include_sentence_text_column:
                cursor.execute(
                    "INSERT INTO analysis_sentences VALUES (?, ?, ?, ?)",
                    (11, 1, 1, sentence_text_value),
                )
            else:
                cursor.execute(
                    "INSERT INTO analysis_sentences VALUES (?, ?, ?)",
                    (11, 1, 1),
                )
            connection.commit()
        finally:
            connection.close()

    def test_schema_matches_contract(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            self._create_minimal_db(
                db_path,
                include_sentence_text_column=True,
                sentence_text_value="第1文",
            )
            result = read_analysis_sentences_result(db_path=db_path)
            self.assertEqual(result.issues, [])
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.columns, list(ANALYSIS_SENTENCES_READ_SCHEMA.keys()))
            self.assertEqual(
                result.data_frame.row(0),
                (11, 1, 1, 0, "第1文"),
            )

    def test_missing_sentence_text_column_yields_empty_string(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            self._create_minimal_db(
                db_path,
                include_sentence_text_column=False,
                sentence_text_value=None,
            )
            result = read_analysis_sentences_result(db_path=db_path)
            self.assertEqual(result.issues, [])
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.get_column("sentence_text").to_list(), [""])

    def test_null_sentence_text_coerces_to_empty_string(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            self._create_minimal_db(
                db_path,
                include_sentence_text_column=True,
                sentence_text_value=None,
            )
            result = read_analysis_sentences_result(db_path=db_path)
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.get_column("sentence_text").to_list(), [""])

    def test_limit_rows(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            connection = sqlite3.connect(db_path)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "CREATE TABLE analysis_paragraphs (paragraph_id INTEGER, document_id INTEGER, is_table_paragraph INTEGER)"
                )
                cursor.execute(
                    """
                    CREATE TABLE analysis_sentences (
                        sentence_id INTEGER,
                        paragraph_id INTEGER,
                        sentence_no_in_paragraph INTEGER,
                        sentence_text TEXT
                    )
                    """
                )
                cursor.executemany(
                    "INSERT INTO analysis_paragraphs VALUES (?, ?, ?)",
                    [(1, 100, 0), (2, 100, 0)],
                )
                cursor.executemany(
                    "INSERT INTO analysis_sentences VALUES (?, ?, ?, ?)",
                    [
                        (11, 1, 1, "a"),
                        (21, 2, 1, "b"),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            result = read_analysis_sentences_result(db_path=db_path, limit_rows=1)
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.height, 1)

class ReadDocumentMetadataCompatibilityTest(unittest.TestCase):
    def test_paragraph_metadata_prefers_category_columns(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            connection = sqlite3.connect(db_path)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    CREATE TABLE analysis_paragraphs (
                        paragraph_id INTEGER,
                        document_id INTEGER,
                        is_table_paragraph INTEGER
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE analysis_documents (
                        document_id INTEGER,
                        category1 TEXT,
                        category2 TEXT
                    )
                    """
                )
                cursor.execute("INSERT INTO analysis_paragraphs VALUES (1, 100, 0)")
                cursor.execute(
                    "INSERT INTO analysis_documents VALUES (?, ?, ?)",
                    (100, "カテゴリA", "カテゴリB"),
                )
                connection.commit()
            finally:
                connection.close()

            result = read_paragraph_document_metadata_result(db_path=db_path, paragraph_ids=[1])
            self.assertEqual(result.issues, [])
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.columns, list(PARAGRAPH_METADATA_SCHEMA.keys()))
            self.assertEqual(
                result.data_frame.row(0),
                (1, 100, "カテゴリA", "カテゴリB", 0),
            )

    def test_sentence_metadata_reads_category_columns(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "t.db"
            connection = sqlite3.connect(db_path)
            try:
                cursor = connection.cursor()
                cursor.execute(
                    """
                    CREATE TABLE analysis_paragraphs (
                        paragraph_id INTEGER,
                        document_id INTEGER,
                        is_table_paragraph INTEGER
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE analysis_sentences (
                        sentence_id INTEGER,
                        paragraph_id INTEGER,
                        sentence_no_in_paragraph INTEGER,
                        sentence_no_in_document INTEGER,
                        sentence_text TEXT
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE analysis_documents (
                        document_id INTEGER,
                        category1 TEXT,
                        category2 TEXT
                    )
                    """
                )
                cursor.execute("INSERT INTO analysis_paragraphs VALUES (1, 100, 1)")
                cursor.execute("INSERT INTO analysis_sentences VALUES (11, 1, 2, 5, '本文')")
                cursor.execute(
                    "INSERT INTO analysis_documents VALUES (?, ?, ?)",
                    (100, "カテゴリA", "カテゴリB"),
                )
                connection.commit()
            finally:
                connection.close()

            result = read_sentence_document_metadata_result(db_path=db_path, sentence_ids=[11])
            self.assertEqual(result.issues, [])
            assert result.data_frame is not None
            self.assertEqual(result.data_frame.columns, list(SENTENCE_METADATA_SCHEMA.keys()))
            self.assertEqual(
                result.data_frame.row(0),
                (11, 1, 100, "カテゴリA", "カテゴリB", 2, 5, "本文", 1),
            )


if __name__ == "__main__":
    unittest.main()
