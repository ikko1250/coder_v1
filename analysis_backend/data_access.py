from __future__ import annotations

from pathlib import Path
import sqlite3

import polars as pl

from .condition_model import DataAccessIssue
from .condition_model import DataAccessResult
from .frame_schema import ANALYSIS_SENTENCES_READ_SCHEMA
from .frame_schema import PARAGRAPH_METADATA_SCHEMA
from .frame_schema import SENTENCE_METADATA_SCHEMA
from .frame_schema import empty_df


PARAGRAPH_METADATA_CHUNK_SIZE = 900


def _sqlite_select_to_polars(
    conn: sqlite3.Connection,
    query: str,
    *,
    empty_schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Run a SELECT and build a DataFrame without Polars holding the SQLite handle (Windows-safe)."""
    cursor = conn.execute(query.strip())
    column_names = [str(d[0]) for d in cursor.description]
    rows = cursor.fetchall()
    if not rows:
        return empty_df(empty_schema)
    return pl.DataFrame(rows, schema=column_names, orient="row")


def _build_data_access_issue(
    *,
    code: str,
    message: str,
    query_name: str,
    db_path: Path,
) -> DataAccessIssue:
    return DataAccessIssue(
        code=code,
        severity="error",
        message=message,
        query_name=query_name,
        db_path=str(db_path),
    )


def _read_table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def _read_database_df_result(db_path: Path, query: str, *, query_name: str) -> DataAccessResult:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            return DataAccessResult(
                data_frame=pl.read_database(query=query, connection=conn),
                issues=[],
            )
    except sqlite3.Error as exc:
        return DataAccessResult(
            data_frame=None,
            issues=[
                _build_data_access_issue(
                    code="sqlite_read_failed",
                    message=f"SQLite read failed: {db_path} ({exc})",
                    query_name=query_name,
                    db_path=db_path,
                )
            ],
        )


def _unwrap_data_access_result(result: DataAccessResult) -> pl.DataFrame:
    if result.data_frame is not None:
        return result.data_frame
    issue = result.issues[0]
    raise RuntimeError(issue.message)


def read_analysis_tokens_result(db_path: Path, limit_rows: int | None = None) -> DataAccessResult:
    query = "SELECT * FROM analysis_tokens"
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df_result(db_path=db_path, query=query, query_name="analysis_tokens")


def read_analysis_tokens(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _unwrap_data_access_result(
        read_analysis_tokens_result(db_path=db_path, limit_rows=limit_rows)
    )


def read_analysis_sentences_result(db_path: Path, limit_rows: int | None = None) -> DataAccessResult:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            sentence_columns = _read_table_columns(conn, "analysis_sentences")
            paragraph_columns = _read_table_columns(conn, "analysis_paragraphs")
            sentence_text_joined = (
                "COALESCE(s.sentence_text, '') AS sentence_text"
                if "sentence_text" in sentence_columns
                else "'' AS sentence_text"
            )
            sentence_text_bare = (
                "COALESCE(sentence_text, '') AS sentence_text"
                if "sentence_text" in sentence_columns
                else "'' AS sentence_text"
            )
            if "is_table_paragraph" in paragraph_columns:
                query = f"""
                    SELECT
                        s.sentence_id,
                        s.paragraph_id,
                        s.sentence_no_in_paragraph,
                        COALESCE(CAST(p.is_table_paragraph AS INTEGER), 0) AS is_table_paragraph,
                        {sentence_text_joined}
                    FROM analysis_sentences AS s
                    LEFT JOIN analysis_paragraphs AS p
                      ON p.paragraph_id = s.paragraph_id
                """
            else:
                query = f"""
                    SELECT
                        sentence_id,
                        paragraph_id,
                        sentence_no_in_paragraph,
                        0 AS is_table_paragraph,
                        {sentence_text_bare}
                    FROM analysis_sentences
                """
            if limit_rows is not None:
                query = f"{query} LIMIT {int(limit_rows)}"
            raw_df = _sqlite_select_to_polars(
                conn,
                query,
                empty_schema=ANALYSIS_SENTENCES_READ_SCHEMA,
            )
        typed_df = raw_df.with_columns([
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("is_table_paragraph").cast(pl.Int64),
            pl.col("sentence_text").cast(pl.String).fill_null(""),
        ])
        return DataAccessResult(
            data_frame=typed_df.select(list(ANALYSIS_SENTENCES_READ_SCHEMA.keys())),
            issues=[],
        )
    except sqlite3.Error as exc:
        return DataAccessResult(
            data_frame=None,
            issues=[
                _build_data_access_issue(
                    code="sqlite_read_failed",
                    message=f"SQLite read failed: {db_path} ({exc})",
                    query_name="analysis_sentences",
                    db_path=db_path,
                )
            ],
        )


def read_analysis_sentences(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _unwrap_data_access_result(
        read_analysis_sentences_result(db_path=db_path, limit_rows=limit_rows)
    )


def read_paragraph_document_metadata_result(
    db_path: Path,
    paragraph_ids: list[int],
) -> DataAccessResult:
    if not paragraph_ids:
        return DataAccessResult(
            data_frame=empty_df(PARAGRAPH_METADATA_SCHEMA),
            issues=[],
        )

    rows: list[tuple[int, int, str | None, str | None, int]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            paragraph_columns = _read_table_columns(conn, "analysis_paragraphs")
            if "is_table_paragraph" in paragraph_columns:
                table_flag_select = "COALESCE(CAST(p.is_table_paragraph AS INTEGER), 0) AS is_table_paragraph"
            else:
                table_flag_select = "0 AS is_table_paragraph"
            for start_idx in range(0, len(paragraph_ids), PARAGRAPH_METADATA_CHUNK_SIZE):
                chunk_ids = paragraph_ids[start_idx:start_idx + PARAGRAPH_METADATA_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk_ids)
                query = f"""
                    SELECT
                        p.paragraph_id,
                        p.document_id,
                        d.municipality_name,
                        d.doc_type,
                        {table_flag_select}
                    FROM analysis_paragraphs AS p
                    JOIN analysis_documents AS d
                      ON d.document_id = p.document_id
                    WHERE p.paragraph_id IN ({placeholders})
                """
                rows.extend(conn.execute(query, tuple(chunk_ids)).fetchall())
    except sqlite3.Error as exc:
        return DataAccessResult(
            data_frame=None,
            issues=[
                _build_data_access_issue(
                    code="sqlite_metadata_read_failed",
                    message=f"SQLite metadata read failed: {db_path} ({exc})",
                    query_name="paragraph_document_metadata",
                    db_path=db_path,
                )
            ],
        )

    if not rows:
        return DataAccessResult(
            data_frame=empty_df(PARAGRAPH_METADATA_SCHEMA),
            issues=[],
        )

    return DataAccessResult(
        data_frame=(
            pl.DataFrame(rows, schema=list(PARAGRAPH_METADATA_SCHEMA.keys()), orient="row")
            .with_columns([
                pl.col("paragraph_id").cast(pl.Int64),
                pl.col("document_id").cast(pl.Int64),
            ])
            .sort("paragraph_id")
        ),
        issues=[],
    )


def read_paragraph_document_metadata(db_path: Path, paragraph_ids: list[int]) -> pl.DataFrame:
    return _unwrap_data_access_result(
        read_paragraph_document_metadata_result(db_path=db_path, paragraph_ids=paragraph_ids)
    )


def read_sentence_document_metadata_result(
    db_path: Path,
    sentence_ids: list[int],
) -> DataAccessResult:
    if not sentence_ids:
        return DataAccessResult(
            data_frame=empty_df(SENTENCE_METADATA_SCHEMA),
            issues=[],
        )

    rows: list[tuple[int, int, int, str | None, str | None, int | None, int | None, str | None, int]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            sentence_columns = _read_table_columns(conn, "analysis_sentences")
            paragraph_columns = _read_table_columns(conn, "analysis_paragraphs")
            sentence_no_in_document_select = (
                "COALESCE(CAST(s.sentence_no_in_document AS INTEGER), 0) AS sentence_no_in_document"
                if "sentence_no_in_document" in sentence_columns
                else "0 AS sentence_no_in_document"
            )
            sentence_text_select = (
                "COALESCE(s.sentence_text, '') AS sentence_text"
                if "sentence_text" in sentence_columns
                else "'' AS sentence_text"
            )
            table_flag_select = (
                "COALESCE(CAST(p.is_table_paragraph AS INTEGER), 0) AS is_table_paragraph"
                if "is_table_paragraph" in paragraph_columns
                else "0 AS is_table_paragraph"
            )
            for start_idx in range(0, len(sentence_ids), PARAGRAPH_METADATA_CHUNK_SIZE):
                chunk_ids = sentence_ids[start_idx:start_idx + PARAGRAPH_METADATA_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk_ids)
                query = f"""
                    SELECT
                        s.sentence_id,
                        s.paragraph_id,
                        p.document_id,
                        d.municipality_name,
                        d.doc_type,
                        COALESCE(CAST(s.sentence_no_in_paragraph AS INTEGER), 0) AS sentence_no_in_paragraph,
                        {sentence_no_in_document_select},
                        {sentence_text_select},
                        {table_flag_select}
                    FROM analysis_sentences AS s
                    JOIN analysis_paragraphs AS p
                      ON p.paragraph_id = s.paragraph_id
                    JOIN analysis_documents AS d
                      ON d.document_id = p.document_id
                    WHERE s.sentence_id IN ({placeholders})
                """
                rows.extend(conn.execute(query, tuple(chunk_ids)).fetchall())
    except sqlite3.Error as exc:
        return DataAccessResult(
            data_frame=None,
            issues=[
                _build_data_access_issue(
                    code="sqlite_metadata_read_failed",
                    message=f"SQLite metadata read failed: {db_path} ({exc})",
                    query_name="sentence_document_metadata",
                    db_path=db_path,
                )
            ],
        )

    if not rows:
        return DataAccessResult(
            data_frame=empty_df(SENTENCE_METADATA_SCHEMA),
            issues=[],
        )

    return DataAccessResult(
        data_frame=(
            pl.DataFrame(rows, schema=list(SENTENCE_METADATA_SCHEMA.keys()), orient="row")
            .with_columns([
                pl.col("sentence_id").cast(pl.Int64),
                pl.col("paragraph_id").cast(pl.Int64),
                pl.col("document_id").cast(pl.Int64),
                pl.col("sentence_no_in_paragraph").cast(pl.Int64),
                pl.col("sentence_no_in_document").cast(pl.Int64),
                pl.col("sentence_text").cast(pl.String).fill_null(""),
                pl.col("is_table_paragraph").cast(pl.Int64),
            ])
            .sort(["paragraph_id", "sentence_no_in_paragraph", "sentence_id"])
        ),
        issues=[],
    )


def read_sentence_document_metadata(db_path: Path, sentence_ids: list[int]) -> pl.DataFrame:
    return _unwrap_data_access_result(
        read_sentence_document_metadata_result(db_path=db_path, sentence_ids=sentence_ids)
    )
