from __future__ import annotations

from pathlib import Path
import sqlite3

import polars as pl

from .condition_model import DataAccessIssue
from .condition_model import DataAccessResult
from .frame_schema import PARAGRAPH_METADATA_SCHEMA
from .frame_schema import empty_df


PARAGRAPH_METADATA_CHUNK_SIZE = 900


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
    query = """
        SELECT sentence_id, paragraph_id, sentence_no_in_paragraph
        FROM analysis_sentences
    """
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df_result(db_path=db_path, query=query, query_name="analysis_sentences")


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

    rows: list[tuple[int, int, str | None, str | None]] = []
    try:
        with sqlite3.connect(str(db_path)) as conn:
            for start_idx in range(0, len(paragraph_ids), PARAGRAPH_METADATA_CHUNK_SIZE):
                chunk_ids = paragraph_ids[start_idx:start_idx + PARAGRAPH_METADATA_CHUNK_SIZE]
                placeholders = ",".join("?" for _ in chunk_ids)
                query = f"""
                    SELECT
                        p.paragraph_id,
                        p.document_id,
                        d.municipality_name,
                        d.doc_type
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
