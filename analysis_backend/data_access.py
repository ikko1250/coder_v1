from __future__ import annotations

from pathlib import Path
import sqlite3

import polars as pl

from .frame_schema import PARAGRAPH_METADATA_SCHEMA
from .frame_schema import empty_df


PARAGRAPH_METADATA_CHUNK_SIZE = 900


def _read_database_df(db_path: Path, query: str) -> pl.DataFrame:
    try:
        with sqlite3.connect(str(db_path)) as conn:
            return pl.read_database(query=query, connection=conn)
    except sqlite3.Error as exc:
        raise RuntimeError(f"SQLite read failed: {db_path} ({exc})") from exc


def read_analysis_tokens(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    query = "SELECT * FROM analysis_tokens"
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df(db_path=db_path, query=query)


def read_analysis_sentences(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    query = """
        SELECT sentence_id, paragraph_id, sentence_no_in_paragraph
        FROM analysis_sentences
    """
    if limit_rows is not None:
        query = f"{query} LIMIT {int(limit_rows)}"
    return _read_database_df(db_path=db_path, query=query)


def read_paragraph_document_metadata(db_path: Path, paragraph_ids: list[int]) -> pl.DataFrame:
    if not paragraph_ids:
        return empty_df(PARAGRAPH_METADATA_SCHEMA)

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
        raise RuntimeError(f"SQLite metadata read failed: {db_path} ({exc})") from exc

    if not rows:
        return empty_df(PARAGRAPH_METADATA_SCHEMA)

    return (
        pl.DataFrame(rows, schema=list(PARAGRAPH_METADATA_SCHEMA.keys()), orient="row")
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("document_id").cast(pl.Int64),
        ])
        .sort("paragraph_id")
    )
