from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .frame_schema import ANALYSIS_SENTENCES_READ_SCHEMA
from .frame_schema import TEXT_UNIT_PARAGRAPH_FRAME_SCHEMA
from .frame_schema import empty_df


@dataclass(frozen=True)
class TextUnitFrames:
    """Canonical full-text carriers for substring conditions (FT-01a).

    Sentence rows follow `ANALYSIS_SENTENCES_READ_SCHEMA` (DB `sentence_text`).
    Paragraph rows join sentences with the same separator rule as paragraph rendering:
    `is_table_paragraph == 1` → ``"\\n"``, otherwise ``""``.
    """

    sentence_frame: pl.DataFrame
    paragraph_frame: pl.DataFrame


def build_text_unit_frames(sentences_df: pl.DataFrame) -> TextUnitFrames:
    """Build sentence- and paragraph-level text frames once per evaluation (FT-01a)."""
    if sentences_df.is_empty():
        return TextUnitFrames(
            sentence_frame=empty_df(ANALYSIS_SENTENCES_READ_SCHEMA),
            paragraph_frame=empty_df(TEXT_UNIT_PARAGRAPH_FRAME_SCHEMA),
        )

    work = sentences_df
    if "sentence_text" not in work.columns:
        work = work.with_columns(pl.lit("").alias("sentence_text"))
    if "is_table_paragraph" not in work.columns:
        work = work.with_columns(pl.lit(0, dtype=pl.Int64).alias("is_table_paragraph"))

    work = (
        work.with_columns(
            [
                pl.col("sentence_id").cast(pl.Int64),
                pl.col("paragraph_id").cast(pl.Int64),
                pl.col("sentence_no_in_paragraph").cast(pl.Int64),
                pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64),
                pl.col("sentence_text").fill_null("").cast(pl.String),
            ]
        )
        .sort(["paragraph_id", "sentence_no_in_paragraph", "sentence_id"])
    )

    sentence_frame = work.select(list(ANALYSIS_SENTENCES_READ_SCHEMA.keys()))

    paragraph_frame = (
        work.group_by("paragraph_id", maintain_order=True)
        .agg(
            [
                pl.col("sentence_text").alias("_parts"),
                pl.col("is_table_paragraph").first().alias("is_table_paragraph"),
                pl.len().cast(pl.UInt32).alias("sentence_count"),
            ]
        )
        .with_columns(
            pl.when(pl.col("is_table_paragraph") == 1)
            .then(pl.col("_parts").list.join("\n"))
            .otherwise(pl.col("_parts").list.join(""))
            .alias("paragraph_text")
        )
        .select(list(TEXT_UNIT_PARAGRAPH_FRAME_SCHEMA.keys()))
        .sort("paragraph_id")
    )

    return TextUnitFrames(sentence_frame=sentence_frame, paragraph_frame=paragraph_frame)
