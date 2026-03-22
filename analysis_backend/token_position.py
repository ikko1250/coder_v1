from __future__ import annotations

import polars as pl

from .frame_schema import POSITIONED_TOKEN_SCHEMA
from .frame_schema import empty_df


def _with_sentence_table_flag(sentences_df: pl.DataFrame) -> pl.DataFrame:
    if "is_table_paragraph" in sentences_df.columns:
        return sentences_df.with_columns(
            pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64)
        )
    return sentences_df.with_columns(
        pl.lit(0, dtype=pl.Int64).alias("is_table_paragraph")
    )


def build_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int] | None = None,
    sentence_ids: list[int] | None = None,
    target_forms: list[str] | None = None,
    exclude_table_paragraphs: bool = False,
) -> pl.DataFrame:
    if (paragraph_ids is not None and not paragraph_ids) or (sentence_ids is not None and not sentence_ids):
        return empty_df(POSITIONED_TOKEN_SCHEMA)

    sentence_order_df = _with_sentence_table_flag(sentences_df).select(
        ["sentence_id", "paragraph_id", "sentence_no_in_paragraph", "is_table_paragraph"]
    )
    base_tokens_df = tokens_df
    if exclude_table_paragraphs:
        sentence_order_df = sentence_order_df.filter(pl.col("is_table_paragraph") == 0)
    if paragraph_ids is not None:
        sentence_order_df = sentence_order_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))
        base_tokens_df = base_tokens_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))
    if sentence_ids is not None:
        sentence_order_df = sentence_order_df.filter(pl.col("sentence_id").is_in(sentence_ids))
        base_tokens_df = base_tokens_df.filter(pl.col("sentence_id").is_in(sentence_ids))

    sentence_token_counts_df = (
        base_tokens_df
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
        .group_by(["paragraph_id", "sentence_no_in_paragraph"])
        .agg(pl.len().alias("sentence_token_count"))
        .sort(["paragraph_id", "sentence_no_in_paragraph"])
        .with_columns(
            (
                pl.col("sentence_token_count")
                .cum_sum()
                .over("paragraph_id", order_by="sentence_no_in_paragraph")
                - pl.col("sentence_token_count")
            )
            .alias("sentence_offset")
        )
        .select(["paragraph_id", "sentence_no_in_paragraph", "sentence_offset"])
    )

    selected_tokens_df = base_tokens_df
    if target_forms is not None:
        selected_tokens_df = selected_tokens_df.filter(pl.col("normalized_form").is_in(target_forms))

    return (
        selected_tokens_df
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
        .join(sentence_token_counts_df, on=["paragraph_id", "sentence_no_in_paragraph"], how="inner")
        .with_columns([
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            # Preserve the source token numbering because downstream code only relies on order and span.
            pl.col("token_no").cast(pl.Int64).alias("sentence_token_position"),
            (pl.col("sentence_offset") + pl.col("token_no")).cast(pl.Int64).alias("paragraph_token_position"),
        ])
        .select([
            "paragraph_id",
            "sentence_id",
            "sentence_no_in_paragraph",
            "is_table_paragraph",
            "token_no",
            "sentence_token_position",
            "paragraph_token_position",
            "normalized_form",
            "surface",
        ])
    )


def build_candidate_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    target_forms: list[str],
    *,
    exclude_table_paragraphs: bool = False,
) -> pl.DataFrame:
    return build_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        paragraph_ids=None,
        target_forms=target_forms,
        exclude_table_paragraphs=exclude_table_paragraphs,
    )
