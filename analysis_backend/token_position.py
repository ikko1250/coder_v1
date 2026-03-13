from __future__ import annotations

import polars as pl


POSITIONED_TOKEN_SCHEMA = {
    "paragraph_id": pl.Int64,
    "sentence_id": pl.Int64,
    "sentence_no_in_paragraph": pl.Int64,
    "token_no": pl.Int64,
    "sentence_token_position": pl.Int64,
    "paragraph_token_position": pl.Int64,
    "normalized_form": pl.String,
    "surface": pl.String,
}


def _empty_df(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def build_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int] | None = None,
    target_forms: list[str] | None = None,
) -> pl.DataFrame:
    if paragraph_ids is not None and not paragraph_ids:
        return _empty_df(POSITIONED_TOKEN_SCHEMA)

    sentence_order_df = sentences_df.select(["sentence_id", "paragraph_id", "sentence_no_in_paragraph"])
    base_tokens_df = tokens_df
    if paragraph_ids is not None:
        sentence_order_df = sentence_order_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))
        base_tokens_df = base_tokens_df.filter(pl.col("paragraph_id").is_in(paragraph_ids))

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
            pl.col("token_no").cast(pl.Int64),
            # Preserve the source token numbering because downstream code only relies on order and span.
            pl.col("token_no").cast(pl.Int64).alias("sentence_token_position"),
            (pl.col("sentence_offset") + pl.col("token_no")).cast(pl.Int64).alias("paragraph_token_position"),
        ])
        .select([
            "paragraph_id",
            "sentence_id",
            "sentence_no_in_paragraph",
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
) -> pl.DataFrame:
    return build_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        paragraph_ids=None,
        target_forms=target_forms,
    )
