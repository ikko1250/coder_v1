from __future__ import annotations

import polars as pl


POSITIONED_TOKEN_SCHEMA = {
    "paragraph_id": pl.Int64,
    "sentence_id": pl.Int64,
    "sentence_no_in_paragraph": pl.Int64,
    "is_table_paragraph": pl.Int64,
    "token_no": pl.Int64,
    "sentence_token_position": pl.Int64,
    "paragraph_token_position": pl.Int64,
    "normalized_form": pl.String,
    "surface": pl.String,
}
CONDITION_HIT_SCHEMA = {
    **POSITIONED_TOKEN_SCHEMA,
    "condition_id": pl.String,
    "category_text": pl.String,
    "categories": pl.List(pl.String),
    "match_group_id": pl.String,
    "match_role": pl.String,
}
PARAGRAPH_METADATA_SCHEMA = {
    "paragraph_id": pl.Int64,
    "document_id": pl.Int64,
    "municipality_name": pl.String,
    "doc_type": pl.String,
    "is_table_paragraph": pl.Int64,
}
SENTENCE_METADATA_SCHEMA = {
    "sentence_id": pl.Int64,
    "paragraph_id": pl.Int64,
    "document_id": pl.Int64,
    "municipality_name": pl.String,
    "doc_type": pl.String,
    "sentence_no_in_paragraph": pl.Int64,
    "sentence_no_in_document": pl.Int64,
    "sentence_text": pl.String,
    "is_table_paragraph": pl.Int64,
}


def empty_df(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)
