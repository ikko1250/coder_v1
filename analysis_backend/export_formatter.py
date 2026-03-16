from __future__ import annotations

from pathlib import Path

import polars as pl

from .condition_model import DataAccessResult
from .data_access import read_paragraph_document_metadata_result

GUI_RECORD_COLUMNS = [
    "paragraph_id",
    "document_id",
    "municipality_name",
    "ordinance_or_rule",
    "doc_type",
    "sentence_count",
    "paragraph_text",
    "paragraph_text_tagged",
    "matched_condition_ids_text",
    "matched_categories_text",
    "match_group_ids_text",
    "match_group_count",
    "annotated_token_count",
]


def enrich_reconstructed_paragraphs_result(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
) -> DataAccessResult:
    paragraph_ids = (
        reconstructed_paragraphs_base_df.get_column("paragraph_id").to_list()
        if not reconstructed_paragraphs_base_df.is_empty()
        else []
    )
    paragraph_metadata_result = read_paragraph_document_metadata_result(
        db_path=db_path,
        paragraph_ids=paragraph_ids,
    )
    if paragraph_metadata_result.data_frame is None:
        return DataAccessResult(
            data_frame=None,
            issues=paragraph_metadata_result.issues,
        )
    return DataAccessResult(
        data_frame=(
            reconstructed_paragraphs_base_df
            .join(paragraph_metadata_result.data_frame, on="paragraph_id", how="left")
            .with_columns(
                pl.when(pl.col("doc_type").fill_null("").str.contains("施行規則", literal=True))
                .then(pl.lit("施行規則"))
                .when(pl.col("doc_type").fill_null("").str.contains("条例", literal=True))
                .then(pl.lit("条例"))
                .otherwise(pl.lit("不明"))
                .alias("ordinance_or_rule")
            )
            .select([
                "paragraph_id",
                "document_id",
                "municipality_name",
                "ordinance_or_rule",
                "doc_type",
                "sentence_count",
                "paragraph_text",
                "paragraph_text_tagged",
                "paragraph_text_highlight_html",
                "matched_condition_ids",
                "matched_condition_ids_text",
                "matched_categories",
                "matched_categories_text",
                "match_group_ids",
                "match_group_count",
                "annotated_token_count",
            ])
        ),
        issues=[],
    )


def enrich_reconstructed_paragraphs_df(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
) -> pl.DataFrame:
    enrichment_result = enrich_reconstructed_paragraphs_result(
        db_path=db_path,
        reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
    )
    if enrichment_result.data_frame is not None:
        return enrichment_result.data_frame
    issue = enrichment_result.issues[0]
    raise RuntimeError(issue.message)


def build_reconstructed_paragraphs_export_df(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    return (
        reconstructed_paragraphs_df
        .with_columns(
            pl.col("match_group_ids")
            .cast(pl.List(pl.String))
            .list.join(", ")
            .fill_null("")
            .alias("match_group_ids_text")
        )
        .select([
            "paragraph_id",
            "document_id",
            "municipality_name",
            "ordinance_or_rule",
            "doc_type",
            "sentence_count",
            "paragraph_text",
            "paragraph_text_tagged",
            "paragraph_text_highlight_html",
            "matched_condition_ids_text",
            "matched_categories_text",
            "match_group_ids_text",
            "match_group_count",
            "annotated_token_count",
        ])
    )


def build_gui_records_df(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    export_df = build_reconstructed_paragraphs_export_df(reconstructed_paragraphs_df)
    return (
        export_df
        .select(GUI_RECORD_COLUMNS)
        .with_columns([
            pl.col("paragraph_id").cast(pl.String).fill_null(""),
            pl.col("document_id").cast(pl.String).fill_null(""),
            pl.col("municipality_name").cast(pl.String).fill_null(""),
            pl.col("ordinance_or_rule").cast(pl.String).fill_null(""),
            pl.col("doc_type").cast(pl.String).fill_null(""),
            pl.col("sentence_count").cast(pl.String).fill_null(""),
            pl.col("paragraph_text").cast(pl.String).fill_null(""),
            pl.col("paragraph_text_tagged").cast(pl.String).fill_null(""),
            pl.col("matched_condition_ids_text").cast(pl.String).fill_null(""),
            pl.col("matched_categories_text").cast(pl.String).fill_null(""),
            pl.col("match_group_ids_text").cast(pl.String).fill_null(""),
            pl.col("match_group_count").cast(pl.String).fill_null(""),
            pl.col("annotated_token_count").cast(pl.String).fill_null(""),
        ])
    )


def build_gui_records(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> list[dict[str, str]]:
    return [
        {key: str(value) for key, value in record.items()}
        for record in build_gui_records_df(reconstructed_paragraphs_df).to_dicts()
    ]
