from __future__ import annotations

from pathlib import Path

import polars as pl

from .condition_model import DataAccessResult
from .data_access import read_paragraph_document_metadata_result


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
        .with_columns(pl.col("match_group_ids").list.join(", ").alias("match_group_ids_text"))
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
