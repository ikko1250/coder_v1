from __future__ import annotations

from pathlib import Path

import polars as pl

from .condition_model import DataAccessResult
from .data_access import read_paragraph_document_metadata_result
from .data_access import read_sentence_document_metadata_result

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
    "matched_form_group_ids_text",
    "matched_form_group_logics_text",
    "form_group_explanations_text",
    "mixed_scope_warning_text",
    "match_group_ids_text",
    "match_group_count",
    "annotated_token_count",
    "manual_annotation_count",
    "manual_annotation_pairs_text",
    "manual_annotation_namespaces_text",
]
MANUAL_ANNOTATION_EXPORT_COLUMNS = [
    "manual_annotation_count",
    "manual_annotation_pairs_text",
    "manual_annotation_namespaces_text",
]
MANUAL_ANNOTATION_SUMMARY_JOIN_COLUMNS = [
    "paragraph_id",
    *MANUAL_ANNOTATION_EXPORT_COLUMNS,
]
FORM_GROUP_EXPLANATION_EXPORT_COLUMNS = [
    "matched_form_group_ids_text",
    "matched_form_group_logics_text",
    "form_group_explanations_text",
    "mixed_scope_warning_text",
]


def _build_manual_annotation_summary_join_df(
    manual_annotation_summary_df: pl.DataFrame | None,
) -> pl.DataFrame:
    if manual_annotation_summary_df is None or manual_annotation_summary_df.is_empty():
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "manual_annotation_count": pl.UInt32,
                "manual_annotation_pairs_text": pl.String,
                "manual_annotation_namespaces_text": pl.String,
            }
        )

    export_df = manual_annotation_summary_df
    if "paragraph_id" not in export_df.columns:
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "manual_annotation_count": pl.UInt32,
                "manual_annotation_pairs_text": pl.String,
                "manual_annotation_namespaces_text": pl.String,
            }
        )
    if "manual_annotation_count" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit(0).cast(pl.UInt32).alias("manual_annotation_count"))
    if "manual_annotation_pairs_text" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit("").alias("manual_annotation_pairs_text"))
    if "manual_annotation_namespaces_text" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit("").alias("manual_annotation_namespaces_text"))
    return export_df.with_columns([
        pl.col("paragraph_id").cast(pl.Int64),
        pl.col("manual_annotation_count").cast(pl.UInt32).fill_null(0),
        pl.col("manual_annotation_pairs_text").cast(pl.String).fill_null(""),
        pl.col("manual_annotation_namespaces_text").cast(pl.String).fill_null(""),
    ]).select(MANUAL_ANNOTATION_SUMMARY_JOIN_COLUMNS)


def _with_manual_annotation_export_columns(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    export_df = reconstructed_paragraphs_df
    if "manual_annotation_count" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit(0).cast(pl.UInt32).alias("manual_annotation_count"))
    if "manual_annotation_pairs_text" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit("").alias("manual_annotation_pairs_text"))
    if "manual_annotation_namespaces_text" not in export_df.columns:
        export_df = export_df.with_columns(pl.lit("").alias("manual_annotation_namespaces_text"))
    return export_df.with_columns([
        pl.col("manual_annotation_count").cast(pl.UInt32).fill_null(0),
        pl.col("manual_annotation_pairs_text").cast(pl.String).fill_null(""),
        pl.col("manual_annotation_namespaces_text").cast(pl.String).fill_null(""),
    ])


def _with_form_group_explanation_export_columns(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    export_df = reconstructed_paragraphs_df
    for column_name in FORM_GROUP_EXPLANATION_EXPORT_COLUMNS:
        if column_name not in export_df.columns:
            export_df = export_df.with_columns(pl.lit("").alias(column_name))
    return export_df.with_columns([
        pl.col("matched_form_group_ids_text").cast(pl.String).fill_null(""),
        pl.col("matched_form_group_logics_text").cast(pl.String).fill_null(""),
        pl.col("form_group_explanations_text").cast(pl.String).fill_null(""),
        pl.col("mixed_scope_warning_text").cast(pl.String).fill_null(""),
    ])


def enrich_reconstructed_paragraphs_result(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
    manual_annotation_summary_df: pl.DataFrame | None = None,
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
            _with_form_group_explanation_export_columns(reconstructed_paragraphs_base_df)
            .join(paragraph_metadata_result.data_frame, on="paragraph_id", how="left")
            .join(
                _build_manual_annotation_summary_join_df(manual_annotation_summary_df),
                on="paragraph_id",
                how="left",
            )
            .with_columns(
                [
                    pl.when(pl.col("doc_type").fill_null("").str.contains("施行規則", literal=True))
                    .then(pl.lit("施行規則"))
                    .when(pl.col("doc_type").fill_null("").str.contains("条例", literal=True))
                    .then(pl.lit("条例"))
                    .otherwise(pl.lit("不明"))
                    .alias("ordinance_or_rule"),
                    pl.col("manual_annotation_count").fill_null(0).cast(pl.UInt32),
                    pl.col("manual_annotation_pairs_text").fill_null(""),
                    pl.col("manual_annotation_namespaces_text").fill_null(""),
                ]
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
                "matched_form_group_ids_text",
                "matched_form_group_logics_text",
                "form_group_explanations_text",
                "mixed_scope_warning_text",
                "match_group_ids",
                "match_group_count",
                "annotated_token_count",
                "manual_annotation_count",
                "manual_annotation_pairs_text",
                "manual_annotation_namespaces_text",
            ])
        ),
        issues=[],
    )


def enrich_reconstructed_paragraphs_df(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
    manual_annotation_summary_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    enrichment_result = enrich_reconstructed_paragraphs_result(
        db_path=db_path,
        reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
        manual_annotation_summary_df=manual_annotation_summary_df,
    )
    if enrichment_result.data_frame is not None:
        return enrichment_result.data_frame
    issue = enrichment_result.issues[0]
    raise RuntimeError(issue.message)


def build_reconstructed_paragraphs_export_df(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    return (
        _with_manual_annotation_export_columns(
            _with_form_group_explanation_export_columns(reconstructed_paragraphs_df)
        )
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
            "matched_form_group_ids_text",
            "matched_form_group_logics_text",
            "form_group_explanations_text",
            "mixed_scope_warning_text",
            "match_group_ids_text",
            "match_group_count",
            "annotated_token_count",
            "manual_annotation_count",
            "manual_annotation_pairs_text",
            "manual_annotation_namespaces_text",
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
            pl.col("matched_form_group_ids_text").cast(pl.String).fill_null(""),
            pl.col("matched_form_group_logics_text").cast(pl.String).fill_null(""),
            pl.col("form_group_explanations_text").cast(pl.String).fill_null(""),
            pl.col("mixed_scope_warning_text").cast(pl.String).fill_null(""),
            pl.col("match_group_ids_text").cast(pl.String).fill_null(""),
            pl.col("match_group_count").cast(pl.String).fill_null(""),
            pl.col("annotated_token_count").cast(pl.String).fill_null(""),
            pl.col("manual_annotation_count").cast(pl.String).fill_null(""),
            pl.col("manual_annotation_pairs_text").cast(pl.String).fill_null(""),
            pl.col("manual_annotation_namespaces_text").cast(pl.String).fill_null(""),
        ])
    )


def build_gui_records(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> list[dict[str, str]]:
    return [
        {key: str(value) for key, value in record.items()}
        for record in build_gui_records_df(reconstructed_paragraphs_df).to_dicts()
    ]


def enrich_reconstructed_sentences_result(
    db_path: Path,
    reconstructed_sentences_base_df: pl.DataFrame,
) -> DataAccessResult:
    sentence_ids = (
        reconstructed_sentences_base_df.get_column("sentence_id").to_list()
        if not reconstructed_sentences_base_df.is_empty()
        else []
    )
    sentence_metadata_result = read_sentence_document_metadata_result(
        db_path=db_path,
        sentence_ids=sentence_ids,
    )
    if sentence_metadata_result.data_frame is None:
        return DataAccessResult(
            data_frame=None,
            issues=sentence_metadata_result.issues,
        )

    return DataAccessResult(
        data_frame=(
            reconstructed_sentences_base_df
            .join(
                sentence_metadata_result.data_frame,
                on=["sentence_id", "paragraph_id"],
                how="left",
                suffix="_metadata",
            )
            .with_columns([
                pl.when(pl.col("doc_type").fill_null("").str.contains("施行規則", literal=True))
                .then(pl.lit("施行規則"))
                .when(pl.col("doc_type").fill_null("").str.contains("条例", literal=True))
                .then(pl.lit("条例"))
                .otherwise(pl.lit("不明"))
                .alias("ordinance_or_rule"),
                pl.col("sentence_no_in_document").fill_null(0).cast(pl.Int64),
            ])
            .select([
                "sentence_id",
                "paragraph_id",
                "document_id",
                "municipality_name",
                "ordinance_or_rule",
                "doc_type",
                "sentence_no_in_paragraph",
                "sentence_no_in_document",
                "sentence_text",
                "sentence_text_tagged",
                "sentence_text_highlight_html",
                "matched_condition_ids",
                "matched_condition_ids_text",
                "matched_categories",
                "matched_categories_text",
                "match_group_ids",
                "match_group_ids_text",
                "match_group_count",
                "annotated_token_count",
            ])
        ),
        issues=[],
    )


def build_reconstructed_sentences_export_df(
    reconstructed_sentences_df: pl.DataFrame,
) -> pl.DataFrame:
    return reconstructed_sentences_df.select([
        "sentence_id",
        "paragraph_id",
        "document_id",
        "municipality_name",
        "ordinance_or_rule",
        "doc_type",
        "sentence_no_in_paragraph",
        "sentence_no_in_document",
        "sentence_text",
        "sentence_text_tagged",
        "sentence_text_highlight_html",
        "matched_condition_ids_text",
        "matched_categories_text",
        "match_group_ids_text",
        "match_group_count",
        "annotated_token_count",
    ])
