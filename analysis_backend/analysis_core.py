from __future__ import annotations

from pathlib import Path

import polars as pl

from .condition_model import ConditionHitResult
from .condition_model import DataAccessResult
from .condition_model import DistanceMatchingMode
from .condition_model import FilterConfig
from .condition_model import LoadFilterConfigResult
from .condition_model import NormalizedCondition
from .condition_evaluator import normalize_cooccurrence_conditions as _normalize_cooccurrence_conditions_impl
from .condition_evaluator import select_target_ids_by_conditions_result as _select_target_ids_by_conditions_result_impl
from .data_access import read_analysis_sentences as _read_analysis_sentences_impl
from .data_access import read_analysis_sentences_result as _read_analysis_sentences_result_impl
from .data_access import read_analysis_tokens as _read_analysis_tokens_impl
from .data_access import read_analysis_tokens_result as _read_analysis_tokens_result_impl
from .data_access import read_paragraph_document_metadata as _read_paragraph_document_metadata_impl
from .data_access import read_paragraph_document_metadata_result as _read_paragraph_document_metadata_result_impl
from .data_access import read_sentence_document_metadata as _read_sentence_document_metadata_impl
from .data_access import read_sentence_document_metadata_result as _read_sentence_document_metadata_result_impl
from .distance_matcher import build_condition_hit_result as _build_condition_hit_result_impl
from .export_formatter import build_reconstructed_paragraphs_export_df as _build_reconstructed_paragraphs_export_df_impl
from .export_formatter import build_reconstructed_sentences_export_df as _build_reconstructed_sentences_export_df_impl
from .export_formatter import enrich_reconstructed_paragraphs_df as _enrich_reconstructed_paragraphs_df_impl
from .export_formatter import enrich_reconstructed_paragraphs_result as _enrich_reconstructed_paragraphs_result_impl
from .export_formatter import enrich_reconstructed_sentences_result as _enrich_reconstructed_sentences_result_impl
from .filter_config import load_filter_config as _load_filter_config_impl
from .filter_config import load_filter_config_result as _load_filter_config_result_impl
from .frame_schema import CONDITION_HIT_SCHEMA
from .frame_schema import POSITIONED_TOKEN_SCHEMA
from .frame_schema import empty_df
from .rendering import build_rendered_paragraphs_df as _build_rendered_paragraphs_df_impl
from .rendering import build_rendered_sentences_df as _build_rendered_sentences_df_impl
from .rendering import build_token_annotations_df as _build_token_annotations_df_impl
from .rendering import render_tagged_token as _render_tagged_token_impl
from .token_position import build_tokens_with_position_df as _build_tokens_with_position_df_impl

__all__ = [
    "build_condition_hit_tokens_df",
    "build_condition_hit_result",
    "build_reconstructed_paragraphs_export_df",
    "build_reconstructed_sentences_export_df",
    "build_rendered_paragraphs_df",
    "build_rendered_sentences_df",
    "build_token_annotations_df",
    "build_tokens_with_position_df",
    "enrich_reconstructed_paragraphs_df",
    "enrich_reconstructed_paragraphs_result",
    "load_filter_config",
    "load_filter_config_result",
    "read_analysis_sentences",
    "read_analysis_sentences_result",
    "read_analysis_tokens",
    "read_analysis_tokens_result",
    "read_paragraph_document_metadata",
    "read_paragraph_document_metadata_result",
    "read_sentence_document_metadata",
    "read_sentence_document_metadata_result",
    "reconstruct_paragraphs_by_ids",
    "reconstruct_sentences_by_ids",
    "select_target_ids_by_cooccurrence_conditions",
]

PARAGRAPH_ID_COL = "paragraph_id"
SENTENCE_ID_COL = "sentence_id"
SENTENCE_NO_COL = "sentence_no_in_paragraph"
TOKEN_NO_COL = "token_no"
NORMALIZED_FORM_COL = "normalized_form"
SURFACE_COL = "surface"
SENTENCE_TOKEN_POSITION_COL = "sentence_token_position"
PARAGRAPH_TOKEN_POSITION_COL = "paragraph_token_position"
def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return empty_df(CONDITION_HIT_SCHEMA)


def load_filter_config(filter_config_path: Path) -> FilterConfig:
    return _load_filter_config_impl(filter_config_path)


def load_filter_config_result(filter_config_path: Path) -> LoadFilterConfigResult:
    return _load_filter_config_result_impl(filter_config_path)


def read_analysis_tokens(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _read_analysis_tokens_impl(db_path=db_path, limit_rows=limit_rows)


def read_analysis_tokens_result(db_path: Path, limit_rows: int | None = None) -> DataAccessResult:
    return _read_analysis_tokens_result_impl(db_path=db_path, limit_rows=limit_rows)


def read_analysis_sentences(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _read_analysis_sentences_impl(db_path=db_path, limit_rows=limit_rows)


def read_analysis_sentences_result(db_path: Path, limit_rows: int | None = None) -> DataAccessResult:
    return _read_analysis_sentences_result_impl(db_path=db_path, limit_rows=limit_rows)


def read_paragraph_document_metadata(db_path: Path, paragraph_ids: list[int]) -> pl.DataFrame:
    return _read_paragraph_document_metadata_impl(db_path=db_path, paragraph_ids=paragraph_ids)


def read_paragraph_document_metadata_result(
    db_path: Path,
    paragraph_ids: list[int],
) -> DataAccessResult:
    return _read_paragraph_document_metadata_result_impl(
        db_path=db_path,
        paragraph_ids=paragraph_ids,
    )


def read_sentence_document_metadata(db_path: Path, sentence_ids: list[int]) -> pl.DataFrame:
    return _read_sentence_document_metadata_impl(db_path=db_path, sentence_ids=sentence_ids)


def read_sentence_document_metadata_result(
    db_path: Path,
    sentence_ids: list[int],
) -> DataAccessResult:
    return _read_sentence_document_metadata_result_impl(
        db_path=db_path,
        sentence_ids=sentence_ids,
    )


def build_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int] | None = None,
    sentence_ids: list[int] | None = None,
    target_forms: list[str] | None = None,
    exclude_table_paragraphs: bool = False,
) -> pl.DataFrame:
    return _build_tokens_with_position_df_impl(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        paragraph_ids=paragraph_ids,
        sentence_ids=sentence_ids,
        target_forms=target_forms,
        exclude_table_paragraphs=exclude_table_paragraphs,
    )


def render_tagged_token(
    surface: str,
    annotation: dict[str, object] | None,
) -> tuple[str, str, list[str], list[str], list[str], int]:
    return _render_tagged_token_impl(surface=surface, annotation=annotation)

def _normalized_conditions_to_dicts(
    normalized_conditions: list[NormalizedCondition],
) -> list[dict[str, object]]:
    return [
        {
            "condition_id": condition.condition_id,
            "categories": condition.categories,
            "category_text": condition.category_text,
            "overall_search_scope": condition.overall_search_scope,
            "forms": condition.forms,
            "form_groups": [
                {
                    "forms": form_group.forms,
                    "match_logic": form_group.match_logic,
                    "combine_logic": form_group.combine_logic,
                    "search_scope": form_group.search_scope,
                    "requested_max_token_distance": form_group.requested_max_token_distance,
                    "effective_max_token_distance": form_group.effective_max_token_distance,
                    "anchor_form": form_group.anchor_form,
                    "exclude_forms_any": form_group.exclude_forms_any,
                }
                for form_group in condition.form_groups
            ],
            "annotation_filters": [
                {
                    "namespace": annotation_filter.label_namespace,
                    "key": annotation_filter.label_key,
                    "value": annotation_filter.label_value,
                    "operator": annotation_filter.operator,
                }
                for annotation_filter in condition.annotation_filters
            ],
            "required_categories_all": condition.required_categories_all,
            "required_categories_any": condition.required_categories_any,
            "search_scope": condition.search_scope,
            "form_match_logic": condition.form_match_logic,
            "requested_max_token_distance": condition.requested_max_token_distance,
            "effective_max_token_distance": condition.effective_max_token_distance,
        }
        for condition in normalized_conditions
    ]


def build_condition_hit_tokens_df(
    tokens_with_position_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    distance_matching_mode: DistanceMatchingMode = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> pl.DataFrame:
    # Legacy facade: keep returning a DataFrame while the matcher uses structured results internally.
    if tokens_with_position_df.is_empty():
        return _empty_condition_hit_tokens_df()

    normalized_conditions = _normalize_cooccurrence_conditions_impl(cooccurrence_conditions)
    if not normalized_conditions:
        return _empty_condition_hit_tokens_df()

    return _build_condition_hit_result_impl(
        tokens_with_position_df=tokens_with_position_df,
        cooccurrence_conditions=_normalized_conditions_to_dicts(normalized_conditions),
        distance_matching_mode=distance_matching_mode,
        distance_match_combination_cap=distance_match_combination_cap,
        distance_match_strict_safety_limit=distance_match_strict_safety_limit,
    ).condition_hit_tokens_df


def build_condition_hit_result(
    tokens_with_position_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    distance_matching_mode: DistanceMatchingMode = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> ConditionHitResult:
    normalized_conditions = _normalize_cooccurrence_conditions_impl(cooccurrence_conditions)
    return _build_condition_hit_result_impl(
        tokens_with_position_df=tokens_with_position_df,
        cooccurrence_conditions=_normalized_conditions_to_dicts(normalized_conditions),
        distance_matching_mode=distance_matching_mode,
        distance_match_combination_cap=distance_match_combination_cap,
        distance_match_strict_safety_limit=distance_match_strict_safety_limit,
    )


def build_token_annotations_df(condition_hit_tokens_df: pl.DataFrame) -> pl.DataFrame:
    return _build_token_annotations_df_impl(condition_hit_tokens_df=condition_hit_tokens_df)


def build_rendered_paragraphs_df(
    tokens_with_position_df: pl.DataFrame,
    token_annotations_df: pl.DataFrame,
    paragraph_match_summary_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    return _build_rendered_paragraphs_df_impl(
        tokens_with_position_df=tokens_with_position_df,
        token_annotations_df=token_annotations_df,
        paragraph_match_summary_df=paragraph_match_summary_df,
    )


def build_rendered_sentences_df(
    tokens_with_position_df: pl.DataFrame,
    token_annotations_df: pl.DataFrame,
    sentence_match_summary_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    return _build_rendered_sentences_df_impl(
        tokens_with_position_df=tokens_with_position_df,
        token_annotations_df=token_annotations_df,
        sentence_match_summary_df=sentence_match_summary_df,
    )


def enrich_reconstructed_paragraphs_result(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
    manual_annotation_summary_df: pl.DataFrame | None = None,
) -> DataAccessResult:
    return _enrich_reconstructed_paragraphs_result_impl(
        db_path=db_path,
        reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
        manual_annotation_summary_df=manual_annotation_summary_df,
    )


def select_target_ids_by_cooccurrence_conditions(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    condition_match_logic: str = "any",
    max_paragraph_ids: int = 100,
    normalized_paragraph_annotations_df: pl.DataFrame | None = None,
    analysis_unit: str = "paragraph",
    distance_matching_mode: DistanceMatchingMode = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list[int], list[int]]:
    # Legacy facade: keep returning the historical 5-item tuple for existing callers.
    normalized_conditions = _normalize_cooccurrence_conditions_impl(cooccurrence_conditions)
    selection_result = _select_target_ids_by_conditions_result_impl(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        normalized_conditions=normalized_conditions,
        condition_match_logic=condition_match_logic,
        max_paragraph_ids=max_paragraph_ids,
        normalized_paragraph_annotations_df=normalized_paragraph_annotations_df,
        analysis_unit=analysis_unit,
        distance_matching_mode=distance_matching_mode,
        distance_match_combination_cap=distance_match_combination_cap,
        distance_match_strict_safety_limit=distance_match_strict_safety_limit,
    )
    return (
        selection_result.candidate_tokens_df,
        selection_result.condition_eval_df,
        selection_result.paragraph_match_summary_df,
        selection_result.target_paragraph_ids,
        selection_result.target_sentence_ids,
    )


def reconstruct_sentences_by_ids(tokens_df: pl.DataFrame, sentence_ids: list[int]) -> pl.DataFrame:
    if not sentence_ids:
        return pl.DataFrame(
            schema={
                "sentence_id": pl.Int64,
                "paragraph_id": pl.Int64,
                "token_count": pl.UInt32,
                "sentence_text": pl.String,
            }
        )

    return (
        tokens_df
        .filter(pl.col("sentence_id").is_in(sentence_ids))
        .group_by(["sentence_id", "paragraph_id"])
        .agg([
            pl.len().alias("token_count"),
            pl.col("surface").sort_by("token_no").list.join("").alias("sentence_text"),
        ])
        .sort("sentence_id")
    )


def reconstruct_paragraphs_by_ids(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int],
) -> pl.DataFrame:
    if not paragraph_ids:
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "sentence_count": pl.UInt32,
                "paragraph_text": pl.String,
            }
        )

    sentence_order_df = (
        sentences_df
        .filter(pl.col("paragraph_id").is_in(paragraph_ids))
        .with_columns(
            pl.col("is_table_paragraph").fill_null(0).cast(pl.Int64)
            if "is_table_paragraph" in sentences_df.columns
            else pl.lit(0, dtype=pl.Int64).alias("is_table_paragraph")
        )
        .select(["sentence_id", "paragraph_id", "sentence_no_in_paragraph", "is_table_paragraph"])
    )
    joined_df = (
        tokens_df
        .filter(pl.col("paragraph_id").is_in(paragraph_ids))
        .join(sentence_order_df, on=["sentence_id", "paragraph_id"], how="inner")
    )
    if joined_df.is_empty():
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "sentence_count": pl.UInt32,
                "paragraph_text": pl.String,
            }
        )

    sentence_rows: list[dict[str, object]] = []
    for sentence_df in joined_df.partition_by(["paragraph_id", "sentence_id"]):
        sorted_sentence_df = sentence_df.sort("token_no")
        sentence_rows.append(
            {
                "paragraph_id": int(sorted_sentence_df.get_column("paragraph_id")[0]),
                "sentence_id": int(sorted_sentence_df.get_column("sentence_id")[0]),
                "sentence_no_in_paragraph": int(sorted_sentence_df.get_column("sentence_no_in_paragraph")[0]),
                "is_table_paragraph": int(sorted_sentence_df.get_column("is_table_paragraph")[0]),
                "sentence_text": "".join(sorted_sentence_df.get_column("surface").to_list()),
            }
        )

    sentence_text_df = pl.DataFrame(sentence_rows)

    paragraph_rows: list[dict[str, object]] = []
    for paragraph_df in sentence_text_df.partition_by("paragraph_id"):
        sorted_sentence_df = paragraph_df.sort("sentence_no_in_paragraph")
        separator = "\n" if int(sorted_sentence_df.get_column("is_table_paragraph")[0]) == 1 else ""
        paragraph_rows.append(
            {
                "paragraph_id": int(sorted_sentence_df.get_column("paragraph_id")[0]),
                "sentence_count": int(sorted_sentence_df.height),
                "paragraph_text": separator.join(sorted_sentence_df.get_column("sentence_text").to_list()),
            }
        )

    return (
        pl.DataFrame(paragraph_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_count").cast(pl.UInt32),
        ])
        .sort("paragraph_id")
    )


def enrich_reconstructed_paragraphs_df(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
    manual_annotation_summary_df: pl.DataFrame | None = None,
) -> pl.DataFrame:
    return _enrich_reconstructed_paragraphs_df_impl(
        db_path=db_path,
        reconstructed_paragraphs_base_df=reconstructed_paragraphs_base_df,
        manual_annotation_summary_df=manual_annotation_summary_df,
    )


def build_reconstructed_paragraphs_export_df(
    reconstructed_paragraphs_df: pl.DataFrame,
) -> pl.DataFrame:
    return _build_reconstructed_paragraphs_export_df_impl(
        reconstructed_paragraphs_df=reconstructed_paragraphs_df
    )


def enrich_reconstructed_sentences_result(
    db_path: Path,
    reconstructed_sentences_base_df: pl.DataFrame,
) -> DataAccessResult:
    return _enrich_reconstructed_sentences_result_impl(
        db_path=db_path,
        reconstructed_sentences_base_df=reconstructed_sentences_base_df,
    )


def build_reconstructed_sentences_export_df(
    reconstructed_sentences_df: pl.DataFrame,
) -> pl.DataFrame:
    return _build_reconstructed_sentences_export_df_impl(
        reconstructed_sentences_df=reconstructed_sentences_df,
    )
