from __future__ import annotations

from html import escape
from pathlib import Path

import polars as pl

from .condition_model import DistanceMatchingMode
from .condition_model import FilterConfig
from .condition_model import NormalizedCondition
from .condition_evaluator import normalize_cooccurrence_conditions as _normalize_cooccurrence_conditions_impl
from .condition_evaluator import select_target_ids_by_conditions_result as _select_target_ids_by_conditions_result_impl
from .data_access import read_analysis_sentences as _read_analysis_sentences_impl
from .data_access import read_analysis_tokens as _read_analysis_tokens_impl
from .data_access import read_paragraph_document_metadata as _read_paragraph_document_metadata_impl
from .distance_matcher import build_condition_hit_result as _build_condition_hit_result_impl
from .filter_config import load_filter_config as _load_filter_config_impl
from .token_position import build_tokens_with_position_df as _build_tokens_with_position_df_impl

__all__ = [
    "build_condition_hit_tokens_df",
    "build_reconstructed_paragraphs_export_df",
    "build_rendered_paragraphs_df",
    "build_token_annotations_df",
    "build_tokens_with_position_df",
    "enrich_reconstructed_paragraphs_df",
    "load_filter_config",
    "read_analysis_sentences",
    "read_analysis_tokens",
    "read_paragraph_document_metadata",
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
POSITIONED_TOKEN_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    SENTENCE_ID_COL: pl.Int64,
    SENTENCE_NO_COL: pl.Int64,
    TOKEN_NO_COL: pl.Int64,
    SENTENCE_TOKEN_POSITION_COL: pl.Int64,
    PARAGRAPH_TOKEN_POSITION_COL: pl.Int64,
    NORMALIZED_FORM_COL: pl.String,
    SURFACE_COL: pl.String,
}
CONDITION_HIT_SCHEMA = {
    **POSITIONED_TOKEN_SCHEMA,
    "condition_id": pl.String,
    "category_text": pl.String,
    "categories": pl.List(pl.String),
    "match_group_id": pl.String,
    "match_role": pl.String,
}
TOKEN_ANNOTATION_SCHEMA = {
    **POSITIONED_TOKEN_SCHEMA,
    "condition_ids": pl.List(pl.String),
    "category_texts": pl.List(pl.String),
    "categories": pl.List(pl.String),
    "match_group_ids": pl.List(pl.String),
    "match_roles": pl.List(pl.String),
    "annotation_count": pl.UInt32,
}
RENDERED_PARAGRAPH_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "sentence_count": pl.UInt32,
    "paragraph_text": pl.String,
    "paragraph_text_tagged": pl.String,
    "paragraph_text_highlight_html": pl.String,
    "matched_condition_ids": pl.List(pl.String),
    "matched_condition_ids_text": pl.String,
    "matched_categories": pl.List(pl.String),
    "matched_categories_text": pl.String,
    "match_group_ids": pl.List(pl.String),
    "match_group_count": pl.UInt32,
    "annotated_token_count": pl.UInt32,
}

def _empty_df(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)
def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return _empty_df(CONDITION_HIT_SCHEMA)


def _empty_token_annotations_df() -> pl.DataFrame:
    return _empty_df(TOKEN_ANNOTATION_SCHEMA)


def _empty_rendered_paragraphs_df() -> pl.DataFrame:
    return _empty_df(RENDERED_PARAGRAPH_SCHEMA)


def load_filter_config(filter_config_path: Path) -> FilterConfig:
    return _load_filter_config_impl(filter_config_path)


def read_analysis_tokens(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _read_analysis_tokens_impl(db_path=db_path, limit_rows=limit_rows)


def read_analysis_sentences(db_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    return _read_analysis_sentences_impl(db_path=db_path, limit_rows=limit_rows)


def read_paragraph_document_metadata(db_path: Path, paragraph_ids: list[int]) -> pl.DataFrame:
    return _read_paragraph_document_metadata_impl(db_path=db_path, paragraph_ids=paragraph_ids)


def build_tokens_with_position_df(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    paragraph_ids: list[int] | None = None,
    target_forms: list[str] | None = None,
) -> pl.DataFrame:
    return _build_tokens_with_position_df_impl(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        paragraph_ids=paragraph_ids,
        target_forms=target_forms,
    )


def _unique_in_order(values: list[str]) -> list[str]:
    unique_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        if value and value not in seen_values:
            seen_values.add(value)
            unique_values.append(value)
    return unique_values


def _escape_tag_attribute(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\"", "\\\"")

def _normalized_conditions_to_dicts(
    normalized_conditions: list[NormalizedCondition],
) -> list[dict[str, object]]:
    return [
        {
            "condition_id": condition.condition_id,
            "categories": condition.categories,
            "category_text": condition.category_text,
            "forms": condition.forms,
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


def build_token_annotations_df(condition_hit_tokens_df: pl.DataFrame) -> pl.DataFrame:
    if condition_hit_tokens_df.is_empty():
        return _empty_token_annotations_df()

    annotation_rows: list[dict[str, object]] = []
    grouped_rows: dict[tuple[int, int, int], dict[str, object]] = {}
    sorted_hit_rows = condition_hit_tokens_df.sort([
        "paragraph_id",
        "sentence_id",
        "sentence_token_position",
        "condition_id",
        "match_group_id",
    ]).iter_rows(named=True)

    for hit_row in sorted_hit_rows:
        key = (
            int(hit_row["paragraph_id"]),
            int(hit_row["sentence_id"]),
            int(hit_row["token_no"]),
        )
        if key not in grouped_rows:
            grouped_rows[key] = {
                "paragraph_id": int(hit_row["paragraph_id"]),
                "sentence_id": int(hit_row["sentence_id"]),
                "sentence_no_in_paragraph": int(hit_row["sentence_no_in_paragraph"]),
                "token_no": int(hit_row["token_no"]),
                "sentence_token_position": int(hit_row["sentence_token_position"]),
                "paragraph_token_position": int(hit_row["paragraph_token_position"]),
                "normalized_form": str(hit_row["normalized_form"]) if hit_row["normalized_form"] is not None else "",
                "surface": str(hit_row["surface"]) if hit_row["surface"] is not None else "",
                "condition_ids": [],
                "category_texts": [],
                "categories": [],
                "match_group_ids": [],
                "match_roles": [],
            }

        grouped_row = grouped_rows[key]
        condition_id = str(hit_row["condition_id"])
        category_text = str(hit_row["category_text"])
        match_group_id = str(hit_row["match_group_id"])
        match_role = str(hit_row["match_role"])
        if condition_id not in grouped_row["condition_ids"]:
            grouped_row["condition_ids"].append(condition_id)
        if category_text and category_text not in grouped_row["category_texts"]:
            grouped_row["category_texts"].append(category_text)
        for category in hit_row["categories"]:
            category_value = str(category)
            if category_value and category_value not in grouped_row["categories"]:
                grouped_row["categories"].append(category_value)
        if match_group_id not in grouped_row["match_group_ids"]:
            grouped_row["match_group_ids"].append(match_group_id)
        if match_role not in grouped_row["match_roles"]:
            grouped_row["match_roles"].append(match_role)

    for grouped_row in grouped_rows.values():
        annotation_rows.append({**grouped_row, "annotation_count": len(grouped_row["condition_ids"])})

    return (
        pl.DataFrame(annotation_rows)
        .with_columns([
            pl.col("paragraph_id").cast(pl.Int64),
            pl.col("sentence_id").cast(pl.Int64),
            pl.col("sentence_no_in_paragraph").cast(pl.Int64),
            pl.col("token_no").cast(pl.Int64),
            pl.col("sentence_token_position").cast(pl.Int64),
            pl.col("paragraph_token_position").cast(pl.Int64),
            pl.col("annotation_count").cast(pl.UInt32),
        ])
        .sort(["paragraph_id", "sentence_id", "sentence_token_position"])
    )


def _build_annotation_lookup(
    token_annotations_df: pl.DataFrame,
) -> dict[tuple[int, int, int], dict[str, object]]:
    annotation_lookup: dict[tuple[int, int, int], dict[str, object]] = {}
    if token_annotations_df.is_empty():
        return annotation_lookup

    for annotation_row in token_annotations_df.iter_rows(named=True):
        annotation_lookup[
            (
                int(annotation_row[PARAGRAPH_ID_COL]),
                int(annotation_row[SENTENCE_ID_COL]),
                int(annotation_row[TOKEN_NO_COL]),
            )
        ] = dict(annotation_row)
    return annotation_lookup


def render_tagged_token(
    surface: str,
    annotation: dict[str, object] | None,
) -> tuple[str, str, list[str], list[str], list[str], int]:
    if annotation is None:
        return surface, escape(surface), [], [], [], 0

    condition_ids = list(annotation["condition_ids"])
    categories = list(annotation["categories"])
    category_texts = list(annotation["category_texts"])
    match_group_ids = list(annotation["match_group_ids"])

    tagged_fragment = (
        "[[HIT "
        f"condition_ids=\"{_escape_tag_attribute(','.join(condition_ids))}\" "
        f"categories=\"{_escape_tag_attribute(','.join(categories))}\" "
        f"groups=\"{_escape_tag_attribute(','.join(match_group_ids))}\""
        f"]]{surface}[[/HIT]]"
    )
    title_text = " / ".join(_unique_in_order(category_texts + condition_ids))
    html_fragment = (
        "<mark "
        "class=\"co-hit\" "
        f"data-condition-ids=\"{escape(' '.join(condition_ids))}\" "
        f"data-categories=\"{escape(' | '.join(categories))}\" "
        f"title=\"{escape(title_text)}\""
        f">{escape(surface)}</mark>"
    )
    return tagged_fragment, html_fragment, condition_ids, categories, match_group_ids, 1


def _render_sentence_fragment(
    sentence_df: pl.DataFrame,
    annotation_lookup: dict[tuple[int, int, int], dict[str, object]],
) -> dict[str, object]:
    sentence_no = int(sentence_df.get_column(SENTENCE_NO_COL)[0])
    plain_parts: list[str] = []
    tagged_parts: list[str] = []
    html_parts: list[str] = []
    matched_condition_ids: list[str] = []
    matched_categories: list[str] = []
    match_group_ids: list[str] = []
    annotated_token_count = 0

    for token_row in sentence_df.sort(TOKEN_NO_COL).iter_rows(named=True):
        key = (
            int(token_row[PARAGRAPH_ID_COL]),
            int(token_row[SENTENCE_ID_COL]),
            int(token_row[TOKEN_NO_COL]),
        )
        surface = str(token_row[SURFACE_COL]) if token_row[SURFACE_COL] is not None else ""
        plain_parts.append(surface)

        tagged_fragment, html_fragment, condition_ids, categories, grouped_match_ids, annotated_increment = (
            render_tagged_token(surface=surface, annotation=annotation_lookup.get(key))
        )
        tagged_parts.append(tagged_fragment)
        html_parts.append(html_fragment)
        matched_condition_ids.extend(condition_ids)
        matched_categories.extend(categories)
        match_group_ids.extend(grouped_match_ids)
        annotated_token_count += annotated_increment

    return {
        "sentence_no": sentence_no,
        "plain_text": "".join(plain_parts),
        "tagged_text": "".join(tagged_parts),
        "html_text": "".join(html_parts),
        "matched_condition_ids": matched_condition_ids,
        "matched_categories": matched_categories,
        "match_group_ids": match_group_ids,
        "annotated_token_count": annotated_token_count,
    }


def build_rendered_paragraphs_df(
    tokens_with_position_df: pl.DataFrame,
    token_annotations_df: pl.DataFrame,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_rendered_paragraphs_df()

    annotation_lookup = _build_annotation_lookup(token_annotations_df)
    paragraph_rows: list[dict[str, object]] = []
    for paragraph_df in tokens_with_position_df.sort([
        PARAGRAPH_ID_COL,
        SENTENCE_NO_COL,
        TOKEN_NO_COL,
    ]).partition_by(PARAGRAPH_ID_COL):
        paragraph_id = int(paragraph_df.get_column(PARAGRAPH_ID_COL)[0])
        sentence_fragments: list[dict[str, object]] = []
        matched_condition_ids: list[str] = []
        matched_categories: list[str] = []
        match_group_ids: list[str] = []
        annotated_token_count = 0

        for sentence_df in paragraph_df.partition_by(SENTENCE_ID_COL):
            sentence_fragment = _render_sentence_fragment(
                sentence_df=sentence_df,
                annotation_lookup=annotation_lookup,
            )
            sentence_fragments.append(sentence_fragment)
            matched_condition_ids.extend(sentence_fragment["matched_condition_ids"])
            matched_categories.extend(sentence_fragment["matched_categories"])
            match_group_ids.extend(sentence_fragment["match_group_ids"])
            annotated_token_count += int(sentence_fragment["annotated_token_count"])

        sorted_sentence_fragments = sorted(sentence_fragments, key=lambda fragment: int(fragment["sentence_no"]))
        paragraph_rows.append(
            {
                PARAGRAPH_ID_COL: paragraph_id,
                "sentence_count": len(sorted_sentence_fragments),
                "paragraph_text": "".join(fragment["plain_text"] for fragment in sorted_sentence_fragments),
                "paragraph_text_tagged": "".join(fragment["tagged_text"] for fragment in sorted_sentence_fragments),
                "paragraph_text_highlight_html": "".join(
                    fragment["html_text"] for fragment in sorted_sentence_fragments
                ),
                "matched_condition_ids": _unique_in_order(matched_condition_ids),
                "matched_condition_ids_text": ", ".join(_unique_in_order(matched_condition_ids)),
                "matched_categories": _unique_in_order(matched_categories),
                "matched_categories_text": ", ".join(_unique_in_order(matched_categories)),
                "match_group_ids": _unique_in_order(match_group_ids),
                "match_group_count": len(_unique_in_order(match_group_ids)),
                "annotated_token_count": annotated_token_count,
            }
        )

    return (
        pl.DataFrame(paragraph_rows)
        .with_columns([
            pl.col(PARAGRAPH_ID_COL).cast(pl.Int64),
            pl.col("sentence_count").cast(pl.UInt32),
            pl.col("match_group_count").cast(pl.UInt32),
            pl.col("annotated_token_count").cast(pl.UInt32),
        ])
        .sort(PARAGRAPH_ID_COL)
    )


def select_target_ids_by_cooccurrence_conditions(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    condition_match_logic: str = "any",
    max_paragraph_ids: int = 100,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, list[int], list[int]]:
    # Legacy facade: keep returning the historical 5-item tuple for existing callers.
    normalized_conditions = _normalize_cooccurrence_conditions_impl(cooccurrence_conditions)
    selection_result = _select_target_ids_by_conditions_result_impl(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        normalized_conditions=normalized_conditions,
        condition_match_logic=condition_match_logic,
        max_paragraph_ids=max_paragraph_ids,
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
        .select(["sentence_id", "paragraph_id", "sentence_no_in_paragraph"])
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

    sentence_text_df = (
        joined_df
        .group_by(["paragraph_id", "sentence_id", "sentence_no_in_paragraph"])
        .agg(pl.col("surface").sort_by("token_no").list.join("").alias("sentence_text"))
    )
    return (
        sentence_text_df
        .group_by("paragraph_id")
        .agg([
            pl.len().alias("sentence_count"),
            pl.col("sentence_text").sort_by("sentence_no_in_paragraph").list.join("").alias("paragraph_text"),
        ])
        .sort("paragraph_id")
    )


def enrich_reconstructed_paragraphs_df(
    db_path: Path,
    reconstructed_paragraphs_base_df: pl.DataFrame,
) -> pl.DataFrame:
    paragraph_ids = (
        reconstructed_paragraphs_base_df.get_column("paragraph_id").to_list()
        if not reconstructed_paragraphs_base_df.is_empty()
        else []
    )
    paragraph_metadata_df = read_paragraph_document_metadata(
        db_path=db_path,
        paragraph_ids=paragraph_ids,
    )
    return (
        reconstructed_paragraphs_base_df
        .join(paragraph_metadata_df, on="paragraph_id", how="left")
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
    )


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
