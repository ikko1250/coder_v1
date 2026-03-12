from __future__ import annotations

from html import escape
from pathlib import Path

import polars as pl

from .condition_model import DistanceMatchingMode
from .data_access import read_analysis_sentences
from .data_access import read_analysis_tokens
from .data_access import read_paragraph_document_metadata
from .distance_matcher import build_condition_hit_result
from .distance_matcher import evaluate_distance_matches_by_unit
from .filter_config import load_filter_config
from .token_position import build_candidate_tokens_with_position_df
from .token_position import build_tokens_with_position_df

PARAGRAPH_ID_COL = "paragraph_id"
SENTENCE_ID_COL = "sentence_id"
SENTENCE_NO_COL = "sentence_no_in_paragraph"
TOKEN_NO_COL = "token_no"
NORMALIZED_FORM_COL = "normalized_form"
SURFACE_COL = "surface"
SENTENCE_TOKEN_POSITION_COL = "sentence_token_position"
PARAGRAPH_TOKEN_POSITION_COL = "paragraph_token_position"
CONDITION_EVAL_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "condition_id": pl.String,
    "category_text": pl.String,
    "search_scope": pl.String,
    "form_match_logic": pl.String,
    "condition_forms": pl.String,
    "required_form_count": pl.UInt32,
    "matched_form_count": pl.UInt32,
    "evaluated_unit_count": pl.UInt32,
    "matched_unit_count": pl.UInt32,
    "requested_max_token_distance": pl.Int64,
    "effective_max_token_distance": pl.Int64,
    "distance_check_applied": pl.Boolean,
    "distance_is_match": pl.Boolean,
    "is_match": pl.Boolean,
}
PARAGRAPH_SUMMARY_SCHEMA = {
    PARAGRAPH_ID_COL: pl.Int64,
    "condition_count": pl.UInt32,
    "matched_condition_count": pl.UInt32,
    "is_selected": pl.Boolean,
    "matched_condition_ids": pl.List(pl.String),
}
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

def _empty_condition_eval_df() -> pl.DataFrame:
    return _empty_df(CONDITION_EVAL_SCHEMA)


def _empty_paragraph_summary_df() -> pl.DataFrame:
    return _empty_df(PARAGRAPH_SUMMARY_SCHEMA)


def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return _empty_df(CONDITION_HIT_SCHEMA)


def _empty_token_annotations_df() -> pl.DataFrame:
    return _empty_df(TOKEN_ANNOTATION_SCHEMA)


def _empty_rendered_paragraphs_df() -> pl.DataFrame:
    return _empty_df(RENDERED_PARAGRAPH_SCHEMA)


def _normalize_condition_categories(raw_categories: object) -> list[str]:
    raw_category_values = raw_categories if isinstance(raw_categories, list) else [raw_categories]
    categories: list[str] = []
    for raw_category in raw_category_values:
        category = str(raw_category).strip() if raw_category is not None else ""
        if category and category not in categories:
            categories.append(category)
    if not categories:
        categories = ["未分類"]
    return categories


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


def _clean_cooccurrence_conditions(
    cooccurrence_conditions: list[dict[str, object]],
) -> list[dict[str, object]]:
    cleaned_conditions: list[dict[str, object]] = []
    used_condition_ids: set[str] = set()

    for idx, raw_condition in enumerate(cooccurrence_conditions, start=1):
        if not isinstance(raw_condition, dict):
            continue

        raw_forms = raw_condition.get("forms", [])
        if not isinstance(raw_forms, list):
            continue

        forms: list[str] = []
        for raw_form in raw_forms:
            form = str(raw_form).strip()
            if form:
                forms.append(form)
        unique_forms = list(dict.fromkeys(forms))
        if not unique_forms:
            continue

        raw_condition_id_val = raw_condition.get("condition_id")
        raw_condition_id = (
            str(raw_condition_id_val).strip() if raw_condition_id_val is not None else ""
        )
        base_condition_id = raw_condition_id or f"condition_{idx}"
        condition_id = base_condition_id
        suffix = 2
        while condition_id in used_condition_ids:
            condition_id = f"{base_condition_id}_{suffix}"
            suffix += 1
        used_condition_ids.add(condition_id)

        raw_form_match_logic = str(raw_condition.get("form_match_logic", "all")).strip().lower()
        form_match_logic = raw_form_match_logic if raw_form_match_logic in {"all", "any"} else "all"
        raw_search_scope = str(raw_condition.get("search_scope", "paragraph")).strip().lower()
        search_scope = raw_search_scope if raw_search_scope in {"paragraph", "sentence"} else "paragraph"

        requested_max_token_distance: int | None = None
        raw_distance = raw_condition.get("max_token_distance")
        if raw_distance is not None:
            try:
                parsed_distance = int(raw_distance)
                if parsed_distance >= 0:
                    requested_max_token_distance = parsed_distance
            except (TypeError, ValueError):
                requested_max_token_distance = None

        effective_max_token_distance = (
            requested_max_token_distance if form_match_logic == "all" else None
        )
        categories = _normalize_condition_categories(raw_condition.get("categories"))

        cleaned_conditions.append(
            {
                "condition_id": condition_id,
                "categories": categories,
                "category_text": ", ".join(categories),
                "forms": unique_forms,
                "search_scope": search_scope,
                "form_match_logic": form_match_logic,
                "requested_max_token_distance": requested_max_token_distance,
                "effective_max_token_distance": effective_max_token_distance,
            }
        )

    return cleaned_conditions


def build_condition_hit_tokens_df(
    tokens_with_position_df: pl.DataFrame,
    cooccurrence_conditions: list[dict[str, object]],
    distance_matching_mode: DistanceMatchingMode = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> pl.DataFrame:
    if tokens_with_position_df.is_empty():
        return _empty_condition_hit_tokens_df()

    cleaned_conditions = _clean_cooccurrence_conditions(cooccurrence_conditions)
    if not cleaned_conditions:
        return _empty_condition_hit_tokens_df()

    return build_condition_hit_result(
        tokens_with_position_df=tokens_with_position_df,
        cooccurrence_conditions=cleaned_conditions,
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
    cleaned_conditions = _clean_cooccurrence_conditions(cooccurrence_conditions)
    if not cleaned_conditions:
        return tokens_df.clear(), _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    all_forms = sorted({form for condition in cleaned_conditions for form in condition["forms"]})
    candidate_tokens_df = build_candidate_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        target_forms=all_forms,
    )
    if candidate_tokens_df.is_empty():
        return candidate_tokens_df, _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    condition_eval_frames: list[pl.DataFrame] = []
    for condition in cleaned_conditions:
        condition_id = str(condition["condition_id"])
        category_text = str(condition["category_text"])
        forms = list(condition["forms"])
        search_scope = str(condition["search_scope"])
        form_match_logic = str(condition["form_match_logic"])
        requested_max_token_distance = condition["requested_max_token_distance"]
        effective_max_token_distance = condition["effective_max_token_distance"]
        distance_check_applied = effective_max_token_distance is not None
        required_form_count = len(forms)

        if search_scope == "sentence":
            unit_column = "sentence_id"
            position_column = "sentence_token_position"
            unit_df = (
                candidate_tokens_df
                .select([pl.col("sentence_id").alias("unit_id"), "paragraph_id"])
                .unique()
            )
            unit_form_df = (
                candidate_tokens_df
                .select([pl.col("sentence_id").alias("unit_id"), "normalized_form"])
                .unique()
            )
        else:
            unit_column = "paragraph_id"
            position_column = "paragraph_token_position"
            unit_df = (
                candidate_tokens_df
                .select(pl.col("paragraph_id").alias("unit_id"))
                .unique()
                .with_columns(pl.col("unit_id").alias("paragraph_id"))
            )
            unit_form_df = (
                candidate_tokens_df
                .select([pl.col("paragraph_id").alias("unit_id"), "normalized_form"])
                .unique()
            )

        matched_counts_df = (
            unit_form_df
            .filter(pl.col("normalized_form").is_in(forms))
            .group_by("unit_id")
            .agg(pl.col("normalized_form").n_unique().alias("matched_form_count"))
        )
        base_match_expr = (
            pl.col("matched_form_count") >= 1
            if form_match_logic == "any"
            else pl.col("matched_form_count") >= required_form_count
        )

        unit_eval_df = (
            unit_df
            .join(matched_counts_df, on="unit_id", how="left")
            .with_columns(pl.col("matched_form_count").fill_null(0).cast(pl.UInt32))
        )
        if distance_check_applied:
            distance_match_df = evaluate_distance_matches_by_unit(
                tokens_with_position_df=candidate_tokens_df,
                forms=forms,
                unit_column=unit_column,
                position_column=position_column,
                max_token_distance=int(effective_max_token_distance),
            )
            unit_eval_df = (
                unit_eval_df
                .join(distance_match_df, on="unit_id", how="left")
                .with_columns(pl.col("distance_is_match").fill_null(False))
            )
            condition_match_expr = base_match_expr & pl.col("distance_is_match")
        else:
            unit_eval_df = unit_eval_df.with_columns(pl.lit(True).alias("distance_is_match"))
            condition_match_expr = base_match_expr

        paragraph_eval_df = (
            unit_eval_df
            .with_columns(condition_match_expr.alias("is_match"))
            .group_by("paragraph_id")
            .agg([
                pl.col("matched_form_count").max().cast(pl.UInt32).alias("matched_form_count"),
                pl.len().cast(pl.UInt32).alias("evaluated_unit_count"),
                pl.col("is_match").sum().cast(pl.UInt32).alias("matched_unit_count"),
                pl.col("distance_is_match").any().alias("distance_is_match"),
                pl.col("is_match").any().alias("is_match"),
            ])
        )

        condition_eval_frames.append(
            paragraph_eval_df
            .with_columns([
                pl.lit(condition_id).alias("condition_id"),
                pl.lit(category_text).alias("category_text"),
                pl.lit(search_scope).alias("search_scope"),
                pl.lit(form_match_logic).alias("form_match_logic"),
                pl.lit(", ".join(forms)).alias("condition_forms"),
                pl.lit(required_form_count).cast(pl.UInt32).alias("required_form_count"),
                pl.lit(requested_max_token_distance, dtype=pl.Int64).alias("requested_max_token_distance"),
                pl.lit(effective_max_token_distance, dtype=pl.Int64).alias("effective_max_token_distance"),
                pl.lit(distance_check_applied).alias("distance_check_applied"),
            ])
            .select([
                "paragraph_id",
                "condition_id",
                "category_text",
                "search_scope",
                "form_match_logic",
                "condition_forms",
                "required_form_count",
                "matched_form_count",
                "evaluated_unit_count",
                "matched_unit_count",
                "requested_max_token_distance",
                "effective_max_token_distance",
                "distance_check_applied",
                "distance_is_match",
                "is_match",
            ])
        )

    if not condition_eval_frames:
        return candidate_tokens_df, _empty_condition_eval_df(), _empty_paragraph_summary_df(), [], []

    condition_eval_df = pl.concat(condition_eval_frames, how="vertical")
    match_logic = condition_match_logic.strip().lower()
    selected_expr = pl.col("is_match").all() if match_logic == "all" else pl.col("is_match").any()
    paragraph_match_summary_df = (
        condition_eval_df
        .group_by("paragraph_id")
        .agg([
            pl.len().alias("condition_count"),
            pl.col("is_match").sum().alias("matched_condition_count"),
            selected_expr.alias("is_selected"),
            pl.col("condition_id").filter(pl.col("is_match")).alias("matched_condition_ids"),
        ])
        .sort("paragraph_id")
    )

    target_paragraph_ids = (
        paragraph_match_summary_df
        .filter(pl.col("is_selected"))
        .sort(["matched_condition_count", "paragraph_id"], descending=[True, False])
        .head(max_paragraph_ids)
        .sort("paragraph_id")
        .get_column("paragraph_id")
        .to_list()
    )
    target_sentence_ids = (
        tokens_df
        .filter(pl.col("paragraph_id").is_in(target_paragraph_ids))
        .select("sentence_id")
        .unique()
        .sort("sentence_id")
        .get_column("sentence_id")
        .to_list()
    )
    return (
        candidate_tokens_df,
        condition_eval_df,
        paragraph_match_summary_df,
        target_paragraph_ids,
        target_sentence_ids,
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
