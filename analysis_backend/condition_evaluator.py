from __future__ import annotations

import polars as pl

from .condition_model import NormalizedCondition
from .condition_model import TargetSelectionResult
from .distance_matcher import evaluate_distance_matches_by_unit
from .frame_schema import empty_df
from .token_position import build_candidate_tokens_with_position_df


CONDITION_EVAL_SCHEMA = {
    "paragraph_id": pl.Int64,
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
    "paragraph_id": pl.Int64,
    "condition_count": pl.UInt32,
    "matched_condition_count": pl.UInt32,
    "is_selected": pl.Boolean,
    "matched_condition_ids": pl.List(pl.String),
}

def _empty_condition_eval_df() -> pl.DataFrame:
    return empty_df(CONDITION_EVAL_SCHEMA)


def _empty_paragraph_summary_df() -> pl.DataFrame:
    return empty_df(PARAGRAPH_SUMMARY_SCHEMA)


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


def normalize_cooccurrence_conditions(
    cooccurrence_conditions: list[dict[str, object]],
) -> list[NormalizedCondition]:
    cleaned_conditions: list[NormalizedCondition] = []
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
            NormalizedCondition(
                condition_id=condition_id,
                categories=categories,
                category_text=", ".join(categories),
                forms=unique_forms,
                search_scope=search_scope,
                form_match_logic=form_match_logic,
                requested_max_token_distance=requested_max_token_distance,
                effective_max_token_distance=effective_max_token_distance,
            )
        )

    return cleaned_conditions


def select_target_ids_by_conditions_result(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    normalized_conditions: list[NormalizedCondition],
    condition_match_logic: str = "any",
    max_paragraph_ids: int = 100,
) -> TargetSelectionResult:
    if not normalized_conditions:
        return TargetSelectionResult(
            candidate_tokens_df=tokens_df.clear(),
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    all_forms = sorted({
        form
        for condition in normalized_conditions
        for form in condition.forms
    })
    candidate_tokens_df = build_candidate_tokens_with_position_df(
        tokens_df=tokens_df,
        sentences_df=sentences_df,
        target_forms=all_forms,
    )
    if candidate_tokens_df.is_empty():
        return TargetSelectionResult(
            candidate_tokens_df=candidate_tokens_df,
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    condition_eval_frames: list[pl.DataFrame] = []
    for condition in normalized_conditions:
        distance_check_applied = condition.effective_max_token_distance is not None
        required_form_count = len(condition.forms)

        if condition.search_scope == "sentence":
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
            .filter(pl.col("normalized_form").is_in(condition.forms))
            .group_by("unit_id")
            .agg(pl.col("normalized_form").n_unique().alias("matched_form_count"))
        )
        base_match_expr = (
            pl.col("matched_form_count") >= 1
            if condition.form_match_logic == "any"
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
                forms=condition.forms,
                unit_column=unit_column,
                position_column=position_column,
                max_token_distance=int(condition.effective_max_token_distance),
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
                pl.lit(condition.condition_id).alias("condition_id"),
                pl.lit(condition.category_text).alias("category_text"),
                pl.lit(condition.search_scope).alias("search_scope"),
                pl.lit(condition.form_match_logic).alias("form_match_logic"),
                pl.lit(", ".join(condition.forms)).alias("condition_forms"),
                pl.lit(required_form_count).cast(pl.UInt32).alias("required_form_count"),
                pl.lit(condition.requested_max_token_distance, dtype=pl.Int64).alias("requested_max_token_distance"),
                pl.lit(condition.effective_max_token_distance, dtype=pl.Int64).alias("effective_max_token_distance"),
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
        return TargetSelectionResult(
            candidate_tokens_df=candidate_tokens_df,
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

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

    return TargetSelectionResult(
        candidate_tokens_df=candidate_tokens_df,
        condition_eval_df=condition_eval_df,
        paragraph_match_summary_df=paragraph_match_summary_df,
        target_paragraph_ids=target_paragraph_ids,
        target_sentence_ids=target_sentence_ids,
    )
