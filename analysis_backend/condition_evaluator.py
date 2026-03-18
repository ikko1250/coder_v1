from __future__ import annotations

import polars as pl

from .condition_model import AnnotationFilter
from .condition_model import ConfigIssue
from .condition_model import NormalizedCondition
from .condition_model import NormalizeConditionsResult
from .condition_model import TargetSelectionResult
from .distance_matcher import evaluate_distance_matches_by_unit
from .frame_schema import POSITIONED_TOKEN_SCHEMA
from .frame_schema import empty_df
from .token_position import build_candidate_tokens_with_position_df


CONDITION_EVAL_SCHEMA = {
    "paragraph_id": pl.Int64,
    "condition_id": pl.String,
    "categories": pl.List(pl.String),
    "category_text": pl.String,
    "search_scope": pl.String,
    "form_match_logic": pl.String,
    "condition_forms": pl.String,
    "annotation_filters_text": pl.String,
    "required_form_count": pl.UInt32,
    "matched_form_count": pl.UInt32,
    "evaluated_unit_count": pl.UInt32,
    "matched_unit_count": pl.UInt32,
    "required_annotation_filter_count": pl.UInt32,
    "matched_annotation_filter_count": pl.UInt32,
    "requested_max_token_distance": pl.Int64,
    "effective_max_token_distance": pl.Int64,
    "distance_check_applied": pl.Boolean,
    "distance_is_match": pl.Boolean,
    "token_is_match": pl.Boolean,
    "annotation_is_match": pl.Boolean,
    "is_match": pl.Boolean,
}
PARAGRAPH_SUMMARY_SCHEMA = {
    "paragraph_id": pl.Int64,
    "condition_count": pl.UInt32,
    "matched_condition_count": pl.UInt32,
    "is_selected": pl.Boolean,
    "matched_condition_ids": pl.List(pl.String),
    "matched_condition_ids_text": pl.String,
    "matched_categories": pl.List(pl.String),
    "matched_categories_text": pl.String,
}
NORMALIZED_PARAGRAPH_ANNOTATION_SCHEMA = {
    "paragraph_id": pl.Int64,
    "label_namespace": pl.String,
    "label_key": pl.String,
    "label_value": pl.String,
}


def _empty_candidate_tokens_df() -> pl.DataFrame:
    return empty_df(POSITIONED_TOKEN_SCHEMA)


def _empty_condition_eval_df() -> pl.DataFrame:
    return empty_df(CONDITION_EVAL_SCHEMA)


def _empty_paragraph_summary_df() -> pl.DataFrame:
    return empty_df(PARAGRAPH_SUMMARY_SCHEMA)


def _empty_normalized_paragraph_annotations_df() -> pl.DataFrame:
    return empty_df(NORMALIZED_PARAGRAPH_ANNOTATION_SCHEMA)


def _paragraph_matched_form_count_expr() -> pl.Expr:
    # This is a diagnostic summary of the strongest unit-level coverage in the paragraph.
    return pl.col("matched_form_count").max().cast(pl.UInt32).alias("matched_form_count")


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


def _build_condition_issue(
    *,
    code: str,
    severity: str,
    message: str,
    condition_index: int,
    condition_id: str | None = None,
    field_name: str | None = None,
) -> ConfigIssue:
    return ConfigIssue(
        code=code,
        severity=severity,
        scope="condition",
        message=message,
        condition_index=condition_index,
        condition_id=condition_id,
        field_name=field_name,
    )


def _normalize_annotation_filters(
    raw_annotation_filters: object,
    *,
    condition_index: int,
    condition_id: str,
) -> tuple[list[AnnotationFilter], list[ConfigIssue], bool]:
    if raw_annotation_filters is None:
        return [], [], False
    if not isinstance(raw_annotation_filters, list):
        return (
            [],
            [
                _build_condition_issue(
                    code="annotation_filters_not_list",
                    severity="error",
                    message="'annotation_filters' must be a list.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="annotation_filters",
                )
            ],
            True,
        )

    issues: list[ConfigIssue] = []
    normalized_filters: list[AnnotationFilter] = []
    invalid_condition = False
    seen_filters: set[tuple[str, str, str, str]] = set()

    for raw_filter in raw_annotation_filters:
        if not isinstance(raw_filter, dict):
            issues.append(
                _build_condition_issue(
                    code="annotation_filter_not_object",
                    severity="error",
                    message="Each annotation filter must be an object.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="annotation_filters",
                )
            )
            invalid_condition = True
            continue

        label_namespace = str(raw_filter.get("namespace", "")).strip()
        label_key = str(raw_filter.get("key", "")).strip()
        label_value = str(raw_filter.get("value", "")).strip()
        raw_operator = str(raw_filter.get("operator", "eq")).strip().lower()
        operator = raw_operator if raw_operator == "eq" else "eq"
        if raw_operator != "eq":
            issues.append(
                _build_condition_issue(
                    code="annotation_filter_operator_defaulted",
                    severity="warning",
                    message="Unknown annotation filter operator was replaced with 'eq'.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="annotation_filters",
                )
            )

        missing_fields: list[str] = []
        if not label_namespace:
            missing_fields.append("namespace")
        if not label_key:
            missing_fields.append("key")
        if not label_value:
            missing_fields.append("value")
        if missing_fields:
            issues.append(
                _build_condition_issue(
                    code="annotation_filter_field_missing",
                    severity="error",
                    message=(
                        "annotation filter is missing required fields: "
                        + ", ".join(missing_fields)
                    ),
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="annotation_filters",
                )
            )
            invalid_condition = True
            continue

        dedup_key = (label_namespace, label_key, label_value, operator)
        if dedup_key in seen_filters:
            continue
        seen_filters.add(dedup_key)
        normalized_filters.append(
            AnnotationFilter(
                label_namespace=label_namespace,
                label_key=label_key,
                label_value=label_value,
                operator=operator,
            )
        )

    return normalized_filters, issues, invalid_condition


def _annotation_filters_text(annotation_filters: list[AnnotationFilter]) -> str:
    return ", ".join(
        [
            f"{annotation_filter.label_namespace}:{annotation_filter.label_key}={annotation_filter.label_value}"
            for annotation_filter in annotation_filters
        ]
    )


def _build_global_candidate_paragraphs_df(
    candidate_tokens_df: pl.DataFrame,
    normalized_paragraph_annotations_df: pl.DataFrame,
) -> pl.DataFrame:
    paragraph_frames: list[pl.DataFrame] = []
    if not candidate_tokens_df.is_empty():
        paragraph_frames.append(candidate_tokens_df.select("paragraph_id").unique())
    if not normalized_paragraph_annotations_df.is_empty():
        paragraph_frames.append(normalized_paragraph_annotations_df.select("paragraph_id").unique())
    if not paragraph_frames:
        return empty_df({"paragraph_id": pl.Int64})
    return (
        pl.concat(paragraph_frames, how="vertical")
        .unique()
        .sort("paragraph_id")
    )


def _build_token_paragraph_eval_df(
    *,
    candidate_tokens_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    if not condition.forms or candidate_tokens_df.is_empty():
        return empty_df(
            {
                "paragraph_id": pl.Int64,
                "matched_form_count": pl.UInt32,
                "evaluated_unit_count": pl.UInt32,
                "matched_unit_count": pl.UInt32,
                "distance_is_match": pl.Boolean,
                "token_is_match": pl.Boolean,
            }
        )

    relevant_tokens_df = candidate_tokens_df.filter(pl.col("normalized_form").is_in(condition.forms))
    if relevant_tokens_df.is_empty():
        return empty_df(
            {
                "paragraph_id": pl.Int64,
                "matched_form_count": pl.UInt32,
                "evaluated_unit_count": pl.UInt32,
                "matched_unit_count": pl.UInt32,
                "distance_is_match": pl.Boolean,
                "token_is_match": pl.Boolean,
            }
        )

    required_form_count = len(condition.forms)
    distance_check_applied = condition.effective_max_token_distance is not None
    if condition.search_scope == "sentence":
        unit_column = "sentence_id"
        position_column = "sentence_token_position"
        unit_df = (
            relevant_tokens_df
            .select([pl.col("sentence_id").alias("unit_id"), "paragraph_id"])
            .unique()
        )
        unit_form_df = (
            relevant_tokens_df
            .select([pl.col("sentence_id").alias("unit_id"), "normalized_form"])
            .unique()
        )
    else:
        unit_column = "paragraph_id"
        position_column = "paragraph_token_position"
        unit_df = (
            relevant_tokens_df
            .select(pl.col("paragraph_id").alias("unit_id"))
            .unique()
            .with_columns(pl.col("unit_id").alias("paragraph_id"))
        )
        unit_form_df = (
            relevant_tokens_df
            .select([pl.col("paragraph_id").alias("unit_id"), "normalized_form"])
            .unique()
        )

    matched_counts_df = (
        unit_form_df
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
            tokens_with_position_df=relevant_tokens_df,
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
        unit_match_expr = base_match_expr & pl.col("distance_is_match")
    else:
        unit_eval_df = unit_eval_df.with_columns(pl.lit(True).alias("distance_is_match"))
        unit_match_expr = base_match_expr

    return (
        unit_eval_df
        .with_columns(unit_match_expr.alias("unit_is_match"))
        .group_by("paragraph_id")
        .agg([
            _paragraph_matched_form_count_expr(),
            pl.len().cast(pl.UInt32).alias("evaluated_unit_count"),
            pl.col("unit_is_match").sum().cast(pl.UInt32).alias("matched_unit_count"),
            pl.col("distance_is_match").any().alias("distance_is_match"),
            pl.col("unit_is_match").any().alias("token_is_match"),
        ])
    )


def _build_annotation_paragraph_eval_df(
    *,
    normalized_paragraph_annotations_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    if not condition.annotation_filters or normalized_paragraph_annotations_df.is_empty():
        return empty_df(
            {
                "paragraph_id": pl.Int64,
                "matched_annotation_filter_count": pl.UInt32,
                "annotation_is_match": pl.Boolean,
            }
        )

    matched_filter_frames: list[pl.DataFrame] = []
    for filter_index, annotation_filter in enumerate(condition.annotation_filters, start=1):
        matched_filter_df = (
            normalized_paragraph_annotations_df
            .filter(
                (pl.col("label_namespace") == annotation_filter.label_namespace)
                & (pl.col("label_key") == annotation_filter.label_key)
                & (pl.col("label_value") == annotation_filter.label_value)
            )
            .select("paragraph_id")
            .unique()
        )
        if matched_filter_df.is_empty():
            continue
        matched_filter_frames.append(
            matched_filter_df.with_columns(pl.lit(filter_index).cast(pl.UInt32).alias("matched_filter_id"))
        )

    if not matched_filter_frames:
        return empty_df(
            {
                "paragraph_id": pl.Int64,
                "matched_annotation_filter_count": pl.UInt32,
                "annotation_is_match": pl.Boolean,
            }
        )

    matched_filters_df = pl.concat(matched_filter_frames, how="vertical")
    required_filter_count = len(condition.annotation_filters)
    return (
        matched_filters_df
        .group_by("paragraph_id")
        .agg(pl.col("matched_filter_id").n_unique().cast(pl.UInt32).alias("matched_annotation_filter_count"))
        .with_columns(
            (
                pl.col("matched_annotation_filter_count") >= required_filter_count
            ).alias("annotation_is_match")
        )
    )


def _build_condition_eval_df(
    *,
    global_candidate_paragraphs_df: pl.DataFrame,
    token_paragraph_eval_df: pl.DataFrame,
    annotation_paragraph_eval_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    required_form_count = len(condition.forms)
    required_annotation_filter_count = len(condition.annotation_filters)
    distance_check_applied = condition.effective_max_token_distance is not None
    return (
        global_candidate_paragraphs_df
        .join(token_paragraph_eval_df, on="paragraph_id", how="left")
        .join(annotation_paragraph_eval_df, on="paragraph_id", how="left")
        .with_columns([
            pl.col("matched_form_count").fill_null(0).cast(pl.UInt32),
            pl.col("evaluated_unit_count").fill_null(0).cast(pl.UInt32),
            pl.col("matched_unit_count").fill_null(0).cast(pl.UInt32),
            pl.col("distance_is_match").fill_null(True if not condition.forms else False),
            pl.col("token_is_match").fill_null(True if not condition.forms else False),
            pl.col("matched_annotation_filter_count").fill_null(0).cast(pl.UInt32),
            pl.col("annotation_is_match").fill_null(True if not condition.annotation_filters else False),
        ])
        .with_columns((pl.col("token_is_match") & pl.col("annotation_is_match")).alias("is_match"))
        .with_columns([
            pl.lit(condition.condition_id).alias("condition_id"),
            pl.lit(condition.categories).alias("categories"),
            pl.lit(condition.category_text).alias("category_text"),
            pl.lit(condition.search_scope).alias("search_scope"),
            pl.lit(condition.form_match_logic).alias("form_match_logic"),
            pl.lit(", ".join(condition.forms)).alias("condition_forms"),
            pl.lit(_annotation_filters_text(condition.annotation_filters)).alias("annotation_filters_text"),
            pl.lit(required_form_count).cast(pl.UInt32).alias("required_form_count"),
            pl.lit(required_annotation_filter_count).cast(pl.UInt32).alias("required_annotation_filter_count"),
            pl.lit(condition.requested_max_token_distance, dtype=pl.Int64).alias("requested_max_token_distance"),
            pl.lit(condition.effective_max_token_distance, dtype=pl.Int64).alias("effective_max_token_distance"),
            pl.lit(distance_check_applied).alias("distance_check_applied"),
        ])
        .select(list(CONDITION_EVAL_SCHEMA.keys()))
    )


def _build_paragraph_match_summary_df(
    condition_eval_df: pl.DataFrame,
    condition_match_logic: str,
) -> pl.DataFrame:
    if condition_eval_df.is_empty():
        return _empty_paragraph_summary_df()

    normalized_match_logic = condition_match_logic.strip().lower()
    paragraph_rows: list[dict[str, object]] = []
    for paragraph_df in condition_eval_df.sort(["paragraph_id", "condition_id"]).partition_by("paragraph_id"):
        paragraph_id = int(paragraph_df.get_column("paragraph_id")[0])
        condition_count = int(paragraph_df.height)
        matched_condition_count = 0
        matched_condition_ids: list[str] = []
        matched_categories: list[str] = []
        any_match = False
        all_match = True

        for row in paragraph_df.iter_rows(named=True):
            is_match = bool(row["is_match"])
            any_match = any_match or is_match
            all_match = all_match and is_match
            if not is_match:
                continue
            matched_condition_count += 1
            condition_id = str(row["condition_id"])
            if condition_id not in matched_condition_ids:
                matched_condition_ids.append(condition_id)
            for category in row["categories"]:
                category_value = str(category)
                if category_value and category_value not in matched_categories:
                    matched_categories.append(category_value)

        paragraph_rows.append(
            {
                "paragraph_id": paragraph_id,
                "condition_count": condition_count,
                "matched_condition_count": matched_condition_count,
                "is_selected": all_match if normalized_match_logic == "all" else any_match,
                "matched_condition_ids": matched_condition_ids,
                "matched_condition_ids_text": ", ".join(matched_condition_ids),
                "matched_categories": matched_categories,
                "matched_categories_text": ", ".join(matched_categories),
            }
        )

    return (
        pl.DataFrame(paragraph_rows, schema=PARAGRAPH_SUMMARY_SCHEMA)
        .sort("paragraph_id")
    )


def normalize_cooccurrence_conditions_result(
    cooccurrence_conditions: list[dict[str, object]],
) -> NormalizeConditionsResult:
    cleaned_conditions: list[NormalizedCondition] = []
    issues: list[ConfigIssue] = []
    used_condition_ids: set[str] = set()

    for idx, raw_condition in enumerate(cooccurrence_conditions, start=1):
        if not isinstance(raw_condition, dict):
            issues.append(
                _build_condition_issue(
                    code="condition_not_object",
                    severity="error",
                    message="Condition entry must be an object.",
                    condition_index=idx,
                )
            )
            continue

        raw_condition_id_val = raw_condition.get("condition_id")
        raw_condition_id = (
            str(raw_condition_id_val).strip() if raw_condition_id_val is not None else ""
        )
        provisional_condition_id = raw_condition_id or f"condition_{idx}"

        raw_forms = raw_condition.get("forms", [])
        if not isinstance(raw_forms, list):
            issues.append(
                _build_condition_issue(
                    code="forms_not_list",
                    severity="error",
                    message="'forms' must be a list.",
                    condition_index=idx,
                    condition_id=provisional_condition_id,
                    field_name="forms",
                )
            )
            continue
        forms: list[str] = []
        for raw_form in raw_forms:
            form = str(raw_form).strip()
            if form:
                forms.append(form)
        unique_forms = list(dict.fromkeys(forms))

        normalized_annotation_filters, annotation_filter_issues, invalid_annotation_filters = (
            _normalize_annotation_filters(
                raw_condition.get("annotation_filters", []),
                condition_index=idx,
                condition_id=provisional_condition_id,
            )
        )
        issues.extend(annotation_filter_issues)
        if invalid_annotation_filters:
            continue

        if not unique_forms and not normalized_annotation_filters:
            issues.append(
                _build_condition_issue(
                    code="forms_empty",
                    severity="error",
                    message="Normalized 'forms' must not be empty unless annotation_filters are present.",
                    condition_index=idx,
                    condition_id=provisional_condition_id,
                    field_name="forms",
                )
            )
            continue

        base_condition_id = provisional_condition_id
        if not raw_condition_id:
            issues.append(
                _build_condition_issue(
                    code="condition_id_generated",
                    severity="warning",
                    message="Missing condition_id was replaced with an auto-generated value.",
                    condition_index=idx,
                    condition_id=base_condition_id,
                    field_name="condition_id",
                )
            )
        condition_id = base_condition_id
        suffix = 2
        while condition_id in used_condition_ids:
            condition_id = f"{base_condition_id}_{suffix}"
            suffix += 1
        if condition_id != base_condition_id:
            issues.append(
                _build_condition_issue(
                    code="condition_id_deduplicated",
                    severity="warning",
                    message="Duplicate condition_id was rewritten with a numeric suffix.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="condition_id",
                )
            )
        used_condition_ids.add(condition_id)

        raw_form_match_logic = str(raw_condition.get("form_match_logic", "all")).strip().lower()
        form_match_logic = raw_form_match_logic if raw_form_match_logic in {"all", "any"} else "all"
        if raw_form_match_logic not in {"all", "any"}:
            issues.append(
                _build_condition_issue(
                    code="form_match_logic_defaulted",
                    severity="warning",
                    message="Unknown form_match_logic was replaced with 'all'.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="form_match_logic",
                )
            )

        raw_search_scope = str(raw_condition.get("search_scope", "paragraph")).strip().lower()
        search_scope = raw_search_scope if raw_search_scope in {"paragraph", "sentence"} else "paragraph"
        if raw_search_scope not in {"paragraph", "sentence"}:
            issues.append(
                _build_condition_issue(
                    code="search_scope_defaulted",
                    severity="warning",
                    message="Unknown search_scope was replaced with 'paragraph'.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="search_scope",
                )
            )
        if normalized_annotation_filters and search_scope == "sentence":
            search_scope = "paragraph"
            issues.append(
                _build_condition_issue(
                    code="annotation_filters_search_scope_normalized",
                    severity="warning",
                    message="search_scope was normalized to 'paragraph' because annotation_filters are paragraph-level.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="search_scope",
                )
            )

        requested_max_token_distance: int | None = None
        raw_distance = raw_condition.get("max_token_distance")
        if raw_distance is not None:
            try:
                parsed_distance = int(raw_distance)
                if parsed_distance >= 0:
                    requested_max_token_distance = parsed_distance
                else:
                    issues.append(
                        _build_condition_issue(
                            code="max_token_distance_ignored",
                            severity="warning",
                            message="Negative max_token_distance was ignored.",
                            condition_index=idx,
                            condition_id=condition_id,
                            field_name="max_token_distance",
                        )
                    )
            except (TypeError, ValueError):
                issues.append(
                    _build_condition_issue(
                        code="max_token_distance_ignored",
                        severity="warning",
                        message="Invalid max_token_distance was ignored.",
                        condition_index=idx,
                        condition_id=condition_id,
                        field_name="max_token_distance",
                    )
                )

        effective_max_token_distance = (
            requested_max_token_distance
            if form_match_logic == "all" and unique_forms
            else None
        )
        categories = _normalize_condition_categories(raw_condition.get("categories"))
        if categories == ["未分類"]:
            issues.append(
                _build_condition_issue(
                    code="categories_defaulted",
                    severity="warning",
                    message="Missing categories were replaced with '未分類'.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="categories",
                )
            )
        if requested_max_token_distance is not None and (form_match_logic != "all" or not unique_forms):
            issues.append(
                _build_condition_issue(
                    code="max_token_distance_disabled",
                    severity="warning",
                    message="max_token_distance is only applied when form_match_logic is 'all' and forms are present.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="max_token_distance",
                )
            )

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
                annotation_filters=normalized_annotation_filters,
            )
        )

    return NormalizeConditionsResult(
        normalized_conditions=cleaned_conditions,
        issues=issues,
    )


def normalize_cooccurrence_conditions(
    cooccurrence_conditions: list[dict[str, object]],
) -> list[NormalizedCondition]:
    return normalize_cooccurrence_conditions_result(cooccurrence_conditions).normalized_conditions


def select_target_ids_by_conditions_result(
    tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    normalized_conditions: list[NormalizedCondition],
    condition_match_logic: str = "any",
    max_paragraph_ids: int = 100,
    normalized_paragraph_annotations_df: pl.DataFrame | None = None,
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
    candidate_tokens_df = (
        build_candidate_tokens_with_position_df(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
            target_forms=all_forms,
        )
        if all_forms
        else _empty_candidate_tokens_df()
    )
    normalized_annotations_df = (
        normalized_paragraph_annotations_df.select(list(NORMALIZED_PARAGRAPH_ANNOTATION_SCHEMA.keys()))
        if normalized_paragraph_annotations_df is not None and not normalized_paragraph_annotations_df.is_empty()
        else _empty_normalized_paragraph_annotations_df()
    )
    global_candidate_paragraphs_df = _build_global_candidate_paragraphs_df(
        candidate_tokens_df=candidate_tokens_df,
        normalized_paragraph_annotations_df=normalized_annotations_df,
    )
    if global_candidate_paragraphs_df.is_empty():
        return TargetSelectionResult(
            candidate_tokens_df=candidate_tokens_df,
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    condition_eval_frames: list[pl.DataFrame] = []
    for condition in normalized_conditions:
        token_paragraph_eval_df = _build_token_paragraph_eval_df(
            candidate_tokens_df=candidate_tokens_df,
            condition=condition,
        )
        annotation_paragraph_eval_df = _build_annotation_paragraph_eval_df(
            normalized_paragraph_annotations_df=normalized_annotations_df,
            condition=condition,
        )
        condition_eval_frames.append(
            _build_condition_eval_df(
                global_candidate_paragraphs_df=global_candidate_paragraphs_df,
                token_paragraph_eval_df=token_paragraph_eval_df,
                annotation_paragraph_eval_df=annotation_paragraph_eval_df,
                condition=condition,
            )
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
    paragraph_match_summary_df = _build_paragraph_match_summary_df(
        condition_eval_df=condition_eval_df,
        condition_match_logic=condition_match_logic,
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
