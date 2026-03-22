from __future__ import annotations

import polars as pl

from .condition_model import AnnotationFilter
from .condition_model import ConfigIssue
from .condition_model import NormalizedCondition
from .condition_model import NormalizedFormGroup
from .condition_model import NormalizeConditionsResult
from .condition_model import TargetSelectionResult
from .distance_matcher import build_condition_hit_result
from .distance_matcher import evaluate_distance_matches_by_unit
from .frame_schema import CONDITION_HIT_SCHEMA
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
    "required_categories_all_text": pl.String,
    "required_categories_any_text": pl.String,
    "required_form_count": pl.UInt32,
    "matched_form_count": pl.UInt32,
    "evaluated_unit_count": pl.UInt32,
    "matched_unit_count": pl.UInt32,
    "required_annotation_filter_count": pl.UInt32,
    "matched_annotation_filter_count": pl.UInt32,
    "required_category_all_count": pl.UInt32,
    "required_category_any_count": pl.UInt32,
    "matched_required_category_count": pl.UInt32,
    "requested_max_token_distance": pl.Int64,
    "effective_max_token_distance": pl.Int64,
    "distance_check_applied": pl.Boolean,
    "distance_is_match": pl.Boolean,
    "has_base_clause": pl.Boolean,
    "has_reference_clause": pl.Boolean,
    "token_is_match": pl.Boolean,
    "annotation_is_match": pl.Boolean,
    "base_is_match": pl.Boolean,
    "reference_is_match": pl.Boolean,
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
    "matched_form_group_ids_text": pl.String,
    "matched_form_group_logics_text": pl.String,
    "form_group_explanations_text": pl.String,
    "mixed_scope_warning_text": pl.String,
}
NORMALIZED_PARAGRAPH_ANNOTATION_SCHEMA = {
    "paragraph_id": pl.Int64,
    "label_namespace": pl.String,
    "label_key": pl.String,
    "label_value": pl.String,
}
GROUP_MATCHED_UNIT_SCHEMA = {
    "paragraph_id": pl.Int64,
    "unit_scope": pl.String,
    "unit_id": pl.Int64,
    "matched_form_count": pl.UInt32,
    "distance_is_match": pl.Boolean,
}
SENTENCE_SUMMARY_SCHEMA = {
    "sentence_id": pl.Int64,
    "paragraph_id": pl.Int64,
    "condition_count": pl.UInt32,
    "matched_condition_count": pl.UInt32,
    "is_selected": pl.Boolean,
    "matched_condition_ids": pl.List(pl.String),
    "matched_condition_ids_text": pl.String,
    "matched_categories": pl.List(pl.String),
    "matched_categories_text": pl.String,
}


def _empty_candidate_tokens_df() -> pl.DataFrame:
    return empty_df(POSITIONED_TOKEN_SCHEMA)


def _empty_condition_hit_tokens_df() -> pl.DataFrame:
    return empty_df(CONDITION_HIT_SCHEMA)


def _empty_condition_eval_df() -> pl.DataFrame:
    return empty_df(CONDITION_EVAL_SCHEMA)


def _empty_paragraph_summary_df() -> pl.DataFrame:
    return empty_df(PARAGRAPH_SUMMARY_SCHEMA)


def _empty_normalized_paragraph_annotations_df() -> pl.DataFrame:
    return empty_df(NORMALIZED_PARAGRAPH_ANNOTATION_SCHEMA)


def _empty_group_matched_units_df() -> pl.DataFrame:
    return empty_df(GROUP_MATCHED_UNIT_SCHEMA)


def _empty_sentence_summary_df() -> pl.DataFrame:
    return empty_df(SENTENCE_SUMMARY_SCHEMA)


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


def _group_has_anchor_window(form_group: NormalizedFormGroup) -> bool:
    return form_group.effective_max_token_distance is not None


def _group_has_scoped_exclusion(form_group: NormalizedFormGroup) -> bool:
    return bool(form_group.exclude_forms_any)


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


def _normalize_string_clause_list(
    raw_values: object,
    *,
    condition_index: int,
    condition_id: str,
    field_name: str,
    not_list_code: str,
) -> tuple[list[str], list[ConfigIssue], bool]:
    if raw_values is None:
        return [], [], False
    if not isinstance(raw_values, list):
        return (
            [],
            [
                _build_condition_issue(
                    code=not_list_code,
                    severity="error",
                    message=f"'{field_name}' must be a list.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name=field_name,
                )
            ],
            True,
        )

    normalized_values: list[str] = []
    for raw_value in raw_values:
        value = str(raw_value).strip()
        if value and value not in normalized_values:
            normalized_values.append(value)
    if raw_values and not normalized_values:
        return (
            [],
            [
                _build_condition_issue(
                    code="reference_clause_empty",
                    severity="error",
                    message=f"'{field_name}' must contain at least one non-empty value.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name=field_name,
                )
            ],
            True,
        )
    return normalized_values, [], False


def _required_categories_text(categories: list[str]) -> str:
    return ", ".join(categories)


def _form_group_logic_text(form_group: NormalizedFormGroup, group_index: int) -> str:
    if group_index == 1:
        return form_group.match_logic
    return f"{form_group.combine_logic or 'and'} {form_group.match_logic}"


def _format_scope_unit_label(search_scope: str, unit_count: int) -> str:
    if search_scope == "sentence":
        return f"all {unit_count} sentences"
    return f"all {unit_count} paragraphs"


def _build_form_group_explanation_lines(
    *,
    condition: NormalizedCondition,
    eval_row: dict[str, object],
) -> list[str]:
    explanation_lines: list[str] = []
    evaluated_unit_count = int(eval_row.get("evaluated_unit_count", 0) or 0)
    for group_index, form_group in enumerate(condition.form_groups, start=1):
        logic_text = _form_group_logic_text(form_group, group_index)
        line_parts = [f"g{group_index}: {logic_text}"]
        if form_group.match_logic == "not":
            line_parts.append(
                f"absent=[{', '.join(form_group.forms)}] in {_format_scope_unit_label(form_group.search_scope, evaluated_unit_count)}"
            )
        else:
            line_parts.append(f"forms=[{', '.join(form_group.forms)}]")
            line_parts.append(f"scope={form_group.search_scope}")
            if form_group.anchor_form:
                line_parts.append(f"anchor={form_group.anchor_form}")
            if form_group.effective_max_token_distance is not None:
                line_parts.append(f"window<=+{int(form_group.effective_max_token_distance)}")
            if form_group.exclude_forms_any:
                line_parts.append(f"exclude_any=[{', '.join(form_group.exclude_forms_any)}]")
        explanation_lines.append(" ".join(line_parts))
    return explanation_lines


def _build_form_group_summary_df(
    *,
    condition_eval_df: pl.DataFrame,
    normalized_conditions: list[NormalizedCondition],
    match_column: str,
) -> pl.DataFrame:
    summary_schema = {
        "paragraph_id": pl.Int64,
        "matched_form_group_ids_text": pl.String,
        "matched_form_group_logics_text": pl.String,
        "form_group_explanations_text": pl.String,
        "mixed_scope_warning_text": pl.String,
    }
    if condition_eval_df.is_empty():
        return empty_df(summary_schema)

    condition_lookup = {
        condition.condition_id: condition
        for condition in normalized_conditions
        if condition.form_groups
    }
    if not condition_lookup:
        return empty_df(summary_schema)

    paragraph_rows: dict[int, dict[str, object]] = {}
    matched_rows = (
        condition_eval_df
        .filter(pl.col(match_column))
        .sort(["paragraph_id", "condition_id"])
        .iter_rows(named=True)
    )
    for eval_row in matched_rows:
        condition_id = str(eval_row["condition_id"])
        condition = condition_lookup.get(condition_id)
        if condition is None:
            continue
        paragraph_id = int(eval_row["paragraph_id"])
        paragraph_row = paragraph_rows.setdefault(
            paragraph_id,
            {
                "paragraph_id": paragraph_id,
                "matched_form_group_ids_text": [],
                "matched_form_group_logics_text": [],
                "form_group_explanations_text": [],
                "mixed_scope_warning_text": [],
            },
        )
        scope_values = sorted({form_group.search_scope for form_group in condition.form_groups})
        for group_index, form_group in enumerate(condition.form_groups, start=1):
            group_id_text = f"{condition_id}:g{group_index}"
            logic_text = _form_group_logic_text(form_group, group_index)
            paragraph_row["matched_form_group_ids_text"].append(group_id_text)
            paragraph_row["matched_form_group_logics_text"].append(f"{group_id_text}={logic_text}")
        explanation_lines = _build_form_group_explanation_lines(
            condition=condition,
            eval_row=eval_row,
        )
        condition_block_lines = [f"[{condition_id}]"]
        condition_block_lines.extend(explanation_lines)
        paragraph_row["form_group_explanations_text"].append("\n".join(condition_block_lines))
        if len(scope_values) >= 2:
            paragraph_row["mixed_scope_warning_text"].append(
                f"{condition_id}: mixed-scope promoted to paragraph ({' + '.join(scope_values)})"
            )

    if not paragraph_rows:
        return empty_df(summary_schema)

    rows: list[dict[str, object]] = []
    for row in paragraph_rows.values():
        rows.append(
            {
                "paragraph_id": row["paragraph_id"],
                "matched_form_group_ids_text": ", ".join(row["matched_form_group_ids_text"]),
                "matched_form_group_logics_text": " | ".join(row["matched_form_group_logics_text"]),
                "form_group_explanations_text": "\n\n".join(row["form_group_explanations_text"]),
                "mixed_scope_warning_text": "\n".join(row["mixed_scope_warning_text"]),
            }
        )
    return (
        pl.DataFrame(rows, schema=summary_schema)
        .sort("paragraph_id")
    )


def _normalize_form_groups(
    raw_form_groups: object,
    *,
    condition_index: int,
    condition_id: str,
    default_search_scope: str,
) -> tuple[list[NormalizedFormGroup], list[ConfigIssue], bool]:
    if raw_form_groups is None:
        return [], [], False
    if not isinstance(raw_form_groups, list):
        return (
            [],
            [
                _build_condition_issue(
                    code="form_groups_not_list",
                    severity="error",
                    message="'form_groups' must be a list.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            ],
            True,
        )

    issues: list[ConfigIssue] = []
    normalized_groups: list[NormalizedFormGroup] = []
    invalid_condition = False

    for group_index, raw_group in enumerate(raw_form_groups, start=1):
        if not isinstance(raw_group, dict):
            issues.append(
                _build_condition_issue(
                    code="form_group_not_object",
                    severity="error",
                    message="Each form group must be an object.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        raw_forms = raw_group.get("forms", [])
        if not isinstance(raw_forms, list):
            issues.append(
                _build_condition_issue(
                    code="form_group_forms_not_list",
                    severity="error",
                    message="'form_groups[].forms' must be a list.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue
        forms = [str(raw_form).strip() for raw_form in raw_forms if str(raw_form).strip()]
        unique_forms = list(dict.fromkeys(forms))
        if not unique_forms:
            issues.append(
                _build_condition_issue(
                    code="form_group_forms_empty",
                    severity="error",
                    message="Each form group must contain at least one form.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        raw_match_logic = str(raw_group.get("match_logic", "and")).strip().lower()
        if raw_match_logic not in {"and", "or", "not"}:
            issues.append(
                _build_condition_issue(
                    code="group_logic_invalid",
                    severity="error",
                    message="match_logic must be one of: and, or, not.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        raw_combine_logic_value = raw_group.get("combine_logic")
        raw_combine_logic = (
            str(raw_combine_logic_value).strip().lower()
            if raw_combine_logic_value is not None
            else None
        )
        if group_index == 1:
            if raw_combine_logic is not None:
                issues.append(
                    _build_condition_issue(
                        code="combine_logic_invalid",
                        severity="error",
                        message="group 1 must not define combine_logic.",
                        condition_index=condition_index,
                        condition_id=condition_id,
                        field_name="form_groups",
                    )
                )
                invalid_condition = True
                continue
            if raw_match_logic == "not":
                issues.append(
                    _build_condition_issue(
                        code="group1_not_disallowed",
                        severity="error",
                        message="group 1 does not support match_logic='not'.",
                        condition_index=condition_index,
                        condition_id=condition_id,
                        field_name="form_groups",
                    )
                )
                invalid_condition = True
                continue
        elif raw_combine_logic not in {"and", "or"}:
            issues.append(
                _build_condition_issue(
                    code="combine_logic_invalid",
                    severity="error",
                    message="group 2+ must define combine_logic as 'and' or 'or'.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        raw_group_search_scope = str(raw_group.get("search_scope", default_search_scope)).strip().lower()
        group_search_scope = (
            raw_group_search_scope
            if raw_group_search_scope in {"paragraph", "sentence"}
            else default_search_scope
        )
        if raw_group_search_scope not in {"paragraph", "sentence"}:
            issues.append(
                _build_condition_issue(
                    code="search_scope_defaulted",
                    severity="warning",
                    message="Unknown search_scope was replaced with the condition default.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )

        raw_anchor_form = str(raw_group.get("anchor_form", "")).strip()
        anchor_form = raw_anchor_form or None
        if anchor_form is not None and anchor_form not in unique_forms:
            issues.append(
                _build_condition_issue(
                    code="anchor_form_invalid",
                    severity="error",
                    message="anchor_form must be included in forms.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        raw_exclude_forms_any = raw_group.get("exclude_forms_any", [])
        if raw_exclude_forms_any is None:
            exclude_forms_any: list[str] = []
        elif isinstance(raw_exclude_forms_any, list):
            exclude_forms_any = [
                str(raw_form).strip()
                for raw_form in raw_exclude_forms_any
                if str(raw_form).strip()
            ]
        else:
            issues.append(
                _build_condition_issue(
                    code="exclude_forms_any_invalid",
                    severity="error",
                    message="'exclude_forms_any' must be a list when present.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue
        exclude_forms_any = list(dict.fromkeys(exclude_forms_any))
        if exclude_forms_any and raw_match_logic == "not":
            issues.append(
                _build_condition_issue(
                    code="exclude_forms_any_invalid",
                    severity="error",
                    message="exclude_forms_any is only supported for positive groups.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        requested_max_token_distance: int | None = None
        raw_distance = raw_group.get("max_token_distance")
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
                            condition_index=condition_index,
                            condition_id=condition_id,
                            field_name="form_groups",
                        )
                    )
            except (TypeError, ValueError):
                issues.append(
                    _build_condition_issue(
                        code="max_token_distance_ignored",
                        severity="warning",
                        message="Invalid max_token_distance was ignored.",
                        condition_index=condition_index,
                        condition_id=condition_id,
                        field_name="form_groups",
                    )
                )

        effective_max_token_distance: int | None = None
        if requested_max_token_distance is not None and raw_match_logic == "not":
            issues.append(
                _build_condition_issue(
                    code="not_group_distance_disabled",
                    severity="warning",
                    message="max_token_distance was ignored because match_logic='not'.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
        else:
            effective_max_token_distance = requested_max_token_distance

        if (
            raw_match_logic == "and"
            and effective_max_token_distance is not None
            and anchor_form is None
        ):
            issues.append(
                _build_condition_issue(
                    code="anchor_form_missing",
                    severity="error",
                    message="anchor_form is required for and-group distance windows.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        if anchor_form is not None and raw_match_logic != "and":
            issues.append(
                _build_condition_issue(
                    code="anchor_form_invalid",
                    severity="error",
                    message="anchor_form is only supported for and groups.",
                    condition_index=condition_index,
                    condition_id=condition_id,
                    field_name="form_groups",
                )
            )
            invalid_condition = True
            continue

        normalized_groups.append(
            NormalizedFormGroup(
                forms=unique_forms,
                match_logic=raw_match_logic,
                combine_logic=raw_combine_logic,
                search_scope=group_search_scope,
                requested_max_token_distance=requested_max_token_distance,
                effective_max_token_distance=effective_max_token_distance,
                anchor_form=anchor_form,
                exclude_forms_any=exclude_forms_any,
            )
        )

    return normalized_groups, issues, invalid_condition


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


def _uses_group_evaluator(condition: NormalizedCondition) -> bool:
    if not condition.form_groups:
        return False
    if len(condition.form_groups) >= 2:
        return True
    return any(
        form_group.match_logic == "not"
        or _group_has_anchor_window(form_group)
        or _group_has_scoped_exclusion(form_group)
        for form_group in condition.form_groups
    )


def _build_group_unit_universe_df(
    *,
    global_candidate_paragraphs_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    search_scope: str,
) -> pl.DataFrame:
    if search_scope == "sentence":
        if global_candidate_paragraphs_df.is_empty() or sentences_df.is_empty():
            return empty_df({"paragraph_id": pl.Int64, "unit_id": pl.Int64})
        return (
            sentences_df
            .join(global_candidate_paragraphs_df, on="paragraph_id", how="inner")
            .select(["paragraph_id", pl.col("sentence_id").alias("unit_id")])
            .unique()
            .sort(["paragraph_id", "unit_id"])
        )
    if global_candidate_paragraphs_df.is_empty():
        return empty_df({"paragraph_id": pl.Int64, "unit_id": pl.Int64})
    return (
        global_candidate_paragraphs_df
        .select(["paragraph_id"])
        .unique()
        .with_columns(pl.col("paragraph_id").alias("unit_id"))
        .sort(["paragraph_id", "unit_id"])
    )


def _build_positive_group_unit_match_with_window_df(
    *,
    candidate_tokens_df: pl.DataFrame,
    form_group: NormalizedFormGroup,
) -> pl.DataFrame:
    search_scope = form_group.search_scope
    position_column = "sentence_token_position" if search_scope == "sentence" else "paragraph_token_position"
    unit_column = "sentence_id" if search_scope == "sentence" else "paragraph_id"
    target_forms = list(dict.fromkeys(form_group.forms + form_group.exclude_forms_any))
    relevant_tokens_df = candidate_tokens_df.filter(pl.col("normalized_form").is_in(target_forms))
    if relevant_tokens_df.is_empty():
        return _empty_group_matched_units_df()

    rows: list[dict[str, object]] = []
    for unit_df in relevant_tokens_df.partition_by(unit_column, maintain_order=True):
        paragraph_id = int(unit_df.get_column("paragraph_id")[0])
        unit_id = int(unit_df.get_column(unit_column)[0])
        positions_by_form: dict[str, list[int]] = {}
        for form in target_forms:
            positions_by_form[form] = (
                unit_df
                .filter(pl.col("normalized_form") == form)
                .sort(position_column)
                .get_column(position_column)
                .to_list()
            )

        exclude_positions = [
            position
            for form in form_group.exclude_forms_any
            for position in positions_by_form.get(form, [])
        ]
        matched_row: dict[str, object] | None = None
        if form_group.match_logic == "and":
            anchor_form = form_group.anchor_form or ""
            anchor_positions = positions_by_form.get(anchor_form, [])
            for anchor_position in anchor_positions:
                window_end = anchor_position + int(form_group.effective_max_token_distance or 0)
                other_forms_match = all(
                    any(
                        anchor_position <= position <= window_end
                        for position in positions_by_form.get(form, [])
                    )
                    for form in form_group.forms
                    if form != anchor_form
                )
                if not other_forms_match:
                    continue
                if any(anchor_position <= position <= window_end for position in exclude_positions):
                    continue
                matched_row = {
                    "paragraph_id": paragraph_id,
                    "unit_scope": search_scope,
                    "unit_id": unit_id,
                    "matched_form_count": len(form_group.forms),
                    "distance_is_match": True,
                }
                break
        else:
            for form in form_group.forms:
                for anchor_position in positions_by_form.get(form, []):
                    window_end = anchor_position + int(form_group.effective_max_token_distance or 0)
                    if any(anchor_position <= position <= window_end for position in exclude_positions):
                        continue
                    matched_form_count = sum(
                        1
                        for candidate_form in form_group.forms
                        if any(
                            anchor_position <= position <= window_end
                            for position in positions_by_form.get(candidate_form, [])
                        )
                    )
                    matched_row = {
                        "paragraph_id": paragraph_id,
                        "unit_scope": search_scope,
                        "unit_id": unit_id,
                        "matched_form_count": matched_form_count,
                        "distance_is_match": True,
                    }
                    break
                if matched_row is not None:
                    break

        if matched_row is not None:
            rows.append(matched_row)

    if not rows:
        return _empty_group_matched_units_df()
    return pl.DataFrame(rows, schema=GROUP_MATCHED_UNIT_SCHEMA)


def _build_positive_group_unit_match_df(
    *,
    candidate_tokens_df: pl.DataFrame,
    form_group: NormalizedFormGroup,
) -> pl.DataFrame:
    search_scope = form_group.search_scope
    target_forms = list(dict.fromkeys(form_group.forms + form_group.exclude_forms_any))
    relevant_tokens_df = candidate_tokens_df.filter(pl.col("normalized_form").is_in(target_forms))
    if relevant_tokens_df.is_empty():
        return _empty_group_matched_units_df()

    if search_scope == "sentence":
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

    required_form_count = len(form_group.forms)
    matched_counts_df = (
        unit_form_df
        .filter(pl.col("normalized_form").is_in(form_group.forms))
        .group_by("unit_id")
        .agg(pl.col("normalized_form").n_unique().cast(pl.UInt32).alias("matched_form_count"))
    )
    base_match_expr = (
        pl.col("matched_form_count") >= 1
        if form_group.match_logic == "or"
        else pl.col("matched_form_count") >= required_form_count
    )
    unit_eval_df = (
        unit_df
        .join(matched_counts_df, on="unit_id", how="left")
        .with_columns(pl.col("matched_form_count").fill_null(0).cast(pl.UInt32))
    )
    if form_group.effective_max_token_distance is not None:
        distance_match_df = evaluate_distance_matches_by_unit(
            tokens_with_position_df=relevant_tokens_df.filter(pl.col("normalized_form").is_in(form_group.forms)),
            forms=form_group.forms,
            unit_column=unit_column,
            position_column=position_column,
            max_token_distance=int(form_group.effective_max_token_distance),
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

    if form_group.exclude_forms_any:
        excluded_units_df = (
            relevant_tokens_df
            .filter(pl.col("normalized_form").is_in(form_group.exclude_forms_any))
            .select(pl.col(unit_column).alias("unit_id"))
            .unique()
            .with_columns(pl.lit(True).alias("has_excluded_form"))
        )
        unit_eval_df = (
            unit_eval_df
            .join(excluded_units_df, on="unit_id", how="left")
            .with_columns(pl.col("has_excluded_form").fill_null(False))
        )
        unit_match_expr = unit_match_expr & (~pl.col("has_excluded_form"))

    return (
        unit_eval_df
        .with_columns(unit_match_expr.alias("unit_is_match"))
        .filter(pl.col("unit_is_match"))
        .with_columns(pl.lit(search_scope).alias("unit_scope"))
        .select(list(GROUP_MATCHED_UNIT_SCHEMA.keys()))
    )


def _build_form_group_matched_units_df(
    *,
    candidate_tokens_df: pl.DataFrame,
    global_candidate_paragraphs_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    form_group: NormalizedFormGroup,
) -> pl.DataFrame:
    search_scope = form_group.search_scope
    relevant_tokens_df = candidate_tokens_df.filter(pl.col("normalized_form").is_in(form_group.forms))

    if form_group.match_logic == "not":
        unit_df = _build_group_unit_universe_df(
            global_candidate_paragraphs_df=global_candidate_paragraphs_df,
            sentences_df=sentences_df,
            search_scope=search_scope,
        )
        if unit_df.is_empty():
            return _empty_group_matched_units_df()

        matched_counts_df = (
            relevant_tokens_df
            .select([
                ("sentence_id" if search_scope == "sentence" else "paragraph_id"),
                "normalized_form",
            ])
            .rename({
                "sentence_id" if search_scope == "sentence" else "paragraph_id": "unit_id",
            })
            .unique()
            .group_by("unit_id")
            .agg(pl.col("normalized_form").n_unique().cast(pl.UInt32).alias("matched_form_count"))
            if not relevant_tokens_df.is_empty()
            else empty_df({"unit_id": pl.Int64, "matched_form_count": pl.UInt32})
        )
        return (
            unit_df
            .join(matched_counts_df, on="unit_id", how="left")
            .with_columns(pl.col("matched_form_count").fill_null(0).cast(pl.UInt32))
            .filter(pl.col("matched_form_count") == 0)
            .with_columns([
                pl.lit(search_scope).alias("unit_scope"),
                pl.lit(len(form_group.forms)).cast(pl.UInt32).alias("matched_form_count"),
                pl.lit(True).alias("distance_is_match"),
            ])
            .select(list(GROUP_MATCHED_UNIT_SCHEMA.keys()))
        )

    if relevant_tokens_df.is_empty():
        return _empty_group_matched_units_df()
    if _group_has_anchor_window(form_group):
        return _build_positive_group_unit_match_with_window_df(
            candidate_tokens_df=candidate_tokens_df,
            form_group=form_group,
        )
    return _build_positive_group_unit_match_df(
        candidate_tokens_df=candidate_tokens_df,
        form_group=form_group,
    )


def _promote_group_matches_to_paragraph_df(group_matches_df: pl.DataFrame) -> pl.DataFrame:
    if group_matches_df.is_empty():
        return _empty_group_matched_units_df()
    return (
        group_matches_df
        .group_by("paragraph_id")
        .agg([
            pl.col("matched_form_count").max().cast(pl.UInt32).alias("matched_form_count"),
            pl.col("distance_is_match").any().alias("distance_is_match"),
        ])
        .with_columns([
            pl.lit("paragraph").alias("unit_scope"),
            pl.col("paragraph_id").alias("unit_id"),
        ])
        .select(list(GROUP_MATCHED_UNIT_SCHEMA.keys()))
    )


def _combine_group_matches_df(
    *,
    left_matches_df: pl.DataFrame,
    left_scope: str,
    right_matches_df: pl.DataFrame,
    right_scope: str,
    combine_logic: str,
) -> tuple[pl.DataFrame, str]:
    effective_left_df = left_matches_df
    effective_right_df = right_matches_df
    result_scope = left_scope
    if left_scope != right_scope:
        effective_left_df = _promote_group_matches_to_paragraph_df(left_matches_df)
        effective_right_df = _promote_group_matches_to_paragraph_df(right_matches_df)
        result_scope = "paragraph"
    key_columns = ["paragraph_id", "unit_scope", "unit_id"]
    universe_df = (
        pl.concat(
            [
                effective_left_df.select(key_columns),
                effective_right_df.select(key_columns),
            ],
            how="vertical",
        )
        .unique()
    )
    combined_df = (
        universe_df
        .join(
            effective_left_df.rename(
                {
                    "matched_form_count": "matched_form_count_left",
                    "distance_is_match": "distance_is_match_left",
                }
            ),
            on=key_columns,
            how="left",
        )
        .join(
            effective_right_df.rename(
                {
                    "matched_form_count": "matched_form_count_right",
                    "distance_is_match": "distance_is_match_right",
                }
            ),
            on=key_columns,
            how="left",
        )
        .with_columns([
            pl.col("matched_form_count_left").fill_null(0).cast(pl.UInt32),
            pl.col("matched_form_count_right").fill_null(0).cast(pl.UInt32),
            pl.col("distance_is_match_left").fill_null(False),
            pl.col("distance_is_match_right").fill_null(False),
        ])
        .with_columns([
            (pl.col("matched_form_count_left") > 0).alias("left_is_match"),
            (pl.col("matched_form_count_right") > 0).alias("right_is_match"),
        ])
    )
    combined_expr = (
        pl.col("left_is_match") & pl.col("right_is_match")
        if combine_logic == "and"
        else pl.col("left_is_match") | pl.col("right_is_match")
    )
    return (
        combined_df
        .with_columns(combined_expr.alias("is_match"))
        .filter(pl.col("is_match"))
        .with_columns([
            pl.when(pl.col("matched_form_count_left") >= pl.col("matched_form_count_right"))
            .then(pl.col("matched_form_count_left"))
            .otherwise(pl.col("matched_form_count_right"))
            .cast(pl.UInt32)
            .alias("matched_form_count"),
            (pl.col("distance_is_match_left") | pl.col("distance_is_match_right")).alias("distance_is_match"),
        ])
        .select(list(GROUP_MATCHED_UNIT_SCHEMA.keys())),
        result_scope,
    )


def _build_form_group_token_paragraph_eval_df(
    *,
    candidate_tokens_df: pl.DataFrame,
    sentences_df: pl.DataFrame,
    global_candidate_paragraphs_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    if not condition.form_groups:
        return _build_token_paragraph_eval_df(
            candidate_tokens_df=candidate_tokens_df,
            condition=condition,
        )

    current_matches_df = _build_form_group_matched_units_df(
        candidate_tokens_df=candidate_tokens_df,
        global_candidate_paragraphs_df=global_candidate_paragraphs_df,
        sentences_df=sentences_df,
        form_group=condition.form_groups[0],
    )
    current_scope = condition.form_groups[0].search_scope
    for form_group in condition.form_groups[1:]:
        next_matches_df = _build_form_group_matched_units_df(
            candidate_tokens_df=candidate_tokens_df,
            global_candidate_paragraphs_df=global_candidate_paragraphs_df,
            sentences_df=sentences_df,
            form_group=form_group,
        )
        current_matches_df, current_scope = _combine_group_matches_df(
            left_matches_df=current_matches_df,
            left_scope=current_scope,
            right_matches_df=next_matches_df,
            right_scope=form_group.search_scope,
            combine_logic=form_group.combine_logic or "and",
        )

    if current_matches_df.is_empty():
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

    return (
        current_matches_df
        .group_by("paragraph_id")
        .agg([
            pl.col("matched_form_count").max().cast(pl.UInt32).alias("matched_form_count"),
            pl.len().cast(pl.UInt32).alias("evaluated_unit_count"),
            pl.len().cast(pl.UInt32).alias("matched_unit_count"),
            pl.col("distance_is_match").any().alias("distance_is_match"),
        ])
        .with_columns(pl.lit(True).alias("token_is_match"))
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


def _build_base_condition_eval_df(
    *,
    global_candidate_paragraphs_df: pl.DataFrame,
    token_paragraph_eval_df: pl.DataFrame,
    annotation_paragraph_eval_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    required_form_count = len(condition.forms)
    required_annotation_filter_count = len(condition.annotation_filters)
    required_category_all_count = len(condition.required_categories_all)
    required_category_any_count = len(condition.required_categories_any)
    distance_check_applied = condition.effective_max_token_distance is not None
    has_base_clause = bool(condition.forms or condition.annotation_filters)
    has_reference_clause = bool(
        condition.required_categories_all or condition.required_categories_any
    )
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
            pl.lit(0).cast(pl.UInt32).alias("matched_required_category_count"),
            pl.lit(True).alias("reference_is_match"),
        ])
        .with_columns([
            pl.lit(has_base_clause).alias("has_base_clause"),
            pl.lit(has_reference_clause).alias("has_reference_clause"),
            pl.when(pl.lit(has_base_clause))
            .then(pl.col("token_is_match") & pl.col("annotation_is_match"))
            .otherwise(pl.lit(False))
            .alias("base_is_match"),
        ])
        .with_columns(pl.col("base_is_match").alias("is_match"))
        .with_columns([
            pl.lit(condition.condition_id).alias("condition_id"),
            pl.lit(condition.categories).alias("categories"),
            pl.lit(condition.category_text).alias("category_text"),
            pl.lit(condition.search_scope).alias("search_scope"),
            pl.lit(condition.form_match_logic).alias("form_match_logic"),
            pl.lit(", ".join(condition.forms)).alias("condition_forms"),
            pl.lit(_annotation_filters_text(condition.annotation_filters)).alias("annotation_filters_text"),
            pl.lit(_required_categories_text(condition.required_categories_all)).alias("required_categories_all_text"),
            pl.lit(_required_categories_text(condition.required_categories_any)).alias("required_categories_any_text"),
            pl.lit(required_form_count).cast(pl.UInt32).alias("required_form_count"),
            pl.lit(required_annotation_filter_count).cast(pl.UInt32).alias("required_annotation_filter_count"),
            pl.lit(required_category_all_count).cast(pl.UInt32).alias("required_category_all_count"),
            pl.lit(required_category_any_count).cast(pl.UInt32).alias("required_category_any_count"),
            pl.lit(condition.requested_max_token_distance, dtype=pl.Int64).alias("requested_max_token_distance"),
            pl.lit(condition.effective_max_token_distance, dtype=pl.Int64).alias("effective_max_token_distance"),
            pl.lit(distance_check_applied).alias("distance_check_applied"),
        ])
        .select(list(CONDITION_EVAL_SCHEMA.keys()))
    )


def _build_paragraph_match_summary_df(
    condition_eval_df: pl.DataFrame,
    condition_match_logic: str,
    normalized_conditions: list[NormalizedCondition],
    *,
    match_column: str = "is_match",
) -> pl.DataFrame:
    if condition_eval_df.is_empty():
        return _empty_paragraph_summary_df()

    normalized_match_logic = condition_match_logic.strip().lower()
    sorted_eval_df = condition_eval_df.sort(["paragraph_id", "condition_id"])
    selected_expr = (
        pl.col(match_column).all()
        if normalized_match_logic == "all"
        else pl.col(match_column).any()
    )
    base_summary_df = (
        sorted_eval_df
        .group_by("paragraph_id")
        .agg([
            pl.len().cast(pl.UInt32).alias("condition_count"),
            pl.col(match_column).sum().cast(pl.UInt32).alias("matched_condition_count"),
            selected_expr.alias("is_selected"),
        ])
    )
    matched_condition_ids_df = (
        sorted_eval_df
        .filter(pl.col(match_column))
        .group_by("paragraph_id")
        .agg(pl.col("condition_id").sort().unique().alias("matched_condition_ids"))
    )
    matched_categories_df = (
        sorted_eval_df
        .filter(pl.col(match_column))
        .explode("categories")
        .filter(pl.col("categories").is_not_null() & (pl.col("categories") != ""))
        .group_by("paragraph_id")
        .agg(pl.col("categories").sort().unique().alias("matched_categories"))
    )
    form_group_summary_df = _build_form_group_summary_df(
        condition_eval_df=sorted_eval_df,
        normalized_conditions=normalized_conditions,
        match_column=match_column,
    )
    return (
        base_summary_df
        .join(matched_condition_ids_df, on="paragraph_id", how="left")
        .join(matched_categories_df, on="paragraph_id", how="left")
        .join(form_group_summary_df, on="paragraph_id", how="left")
        .with_columns([
            pl.when(pl.col("matched_condition_ids").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_condition_ids"))
            .alias("matched_condition_ids"),
            pl.when(pl.col("matched_categories").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories"),
        ])
        .with_columns([
            pl.col("matched_condition_ids").list.join(", ").alias("matched_condition_ids_text"),
            pl.col("matched_categories").list.join(", ").alias("matched_categories_text"),
            pl.col("matched_form_group_ids_text").fill_null(""),
            pl.col("matched_form_group_logics_text").fill_null(""),
            pl.col("form_group_explanations_text").fill_null(""),
            pl.col("mixed_scope_warning_text").fill_null(""),
        ])
        .select(list(PARAGRAPH_SUMMARY_SCHEMA.keys()))
        .sort("paragraph_id")
    )


def _build_sentence_match_summary_df(
    condition_hit_tokens_df: pl.DataFrame,
    *,
    condition_match_logic: str,
    normalized_conditions: list[NormalizedCondition],
) -> pl.DataFrame:
    if condition_hit_tokens_df.is_empty() or not normalized_conditions:
        return _empty_sentence_summary_df()

    normalized_match_logic = condition_match_logic.strip().lower()
    condition_count = len(normalized_conditions)
    sorted_hit_df = condition_hit_tokens_df.sort(["paragraph_id", "sentence_id", "condition_id"])
    matched_categories_df = (
        sorted_hit_df
        .select(["sentence_id", "paragraph_id", "categories"])
        .explode("categories")
        .filter(pl.col("categories").is_not_null() & (pl.col("categories") != ""))
        .group_by(["sentence_id", "paragraph_id"])
        .agg(pl.col("categories").sort().unique().alias("matched_categories"))
    )
    summary_df = (
        sorted_hit_df
        .group_by(["sentence_id", "paragraph_id"])
        .agg([
            pl.col("condition_id").sort().unique().alias("matched_condition_ids"),
        ])
        .join(matched_categories_df, on=["sentence_id", "paragraph_id"], how="left")
        .with_columns([
            pl.lit(condition_count).cast(pl.UInt32).alias("condition_count"),
            pl.col("matched_condition_ids").list.len().cast(pl.UInt32).alias("matched_condition_count"),
            pl.when(pl.col("matched_categories").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories"),
        ])
        .with_columns([
            (
                pl.col("matched_condition_count") == condition_count
                if normalized_match_logic == "all"
                else pl.col("matched_condition_count") > 0
            ).alias("is_selected"),
            pl.col("matched_condition_ids").list.join(", ").alias("matched_condition_ids_text"),
            pl.col("matched_categories").list.join(", ").alias("matched_categories_text"),
        ])
        .select(list(SENTENCE_SUMMARY_SCHEMA.keys()))
        .sort(["paragraph_id", "sentence_id"])
    )
    return summary_df


def _build_paragraph_categories_from_sentence_hits(
    condition_hit_tokens_df: pl.DataFrame,
) -> pl.DataFrame:
    if condition_hit_tokens_df.is_empty():
        return pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "matched_categories": pl.List(pl.String),
            }
        )

    matched_categories_df = (
        condition_hit_tokens_df
        .select(["paragraph_id", "categories"])
        .explode("categories")
        .filter(pl.col("categories").is_not_null() & (pl.col("categories") != ""))
        .group_by("paragraph_id")
        .agg(pl.col("categories").sort().unique().alias("matched_categories"))
    )
    paragraph_ids_df = condition_hit_tokens_df.select("paragraph_id").unique().sort("paragraph_id")
    return (
        paragraph_ids_df
        .join(matched_categories_df, on="paragraph_id", how="left")
        .with_columns(
            pl.when(pl.col("matched_categories").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories")
        )
    )


def _build_paragraph_match_summary_from_sentence_summary_df(
    sentence_match_summary_df: pl.DataFrame,
    *,
    condition_match_logic: str,
    normalized_conditions: list[NormalizedCondition],
) -> pl.DataFrame:
    if sentence_match_summary_df.is_empty() or not normalized_conditions:
        return _empty_paragraph_summary_df()

    normalized_match_logic = condition_match_logic.strip().lower()
    condition_count = len(normalized_conditions)
    summary_df = (
        sentence_match_summary_df
        .group_by("paragraph_id")
        .agg([
            pl.col("matched_condition_ids").explode().drop_nulls().unique().sort().alias("matched_condition_ids"),
            pl.col("matched_categories").explode().drop_nulls().unique().sort().alias("matched_categories"),
        ])
        .with_columns([
            pl.lit(condition_count).cast(pl.UInt32).alias("condition_count"),
            pl.col("matched_condition_ids").list.len().cast(pl.UInt32).alias("matched_condition_count"),
            pl.when(pl.col("matched_categories").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories"),
        ])
        .with_columns([
            (
                pl.col("matched_condition_count") == condition_count
                if normalized_match_logic == "all"
                else pl.col("matched_condition_count") > 0
            ).alias("is_selected"),
            pl.col("matched_condition_ids").list.join(", ").alias("matched_condition_ids_text"),
            pl.col("matched_categories").list.join(", ").alias("matched_categories_text"),
            pl.lit("").alias("matched_form_group_ids_text"),
            pl.lit("").alias("matched_form_group_logics_text"),
            pl.lit("").alias("form_group_explanations_text"),
            pl.lit("").alias("mixed_scope_warning_text"),
        ])
        .select(list(PARAGRAPH_SUMMARY_SCHEMA.keys()))
        .sort("paragraph_id")
    )
    return summary_df


def _filter_sentence_hit_tokens_by_reference_clauses(
    *,
    condition_hit_tokens_df: pl.DataFrame,
    normalized_conditions: list[NormalizedCondition],
) -> pl.DataFrame:
    if condition_hit_tokens_df.is_empty() or not normalized_conditions:
        return _empty_condition_hit_tokens_df()

    global_candidate_paragraphs_df = (
        condition_hit_tokens_df.select("paragraph_id").unique().sort("paragraph_id")
    )
    base_paragraph_match_summary_df = _build_paragraph_categories_from_sentence_hits(
        condition_hit_tokens_df=condition_hit_tokens_df,
    )

    filtered_frames: list[pl.DataFrame] = []
    for condition in normalized_conditions:
        condition_hits_df = condition_hit_tokens_df.filter(
            pl.col("condition_id") == condition.condition_id
        )
        if condition_hits_df.is_empty():
            continue
        if condition.required_categories_all or condition.required_categories_any:
            category_reference_eval_df = _build_category_reference_eval_df(
                global_candidate_paragraphs_df=global_candidate_paragraphs_df,
                base_paragraph_match_summary_df=base_paragraph_match_summary_df,
                condition=condition,
            )
            condition_hits_df = (
                condition_hits_df
                .join(category_reference_eval_df, on="paragraph_id", how="left")
                .filter(pl.col("reference_is_match").fill_null(False))
                .select(condition_hit_tokens_df.columns)
            )
        filtered_frames.append(condition_hits_df)

    if not filtered_frames:
        return _empty_condition_hit_tokens_df()
    return pl.concat(filtered_frames, how="vertical")


def _build_category_reference_eval_df(
    *,
    global_candidate_paragraphs_df: pl.DataFrame,
    base_paragraph_match_summary_df: pl.DataFrame,
    condition: NormalizedCondition,
) -> pl.DataFrame:
    reference_schema = {
        "paragraph_id": pl.Int64,
        "matched_required_category_count": pl.UInt32,
        "reference_is_match": pl.Boolean,
    }
    if not condition.required_categories_all and not condition.required_categories_any:
        return empty_df(reference_schema)

    all_required_categories = list(
        dict.fromkeys(condition.required_categories_all + condition.required_categories_any)
    )
    available_categories_df = (
        global_candidate_paragraphs_df
        .join(
            base_paragraph_match_summary_df.select(["paragraph_id", "matched_categories"]),
            on="paragraph_id",
            how="left",
        )
        .with_columns(
            pl.when(pl.col("matched_categories").is_null())
            .then(pl.lit([], dtype=pl.List(pl.String)))
            .otherwise(pl.col("matched_categories"))
            .alias("matched_categories")
        )
    )
    if all_required_categories:
        available_categories_df = available_categories_df.with_columns(
            pl.col("matched_categories")
            .list.set_intersection(pl.lit(all_required_categories, dtype=pl.List(pl.String)))
            .list.len()
            .cast(pl.UInt32)
            .alias("matched_required_category_count")
        )
    else:
        available_categories_df = available_categories_df.with_columns(
            pl.lit(0).cast(pl.UInt32).alias("matched_required_category_count")
        )

    reference_match_expr = pl.lit(True)
    if condition.required_categories_all:
        reference_match_expr = reference_match_expr & (
            pl.col("matched_categories")
            .list.set_intersection(
                pl.lit(condition.required_categories_all, dtype=pl.List(pl.String))
            )
            .list.len()
            >= len(condition.required_categories_all)
        )
    if condition.required_categories_any:
        reference_match_expr = reference_match_expr & (
            pl.col("matched_categories")
            .list.set_intersection(
                pl.lit(condition.required_categories_any, dtype=pl.List(pl.String))
            )
            .list.len()
            >= 1
        )

    return (
        available_categories_df
        .with_columns(reference_match_expr.alias("reference_is_match"))
        .select(list(reference_schema.keys()))
    )


def _apply_category_reference_eval(
    *,
    base_condition_eval_df: pl.DataFrame,
    category_reference_eval_df: pl.DataFrame,
) -> pl.DataFrame:
    return (
        base_condition_eval_df
        .join(category_reference_eval_df, on="paragraph_id", how="left", suffix="_reference")
        .with_columns([
            pl.col("matched_required_category_count_reference")
            .fill_null(0)
            .cast(pl.UInt32)
            .alias("matched_required_category_count"),
            pl.when(pl.col("has_reference_clause"))
            .then(pl.col("reference_is_match_reference").fill_null(False))
            .otherwise(pl.lit(True))
            .alias("reference_is_match"),
        ])
        .with_columns(
            (
                pl.when(pl.col("has_base_clause"))
                .then(pl.col("base_is_match"))
                .otherwise(pl.lit(True))
                & pl.col("reference_is_match")
            ).alias("is_match")
        )
        .select(list(CONDITION_EVAL_SCHEMA.keys()))
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

        raw_overall_search_scope = str(raw_condition.get("overall_search_scope", "paragraph")).strip().lower()
        overall_search_scope = (
            raw_overall_search_scope if raw_overall_search_scope in {"paragraph", "sentence"} else "paragraph"
        )
        if raw_overall_search_scope not in {"paragraph", "sentence"}:
            issues.append(
                _build_condition_issue(
                    code="search_scope_defaulted",
                    severity="warning",
                    message="Unknown overall_search_scope was replaced with 'paragraph'.",
                    condition_index=idx,
                    condition_id=provisional_condition_id,
                    field_name="overall_search_scope",
                )
            )

        has_form_groups = "form_groups" in raw_condition
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

        normalized_form_groups, form_group_issues, invalid_form_groups = _normalize_form_groups(
            raw_condition.get("form_groups"),
            condition_index=idx,
            condition_id=provisional_condition_id,
            default_search_scope=overall_search_scope,
        )
        issues.extend(form_group_issues)
        if invalid_form_groups:
            continue

        if has_form_groups and unique_forms:
            issues.append(
                _build_condition_issue(
                    code="legacy_and_form_groups_mixed",
                    severity="error",
                    message="Do not mix legacy forms fields with form_groups in the same condition.",
                    condition_index=idx,
                    condition_id=provisional_condition_id,
                    field_name="form_groups",
                )
            )
            continue

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

        required_categories_all, required_categories_all_issues, invalid_required_categories_all = (
            _normalize_string_clause_list(
                raw_condition.get("required_categories_all"),
                condition_index=idx,
                condition_id=provisional_condition_id,
                field_name="required_categories_all",
                not_list_code="required_categories_all_not_list",
            )
        )
        issues.extend(required_categories_all_issues)
        if invalid_required_categories_all:
            continue

        required_categories_any, required_categories_any_issues, invalid_required_categories_any = (
            _normalize_string_clause_list(
                raw_condition.get("required_categories_any"),
                condition_index=idx,
                condition_id=provisional_condition_id,
                field_name="required_categories_any",
                not_list_code="required_categories_any_not_list",
            )
        )
        issues.extend(required_categories_any_issues)
        if invalid_required_categories_any:
            continue

        if normalized_form_groups:
            unique_forms = list(
                dict.fromkeys(
                    form
                    for form_group in normalized_form_groups
                    for form in form_group.forms
                )
            )
            first_form_group = normalized_form_groups[0]
            raw_form_match_logic = "all" if first_form_group.match_logic == "and" else "any"
            raw_search_scope = overall_search_scope
            requested_max_token_distance = (
                first_form_group.requested_max_token_distance
                if len(normalized_form_groups) == 1
                else None
            )
            effective_max_token_distance = (
                first_form_group.effective_max_token_distance
                if len(normalized_form_groups) == 1
                else None
            )
        else:
            raw_form_match_logic = str(raw_condition.get("form_match_logic", "all")).strip().lower()
            raw_search_scope = str(raw_condition.get("search_scope", overall_search_scope)).strip().lower()
            requested_max_token_distance = None
            effective_max_token_distance = None

        has_reference_clause = bool(required_categories_all or required_categories_any)
        if not unique_forms and not normalized_annotation_filters and not has_reference_clause:
            issues.append(
                _build_condition_issue(
                    code="forms_empty",
                    severity="error",
                    message=(
                        "Condition must define at least one clause: forms, annotation_filters, "
                        "required_categories_all, or required_categories_any."
                    ),
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

        legacy_token_clause_used = (
            bool(unique_forms)
            or raw_condition.get("form_match_logic") is not None
            or raw_condition.get("max_token_distance") is not None
        )
        if not normalized_form_groups and legacy_token_clause_used:
            issues.append(
                _build_condition_issue(
                    code="legacy_schema_migrated",
                    severity="warning",
                    message="Legacy forms/form_match_logic was normalized into a single form_group.",
                    condition_index=idx,
                    condition_id=condition_id,
                    field_name="forms",
                )
            )
            normalized_form_groups = [
                NormalizedFormGroup(
                    forms=unique_forms,
                    match_logic="and" if raw_form_match_logic != "any" else "or",
                    combine_logic=None,
                    search_scope=search_scope,
                    requested_max_token_distance=requested_max_token_distance,
                    effective_max_token_distance=effective_max_token_distance,
                    anchor_form=None,
                    exclude_forms_any=[],
                )
            ]

        if not has_form_groups:
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
                overall_search_scope=overall_search_scope,
                forms=unique_forms,
                search_scope=search_scope,
                form_match_logic=form_match_logic,
                requested_max_token_distance=requested_max_token_distance,
                effective_max_token_distance=effective_max_token_distance,
                form_groups=normalized_form_groups,
                annotation_filters=normalized_annotation_filters,
                required_categories_all=required_categories_all,
                required_categories_any=required_categories_any,
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
    analysis_unit: str = "paragraph",
    distance_matching_mode: str = "auto-approx",
    distance_match_combination_cap: int = 10000,
    distance_match_strict_safety_limit: int = 1000000,
) -> TargetSelectionResult:
    if not normalized_conditions:
        return TargetSelectionResult(
            candidate_tokens_df=tokens_df.clear(),
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            sentence_match_summary_df=_empty_sentence_summary_df(),
            sentence_hit_tokens_df=_empty_condition_hit_tokens_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    if analysis_unit == "sentence":
        sentence_conditions = [
            condition for condition in normalized_conditions if condition.search_scope == "sentence"
        ]
        if not sentence_conditions:
            return TargetSelectionResult(
                candidate_tokens_df=tokens_df.clear(),
                condition_eval_df=_empty_condition_eval_df(),
                paragraph_match_summary_df=_empty_paragraph_summary_df(),
                sentence_match_summary_df=_empty_sentence_summary_df(),
                sentence_hit_tokens_df=_empty_condition_hit_tokens_df(),
                target_paragraph_ids=[],
                target_sentence_ids=[],
            )

        sentence_forms = sorted({
            form
            for condition in sentence_conditions
            for form in (
                condition.forms
                + [
                    exclude_form
                    for form_group in condition.form_groups
                    for exclude_form in form_group.exclude_forms_any
                ]
            )
        })
        candidate_tokens_df = (
            build_candidate_tokens_with_position_df(
                tokens_df=tokens_df,
                sentences_df=sentences_df,
                target_forms=sentence_forms,
            )
            if sentence_forms
            else _empty_candidate_tokens_df()
        )
        condition_hit_result = build_condition_hit_result(
            tokens_with_position_df=candidate_tokens_df,
            cooccurrence_conditions=_normalized_conditions_to_dicts(sentence_conditions),
            distance_matching_mode=distance_matching_mode,
            distance_match_combination_cap=distance_match_combination_cap,
            distance_match_strict_safety_limit=distance_match_strict_safety_limit,
        )
        filtered_sentence_hit_tokens_df = _filter_sentence_hit_tokens_by_reference_clauses(
            condition_hit_tokens_df=condition_hit_result.condition_hit_tokens_df,
            normalized_conditions=sentence_conditions,
        )
        sentence_match_summary_df = _build_sentence_match_summary_df(
            condition_hit_tokens_df=filtered_sentence_hit_tokens_df,
            condition_match_logic=condition_match_logic,
            normalized_conditions=sentence_conditions,
        )
        paragraph_match_summary_df = _build_paragraph_match_summary_from_sentence_summary_df(
            sentence_match_summary_df=sentence_match_summary_df,
            condition_match_logic=condition_match_logic,
            normalized_conditions=sentence_conditions,
        )
        target_paragraph_ids = (
            paragraph_match_summary_df
            .filter(pl.col("is_selected"))
            .sort(
                ["matched_condition_count", "paragraph_id"],
                descending=[True, False],
            )
            .head(max_paragraph_ids)
            .sort("paragraph_id")
            .get_column("paragraph_id")
            .to_list()
        )
        target_sentence_ids = (
            sentence_match_summary_df
            .filter(
                pl.col("is_selected")
                & pl.col("paragraph_id").is_in(target_paragraph_ids)
            )
            .sort(["paragraph_id", "sentence_id"])
            .get_column("sentence_id")
            .to_list()
            if target_paragraph_ids
            else []
        )
        return TargetSelectionResult(
            candidate_tokens_df=candidate_tokens_df,
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=paragraph_match_summary_df,
            sentence_match_summary_df=sentence_match_summary_df,
            sentence_hit_tokens_df=(
                filtered_sentence_hit_tokens_df
                .filter(pl.col("sentence_id").is_in(target_sentence_ids))
                if target_sentence_ids
                else _empty_condition_hit_tokens_df()
            ),
            target_paragraph_ids=target_paragraph_ids,
            target_sentence_ids=target_sentence_ids,
            warning_messages=condition_hit_result.warning_messages,
        )

    all_forms = sorted({
        form
        for condition in normalized_conditions
        for form in (
            condition.forms
            + [
                exclude_form
                for form_group in condition.form_groups
                for exclude_form in form_group.exclude_forms_any
            ]
        )
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
            sentence_match_summary_df=_empty_sentence_summary_df(),
            sentence_hit_tokens_df=_empty_condition_hit_tokens_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    base_condition_eval_frames: list[pl.DataFrame] = []
    for condition in normalized_conditions:
        token_paragraph_eval_df = (
            _build_form_group_token_paragraph_eval_df(
                candidate_tokens_df=candidate_tokens_df,
                sentences_df=sentences_df,
                global_candidate_paragraphs_df=global_candidate_paragraphs_df,
                condition=condition,
            )
            if _uses_group_evaluator(condition)
            else _build_token_paragraph_eval_df(
                candidate_tokens_df=candidate_tokens_df,
                condition=condition,
            )
        )
        annotation_paragraph_eval_df = _build_annotation_paragraph_eval_df(
            normalized_paragraph_annotations_df=normalized_annotations_df,
            condition=condition,
        )
        base_condition_eval_frames.append(
            _build_base_condition_eval_df(
                global_candidate_paragraphs_df=global_candidate_paragraphs_df,
                token_paragraph_eval_df=token_paragraph_eval_df,
                annotation_paragraph_eval_df=annotation_paragraph_eval_df,
                condition=condition,
            )
        )

    if not base_condition_eval_frames:
        return TargetSelectionResult(
            candidate_tokens_df=candidate_tokens_df,
            condition_eval_df=_empty_condition_eval_df(),
            paragraph_match_summary_df=_empty_paragraph_summary_df(),
            sentence_match_summary_df=_empty_sentence_summary_df(),
            sentence_hit_tokens_df=_empty_condition_hit_tokens_df(),
            target_paragraph_ids=[],
            target_sentence_ids=[],
        )

    base_condition_eval_df = pl.concat(base_condition_eval_frames, how="vertical")
    base_paragraph_match_summary_df = _build_paragraph_match_summary_df(
        condition_eval_df=base_condition_eval_df,
        condition_match_logic=condition_match_logic,
        normalized_conditions=normalized_conditions,
        match_column="base_is_match",
    )
    condition_eval_frames: list[pl.DataFrame] = []
    for condition, base_condition_df in zip(normalized_conditions, base_condition_eval_frames):
        category_reference_eval_df = _build_category_reference_eval_df(
            global_candidate_paragraphs_df=global_candidate_paragraphs_df,
            base_paragraph_match_summary_df=base_paragraph_match_summary_df,
            condition=condition,
        )
        condition_eval_frames.append(
            _apply_category_reference_eval(
                base_condition_eval_df=base_condition_df,
                category_reference_eval_df=category_reference_eval_df,
            )
        )

    condition_eval_df = pl.concat(condition_eval_frames, how="vertical")
    paragraph_match_summary_df = _build_paragraph_match_summary_df(
        condition_eval_df=condition_eval_df,
        condition_match_logic=condition_match_logic,
        normalized_conditions=normalized_conditions,
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
        sentence_match_summary_df=_empty_sentence_summary_df(),
        sentence_hit_tokens_df=_empty_condition_hit_tokens_df(),
        target_paragraph_ids=target_paragraph_ids,
        target_sentence_ids=target_sentence_ids,
    )
