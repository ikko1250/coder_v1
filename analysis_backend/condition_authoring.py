from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .condition_evaluator import normalize_cooccurrence_conditions_result
from .condition_model import ConfigIssue
from .condition_model import FilterConfig


@dataclass(frozen=True)
class CompileAuthoringResult:
    raw_config: dict[str, object] | None
    filter_config: FilterConfig | None = None
    issues: list[ConfigIssue] = field(default_factory=list)


_SETTINGS_FIELDS = {
    "condition_match_logic",
    "analysis_unit",
    "max_reconstructed_paragraphs",
    "distance_matching_mode",
    "distance_match_combination_cap",
    "distance_match_strict_safety_limit",
}

_VALID_STRING_SETTINGS: dict[str, set[str]] = {
    "condition_match_logic": {"any", "all"},
    "analysis_unit": {"paragraph", "sentence"},
    "distance_matching_mode": {"strict", "auto-approx", "approx"},
}

_INT_SETTINGS = {
    "max_reconstructed_paragraphs",
    "distance_match_combination_cap",
    "distance_match_strict_safety_limit",
}

_DEFAULTS: dict[str, object] = {
    "condition_match_logic": "any",
    "analysis_unit": "paragraph",
    "max_reconstructed_paragraphs": 10000,
    "distance_matching_mode": "auto-approx",
    "distance_match_combination_cap": 10000,
    "distance_match_strict_safety_limit": 1000000,
}

_RUNTIME_RULE_KEYS = {
    "condition_id",
    "categories",
    "required_categories_all",
    "required_categories_any",
    "required_condition_ids_all",
    "required_condition_ids_any",
    "excluded_condition_ids_any",
    "form_groups",
    "text_groups",
    "annotation_filters",
    "forms",
    "search_scope",
    "form_match_logic",
    "requested_max_token_distance",
    "effective_max_token_distance",
    "overall_search_scope",
    "category_text",
}


def _build_issue(
    *,
    code: str,
    severity: str,
    message: str,
    field_name: str | None = None,
    condition_index: int | None = None,
    condition_id: str | None = None,
) -> ConfigIssue:
    return ConfigIssue(
        code=code,
        severity=severity,
        scope="filter_config",
        message=message,
        field_name=field_name,
        condition_index=condition_index,
        condition_id=condition_id,
    )


def _validate_int_setting(
    raw_settings: dict[str, object],
    field_name: str,
    issues: list[ConfigIssue],
) -> int | None:
    raw_value = raw_settings.get(field_name)
    if raw_value is None:
        return _DEFAULTS[field_name]

    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        issues.append(
            _build_issue(
                code="invalid_settings_value",
                severity="error",
                message=f"Invalid value for '{field_name}': must be a positive integer.",
                field_name=field_name,
            )
        )
        return None

    if parsed < 1:
        issues.append(
            _build_issue(
                code="invalid_settings_value",
                severity="error",
                message=f"Invalid value for '{field_name}': must be >= 1, got {parsed}.",
                field_name=field_name,
            )
        )
        return None

    return parsed


def _validate_string_setting(
    raw_settings: dict[str, object],
    field_name: str,
    valid_values: set[str],
    issues: list[ConfigIssue],
) -> str | None:
    raw_value = raw_settings.get(field_name)
    if raw_value is None:
        return _DEFAULTS[field_name]

    str_value = str(raw_value).strip().lower()
    if str_value not in valid_values:
        issues.append(
            _build_issue(
                code="invalid_settings_value",
                severity="error",
                message=f"Invalid value for '{field_name}': '{raw_value}' is not one of {sorted(valid_values)}.",
                field_name=field_name,
            )
        )
        return None

    return str_value


def _expand_text_list(
    items: list[object],
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> list[str] | None:
    """Expand @set and bare set references in a text/term list.
    Returns expanded list or None on error.
    """
    expanded: list[str] = []
    seen: set[str] = set()
    for item in items:
        s = str(item).strip()
        if not s:
            continue
        if s.startswith("@"):
            set_name = s[1:]
            if set_name not in sets:
                issues.append(
                    _build_issue(
                        code="unknown_set_reference",
                        severity="error",
                        message=f"Rule '{condition_id}' references unknown set '{set_name}'.",
                        condition_id=condition_id,
                    )
                )
                return None
            for term in sets[set_name]:
                if term not in seen:
                    seen.add(term)
                    expanded.append(term)
        elif s in sets:
            if s in rule_ids:
                issues.append(
                    _build_issue(
                        code="ambiguous_set_reference",
                        severity="error",
                        message=f"Rule '{condition_id}' bare reference '{s}' is ambiguous: matches both a set and a rule id.",
                        condition_id=condition_id,
                    )
                )
                return None
            for term in sets[s]:
                if term not in seen:
                    seen.add(term)
                    expanded.append(term)
        else:
            if s not in seen:
                seen.add(s)
                expanded.append(s)
    return expanded


def _parse_text_any(
    value: object,
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> dict[str, object] | None:
    if not isinstance(value, list):
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' text_any must be a list.",
                condition_id=condition_id,
            )
        )
        return None
    texts = _expand_text_list(value, condition_id, issues, sets, rule_ids)
    if texts is None:
        return None
    if not texts:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' text_any must contain at least one non-empty text.",
                condition_id=condition_id,
            )
        )
        return None
    return {"texts": texts, "match_logic": "or"}


def _parse_text_all(
    value: object,
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> dict[str, object] | None:
    if not isinstance(value, list):
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' text_all must be a list.",
                condition_id=condition_id,
            )
        )
        return None
    texts = _expand_text_list(value, condition_id, issues, sets, rule_ids)
    if texts is None:
        return None
    if not texts:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' text_all must contain at least one non-empty text.",
                condition_id=condition_id,
            )
        )
        return None
    return {"texts": texts, "match_logic": "and"}


def _parse_token_window(
    token_window: dict[str, object],
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> dict[str, object] | None:
    raw_terms = token_window.get("terms")
    if not isinstance(raw_terms, list):
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.terms must be a list.",
                condition_id=condition_id,
            )
        )
        return None
    terms = _expand_text_list(raw_terms, condition_id, issues, sets, rule_ids)
    if terms is None:
        return None
    if not terms:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.terms must contain at least one non-empty term.",
                condition_id=condition_id,
            )
        )
        return None
    raw_anchor = token_window.get("anchor")
    if raw_anchor is None:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.anchor is required.",
                condition_id=condition_id,
            )
        )
        return None
    anchor = str(raw_anchor).strip()
    if anchor not in terms:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.anchor must be one of the terms.",
                condition_id=condition_id,
            )
        )
        return None
    raw_distance = token_window.get("distance")
    if raw_distance is None:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.distance is required.",
                condition_id=condition_id,
            )
        )
        return None
    try:
        distance = int(raw_distance)
    except (TypeError, ValueError):
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.distance must be an integer.",
                condition_id=condition_id,
            )
        )
        return None
    if distance < 0:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' token_window.distance must be >= 0, got {distance}.",
                condition_id=condition_id,
            )
        )
        return None
    return {
        "forms": terms,
        "match_logic": "and",
        "anchor_form": anchor,
        "max_token_distance": distance,
    }


def _process_match_item(
    item: dict[str, object],
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> tuple[dict[str, object] | None, str | None]:
    """Parse a single shorthand item inside any/all list.
    Returns (group_dict, group_type) where group_type is 'text' or 'form'.
    """
    allowed_keys = {"text_any", "text_all", "token_window"}
    unknown_keys = set(item.keys()) - allowed_keys
    if unknown_keys:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match item contains unknown keys: {sorted(unknown_keys)}.",
                condition_id=condition_id,
            )
        )
        return None, None
    if len(item) != 1:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match item must have exactly one key.",
                condition_id=condition_id,
            )
        )
        return None, None
    key = next(iter(item.keys()))
    value = item[key]
    if key == "text_any":
        group = _parse_text_any(value, condition_id, issues, sets, rule_ids)
        return group, "text"
    if key == "text_all":
        group = _parse_text_all(value, condition_id, issues, sets, rule_ids)
        return group, "text"
    if key == "token_window":
        if not isinstance(value, dict):
            issues.append(
                _build_issue(
                    code="authoring_match_invalid",
                    severity="error",
                    message=f"Rule '{condition_id}' token_window must be an object.",
                    condition_id=condition_id,
                )
            )
            return None, None
        group = _parse_token_window(value, condition_id, issues, sets, rule_ids)
        return group, "form"
    return None, None


def _apply_combine_logic(
    groups: list[dict[str, object]],
    combine_logic: str,
) -> None:
    for i, group in enumerate(groups):
        if i > 0:
            group["combine_logic"] = combine_logic


def _process_match_any_all(
    match: dict[str, object],
    condition_id: str,
    issues: list[ConfigIssue],
    sets: dict[str, list[str]],
    rule_ids: set[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]]] | None:
    """Process match dict containing any/all. Returns (text_groups, form_groups) or None on error."""
    has_any = "any" in match
    has_all = "all" in match
    if not has_any and not has_all:
        return None

    # Reject if any other keys present alongside any/all
    other_keys = set(match.keys()) - {"any", "all"}
    if other_keys:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match contains unknown keys: {sorted(other_keys)}.",
                condition_id=condition_id,
            )
        )
        return None

    key = "any" if has_any else "all"
    raw_list = match[key]
    if not isinstance(raw_list, list):
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match.{key} must be a list.",
                condition_id=condition_id,
            )
        )
        return None
    if not raw_list:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match.{key} must not be empty.",
                condition_id=condition_id,
            )
        )
        return None

    text_groups: list[dict[str, object]] = []
    form_groups: list[dict[str, object]] = []

    for item in raw_list:
        if not isinstance(item, dict):
            issues.append(
                _build_issue(
                    code="authoring_match_invalid",
                    severity="error",
                    message=f"Rule '{condition_id}' match.{key} items must be objects.",
                    condition_id=condition_id,
                )
            )
            return None
        # Reject nested any/all
        if "any" in item or "all" in item:
            issues.append(
                _build_issue(
                    code="authoring_match_invalid",
                    severity="error",
                    message=f"Rule '{condition_id}' match.{key} cannot contain nested any/all.",
                    condition_id=condition_id,
                )
            )
            return None
        group, group_type = _process_match_item(item, condition_id, issues, sets, rule_ids)
        if group is None or group_type is None:
            return None
        if group_type == "text":
            text_groups.append(group)
        else:
            form_groups.append(group)

    # For any: mixed text+form is error
    if has_any and text_groups and form_groups:
        issues.append(
            _build_issue(
                code="authoring_match_invalid",
                severity="error",
                message=f"Rule '{condition_id}' match.any cannot mix text and token clauses.",
                condition_id=condition_id,
            )
        )
        return None

    combine_logic = "or" if has_any else "and"
    _apply_combine_logic(text_groups, combine_logic)
    _apply_combine_logic(form_groups, combine_logic)
    return text_groups, form_groups


def _validate_rules(
    raw_document: dict[str, object],
    issues: list[ConfigIssue],
) -> list[dict[str, object]] | None:
    raw_rules = raw_document.get("rules")
    if raw_rules is None:
        issues.append(
            _build_issue(
                code="authoring_rules_missing",
                severity="error",
                message="Missing required field 'rules'.",
                field_name="rules",
            )
        )
        return None
    if not isinstance(raw_rules, list):
        issues.append(
            _build_issue(
                code="authoring_rules_invalid",
                severity="error",
                message="'rules' must be a list.",
                field_name="rules",
            )
        )
        return None

    # Validate sets
    raw_sets = raw_document.get("sets")
    sets: dict[str, list[str]] = {}
    if raw_sets is not None:
        if not isinstance(raw_sets, dict):
            issues.append(
                _build_issue(
                    code="authoring_sets_invalid",
                    severity="error",
                    message="'sets' must be an object.",
                    field_name="sets",
                )
            )
            return None
        for set_name, set_value in raw_sets.items():
            if not isinstance(set_value, list) or not set_value:
                issues.append(
                    _build_issue(
                        code="authoring_set_invalid",
                        severity="error",
                        message=f"Set '{set_name}' must be a non-empty list.",
                        field_name=f"sets.{set_name}",
                    )
                )
                return None
            cleaned = [str(item).strip() for item in set_value if str(item).strip()]
            if len(cleaned) != len(set_value):
                issues.append(
                    _build_issue(
                        code="authoring_set_invalid",
                        severity="error",
                        message=f"Set '{set_name}' must contain only non-empty strings.",
                        field_name=f"sets.{set_name}",
                    )
                )
                return None
            sets[str(set_name)] = cleaned

    # Validate defaults
    raw_defaults = raw_document.get("defaults")
    default_scope = "paragraph"
    default_exclude_any: list[str] | None = None
    if raw_defaults is not None:
        if not isinstance(raw_defaults, dict):
            issues.append(
                _build_issue(
                    code="authoring_defaults_invalid",
                    severity="error",
                    message="'defaults' must be an object.",
                    field_name="defaults",
                )
            )
            return None
        raw_scope = raw_defaults.get("scope")
        if raw_scope is not None:
            str_scope = str(raw_scope).strip().lower()
            if str_scope not in {"sentence", "paragraph"}:
                issues.append(
                    _build_issue(
                        code="authoring_scope_invalid",
                        severity="error",
                        message=f"Invalid value for 'defaults.scope': '{raw_scope}' is not one of ['paragraph', 'sentence'].",
                        field_name="defaults.scope",
                    )
                )
                return None
            default_scope = str_scope
        raw_default_exclude_any = raw_defaults.get("exclude_any")
        if raw_default_exclude_any is not None:
            if not isinstance(raw_default_exclude_any, list):
                issues.append(
                    _build_issue(
                        code="authoring_defaults_invalid",
                        severity="error",
                        message="'defaults.exclude_any' must be a list.",
                        field_name="defaults.exclude_any",
                    )
                )
                return None
            default_exclude_any = [str(item).strip() for item in raw_default_exclude_any if str(item).strip()]

    seen_ids: set[str] = set()
    runtime_conditions: list[dict[str, object]] = []

    # First pass: collect all rule ids (including skipped) for set ambiguity detection
    all_rule_ids: set[str] = set()
    for rule in raw_rules:
        if isinstance(rule, dict):
            rid = rule.get("id")
            if isinstance(rid, str) and rid.strip():
                all_rule_ids.add(rid.strip())

    # Determine output condition ids (non-skipped) for reference validation
    output_rule_ids: set[str] = set()
    for rule in raw_rules:
        if isinstance(rule, dict):
            rid = rule.get("id")
            if isinstance(rid, str) and rid.strip() and rule.get("skip") is not True:
                output_rule_ids.add(rid.strip())

    # Validate defaults.exclude_any references against output_rule_ids
    if default_exclude_any is not None:
        for ref in default_exclude_any:
            if ref not in output_rule_ids:
                issues.append(
                    _build_issue(
                        code="unknown_condition_reference",
                        severity="error",
                        message=f"defaults.exclude_any references unknown or skipped condition '{ref}'.",
                        field_name="defaults.exclude_any",
                    )
                )
                return None

    for index, rule in enumerate(raw_rules):
        if not isinstance(rule, dict):
            issues.append(
                _build_issue(
                    code="authoring_rule_invalid",
                    severity="error",
                    message=f"Rule at index {index} must be an object.",
                    condition_index=index,
                )
            )
            return None

        rule_id = rule.get("id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            issues.append(
                _build_issue(
                    code="authoring_rule_id_invalid",
                    severity="error",
                    message=f"Rule at index {index} must have a non-empty 'id'.",
                    condition_index=index,
                )
            )
            return None

        stripped_id = rule_id.strip()
        if stripped_id in seen_ids:
            issues.append(
                _build_issue(
                    code="authoring_rule_id_duplicate",
                    severity="error",
                    message=f"Duplicate rule id: '{stripped_id}'.",
                    condition_id=stripped_id,
                )
            )
            return None
        seen_ids.add(stripped_id)

        if rule.get("skip") is True:
            continue

        # Validate and expand match field
        match = rule.get("match")
        text_groups: list[dict[str, object]] = []
        form_groups: list[dict[str, object]] = []
        if "match" in rule:
            if match is None or not isinstance(match, dict):
                issues.append(
                    _build_issue(
                        code="authoring_match_invalid",
                        severity="error",
                        message=f"Rule '{stripped_id}' match must be an object.",
                        condition_id=stripped_id,
                    )
                )
                return None
            if not match:
                issues.append(
                    _build_issue(
                        code="authoring_match_invalid",
                        severity="error",
                        message=f"Rule '{stripped_id}' match must not be empty.",
                        condition_id=stripped_id,
                    )
                )
                return None
            # Check for any/all expansion first
            any_all_result = _process_match_any_all(match, stripped_id, issues, sets, all_rule_ids)
            if any_all_result is not None:
                text_groups, form_groups = any_all_result
            elif "any" in match or "all" in match:
                # any/all was present but failed validation; error already added
                return None
            else:
                # Direct match handling (text_any, text_all, token_window)
                allowed_match_keys = {"text_any", "text_all", "token_window"}
                unknown_keys = set(match.keys()) - allowed_match_keys
                if unknown_keys:
                    issues.append(
                        _build_issue(
                            code="authoring_match_invalid",
                            severity="error",
                            message=f"Rule '{stripped_id}' match contains unknown keys: {sorted(unknown_keys)}.",
                            condition_id=stripped_id,
                        )
                    )
                    return None
                has_text_any = "text_any" in match
                has_text_all = "text_all" in match
                has_token_window = "token_window" in match
                if has_text_any and has_text_all:
                    issues.append(
                        _build_issue(
                            code="authoring_match_invalid",
                            severity="error",
                            message=f"Rule '{stripped_id}' match cannot contain both text_any and text_all.",
                            condition_id=stripped_id,
                        )
                    )
                    return None
                if has_token_window and (has_text_any or has_text_all):
                    issues.append(
                        _build_issue(
                            code="authoring_match_invalid",
                            severity="error",
                            message=f"Rule '{stripped_id}' match cannot combine token_window with text_any or text_all.",
                            condition_id=stripped_id,
                        )
                    )
                    return None
                if has_text_any:
                    group = _parse_text_any(match["text_any"], stripped_id, issues, sets, all_rule_ids)
                    if group is None:
                        return None
                    text_groups.append(group)
                if has_text_all:
                    group = _parse_text_all(match["text_all"], stripped_id, issues, sets, all_rule_ids)
                    if group is None:
                        return None
                    text_groups.append(group)
                if has_token_window:
                    token_window = match["token_window"]
                    if not isinstance(token_window, dict):
                        issues.append(
                            _build_issue(
                                code="authoring_match_invalid",
                                severity="error",
                                message=f"Rule '{stripped_id}' token_window must be an object.",
                                condition_id=stripped_id,
                            )
                        )
                        return None
                    group = _parse_token_window(token_window, stripped_id, issues, sets, all_rule_ids)
                    if group is None:
                        return None
                    form_groups.append(group)

        categories: list[str] = []
        labels = rule.get("labels")
        label = rule.get("label")

        if isinstance(labels, list) and labels:
            categories = [str(item) for item in labels if isinstance(item, str)]
            if label is not None:
                issues.append(
                    _build_issue(
                        code="label_ignored",
                        severity="warning",
                        message=f"Both 'label' and 'labels' present for rule '{stripped_id}'; 'label' is ignored.",
                        condition_id=stripped_id,
                    )
                )
        elif isinstance(label, str):
            categories = [label]
        else:
            categories = []

        # Determine overall_search_scope: rule.scope > defaults.scope > paragraph
        overall_search_scope = default_scope
        rule_scope = rule.get("scope")
        if rule_scope is not None:
            str_rule_scope = str(rule_scope).strip().lower()
            if str_rule_scope not in {"sentence", "paragraph"}:
                issues.append(
                    _build_issue(
                        code="authoring_scope_invalid",
                        severity="error",
                        message=f"Invalid value for 'scope': '{rule_scope}' is not one of ['paragraph', 'sentence'].",
                        condition_id=stripped_id,
                    )
                )
                return None
            overall_search_scope = str_rule_scope

        runtime_condition: dict[str, object] = {
            "condition_id": stripped_id,
            "categories": categories,
            "overall_search_scope": overall_search_scope,
        }

        # Preserve known runtime keys if present
        for key in _RUNTIME_RULE_KEYS:
            if key in rule and key not in ("condition_id", "categories", "overall_search_scope"):
                runtime_condition[key] = rule[key]

        # Resolve requires_all / requires_any / exclude_any runtime mapping and validation
        for authoring_key, runtime_key in (
            ("requires_all", "required_condition_ids_all"),
            ("requires_any", "required_condition_ids_any"),
            ("exclude_any", "excluded_condition_ids_any"),
        ):
            if authoring_key in rule:
                refs = rule[authoring_key]
                if isinstance(refs, list):
                    ref_list = [str(item).strip() for item in refs if str(item).strip()]
                    for ref in ref_list:
                        if ref not in output_rule_ids:
                            issues.append(
                                _build_issue(
                                    code="unknown_condition_reference",
                                    severity="error",
                                    message=f"Rule '{stripped_id}' {authoring_key} references unknown or skipped condition '{ref}'.",
                                    condition_id=stripped_id,
                                )
                            )
                            return None
                    runtime_condition[runtime_key] = ref_list

        # Resolve defaults.exclude_any union / override
        if "exclude_any" in rule:
            rule_exclude_any = rule["exclude_any"]
            if isinstance(rule_exclude_any, list):
                if rule_exclude_any:
                    rule_excludes = [str(item).strip() for item in rule_exclude_any if str(item).strip()]
                    combined: list[str] = []
                    seen_excl: set[str] = set()
                    for ex in rule_excludes:
                        if ex not in seen_excl:
                            seen_excl.add(ex)
                            combined.append(ex)
                    if default_exclude_any is not None:
                        for ex in default_exclude_any:
                            if ex not in seen_excl:
                                seen_excl.add(ex)
                                combined.append(ex)
                    if combined:
                        runtime_condition["excluded_condition_ids_any"] = combined
                    else:
                        runtime_condition.pop("excluded_condition_ids_any", None)
                else:
                    # Explicit [] overrides defaults
                    runtime_condition.pop("excluded_condition_ids_any", None)
        else:
            if default_exclude_any is not None and default_exclude_any:
                runtime_condition["excluded_condition_ids_any"] = list(default_exclude_any)

        if text_groups:
            runtime_condition["text_groups"] = text_groups
        if form_groups:
            for fg in form_groups:
                fg["search_scope"] = overall_search_scope
            runtime_condition["form_groups"] = form_groups

        runtime_conditions.append(runtime_condition)

    return runtime_conditions


def compile_authoring_config(raw_document: dict[str, object]) -> CompileAuthoringResult:
    """Validate and compile a raw authoring document into a structured result."""
    if not isinstance(raw_document, dict):
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=[
                ConfigIssue(
                    code="authoring_root_invalid",
                    severity="error",
                    scope="filter_config",
                    message="Root must be an object.",
                ),
            ],
        )

    issues: list[ConfigIssue] = []

    # Validate format (required)
    raw_format = raw_document.get("format")
    if raw_format is None:
        issues.append(
            _build_issue(
                code="authoring_format_missing",
                severity="error",
                message="Missing required field 'format': expected 'condition-authoring/v1'.",
                field_name="format",
            )
        )
    elif raw_format != "condition-authoring/v1":
        issues.append(
            _build_issue(
                code="authoring_format_invalid",
                severity="error",
                message=f"Invalid format: expected 'condition-authoring/v1', got {raw_format!r}.",
                field_name="format",
            )
        )

    if issues:
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=issues,
        )

    raw_settings = raw_document.get("settings")

    if raw_settings is None:
        raw_settings = {}
    elif not isinstance(raw_settings, dict):
        issues.append(
            _build_issue(
                code="invalid_settings_value",
                severity="error",
                message="'settings' must be an object.",
                field_name="settings",
            )
        )
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=issues,
        )

    # Detect unknown settings fields
    for key in raw_settings:
        if key not in _SETTINGS_FIELDS:
            issues.append(
                _build_issue(
                    code="unknown_settings_field",
                    severity="warning",
                    message=f"Unknown settings field '{key}'.",
                    field_name=key,
                )
            )

    # Validate string settings
    normalized: dict[str, object] = {}
    has_error = False

    for field_name, valid_values in _VALID_STRING_SETTINGS.items():
        value = _validate_string_setting(raw_settings, field_name, valid_values, issues)
        if value is None:
            has_error = True
        else:
            normalized[field_name] = value

    # Validate int settings
    for field_name in _INT_SETTINGS:
        value = _validate_int_setting(raw_settings, field_name, issues)
        if value is None:
            has_error = True
        else:
            normalized[field_name] = value

    if has_error:
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=issues,
        )

    # Validate rules
    runtime_conditions = _validate_rules(raw_document, issues)
    if runtime_conditions is None:
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=issues,
        )

    normalized["cooccurrence_conditions"] = runtime_conditions

    # loaded_condition_count = number of authoring rules before skip exclusion
    raw_rules = raw_document.get("rules")
    loaded_condition_count = len(raw_rules) if isinstance(raw_rules, list) else 0

    filter_config = FilterConfig(
        condition_match_logic=str(normalized.get("condition_match_logic", "any")),
        cooccurrence_conditions=runtime_conditions,
        loaded_condition_count=loaded_condition_count,
        max_reconstructed_paragraphs=int(normalized.get("max_reconstructed_paragraphs", 10000)),
        analysis_unit=str(normalized.get("analysis_unit", "paragraph")),
        distance_matching_mode=str(normalized.get("distance_matching_mode", "auto-approx")),
        distance_match_combination_cap=int(normalized.get("distance_match_combination_cap", 10000)),
        distance_match_strict_safety_limit=int(normalized.get("distance_match_strict_safety_limit", 1000000)),
    )

    normalizer_result = normalize_cooccurrence_conditions_result(runtime_conditions)
    if normalizer_result.issues:
        issues.extend(normalizer_result.issues)
        has_normalizer_error = any(issue.severity == "error" for issue in normalizer_result.issues)
        if has_normalizer_error:
            return CompileAuthoringResult(
                raw_config=normalized,
                filter_config=None,
                issues=issues,
            )

    return CompileAuthoringResult(
        raw_config=normalized,
        filter_config=filter_config,
        issues=issues,
    )


def load_authoring_config_result(path: Path | str) -> CompileAuthoringResult:
    """Read a JSON authoring config file and compile it.

    YAML files (.yaml / .yml) are explicitly not supported in v1.
    """
    path_obj = Path(path)
    suffix = path_obj.suffix.lower()

    if suffix in (".yaml", ".yml"):
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=[
                ConfigIssue(
                    code="yaml_not_supported_in_v1",
                    severity="error",
                    scope="filter_config",
                    message=f"YAML files are not supported in v1: {path_obj}",
                ),
            ],
        )

    if not path_obj.exists():
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=[
                ConfigIssue(
                    code="authoring_file_not_found",
                    severity="error",
                    scope="filter_config",
                    message=f"Authoring config file not found: {path_obj}",
                ),
            ],
        )

    try:
        with path_obj.open("r", encoding="utf-8") as f:
            parsed = json.load(f)
    except json.JSONDecodeError as exc:
        return CompileAuthoringResult(
            raw_config=None,
            filter_config=None,
            issues=[
                ConfigIssue(
                    code="authoring_json_invalid",
                    severity="error",
                    scope="filter_config",
                    message=f"Invalid JSON in authoring config file {path_obj}: {exc}",
                ),
            ],
        )

    return compile_authoring_config(parsed)
