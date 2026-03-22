from __future__ import annotations

import json
from pathlib import Path

from .condition_model import ConfigIssue
from .condition_model import FilterConfig
from .condition_model import LoadFilterConfigResult


def _read_int_with_default(raw_value: object, default_value: int, *, minimum_value: int = 1) -> int:
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return default_value
    if parsed_value < minimum_value:
        return default_value
    return parsed_value


def _build_filter_config_issue(
    *,
    code: str,
    severity: str,
    message: str,
    field_name: str | None = None,
) -> ConfigIssue:
    return ConfigIssue(
        code=code,
        severity=severity,
        scope="filter_config",
        message=message,
        field_name=field_name,
    )


def load_filter_config_result(filter_config_path: Path) -> LoadFilterConfigResult:
    if not filter_config_path.exists():
        raise FileNotFoundError(f"Filter config JSON not found: {filter_config_path}")

    try:
        raw_config = json.loads(filter_config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON format: {filter_config_path} ({exc})") from exc

    if not isinstance(raw_config, dict):
        raise ValueError(f"JSON root must be object: {filter_config_path}")

    raw_conditions = raw_config.get("cooccurrence_conditions", [])
    if not isinstance(raw_conditions, list):
        raise ValueError(f"'cooccurrence_conditions' must be list: {filter_config_path}")

    issues: list[ConfigIssue] = []
    raw_match_logic = str(raw_config.get("condition_match_logic", "any")).strip().lower()
    condition_match_logic = raw_match_logic if raw_match_logic in {"any", "all"} else "any"
    if raw_match_logic not in {"any", "all"}:
        issues.append(
            _build_filter_config_issue(
                code="condition_match_logic_defaulted",
                severity="warning",
                message="Unknown condition_match_logic was replaced with 'any'.",
                field_name="condition_match_logic",
            )
        )

    raw_max_reconstructed_paragraphs = raw_config.get("max_reconstructed_paragraphs", 10000)
    max_reconstructed_paragraphs = _read_int_with_default(
        raw_max_reconstructed_paragraphs,
        10000,
    )
    if max_reconstructed_paragraphs == 10000 and raw_max_reconstructed_paragraphs != 10000:
        try:
            parsed_value = int(raw_max_reconstructed_paragraphs)
        except (TypeError, ValueError):
            parsed_value = None
        if parsed_value is None or parsed_value < 1:
            issues.append(
                _build_filter_config_issue(
                    code="max_reconstructed_paragraphs_defaulted",
                    severity="warning",
                    message="Invalid max_reconstructed_paragraphs was replaced with 10000.",
                    field_name="max_reconstructed_paragraphs",
                )
            )

    raw_analysis_unit = str(raw_config.get("analysis_unit", "paragraph")).strip().lower()
    analysis_unit = raw_analysis_unit if raw_analysis_unit in {"paragraph", "sentence"} else "paragraph"
    if raw_analysis_unit not in {"paragraph", "sentence"}:
        issues.append(
            _build_filter_config_issue(
                code="analysis_unit_defaulted",
                severity="warning",
                message="Unknown analysis_unit was replaced with 'paragraph'.",
                field_name="analysis_unit",
            )
        )

    raw_matching_mode = str(raw_config.get("distance_matching_mode", "auto-approx")).strip().lower()
    distance_matching_mode = (
        raw_matching_mode
        if raw_matching_mode in {"strict", "auto-approx", "approx"}
        else "auto-approx"
    )
    if raw_matching_mode not in {"strict", "auto-approx", "approx"}:
        issues.append(
            _build_filter_config_issue(
                code="distance_matching_mode_defaulted",
                severity="warning",
                message="Unknown distance_matching_mode was replaced with 'auto-approx'.",
                field_name="distance_matching_mode",
            )
        )
    raw_combination_cap = raw_config.get("distance_match_combination_cap", 10000)
    distance_match_combination_cap = _read_int_with_default(
        raw_combination_cap,
        10000,
    )
    if distance_match_combination_cap == 10000 and raw_combination_cap != 10000:
        try:
            parsed_value = int(raw_combination_cap)
        except (TypeError, ValueError):
            parsed_value = None
        if parsed_value is None or parsed_value < 1:
            issues.append(
                _build_filter_config_issue(
                    code="distance_match_combination_cap_defaulted",
                    severity="warning",
                    message="Invalid distance_match_combination_cap was replaced with 10000.",
                    field_name="distance_match_combination_cap",
                )
            )
    raw_safety_limit = raw_config.get("distance_match_strict_safety_limit", 1000000)
    distance_match_strict_safety_limit = _read_int_with_default(
        raw_safety_limit,
        1000000,
    )
    if distance_match_strict_safety_limit == 1000000 and raw_safety_limit != 1000000:
        try:
            parsed_value = int(raw_safety_limit)
        except (TypeError, ValueError):
            parsed_value = None
        if parsed_value is None or parsed_value < 1:
            issues.append(
                _build_filter_config_issue(
                    code="distance_match_strict_safety_limit_defaulted",
                    severity="warning",
                    message="Invalid distance_match_strict_safety_limit was replaced with 1000000.",
                    field_name="distance_match_strict_safety_limit",
                )
            )

    return LoadFilterConfigResult(
        filter_config=FilterConfig(
            condition_match_logic=condition_match_logic,
            cooccurrence_conditions=raw_conditions,
            loaded_condition_count=len(raw_conditions),
            max_reconstructed_paragraphs=max_reconstructed_paragraphs,
            analysis_unit=analysis_unit,
            distance_matching_mode=distance_matching_mode,
            distance_match_combination_cap=distance_match_combination_cap,
            distance_match_strict_safety_limit=distance_match_strict_safety_limit,
        ),
        issues=issues,
    )


def load_filter_config(filter_config_path: Path) -> FilterConfig:
    return load_filter_config_result(filter_config_path).filter_config
