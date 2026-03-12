from __future__ import annotations

import json
from pathlib import Path

from .condition_model import FilterConfig


def _read_int_with_default(raw_value: object, default_value: int, *, minimum_value: int = 1) -> int:
    try:
        parsed_value = int(raw_value)
    except (TypeError, ValueError):
        return default_value
    if parsed_value < minimum_value:
        return default_value
    return parsed_value


def load_filter_config(filter_config_path: Path) -> FilterConfig:
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

    raw_match_logic = str(raw_config.get("condition_match_logic", "any")).strip().lower()
    condition_match_logic = raw_match_logic if raw_match_logic in {"any", "all"} else "any"

    max_reconstructed_paragraphs = _read_int_with_default(
        raw_config.get("max_reconstructed_paragraphs", 10000),
        10000,
    )
    raw_matching_mode = str(raw_config.get("distance_matching_mode", "auto-approx")).strip().lower()
    distance_matching_mode = (
        raw_matching_mode
        if raw_matching_mode in {"strict", "auto-approx", "approx"}
        else "auto-approx"
    )
    distance_match_combination_cap = _read_int_with_default(
        raw_config.get("distance_match_combination_cap", 10000),
        10000,
    )
    distance_match_strict_safety_limit = _read_int_with_default(
        raw_config.get("distance_match_strict_safety_limit", 1000000),
        1000000,
    )

    return FilterConfig(
        condition_match_logic=condition_match_logic,
        cooccurrence_conditions=raw_conditions,
        loaded_condition_count=len(raw_conditions),
        max_reconstructed_paragraphs=max_reconstructed_paragraphs,
        distance_matching_mode=distance_matching_mode,
        distance_match_combination_cap=distance_match_combination_cap,
        distance_match_strict_safety_limit=distance_match_strict_safety_limit,
    )
