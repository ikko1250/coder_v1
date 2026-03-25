from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import polars as pl

from .text_unit_frames import TextUnitFrames


DistanceMatchingMode = Literal["strict", "auto-approx", "approx"]
ConfigIssueSeverity = Literal["warning", "error"]
ConfigIssueScope = Literal["filter_config", "condition"]
DataAccessIssueSeverity = Literal["error"]


@dataclass(frozen=True)
class FilterConfig:
    condition_match_logic: str
    cooccurrence_conditions: list[dict[str, object]]
    loaded_condition_count: int
    max_reconstructed_paragraphs: int
    analysis_unit: str = "paragraph"
    distance_matching_mode: DistanceMatchingMode = "auto-approx"
    distance_match_combination_cap: int = 10000
    distance_match_strict_safety_limit: int = 1000000


@dataclass(frozen=True)
class LoadFilterConfigResult:
    filter_config: FilterConfig | None
    issues: list[ConfigIssue] = field(default_factory=list)


@dataclass(frozen=True)
class DataAccessIssue:
    code: str
    severity: DataAccessIssueSeverity
    message: str
    query_name: str
    db_path: str


@dataclass(frozen=True)
class DataAccessResult:
    data_frame: pl.DataFrame | None
    issues: list[DataAccessIssue] = field(default_factory=list)


@dataclass(frozen=True)
class AnnotationFilter:
    label_namespace: str
    label_key: str
    label_value: str
    operator: str = "eq"


@dataclass(frozen=True)
class NormalizedFormGroup:
    forms: list[str]
    match_logic: str
    combine_logic: str | None
    search_scope: str
    requested_max_token_distance: int | None
    effective_max_token_distance: int | None
    anchor_form: str | None = None
    exclude_forms_any: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class NormalizedCondition:
    condition_id: str
    categories: list[str]
    category_text: str
    forms: list[str]
    search_scope: str
    form_match_logic: str
    requested_max_token_distance: int | None
    effective_max_token_distance: int | None
    overall_search_scope: str = "paragraph"
    form_groups: list[NormalizedFormGroup] = field(default_factory=list)
    annotation_filters: list[AnnotationFilter] = field(default_factory=list)
    required_categories_all: list[str] = field(default_factory=list)
    required_categories_any: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MatchingWarning:
    code: str
    message: str
    condition_id: str
    unit_id: int | None
    requested_mode: DistanceMatchingMode
    used_mode: DistanceMatchingMode
    combination_count: int | None = None
    combination_cap: int | None = None
    safety_limit: int | None = None


@dataclass(frozen=True)
class ConfigIssue:
    code: str
    severity: ConfigIssueSeverity
    scope: ConfigIssueScope
    message: str
    condition_index: int | None = None
    condition_id: str | None = None
    field_name: str | None = None


@dataclass(frozen=True)
class ConditionHitResult:
    condition_hit_tokens_df: pl.DataFrame
    requested_mode: DistanceMatchingMode
    used_mode: DistanceMatchingMode
    warning_messages: list[MatchingWarning] = field(default_factory=list)


@dataclass(frozen=True)
class NormalizeConditionsResult:
    normalized_conditions: list[NormalizedCondition]
    issues: list[ConfigIssue] = field(default_factory=list)


@dataclass(frozen=True)
class TargetSelectionResult:
    candidate_tokens_df: pl.DataFrame
    condition_eval_df: pl.DataFrame
    paragraph_match_summary_df: pl.DataFrame
    sentence_match_summary_df: pl.DataFrame
    sentence_hit_tokens_df: pl.DataFrame
    target_paragraph_ids: list[int]
    target_sentence_ids: list[int]
    warning_messages: list[MatchingWarning] = field(default_factory=list)
    text_unit_frames: TextUnitFrames | None = None
