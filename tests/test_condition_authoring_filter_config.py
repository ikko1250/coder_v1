from __future__ import annotations

from unittest.mock import patch

from analysis_backend.condition_authoring import compile_authoring_config
from analysis_backend.condition_authoring import CompileAuthoringResult
from analysis_backend.condition_model import ConfigIssue, FilterConfig


class TestCompileAuthoringFilterConfigSuccess:
    def test_success_populates_filter_config(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo"]}},
            ],
        })

        assert result.raw_config is not None
        assert result.filter_config is not None
        assert isinstance(result.filter_config, FilterConfig)
        assert result.filter_config.condition_match_logic == "any"
        assert result.filter_config.analysis_unit == "paragraph"
        assert result.filter_config.loaded_condition_count == 1
        assert len(result.filter_config.cooccurrence_conditions) == 1
        assert result.issues == []

    def test_loaded_condition_count_includes_skipped_rules(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo"]}},
                {"id": "r2", "label": "B", "skip": True, "match": {"text_any": ["bar"]}},
                {"id": "r3", "label": "C", "match": {"text_any": ["baz"]}},
            ],
        })

        assert result.raw_config is not None
        assert result.filter_config is not None
        assert result.filter_config.loaded_condition_count == 3
        assert len(result.filter_config.cooccurrence_conditions) == 2

    def test_raw_config_still_generated(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"condition_match_logic": "all"},
        })

        assert result.raw_config is not None
        assert result.raw_config["condition_match_logic"] == "all"
        assert result.filter_config is not None
        assert result.filter_config.condition_match_logic == "all"

    def test_filter_config_reflects_settings(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {
                "analysis_unit": "sentence",
                "max_reconstructed_paragraphs": 500,
                "distance_matching_mode": "strict",
                "distance_match_combination_cap": 2000,
                "distance_match_strict_safety_limit": 500000,
            },
        })

        assert result.filter_config is not None
        fc = result.filter_config
        assert fc.analysis_unit == "sentence"
        assert fc.max_reconstructed_paragraphs == 500
        assert fc.distance_matching_mode == "strict"
        assert fc.distance_match_combination_cap == 2000
        assert fc.distance_match_strict_safety_limit == 500000


class TestCompileAuthoringNormalizerWarnings:
    def test_normalizer_warnings_propagated_to_issues(self) -> None:
        warning_issue = ConfigIssue(
            code="search_scope_defaulted",
            severity="warning",
            scope="condition",
            message="Unknown overall_search_scope was replaced with 'paragraph'.",
            condition_index=1,
            condition_id="r1",
            field_name="overall_search_scope",
        )

        def fake_normalize(conditions):
            from analysis_backend.condition_model import NormalizeConditionsResult, NormalizedCondition
            return NormalizeConditionsResult(
                normalized_conditions=[
                    NormalizedCondition(
                        condition_id="r1",
                        categories=["A"],
                        category_text="A",
                        forms=["foo"],
                        search_scope="paragraph",
                        form_match_logic="or",
                        requested_max_token_distance=None,
                        effective_max_token_distance=None,
                    )
                ],
                issues=[warning_issue],
            )

        with patch("analysis_backend.condition_authoring.normalize_cooccurrence_conditions_result", fake_normalize):
            result = compile_authoring_config({
                "format": "condition-authoring/v1",
                "rules": [
                    {"id": "r1", "label": "A", "match": {"text_any": ["foo"]}},
                ],
            })

        assert result.raw_config is not None
        assert result.filter_config is not None
        assert len(result.issues) == 1
        assert result.issues[0].code == "search_scope_defaulted"
        assert result.issues[0].severity == "warning"


class TestCompileAuthoringNormalizerErrors:
    def test_normalizer_errors_produce_no_filter_config_and_keep_raw_config(self) -> None:
        error_issue = ConfigIssue(
            code="forms_not_list",
            severity="error",
            scope="condition",
            message="'forms' must be a list.",
            condition_index=1,
            condition_id="r1",
            field_name="forms",
        )

        def fake_normalize(conditions):
            from analysis_backend.condition_model import NormalizeConditionsResult
            return NormalizeConditionsResult(
                normalized_conditions=[],
                issues=[error_issue],
            )

        with patch("analysis_backend.condition_authoring.normalize_cooccurrence_conditions_result", fake_normalize):
            result = compile_authoring_config({
                "format": "condition-authoring/v1",
                "rules": [
                    {"id": "r1", "label": "A", "match": {"text_any": ["foo"]}},
                ],
            })

        assert result.raw_config is not None
        assert result.filter_config is None
        assert len(result.issues) == 1
        assert result.issues[0].code == "forms_not_list"
        assert result.issues[0].severity == "error"

    def test_normalizer_errors_combined_with_compiler_warnings(self) -> None:
        error_issue = ConfigIssue(
            code="forms_not_list",
            severity="error",
            scope="condition",
            message="'forms' must be a list.",
            condition_index=1,
            condition_id="r1",
            field_name="forms",
        )

        def fake_normalize(conditions):
            from analysis_backend.condition_model import NormalizeConditionsResult
            return NormalizeConditionsResult(
                normalized_conditions=[],
                issues=[error_issue],
            )

        with patch("analysis_backend.condition_authoring.normalize_cooccurrence_conditions_result", fake_normalize):
            result = compile_authoring_config({
                "format": "condition-authoring/v1",
                "rules": [],
                "settings": {"unknown_field": "value"},
            })

        assert result.raw_config is not None
        assert result.filter_config is None
        codes = {issue.code for issue in result.issues}
        assert codes == {"unknown_settings_field", "forms_not_list"}


class TestCompileAuthoringExistingErrorsUnchanged:
    def test_settings_error_still_returns_no_filter_config_and_no_raw_config(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"condition_match_logic": "invalid"},
        })

        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        assert result.issues[0].severity == "error"

    def test_rules_error_still_returns_no_filter_config_and_no_raw_config(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": "not_a_list",
        })

        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1
        assert result.issues[0].severity == "error"
