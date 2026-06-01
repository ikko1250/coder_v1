from __future__ import annotations

from analysis_backend.condition_authoring import compile_authoring_config
from analysis_backend.condition_authoring import CompileAuthoringResult


class TestCompileAuthoringConfigRootValidation:
    def test_non_dict_root_returns_error(self) -> None:
        result = compile_authoring_config(["not", "a", "dict"])

        assert isinstance(result, CompileAuthoringResult)
        assert result.raw_config is None
        assert result.filter_config is None
        assert len(result.issues) == 1

        error = result.issues[0]
        assert error.severity == "error"
        assert error.code == "authoring_root_invalid"


class TestCompileAuthoringConfigSettingsNormalization:
    def test_all_defaults_when_no_settings(self) -> None:
        result = compile_authoring_config({"format": "condition-authoring/v1", "rules": []})

        assert result.raw_config is not None
        assert result.raw_config == {
            "condition_match_logic": "any",
            "analysis_unit": "paragraph",
            "max_reconstructed_paragraphs": 10000,
            "distance_matching_mode": "auto-approx",
            "distance_match_combination_cap": 10000,
            "distance_match_strict_safety_limit": 1000000,
            "cooccurrence_conditions": [],
        }
        assert result.filter_config is not None
        assert result.issues == []

    def test_explicit_valid_settings(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {
                "condition_match_logic": "all",
                "analysis_unit": "sentence",
                "max_reconstructed_paragraphs": 500,
                "distance_matching_mode": "strict",
                "distance_match_combination_cap": 2000,
                "distance_match_strict_safety_limit": 500000,
            },
        })

        assert result.raw_config is not None
        assert result.raw_config["condition_match_logic"] == "all"
        assert result.raw_config["analysis_unit"] == "sentence"
        assert result.raw_config["max_reconstructed_paragraphs"] == 500
        assert result.raw_config["distance_matching_mode"] == "strict"
        assert result.raw_config["distance_match_combination_cap"] == 2000
        assert result.raw_config["distance_match_strict_safety_limit"] == 500000
        assert result.raw_config["cooccurrence_conditions"] == []
        assert result.filter_config is not None
        assert result.issues == []

    def test_unknown_settings_field_warns(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"unknown_field": "value"},
        })

        assert result.raw_config is not None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_settings_field"
        assert issue.severity == "warning"
        assert issue.scope == "filter_config"
        assert issue.field_name == "unknown_field"
        assert "unknown_field" in issue.message

    def test_invalid_condition_match_logic_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"condition_match_logic": "invalid"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.severity == "error"
        assert issue.scope == "filter_config"
        assert issue.field_name == "condition_match_logic"
        assert "condition_match_logic" in issue.message

    def test_invalid_analysis_unit_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"analysis_unit": "invalid"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "analysis_unit"

    def test_invalid_distance_matching_mode_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"distance_matching_mode": "invalid"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "distance_matching_mode"

    def test_invalid_max_reconstructed_paragraphs_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"max_reconstructed_paragraphs": 0},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "max_reconstructed_paragraphs"

    def test_invalid_distance_match_combination_cap_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"distance_match_combination_cap": -1},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "distance_match_combination_cap"

    def test_invalid_distance_match_strict_safety_limit_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"distance_match_strict_safety_limit": "abc"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "distance_match_strict_safety_limit"

    def test_non_coercible_int_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"max_reconstructed_paragraphs": "not_a_number"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.field_name == "max_reconstructed_paragraphs"

    def test_multiple_unknown_fields_multiple_warnings(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"foo": 1, "bar": 2},
        })

        assert result.raw_config is not None
        assert len(result.issues) == 2
        codes = {issue.code for issue in result.issues}
        assert codes == {"unknown_settings_field"}

    def test_mixed_valid_and_unknown_fields(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {
                "condition_match_logic": "all",
                "unknown_field": "value",
            },
        })

        assert result.raw_config is not None
        assert result.raw_config["condition_match_logic"] == "all"
        assert len(result.issues) == 1
        assert result.issues[0].code == "unknown_settings_field"
        assert result.issues[0].field_name == "unknown_field"

    def test_coercible_string_int_accepted(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": {"max_reconstructed_paragraphs": "500"},
        })

        assert result.raw_config is not None
        assert result.raw_config["max_reconstructed_paragraphs"] == 500
        assert result.issues == []

    def test_root_level_format_rules_defaults_sets_no_warning(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "defaults": {},
            "sets": {},
        })

        assert result.raw_config is not None
        assert result.issues == []
        assert result.raw_config["condition_match_logic"] == "any"

    def test_settings_not_object_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "settings": "not_an_object",
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "invalid_settings_value"
        assert issue.severity == "error"
        assert issue.field_name == "settings"


class TestCompileAuthoringConfigFormatValidation:
    def test_format_missing_is_error(self) -> None:
        result = compile_authoring_config({"rules": []})

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_format_missing"
        assert issue.severity == "error"

    def test_format_invalid_value_is_error(self) -> None:
        result = compile_authoring_config({"format": "v2", "rules": []})

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_format_invalid"
        assert issue.severity == "error"

    def test_format_valid_condition_authoring_v1(self) -> None:
        result = compile_authoring_config({"format": "condition-authoring/v1", "rules": []})

        assert result.raw_config is not None
        assert result.issues == []


class TestCompileAuthoringConfigRulesValidation:
    def test_rules_missing_is_error(self) -> None:
        result = compile_authoring_config({"format": "condition-authoring/v1"})

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rules_missing"
        assert issue.severity == "error"

    def test_rules_non_list_is_error(self) -> None:
        result = compile_authoring_config({"format": "condition-authoring/v1", "rules": "not_a_list"})

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rules_invalid"
        assert issue.severity == "error"

    def test_rules_item_not_dict_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": ["not_a_dict"],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rule_invalid"
        assert issue.severity == "error"
        assert issue.condition_index == 0

    def test_rule_missing_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"label": "A"}],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rule_id_invalid"
        assert issue.severity == "error"
        assert issue.condition_index == 0

    def test_rule_empty_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "", "label": "A"}],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rule_id_invalid"
        assert issue.severity == "error"
        assert issue.condition_index == 0

    def test_rule_whitespace_only_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "   ", "label": "A"}],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rule_id_invalid"
        assert issue.severity == "error"
        assert issue.condition_index == 0

    def test_duplicate_rule_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "rule1", "label": "A"},
                {"id": "rule1", "label": "B"},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_rule_id_duplicate"
        assert issue.severity == "error"
        assert issue.condition_id == "rule1"

    def test_label_expands_to_categories(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "CategoryA"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["condition_id"] == "r1"
        assert conditions[0]["categories"] == ["CategoryA"]

    def test_labels_expands_to_categories(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "labels": ["CatA", "CatB"]},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["categories"] == ["CatA", "CatB"]

    def test_both_label_and_labels_warns_label_ignored(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "Single", "labels": ["MultiA", "MultiB"], "match": {"text_any": ["foo"]}},
            ],
        })

        assert result.raw_config is not None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "label_ignored"
        assert issue.severity == "warning"
        assert issue.condition_id == "r1"
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["categories"] == ["MultiA", "MultiB"]

    def test_skip_true_excludes_from_output(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
                {"id": "r2", "label": "B", "skip": True},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["condition_id"] == "r1"

    def test_description_dropped_from_runtime(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "description": "This is a description"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert "description" not in conditions[0]

    def test_rule_without_match_but_with_requires_all_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_all": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert conditions[0]["condition_id"] == "r1"

    def test_rule_without_match_but_with_requires_any_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert conditions[0]["condition_id"] == "r1"

    def test_rule_without_match_but_with_exclude_any_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert conditions[0]["condition_id"] == "r1"


class TestCompileAuthoringConfigTextGroups:
    def test_text_any_expands_to_text_groups_or(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["抑制区域", "抑制地区"]}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["condition_id"] == "r1"
        assert conditions[0]["text_groups"] == [
            {"texts": ["抑制区域", "抑制地区"], "match_logic": "or"},
        ]

    def test_text_all_expands_to_text_groups_and(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": ["foo", "bar"]}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["text_groups"] == [
            {"texts": ["foo", "bar"], "match_logic": "and"},
        ]

    def test_text_any_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": []}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_text_all_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": []}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_text_any_non_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": "not_a_list"}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_empty_object_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_null_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": None},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_absent_with_requires_all_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_all": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert conditions[0]["condition_id"] == "r1"
        assert "text_groups" not in conditions[0]

    def test_match_absent_with_requires_any_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert "text_groups" not in conditions[0]

    def test_match_absent_with_exclude_any_is_valid(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 2
        assert "text_groups" not in conditions[0]

    def test_both_text_any_and_text_all_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["a"], "text_all": ["b"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_unknown_match_key_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["a"], "unknown_key": ["b"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_empty_string_in_text_list_is_skipped(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["a", "", "b"]}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["a", "b"], "match_logic": "or"},
        ]

    def test_all_empty_strings_in_text_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["", ""]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_whitespace_only_string_in_text_list_is_skipped(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": ["foo", "   ", "bar"]}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["foo", "bar"], "match_logic": "and"},
        ]

    def test_text_any_all_strings_stripped(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["  foo  ", "bar"]}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["foo", "bar"], "match_logic": "or"},
        ]

    def test_normalizer_no_errors_for_text_any_output(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["抑制区域", "抑制地区"]}},
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].text_groups[0].texts == ["抑制区域", "抑制地区"]
        assert norm_result.normalized_conditions[0].text_groups[0].match_logic == "or"

    def test_normalizer_no_errors_for_text_all_output(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": ["foo", "bar"]}},
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].text_groups[0].texts == ["foo", "bar"]
        assert norm_result.normalized_conditions[0].text_groups[0].match_logic == "and"


class TestCompileAuthoringConfigMatchAnyAll:
    # --- match.any ---
    def test_match_any_text_only_expands_to_text_groups_or(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"text_any": ["抑制区域", "抑制地区"]},
                            {"text_any": ["foo", "bar"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["text_groups"] == [
            {"texts": ["抑制区域", "抑制地区"], "match_logic": "or"},
            {"texts": ["foo", "bar"], "match_logic": "or", "combine_logic": "or"},
        ]

    def test_match_any_token_only_expands_to_form_groups_or(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                            {"token_window": {"terms": ["許可", "区域"], "anchor": "許可", "distance": 5}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["form_groups"] == [
            {"forms": ["禁止", "区域"], "match_logic": "and", "anchor_form": "禁止", "max_token_distance": 3, "search_scope": "paragraph"},
            {"forms": ["許可", "区域"], "match_logic": "and", "anchor_form": "許可", "max_token_distance": 5, "search_scope": "paragraph", "combine_logic": "or"},
        ]

    def test_match_any_mixed_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"text_any": ["foo"]},
                            {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_nested_any_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"any": [{"text_any": ["foo"]}]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_nested_all_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"all": [{"text_any": ["foo"]}]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_item_with_multiple_keys_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"text_any": ["foo"], "text_all": ["bar"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_unknown_key_in_item_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"unknown_key": ["foo"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"any": []}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_non_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"any": "not_a_list"}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_any_item_not_dict_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"any": ["not_a_dict"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    # --- match.all ---
    def test_match_all_text_only_expands_to_text_groups_and(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_all": ["foo", "bar"]},
                            {"text_all": ["baz", "qux"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["text_groups"] == [
            {"texts": ["foo", "bar"], "match_logic": "and"},
            {"texts": ["baz", "qux"], "match_logic": "and", "combine_logic": "and"},
        ]

    def test_match_all_token_only_expands_to_form_groups_and(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                            {"token_window": {"terms": ["許可", "区域"], "anchor": "許可", "distance": 5}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["form_groups"] == [
            {"forms": ["禁止", "区域"], "match_logic": "and", "anchor_form": "禁止", "max_token_distance": 3, "search_scope": "paragraph"},
            {"forms": ["許可", "区域"], "match_logic": "and", "anchor_form": "許可", "max_token_distance": 5, "search_scope": "paragraph", "combine_logic": "and"},
        ]

    def test_match_all_mixed_expands_to_text_and_form_groups(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_any": ["A"]},
                            {"token_window": {"terms": ["B", "C"], "anchor": "B", "distance": 3}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["text_groups"] == [
            {"texts": ["A"], "match_logic": "or"},
        ]
        assert conditions[0]["form_groups"] == [
            {"forms": ["B", "C"], "match_logic": "and", "anchor_form": "B", "max_token_distance": 3, "search_scope": "paragraph"},
        ]

    def test_match_all_mixed_multiple_text_and_form(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_any": ["A"]},
                            {"text_all": ["B", "C"]},
                            {"token_window": {"terms": ["D", "E"], "anchor": "D", "distance": 3}},
                            {"token_window": {"terms": ["F", "G"], "anchor": "F", "distance": 5}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["text_groups"] == [
            {"texts": ["A"], "match_logic": "or"},
            {"texts": ["B", "C"], "match_logic": "and", "combine_logic": "and"},
        ]
        assert conditions[0]["form_groups"] == [
            {"forms": ["D", "E"], "match_logic": "and", "anchor_form": "D", "max_token_distance": 3, "search_scope": "paragraph"},
            {"forms": ["F", "G"], "match_logic": "and", "anchor_form": "F", "max_token_distance": 5, "search_scope": "paragraph", "combine_logic": "and"},
        ]

    def test_match_all_nested_any_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"any": [{"text_any": ["foo"]}]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_nested_all_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"all": [{"text_any": ["foo"]}]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_item_with_multiple_keys_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_any": ["foo"], "text_all": ["bar"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_unknown_key_in_item_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"unknown_key": ["foo"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"all": []}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_non_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"all": "not_a_list"}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_all_item_not_dict_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"all": ["not_a_dict"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    # --- normalizer checks ---
    def test_normalizer_no_errors_for_match_any_text_only(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"text_any": ["抑制区域", "抑制地区"]},
                            {"text_any": ["foo", "bar"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].text_groups[0].texts == ["抑制区域", "抑制地区"]
        assert norm_result.normalized_conditions[0].text_groups[1].texts == ["foo", "bar"]

    def test_normalizer_no_errors_for_match_all_text_only(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_all": ["foo", "bar"]},
                            {"text_all": ["baz", "qux"]},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].text_groups[0].texts == ["foo", "bar"]
        assert norm_result.normalized_conditions[0].text_groups[1].texts == ["baz", "qux"]

    def test_normalizer_no_errors_for_match_all_mixed(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_any": ["A"]},
                            {"token_window": {"terms": ["B", "C"], "anchor": "B", "distance": 3}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].text_groups[0].texts == ["A"]
        fg = norm_result.normalized_conditions[0].form_groups[0]
        assert fg.forms == ["B", "C"]
        assert fg.anchor_form == "B"

    def test_normalizer_no_errors_for_match_any_token_only(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                            {"token_window": {"terms": ["許可", "区域"], "anchor": "許可", "distance": 5}},
                        ]
                    },
                },
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        assert norm_result.normalized_conditions[0].form_groups[0].forms == ["禁止", "区域"]
        assert norm_result.normalized_conditions[0].form_groups[1].forms == ["許可", "区域"]

    def test_match_unknown_key_at_match_level_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"any": [{"text_any": ["foo"]}], "unknown": "x"}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_direct_and_any_together_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo"], "any": [{"text_any": ["bar"]}]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_match_direct_and_all_together_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo"], "all": [{"text_any": ["bar"]}]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"


class TestCompileAuthoringConfigTokenWindow:
    def test_token_window_expands_to_form_groups(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                },
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert len(conditions) == 1
        assert conditions[0]["condition_id"] == "r1"
        assert conditions[0]["form_groups"] == [
            {
                "forms": ["禁止", "区域"],
                "match_logic": "and",
                "anchor_form": "禁止",
                "max_token_distance": 3,
                "search_scope": "paragraph",
            },
        ]

    def test_token_window_terms_non_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": "not_a_list", "anchor": "禁止", "distance": 3}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_terms_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": [], "anchor": "禁止", "distance": 3}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_all_terms_empty_after_strip_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["", "   "], "anchor": "禁止", "distance": 3}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_anchor_missing_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "distance": 3}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_anchor_not_in_terms_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "違反", "distance": 3}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_distance_missing_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止"}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_distance_non_int_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": "abc"}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_distance_negative_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": -1}}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_with_text_any_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}, "text_any": ["a"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_token_window_with_text_all_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}, "text_all": ["a"]}},
            ],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_match_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_normalizer_no_errors_for_token_window_output(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}},
                },
            ],
        })

        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        assert len(norm_result.normalized_conditions) == 1
        fg = norm_result.normalized_conditions[0].form_groups[0]
        assert fg.forms == ["禁止", "区域"]
        assert fg.match_logic == "and"
        assert fg.anchor_form == "禁止"
        assert fg.effective_max_token_distance == 3

    def test_token_window_distance_zero_allowed(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 0}}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["max_token_distance"] == 0

    def test_token_window_strips_terms(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["  禁止  ", "  区域  "], "anchor": "禁止", "distance": 3}}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["forms"] == ["禁止", "区域"]

    def test_token_window_anchor_must_match_stripped_terms(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["  禁止  ", "区域"], "anchor": "  禁止  ", "distance": 3}}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["anchor_form"] == "禁止"

    def test_token_window_uses_rule_scope_for_search_scope(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "scope": "sentence", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}}},
            ],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["search_scope"] == "sentence"

    def test_token_window_uses_defaults_scope_for_search_scope(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}}},
            ],
            "defaults": {"scope": "sentence"},
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["search_scope"] == "sentence"


class TestCompileAuthoringConfigScope:
    def test_default_scope_is_paragraph_when_no_defaults(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A"}],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "paragraph"

    def test_defaults_scope_sets_overall_search_scope(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A"}],
            "defaults": {"scope": "sentence"},
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "sentence"

    def test_rule_scope_overrides_defaults_scope(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A", "scope": "sentence"}],
            "defaults": {"scope": "paragraph"},
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "sentence"

    def test_rule_scope_overrides_default_paragraph(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A", "scope": "sentence"}],
        })

        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "sentence"

    def test_invalid_defaults_scope_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A"}],
            "defaults": {"scope": "invalid"},
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_scope_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "defaults.scope"

    def test_invalid_rule_scope_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A", "scope": "invalid"}],
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_scope_invalid"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_analysis_unit_sentence_defaults_scope_paragraph(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A"}],
            "settings": {"analysis_unit": "sentence"},
            "defaults": {"scope": "paragraph"},
        })

        assert result.raw_config is not None
        assert result.raw_config["analysis_unit"] == "sentence"
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "paragraph"

    def test_analysis_unit_paragraph_rule_scope_sentence(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A", "scope": "sentence"}],
            "settings": {"analysis_unit": "paragraph"},
        })

        assert result.raw_config is not None
        assert result.raw_config["analysis_unit"] == "paragraph"
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["overall_search_scope"] == "sentence"

    def test_defaults_not_object_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [{"id": "r1", "label": "A"}],
            "defaults": "not_an_object",
        })

        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_defaults_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "defaults"
