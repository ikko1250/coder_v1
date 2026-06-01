from __future__ import annotations

from analysis_backend.condition_authoring import compile_authoring_config
from analysis_backend.condition_authoring import CompileAuthoringResult


class TestSetsValidation:
    def test_sets_not_object_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "sets": "not_an_object",
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_sets_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "sets"

    def test_sets_value_not_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "sets": {"regional_terms": "not_a_list"},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_set_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "sets.regional_terms"

    def test_sets_value_empty_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "sets": {"regional_terms": []},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_set_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "sets.regional_terms"

    def test_sets_value_contains_empty_string_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "sets": {"regional_terms": ["foo", ""]},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_set_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "sets.regional_terms"

    def test_sets_value_contains_whitespace_only_string_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [],
            "sets": {"regional_terms": ["foo", "   "]},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_set_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "sets.regional_terms"


class TestSetExpansionTextAny:
    def test_at_set_expands_in_text_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["@regional_terms"]}},
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "or"},
        ]

    def test_bare_set_expands_in_text_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["regional_terms"]}},
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "or"},
        ]

    def test_mixed_literal_and_set_in_text_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo", "@regional_terms", "bar"]}},
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["foo", "区域", "地区", "bar"], "match_logic": "or"},
        ]

    def test_unknown_at_set_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["@unknown_set"]}},
            ],
            "sets": {},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_set_reference"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_bare_unknown_not_set_remains_literal(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["unknown_set"]}},
            ],
            "sets": {},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["unknown_set"], "match_logic": "or"},
        ]

    def test_ambiguous_bare_set_reference_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
                {"id": "ambiguous", "label": "B", "match": {"text_any": ["ambiguous"]}},
            ],
            "sets": {"ambiguous": ["foo", "bar"]},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "ambiguous_set_reference"
        assert issue.severity == "error"
        assert issue.condition_id == "ambiguous"

    def test_expansion_dedupes_preserving_order(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["@set1", "@set2", "foo"]}},
            ],
            "sets": {
                "set1": ["a", "b"],
                "set2": ["b", "c"],
            },
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["a", "b", "c", "foo"], "match_logic": "or"},
        ]


class TestSetExpansionTextAll:
    def test_at_set_expands_in_text_all(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": ["@regional_terms"]}},
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "and"},
        ]

    def test_bare_set_expands_in_text_all(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_all": ["regional_terms"]}},
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "and"},
        ]


class TestSetExpansionTokenWindow:
    def test_at_set_expands_in_token_window_terms(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["@regional_terms"], "anchor": "区域", "distance": 3}},
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"] == [
            {"forms": ["区域", "地区"], "match_logic": "and", "anchor_form": "区域", "max_token_distance": 3, "search_scope": "paragraph"},
        ]

    def test_bare_set_expands_in_token_window_terms(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["regional_terms"], "anchor": "区域", "distance": 3}},
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"] == [
            {"forms": ["区域", "地区"], "match_logic": "and", "anchor_form": "区域", "max_token_distance": 3, "search_scope": "paragraph"},
        ]

    def test_mixed_literal_and_set_in_token_window_terms(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["foo", "@regional_terms", "bar"], "anchor": "foo", "distance": 3}},
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"] == [
            {"forms": ["foo", "区域", "地区", "bar"], "match_logic": "and", "anchor_form": "foo", "max_token_distance": 3, "search_scope": "paragraph"},
        ]

    def test_unknown_at_set_in_token_window_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["@unknown_set"], "anchor": "foo", "distance": 3}},
                },
            ],
            "sets": {},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_set_reference"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_anchor_is_literal_not_expanded(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {"token_window": {"terms": ["@regional_terms"], "anchor": "区域", "distance": 3}},
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["form_groups"][0]["anchor_form"] == "区域"


class TestSetExpansionMatchAnyAll:
    def test_set_expands_inside_match_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "any": [
                            {"text_any": ["@regional_terms"]},
                            {"text_any": ["foo"]},
                        ]
                    },
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "or"},
            {"texts": ["foo"], "match_logic": "or", "combine_logic": "or"},
        ]

    def test_set_expands_inside_match_all(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {
                    "id": "r1",
                    "label": "A",
                    "match": {
                        "all": [
                            {"text_all": ["@regional_terms"]},
                            {"text_all": ["foo"]},
                        ]
                    },
                },
            ],
            "sets": {"regional_terms": ["区域", "地区"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["区域", "地区"], "match_logic": "and"},
            {"texts": ["foo"], "match_logic": "and", "combine_logic": "and"},
        ]


class TestDefaultsExcludeAny:
    def test_defaults_exclude_any_missing_no_excluded(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
            ],
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert "excluded_condition_ids_any" not in conditions[0]

    def test_defaults_exclude_any_list_applied(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
                {"id": "r2", "label": "B"},
            ],
            "defaults": {"exclude_any": ["r2"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["excluded_condition_ids_any"] == ["r2"]

    def test_rule_exclude_any_overrides_defaults(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
            "defaults": {"exclude_any": ["r2"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        r1 = conditions[0]
        assert r1["excluded_condition_ids_any"] == ["r2"]

    def test_rule_exclude_any_empty_list_overrides_defaults(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": []},
                {"id": "r2", "label": "B"},
            ],
            "defaults": {"exclude_any": ["r2"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        r1 = conditions[0]
        assert "excluded_condition_ids_any" not in r1

    def test_defaults_exclude_any_dedupes_with_rule(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2", "r3"]},
                {"id": "r2", "label": "B"},
                {"id": "r3", "label": "C"},
                {"id": "r4", "label": "D"},
            ],
            "defaults": {"exclude_any": ["r2", "r3", "r4"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        r1 = conditions[0]
        assert r1["excluded_condition_ids_any"] == ["r2", "r3", "r4"]

    def test_defaults_exclude_any_non_list_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
            ],
            "defaults": {"exclude_any": "not_a_list"},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "authoring_defaults_invalid"
        assert issue.severity == "error"
        assert issue.field_name == "defaults.exclude_any"

    def test_defaults_exclude_any_unknown_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
            ],
            "defaults": {"exclude_any": ["unknown"]},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_condition_reference"
        assert issue.severity == "error"
        assert issue.field_name == "defaults.exclude_any"

    def test_defaults_exclude_any_skipped_id_is_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
                {"id": "r2", "label": "B", "skip": True},
            ],
            "defaults": {"exclude_any": ["r2"]},
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_condition_reference"
        assert issue.severity == "error"
        assert issue.field_name == "defaults.exclude_any"

    def test_defaults_exclude_any_valid_id_union_override_unchanged(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2", "r3"]},
                {"id": "r2", "label": "B"},
                {"id": "r3", "label": "C"},
                {"id": "r4", "label": "D"},
            ],
            "defaults": {"exclude_any": ["r2", "r4"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        r1 = conditions[0]
        assert r1["excluded_condition_ids_any"] == ["r2", "r3", "r4"]
        r2 = conditions[1]
        assert r2["excluded_condition_ids_any"] == ["r2", "r4"]

    def test_defaults_exclude_any_valid_id_applied_when_rule_has_no_exclude(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
                {"id": "r2", "label": "B"},
            ],
            "defaults": {"exclude_any": ["r2"]},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["excluded_condition_ids_any"] == ["r2"]

    def test_defaults_exclude_any_empty_list_no_effect(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A"},
            ],
            "defaults": {"exclude_any": []},
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert "excluded_condition_ids_any" not in conditions[0]


class TestRequiresAllAnyAndExcludeAnyRuntimeMapping:
    def test_requires_all_maps_to_required_condition_ids_all(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_all": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["required_condition_ids_all"] == ["r2"]

    def test_requires_any_maps_to_required_condition_ids_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["required_condition_ids_any"] == ["r2"]

    def test_exclude_any_maps_to_excluded_condition_ids_any(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "exclude_any": ["r2"]},
                {"id": "r2", "label": "B"},
            ],
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["excluded_condition_ids_any"] == ["r2"]

    def test_unknown_referenced_rule_id_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_all": ["missing"]},
            ],
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_condition_reference"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_skipped_referenced_rule_id_error(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "requires_all": ["r2"]},
                {"id": "r2", "label": "B", "skip": True},
            ],
        })
        assert result.raw_config is None
        assert len(result.issues) == 1
        issue = result.issues[0]
        assert issue.code == "unknown_condition_reference"
        assert issue.severity == "error"
        assert issue.condition_id == "r1"

    def test_normalizer_no_errors_for_valid_references(self) -> None:
        from analysis_backend.condition_evaluator import normalize_cooccurrence_conditions_result
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["foo"]}, "requires_all": ["r2"], "requires_any": ["r3"], "exclude_any": ["r4"]},
                {"id": "r2", "label": "B", "match": {"text_any": ["bar"]}},
                {"id": "r3", "label": "C", "match": {"text_any": ["baz"]}},
                {"id": "r4", "label": "D", "match": {"text_any": ["qux"]}},
            ],
        })
        assert result.raw_config is not None
        norm_result = normalize_cooccurrence_conditions_result(result.raw_config["cooccurrence_conditions"])
        assert len(norm_result.issues) == 0
        nc = norm_result.normalized_conditions[0]
        assert nc.required_condition_ids_all == ["r2"]
        assert nc.required_condition_ids_any == ["r3"]
        assert nc.excluded_condition_ids_any == ["r4"]


class TestSetExpansionDedupeOrder:
    def test_dedupe_across_multiple_sets_and_literals(self) -> None:
        result = compile_authoring_config({
            "format": "condition-authoring/v1",
            "rules": [
                {"id": "r1", "label": "A", "match": {"text_any": ["@set1", "x", "@set2", "@set1", "y"]}},
            ],
            "sets": {
                "set1": ["a", "b"],
                "set2": ["b", "c", "a"],
            },
        })
        assert result.raw_config is not None
        conditions = result.raw_config["cooccurrence_conditions"]
        assert conditions[0]["text_groups"] == [
            {"texts": ["a", "b", "x", "c", "y"], "match_logic": "or"},
        ]
