from __future__ import annotations

from dataclasses import is_dataclass
import json
from pathlib import Path
import tempfile
import unittest

import polars as pl

import analysis_backend
import analysis_backend.analysis_core as analysis_core
import analysis_backend.condition_evaluator as condition_evaluator
import analysis_backend.condition_model as condition_model
import analysis_backend.data_access as data_access
import analysis_backend.distance_matcher as distance_matcher
import analysis_backend.export_formatter as export_formatter
import analysis_backend.filter_config as filter_config
import analysis_backend.frame_schema as frame_schema
import analysis_backend.rendering as rendering
import analysis_backend.token_position as token_position


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AnalysisCoreContractTests(unittest.TestCase):
    def test_condition_models_are_exported_via_package_api(self) -> None:
        self.assertIs(analysis_backend.ConfigIssue, condition_model.ConfigIssue)
        self.assertIs(analysis_backend.FilterConfig, condition_model.FilterConfig)
        self.assertIs(analysis_backend.DataAccessIssue, condition_model.DataAccessIssue)
        self.assertIs(analysis_backend.DataAccessResult, condition_model.DataAccessResult)
        self.assertIs(analysis_backend.LoadFilterConfigResult, condition_model.LoadFilterConfigResult)
        self.assertIs(analysis_backend.MatchingWarning, condition_model.MatchingWarning)
        self.assertIs(analysis_backend.ConditionHitResult, condition_model.ConditionHitResult)
        self.assertIs(analysis_backend.NormalizeConditionsResult, condition_model.NormalizeConditionsResult)
        self.assertIs(analysis_backend.TargetSelectionResult, condition_model.TargetSelectionResult)
        self.assertIs(analysis_backend.load_filter_config, filter_config.load_filter_config)
        self.assertIs(analysis_backend.read_analysis_tokens, data_access.read_analysis_tokens)
        self.assertIs(analysis_backend.read_analysis_sentences, data_access.read_analysis_sentences)
        self.assertIs(
            analysis_backend.read_paragraph_document_metadata,
            data_access.read_paragraph_document_metadata,
        )
        self.assertIs(analysis_backend.build_tokens_with_position_df, token_position.build_tokens_with_position_df)
        self.assertIs(analysis_backend.build_token_annotations_df, rendering.build_token_annotations_df)
        self.assertIs(analysis_backend.build_rendered_paragraphs_df, rendering.build_rendered_paragraphs_df)
        self.assertIs(
            analysis_backend.enrich_reconstructed_paragraphs_df,
            export_formatter.enrich_reconstructed_paragraphs_df,
        )
        self.assertIs(
            analysis_backend.enrich_reconstructed_paragraphs_result,
            export_formatter.enrich_reconstructed_paragraphs_result,
        )
        self.assertIs(
            analysis_backend.build_reconstructed_paragraphs_export_df,
            export_formatter.build_reconstructed_paragraphs_export_df,
        )
        self.assertIs(analysis_backend.build_condition_hit_result, analysis_core.build_condition_hit_result)
        self.assertIn("build_condition_hit_tokens_df", dir(analysis_backend))
        self.assertIn("build_condition_hit_result", dir(analysis_backend))
        self.assertIn("select_target_ids_by_cooccurrence_conditions", dir(analysis_backend))

    def test_shared_frame_schemas_are_reused_across_modules(self) -> None:
        self.assertIs(token_position.POSITIONED_TOKEN_SCHEMA, frame_schema.POSITIONED_TOKEN_SCHEMA)
        self.assertIs(rendering.POSITIONED_TOKEN_SCHEMA, frame_schema.POSITIONED_TOKEN_SCHEMA)
        self.assertIs(analysis_core.POSITIONED_TOKEN_SCHEMA, frame_schema.POSITIONED_TOKEN_SCHEMA)
        self.assertIs(analysis_core.CONDITION_HIT_SCHEMA, frame_schema.CONDITION_HIT_SCHEMA)
        self.assertIs(data_access.PARAGRAPH_METADATA_SCHEMA, frame_schema.PARAGRAPH_METADATA_SCHEMA)
        self.assertTrue(frame_schema.empty_df(frame_schema.POSITIONED_TOKEN_SCHEMA).is_empty())

    def test_analysis_core_keeps_legacy_facade_function_symbols(self) -> None:
        self.assertEqual(analysis_core.load_filter_config.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.read_analysis_tokens.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.read_analysis_sentences.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.read_paragraph_document_metadata.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.build_tokens_with_position_df.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.render_tagged_token.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.build_token_annotations_df.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.build_rendered_paragraphs_df.__module__, "analysis_backend.analysis_core")
        self.assertEqual(analysis_core.enrich_reconstructed_paragraphs_df.__module__, "analysis_backend.analysis_core")
        self.assertEqual(
            analysis_core.enrich_reconstructed_paragraphs_result.__module__,
            "analysis_backend.analysis_core",
        )
        self.assertEqual(
            analysis_core.build_reconstructed_paragraphs_export_df.__module__,
            "analysis_backend.analysis_core",
        )

        self.assertTrue(is_dataclass(condition_model.FilterConfig))
        self.assertTrue(is_dataclass(condition_model.ConfigIssue))
        self.assertTrue(is_dataclass(condition_model.DataAccessIssue))
        self.assertTrue(is_dataclass(condition_model.DataAccessResult))
        self.assertTrue(is_dataclass(condition_model.LoadFilterConfigResult))
        self.assertTrue(is_dataclass(condition_model.NormalizedCondition))
        self.assertTrue(is_dataclass(condition_model.NormalizeConditionsResult))
        self.assertTrue(is_dataclass(condition_model.MatchingWarning))
        self.assertTrue(is_dataclass(condition_model.ConditionHitResult))
        self.assertTrue(is_dataclass(condition_model.TargetSelectionResult))

    def test_load_filter_config_reads_asset_file(self) -> None:
        config = analysis_core.load_filter_config(
            PROJECT_ROOT / "asset" / "cooccurrence-conditions.json"
        )

        self.assertEqual(config.condition_match_logic, "any")
        self.assertEqual(config.max_reconstructed_paragraphs, 10000)
        self.assertEqual(config.loaded_condition_count, 7)
        self.assertEqual(config.distance_matching_mode, "auto-approx")
        self.assertEqual(config.distance_match_combination_cap, 10000)
        self.assertEqual(config.distance_match_strict_safety_limit, 1000000)
        self.assertEqual(
            config.cooccurrence_conditions[0]["condition_id"],
            "suppress_area",
        )

    def test_condition_hit_result_defaults_to_empty_warning_messages(self) -> None:
        result = condition_model.ConditionHitResult(
            condition_hit_tokens_df=pl.DataFrame(schema={"paragraph_id": pl.Int64}),
            requested_mode="auto-approx",
            used_mode="strict",
        )

        self.assertEqual(result.warning_messages, [])

    def test_normalize_cooccurrence_conditions_returns_normalized_condition_models(self) -> None:
        normalized_conditions = condition_evaluator.normalize_cooccurrence_conditions(
            [
                {
                    "condition_id": "duplicate",
                    "categories": ["カテゴリA", "カテゴリA"],
                    "forms": ["抑制", "区域", "区域"],
                    "form_match_logic": "all",
                    "search_scope": "sentence",
                    "max_token_distance": 5,
                },
                {
                    "condition_id": "duplicate",
                    "categories": None,
                    "forms": ["制限", "区域"],
                },
            ]
        )

        self.assertEqual(len(normalized_conditions), 2)
        self.assertIsInstance(normalized_conditions[0], condition_model.NormalizedCondition)
        self.assertEqual(normalized_conditions[0].condition_id, "duplicate")
        self.assertEqual(normalized_conditions[0].categories, ["カテゴリA"])
        self.assertEqual(normalized_conditions[0].forms, ["抑制", "区域"])
        self.assertEqual(normalized_conditions[1].condition_id, "duplicate_2")
        self.assertEqual(normalized_conditions[1].categories, ["未分類"])

    def test_normalize_cooccurrence_conditions_result_reports_issues_without_breaking_legacy_output(self) -> None:
        raw_conditions = [
            "invalid",
            {
                "forms": "not-list",
            },
            {
                "forms": [" ", ""],
            },
            {
                "categories": None,
                "forms": ["抑制", "区域", "区域"],
                "form_match_logic": "unexpected",
                "search_scope": "unexpected",
                "max_token_distance": "bad",
            },
            {
                "condition_id": "condition_4",
                "categories": ["概念:抑制区域"],
                "forms": ["制限", "区域"],
                "form_match_logic": "any",
                "max_token_distance": 5,
            },
        ]
        normalization_result = condition_evaluator.normalize_cooccurrence_conditions_result(raw_conditions)

        self.assertEqual(
            [condition.condition_id for condition in normalization_result.normalized_conditions],
            ["condition_4", "condition_4_2"],
        )
        self.assertEqual(
            [issue.code for issue in normalization_result.issues],
            [
                "condition_not_object",
                "forms_not_list",
                "forms_empty",
                "condition_id_generated",
                "form_match_logic_defaulted",
                "search_scope_defaulted",
                "max_token_distance_ignored",
                "categories_defaulted",
                "condition_id_deduplicated",
                "max_token_distance_disabled",
            ],
        )
        self.assertEqual(
            condition_evaluator.normalize_cooccurrence_conditions(raw_conditions),
            normalization_result.normalized_conditions,
        )

    def test_load_filter_config_reads_explicit_matching_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            filter_config_path = Path(temp_dir) / "conditions.json"
            filter_config_path.write_text(
                json.dumps(
                    {
                        "condition_match_logic": "all",
                        "max_reconstructed_paragraphs": 25,
                        "distance_matching_mode": "strict",
                        "distance_match_combination_cap": 12345,
                        "distance_match_strict_safety_limit": 54321,
                        "cooccurrence_conditions": [
                            {
                                "condition_id": "suppress_area",
                                "forms": ["抑制", "区域"],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            config = analysis_core.load_filter_config(filter_config_path)

        self.assertEqual(config.condition_match_logic, "all")
        self.assertEqual(config.max_reconstructed_paragraphs, 25)
        self.assertEqual(config.distance_matching_mode, "strict")
        self.assertEqual(config.distance_match_combination_cap, 12345)
        self.assertEqual(config.distance_match_strict_safety_limit, 54321)

    def test_load_filter_config_falls_back_to_default_matching_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            filter_config_path = Path(temp_dir) / "conditions.json"
            filter_config_path.write_text(
                json.dumps(
                    {
                        "distance_matching_mode": "unexpected",
                        "distance_match_combination_cap": 0,
                        "distance_match_strict_safety_limit": -1,
                        "cooccurrence_conditions": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            config = analysis_core.load_filter_config(filter_config_path)

        self.assertEqual(config.distance_matching_mode, "auto-approx")
        self.assertEqual(config.distance_match_combination_cap, 10000)
        self.assertEqual(config.distance_match_strict_safety_limit, 1000000)

    def test_load_filter_config_result_reports_defaulting_issues(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            filter_config_path = Path(temp_dir) / "conditions.json"
            filter_config_path.write_text(
                json.dumps(
                    {
                        "condition_match_logic": "unexpected",
                        "max_reconstructed_paragraphs": 0,
                        "distance_matching_mode": "unexpected",
                        "distance_match_combination_cap": 0,
                        "distance_match_strict_safety_limit": -1,
                        "cooccurrence_conditions": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            result = filter_config.load_filter_config_result(filter_config_path)

        self.assertIsInstance(result, condition_model.LoadFilterConfigResult)
        self.assertIsNotNone(result.filter_config)
        self.assertEqual(
            [issue.code for issue in result.issues],
            [
                "condition_match_logic_defaulted",
                "max_reconstructed_paragraphs_defaulted",
                "distance_matching_mode_defaulted",
                "distance_match_combination_cap_defaulted",
                "distance_match_strict_safety_limit_defaulted",
            ],
        )

    def test_read_analysis_tokens_result_reports_structured_issue_when_table_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "empty.db"
            sqlite_conn = __import__("sqlite3").connect(db_path)
            sqlite_conn.close()

            result = data_access.read_analysis_tokens_result(db_path=db_path)

        self.assertIsInstance(result, condition_model.DataAccessResult)
        self.assertIsNone(result.data_frame)
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].code, "sqlite_read_failed")
        self.assertEqual(result.issues[0].query_name, "analysis_tokens")

    def test_enrich_reconstructed_paragraphs_result_reports_metadata_issue_when_tables_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "empty.db"
            sqlite_conn = __import__("sqlite3").connect(db_path)
            sqlite_conn.close()
            base_df = pl.DataFrame(
                {
                    "paragraph_id": [1],
                    "sentence_count": [1],
                    "paragraph_text": ["本文"],
                    "paragraph_text_tagged": ["本文"],
                    "paragraph_text_highlight_html": ["本文"],
                    "matched_condition_ids": [["a"]],
                    "matched_condition_ids_text": ["a"],
                    "matched_categories": [["b"]],
                    "matched_categories_text": ["b"],
                    "match_group_ids": [["g1"]],
                    "match_group_count": [1],
                    "annotated_token_count": [1],
                }
            )

            result = export_formatter.enrich_reconstructed_paragraphs_result(
                db_path=db_path,
                reconstructed_paragraphs_base_df=base_df,
            )

        self.assertIsNone(result.data_frame)
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].code, "sqlite_metadata_read_failed")
        self.assertEqual(result.issues[0].query_name, "paragraph_document_metadata")

    def test_render_tagged_token_escapes_attributes(self) -> None:
        tagged_fragment, html_fragment, condition_ids, categories, match_group_ids, increment = (
            analysis_core.render_tagged_token(
                surface='区"域\\A',
                annotation={
                    "condition_ids": ['cond"1'],
                    "categories": ["cat\\1"],
                    "category_texts": ["カテゴリ"],
                    "match_group_ids": ['group\\"1'],
                },
            )
        )

        self.assertIn('condition_ids="cond\\\"1"', tagged_fragment)
        self.assertIn('categories="cat\\\\1"', tagged_fragment)
        self.assertIn('groups="group\\\\\\\"1"', tagged_fragment)
        self.assertIn('>区&quot;域\\A</mark>', html_fragment)
        self.assertEqual(condition_ids, ['cond"1'])
        self.assertEqual(categories, ["cat\\1"])
        self.assertEqual(match_group_ids, ['group\\"1'])
        self.assertEqual(increment, 1)

    def test_build_tokens_with_position_df_respects_sentence_order(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 1],
                "sentence_id": [11, 11, 12],
                "token_no": [0, 1, 0],
                "normalized_form": ["抑制", "区域", "指定"],
                "surface": ["抑制", "区域", "指定"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [12, 11],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [2, 1],
            }
        )

        positioned_df = analysis_core.build_tokens_with_position_df(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
        ).sort(["sentence_id", "token_no"])

        sentence_11_positions = (
            positioned_df
            .filter(pl.col("sentence_id") == 11)
            .get_column("paragraph_token_position")
            .to_list()
        )
        sentence_12_positions = (
            positioned_df
            .filter(pl.col("sentence_id") == 12)
            .get_column("paragraph_token_position")
            .to_list()
        )

        self.assertEqual(sentence_11_positions, [0, 1])
        self.assertEqual(sentence_12_positions, [2])

    def test_build_tokens_with_position_df_preserves_one_based_token_positions(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 1],
                "sentence_id": [11, 11, 12],
                "token_no": [1, 2, 1],
                "normalized_form": ["抑制", "区域", "指定"],
                "surface": ["抑制", "区域", "指定"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [1, 2],
            }
        )

        positioned_df = analysis_core.build_tokens_with_position_df(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
        ).sort(["sentence_id", "token_no"])

        self.assertEqual(
            positioned_df.select(
                [
                    "sentence_id",
                    "token_no",
                    "sentence_token_position",
                    "paragraph_token_position",
                ]
            ).to_dict(as_series=False),
            {
                "sentence_id": [11, 11, 12],
                "token_no": [1, 2, 1],
                "sentence_token_position": [1, 2, 1],
                "paragraph_token_position": [1, 2, 3],
            },
        )

    def test_build_tokens_with_position_df_preserves_table_flag(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1],
                "sentence_id": [11, 12],
                "token_no": [0, 0],
                "normalized_form": ["表", "行"],
                "surface": ["表", "行"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [1, 2],
                "is_table_paragraph": [1, 1],
            }
        )

        positioned_df = analysis_core.build_tokens_with_position_df(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
        ).sort(["sentence_id", "token_no"])

        self.assertEqual(positioned_df.get_column("is_table_paragraph").to_list(), [1, 1])

    def test_reconstruct_paragraphs_by_ids_inserts_newlines_for_table_paragraph(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1],
                "sentence_id": [11, 12],
                "token_no": [0, 0],
                "surface": ["表", "行"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [1, 2],
                "is_table_paragraph": [1, 1],
            }
        )

        reconstructed_df = analysis_core.reconstruct_paragraphs_by_ids(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
            paragraph_ids=[1],
        )

        self.assertEqual(reconstructed_df.get_column("paragraph_text").to_list(), ["表\n行"])

    def test_reconstruct_paragraphs_by_ids_keeps_empty_join_for_non_table_paragraph(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1],
                "sentence_id": [11, 12],
                "token_no": [0, 0],
                "surface": ["表", "行"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [1, 2],
            }
        )

        reconstructed_df = analysis_core.reconstruct_paragraphs_by_ids(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
            paragraph_ids=[1],
        )

        self.assertEqual(reconstructed_df.get_column("paragraph_text").to_list(), ["表行"])

    def test_build_rendered_paragraphs_df_inserts_newlines_for_table_paragraph(self) -> None:
        tokens_with_position_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1],
                "sentence_id": [11, 12],
                "sentence_no_in_paragraph": [1, 2],
                "is_table_paragraph": [1, 1],
                "token_no": [0, 0],
                "sentence_token_position": [0, 0],
                "paragraph_token_position": [0, 1],
                "normalized_form": ["表", "行"],
                "surface": ["表", "行"],
            }
        )

        rendered_df = analysis_core.build_rendered_paragraphs_df(
            tokens_with_position_df=tokens_with_position_df,
            token_annotations_df=pl.DataFrame(),
        )

        self.assertEqual(rendered_df.get_column("paragraph_text").to_list(), ["表\n行"])
        self.assertEqual(rendered_df.get_column("paragraph_text_tagged").to_list(), ["表\n行"])
        self.assertEqual(rendered_df.get_column("paragraph_text_highlight_html").to_list(), ["表\n行"])

    def test_build_rendered_paragraphs_df_keeps_empty_join_for_non_table_paragraph(self) -> None:
        tokens_with_position_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1],
                "sentence_id": [11, 12],
                "sentence_no_in_paragraph": [1, 2],
                "token_no": [0, 0],
                "sentence_token_position": [0, 0],
                "paragraph_token_position": [0, 1],
                "normalized_form": ["表", "行"],
                "surface": ["表", "行"],
            }
        )

        rendered_df = analysis_core.build_rendered_paragraphs_df(
            tokens_with_position_df=tokens_with_position_df,
            token_annotations_df=pl.DataFrame(),
        )

        self.assertEqual(rendered_df.get_column("paragraph_text").to_list(), ["表行"])

    def test_build_condition_hit_tokens_df_handles_large_distance_combinations(self) -> None:
        rows: list[dict[str, object]] = []
        token_no = 0
        for idx in range(101):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "抑制",
                    "surface": f"抑制{idx}",
                }
            )
            token_no += 1

        for idx in range(100):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "区域",
                    "surface": f"区域{idx}",
                }
            )
            token_no += 1

        tokens_with_position_df = pl.DataFrame(rows)
        condition_hit_tokens_df = analysis_core.build_condition_hit_tokens_df(
            tokens_with_position_df=tokens_with_position_df,
            cooccurrence_conditions=[
                {
                    "condition_id": "distance_fallback",
                    "categories": ["fallback"],
                    "forms": ["抑制", "区域"],
                    "form_match_logic": "all",
                    "max_token_distance": 200,
                    "search_scope": "sentence",
                }
            ],
        )

        self.assertGreater(condition_hit_tokens_df.height, 0)
        self.assertIn("condition_id", condition_hit_tokens_df.columns)
        self.assertIn("match_group_id", condition_hit_tokens_df.columns)

    def test_build_condition_hit_result_reports_auto_approx_warning(self) -> None:
        rows: list[dict[str, object]] = []
        token_no = 0
        for idx in range(101):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "抑制",
                    "surface": f"抑制{idx}",
                }
            )
            token_no += 1

        for idx in range(100):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "区域",
                    "surface": f"区域{idx}",
                }
            )
            token_no += 1

        hit_result = distance_matcher.build_condition_hit_result(
            tokens_with_position_df=pl.DataFrame(rows),
            cooccurrence_conditions=[
                {
                    "condition_id": "distance_fallback",
                    "categories": ["fallback"],
                    "forms": ["抑制", "区域"],
                    "form_match_logic": "all",
                    "max_token_distance": 200,
                    "effective_max_token_distance": 200,
                    "search_scope": "sentence",
                }
            ],
            distance_matching_mode="auto-approx",
            distance_match_combination_cap=10000,
            distance_match_strict_safety_limit=1000000,
        )

        self.assertEqual(hit_result.requested_mode, "auto-approx")
        self.assertEqual(hit_result.used_mode, "approx")
        self.assertEqual(len(hit_result.warning_messages), 1)
        self.assertEqual(hit_result.warning_messages[0].code, "distance_match_fallback")
        self.assertEqual(hit_result.warning_messages[0].combination_count, 10100)
        self.assertEqual(hit_result.warning_messages[0].combination_cap, 10000)

    def test_build_condition_hit_result_strict_and_approx_can_produce_different_group_counts(self) -> None:
        tokens_with_position_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 1, 1],
                "sentence_id": [10, 10, 10, 10],
                "sentence_no_in_paragraph": [1, 1, 1, 1],
                "token_no": [0, 1, 2, 3],
                "sentence_token_position": [0, 1, 2, 3],
                "paragraph_token_position": [0, 1, 2, 3],
                "normalized_form": ["A", "B", "A", "B"],
                "surface": ["A1", "B1", "A2", "B2"],
            }
        )
        cooccurrence_conditions = [
            {
                "condition_id": "pair",
                "categories": ["pair"],
                "forms": ["A", "B"],
                "form_match_logic": "all",
                "effective_max_token_distance": 3,
                "search_scope": "sentence",
            }
        ]

        strict_result = distance_matcher.build_condition_hit_result(
            tokens_with_position_df=tokens_with_position_df,
            cooccurrence_conditions=cooccurrence_conditions,
            distance_matching_mode="strict",
            distance_match_combination_cap=10000,
            distance_match_strict_safety_limit=1000000,
        )
        approx_result = distance_matcher.build_condition_hit_result(
            tokens_with_position_df=tokens_with_position_df,
            cooccurrence_conditions=cooccurrence_conditions,
            distance_matching_mode="approx",
            distance_match_combination_cap=10000,
            distance_match_strict_safety_limit=1000000,
        )

        strict_group_count = strict_result.condition_hit_tokens_df.get_column("match_group_id").n_unique()
        approx_group_count = approx_result.condition_hit_tokens_df.get_column("match_group_id").n_unique()

        self.assertEqual(strict_result.used_mode, "strict")
        self.assertEqual(approx_result.used_mode, "approx")
        self.assertGreater(strict_group_count, approx_group_count)
        self.assertEqual(strict_group_count, 4)
        self.assertEqual(approx_group_count, 2)

    def test_build_condition_hit_tokens_df_strict_mode_raises_on_safety_limit(self) -> None:
        rows: list[dict[str, object]] = []
        token_no = 0
        for idx in range(11):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "抑制",
                    "surface": f"抑制{idx}",
                }
            )
            token_no += 1

        for idx in range(10):
            rows.append(
                {
                    "paragraph_id": 1,
                    "sentence_id": 10,
                    "sentence_no_in_paragraph": 1,
                    "token_no": token_no,
                    "sentence_token_position": token_no,
                    "paragraph_token_position": token_no,
                    "normalized_form": "区域",
                    "surface": f"区域{idx}",
                }
            )
            token_no += 1

        with self.assertRaises(distance_matcher.DistanceMatchLimitExceededError) as context:
            analysis_core.build_condition_hit_tokens_df(
                tokens_with_position_df=pl.DataFrame(rows),
                cooccurrence_conditions=[
                    {
                        "condition_id": "distance_strict",
                        "categories": ["strict"],
                        "forms": ["抑制", "区域"],
                        "form_match_logic": "all",
                        "max_token_distance": 200,
                        "search_scope": "sentence",
                    }
                ],
                distance_matching_mode="strict",
                distance_match_combination_cap=10000,
                distance_match_strict_safety_limit=100,
            )

        self.assertIn("distance_match_strict_limit_exceeded", str(context.exception))

    def test_select_target_ids_by_cooccurrence_conditions_keeps_tuple_contract(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 1],
                "sentence_id": [11, 11, 11],
                "token_no": [0, 1, 2],
                "normalized_form": ["抑制", "区域", "指定"],
                "surface": ["抑制", "区域", "指定"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11],
                "paragraph_id": [1],
                "sentence_no_in_paragraph": [1],
            }
        )

        result = analysis_core.select_target_ids_by_cooccurrence_conditions(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
            cooccurrence_conditions=[
                {
                    "condition_id": "suppress_area",
                    "categories": ["概念:抑制区域"],
                    "forms": ["抑制", "区域"],
                    "form_match_logic": "all",
                    "max_token_distance": 5,
                    "search_scope": "sentence",
                }
            ],
        )

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 5)
        candidate_tokens_df, condition_eval_df, paragraph_match_summary_df, target_paragraph_ids, target_sentence_ids = result
        self.assertIsInstance(candidate_tokens_df, pl.DataFrame)
        self.assertIsInstance(condition_eval_df, pl.DataFrame)
        self.assertIsInstance(paragraph_match_summary_df, pl.DataFrame)
        self.assertEqual(target_paragraph_ids, [1])
        self.assertEqual(target_sentence_ids, [11])

    def test_condition_eval_matched_form_count_uses_best_unit_coverage_within_paragraph(self) -> None:
        tokens_df = pl.DataFrame(
            {
                "paragraph_id": [1, 1, 1, 1],
                "sentence_id": [11, 11, 12, 12],
                "token_no": [1, 2, 1, 2],
                "normalized_form": ["抑制", "区域", "抑制", "その他"],
                "surface": ["抑制", "区域", "抑制", "その他"],
            }
        )
        sentences_df = pl.DataFrame(
            {
                "sentence_id": [11, 12],
                "paragraph_id": [1, 1],
                "sentence_no_in_paragraph": [1, 2],
            }
        )

        result = condition_evaluator.select_target_ids_by_conditions_result(
            tokens_df=tokens_df,
            sentences_df=sentences_df,
            normalized_conditions=[
                condition_model.NormalizedCondition(
                    condition_id="suppress_area",
                    categories=["概念:抑制区域"],
                    category_text="概念:抑制区域",
                    forms=["抑制", "区域"],
                    search_scope="sentence",
                    form_match_logic="all",
                    requested_max_token_distance=None,
                    effective_max_token_distance=None,
                )
            ],
        )

        condition_eval_row = result.condition_eval_df.row(0, named=True)
        self.assertEqual(condition_eval_row["matched_form_count"], 2)
        self.assertEqual(condition_eval_row["evaluated_unit_count"], 2)
        self.assertEqual(condition_eval_row["matched_unit_count"], 1)
        self.assertTrue(condition_eval_row["is_match"])

    def test_build_reconstructed_paragraphs_export_df_keeps_required_columns(self) -> None:
        reconstructed_paragraphs_df = pl.DataFrame(
            {
                "paragraph_id": [1],
                "document_id": [10],
                "municipality_name": ["テスト市"],
                "ordinance_or_rule": ["条例"],
                "doc_type": ["条例"],
                "sentence_count": [1],
                "paragraph_text": ["抑制区域を指定する。"],
                "paragraph_text_tagged": ['[[HIT condition_ids="a" categories="b" groups="c"]]抑制区域[[/HIT]]を指定する。'],
                "paragraph_text_highlight_html": ["<mark>抑制区域</mark>を指定する。"],
                "matched_condition_ids": [["a"]],
                "matched_condition_ids_text": ["a"],
                "matched_categories": [["b"]],
                "matched_categories_text": ["b"],
                "match_group_ids": [["c", "d"]],
                "match_group_count": [2],
                "annotated_token_count": [1],
            }
        )

        export_df = analysis_core.build_reconstructed_paragraphs_export_df(
            reconstructed_paragraphs_df=reconstructed_paragraphs_df
        )

        self.assertEqual(
            export_df.columns,
            [
                "paragraph_id",
                "document_id",
                "municipality_name",
                "ordinance_or_rule",
                "doc_type",
                "sentence_count",
                "paragraph_text",
                "paragraph_text_tagged",
                "paragraph_text_highlight_html",
                "matched_condition_ids_text",
                "matched_categories_text",
                "match_group_ids_text",
                "match_group_count",
                "annotated_token_count",
                "manual_annotation_count",
                "manual_annotation_pairs_text",
                "manual_annotation_namespaces_text",
            ],
        )
        self.assertEqual(export_df.get_column("match_group_ids_text").to_list(), ["c, d"])
        self.assertEqual(export_df.get_column("manual_annotation_count").to_list(), [0])

    def test_build_reconstructed_paragraphs_export_df_keeps_existing_scalar_dtypes(self) -> None:
        reconstructed_paragraphs_df = pl.DataFrame(
            schema={
                "paragraph_id": pl.Int64,
                "document_id": pl.Int64,
                "municipality_name": pl.String,
                "ordinance_or_rule": pl.String,
                "doc_type": pl.String,
                "sentence_count": pl.UInt32,
                "paragraph_text": pl.String,
                "paragraph_text_tagged": pl.String,
                "paragraph_text_highlight_html": pl.String,
                "matched_condition_ids": pl.List(pl.String),
                "matched_condition_ids_text": pl.String,
                "matched_categories": pl.List(pl.String),
                "matched_categories_text": pl.String,
                "match_group_ids": pl.List(pl.String),
                "match_group_count": pl.UInt32,
                "annotated_token_count": pl.UInt32,
            },
            data=[
                (
                    1,
                    10,
                    "テスト市",
                    "条例",
                    "条例",
                    1,
                    "抑制区域を指定する。",
                    '[[HIT condition_ids="a" categories="b" groups="c"]]抑制区域[[/HIT]]を指定する。',
                    "<mark>抑制区域</mark>を指定する。",
                    ["a"],
                    "a",
                    ["b"],
                    "b",
                    ["c", "d"],
                    2,
                    1,
                )
            ],
            orient="row",
        )

        export_df = analysis_core.build_reconstructed_paragraphs_export_df(
            reconstructed_paragraphs_df=reconstructed_paragraphs_df
        )

        self.assertEqual(export_df.schema["paragraph_id"], pl.Int64)
        self.assertEqual(export_df.schema["document_id"], pl.Int64)
        self.assertEqual(export_df.schema["sentence_count"], pl.UInt32)
        self.assertEqual(export_df.schema["match_group_count"], pl.UInt32)
        self.assertEqual(export_df.schema["annotated_token_count"], pl.UInt32)
        self.assertEqual(export_df.schema["match_group_ids_text"], pl.String)
        self.assertEqual(export_df.schema["manual_annotation_count"], pl.UInt32)
        self.assertEqual(export_df.schema["manual_annotation_pairs_text"], pl.String)
        self.assertEqual(export_df.schema["manual_annotation_namespaces_text"], pl.String)


if __name__ == "__main__":
    unittest.main()
