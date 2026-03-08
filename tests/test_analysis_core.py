from __future__ import annotations

from pathlib import Path
import unittest

import polars as pl

import analysis_backend.analysis_core as analysis_core


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AnalysisCoreContractTests(unittest.TestCase):
    def test_load_filter_config_reads_asset_file(self) -> None:
        config = analysis_core.load_filter_config(
            PROJECT_ROOT / "asset" / "cooccurrence-conditions.json"
        )

        self.assertEqual(config.condition_match_logic, "any")
        self.assertEqual(config.max_reconstructed_paragraphs, 10000)
        self.assertEqual(config.loaded_condition_count, 7)
        self.assertEqual(
            config.cooccurrence_conditions[0]["condition_id"],
            "suppress_area",
        )

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
            ],
        )
        self.assertEqual(export_df.get_column("match_group_ids_text").to_list(), ["c, d"])


if __name__ == "__main__":
    unittest.main()
