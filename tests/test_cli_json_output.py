from __future__ import annotations

import unittest

import polars as pl

from analysis_backend.cli import _build_json_response_payload
from analysis_backend.cli import _validate_selected_count
from analysis_backend.export_formatter import build_gui_records


class BuildGuiRecordsTest(unittest.TestCase):
    def test_build_gui_records_normalizes_nulls_and_numbers_to_strings(self) -> None:
        reconstructed_paragraphs_df = pl.DataFrame(
            {
                "paragraph_id": [101],
                "document_id": [202],
                "municipality_name": ["札幌市"],
                "ordinance_or_rule": ["条例"],
                "doc_type": [None],
                "sentence_count": [3],
                "paragraph_text": ["本文"],
                "paragraph_text_tagged": ["<hit>本文</hit>"],
                "paragraph_text_highlight_html": ["<mark>本文</mark>"],
                "matched_condition_ids": [["cond_1"]],
                "matched_condition_ids_text": [None],
                "matched_categories": [["抑制区域"]],
                "matched_categories_text": [None],
                "matched_form_group_ids_text": [None],
                "matched_form_group_logics_text": [None],
                "form_group_explanations_text": [None],
                "text_groups_explanations_text": [None],
                "mixed_scope_warning_text": [None],
                "match_group_ids": [None],
                "match_group_count": [4],
                "annotated_token_count": [None],
            }
        )

        records = build_gui_records(reconstructed_paragraphs_df)

        self.assertEqual(
            records,
            [
                {
                    "paragraph_id": "101",
                    "document_id": "202",
                    "municipality_name": "札幌市",
                    "ordinance_or_rule": "条例",
                    "doc_type": "",
                    "sentence_count": "3",
                    "paragraph_text": "本文",
                    "paragraph_text_tagged": "<hit>本文</hit>",
                    "matched_condition_ids_text": "",
                    "matched_categories_text": "",
                    "matched_form_group_ids_text": "",
                    "matched_form_group_logics_text": "",
                    "form_group_explanations_text": "",
                    "text_groups_explanations_text": "",
                    "mixed_scope_warning_text": "",
                    "match_group_ids_text": "",
                    "match_group_count": "4",
                    "annotated_token_count": "",
                    "manual_annotation_count": "0",
                    "manual_annotation_pairs_text": "",
                    "manual_annotation_namespaces_text": "",
                }
            ],
        )


class CliJsonHelperTest(unittest.TestCase):
    def test_validate_selected_count_returns_error_on_mismatch(self) -> None:
        error_message = _validate_selected_count(
            expected_selected_count=2,
            records=[{"paragraph_id": "1"}],
        )

        self.assertEqual(
            error_message,
            "selectedParagraphCount mismatch: meta=2, records=1",
        )

    def test_build_json_response_payload_includes_empty_records_by_default(self) -> None:
        payload = _build_json_response_payload(meta={"status": "failed"})

        self.assertEqual(
            payload,
            {
                "meta": {"status": "failed"},
                "records": [],
            },
        )


if __name__ == "__main__":
    unittest.main()
