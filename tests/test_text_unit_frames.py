from __future__ import annotations

import unittest

import polars as pl

from analysis_backend.frame_schema import ANALYSIS_SENTENCES_READ_SCHEMA
from analysis_backend.frame_schema import TEXT_UNIT_PARAGRAPH_FRAME_SCHEMA
from analysis_backend.text_unit_frames import TextUnitFrames
from analysis_backend.text_unit_frames import build_text_unit_frames


class BuildTextUnitFramesTest(unittest.TestCase):
    def test_empty_sentences_yields_empty_frames(self) -> None:
        empty = pl.DataFrame(schema=ANALYSIS_SENTENCES_READ_SCHEMA)
        frames = build_text_unit_frames(empty)
        self.assertIsInstance(frames, TextUnitFrames)
        self.assertEqual(frames.sentence_frame.columns, list(ANALYSIS_SENTENCES_READ_SCHEMA.keys()))
        self.assertTrue(frames.sentence_frame.is_empty())
        self.assertEqual(frames.paragraph_frame.columns, list(TEXT_UNIT_PARAGRAPH_FRAME_SCHEMA.keys()))
        self.assertTrue(frames.paragraph_frame.is_empty())

    def test_non_table_paragraph_joins_without_separator(self) -> None:
        sentences = pl.DataFrame(
            {
                "sentence_id": [1, 2],
                "paragraph_id": [10, 10],
                "sentence_no_in_paragraph": [1, 2],
                "is_table_paragraph": [0, 0],
                "sentence_text": ["第1", "第2"],
            }
        )
        frames = build_text_unit_frames(sentences)
        row = frames.paragraph_frame.row(0, named=True)
        self.assertEqual(row["paragraph_id"], 10)
        self.assertEqual(row["is_table_paragraph"], 0)
        self.assertEqual(row["sentence_count"], 2)
        self.assertEqual(row["paragraph_text"], "第1第2")

    def test_table_paragraph_joins_with_newline(self) -> None:
        sentences = pl.DataFrame(
            {
                "sentence_id": [1, 2],
                "paragraph_id": [10, 10],
                "sentence_no_in_paragraph": [1, 2],
                "is_table_paragraph": [1, 1],
                "sentence_text": ["a", "b"],
            }
        )
        frames = build_text_unit_frames(sentences)
        row = frames.paragraph_frame.row(0, named=True)
        self.assertEqual(row["paragraph_text"], "a\nb")

    def test_sorts_by_sentence_no_before_join(self) -> None:
        sentences = pl.DataFrame(
            {
                "sentence_id": [2, 1],
                "paragraph_id": [10, 10],
                "sentence_no_in_paragraph": [2, 1],
                "is_table_paragraph": [0, 0],
                "sentence_text": ["後", "先"],
            }
        )
        frames = build_text_unit_frames(sentences)
        self.assertEqual(
            frames.paragraph_frame.get_column("paragraph_text").to_list(),
            ["先後"],
        )

    def test_missing_is_table_paragraph_defaults_to_zero_join(self) -> None:
        sentences = pl.DataFrame(
            {
                "sentence_id": [1, 2],
                "paragraph_id": [10, 10],
                "sentence_no_in_paragraph": [1, 2],
                "sentence_text": ["x", "y"],
            }
        )
        frames = build_text_unit_frames(sentences)
        self.assertEqual(
            frames.paragraph_frame.get_column("paragraph_text").to_list(),
            ["xy"],
        )

    def test_missing_sentence_text_column_filled_empty(self) -> None:
        sentences = pl.DataFrame(
            {
                "sentence_id": [1],
                "paragraph_id": [10],
                "sentence_no_in_paragraph": [1],
                "is_table_paragraph": [0],
            }
        )
        frames = build_text_unit_frames(sentences)
        self.assertEqual(
            frames.sentence_frame.get_column("sentence_text").to_list(),
            [""],
        )
        self.assertEqual(
            frames.paragraph_frame.get_column("paragraph_text").to_list(),
            [""],
        )


if __name__ == "__main__":
    unittest.main()
