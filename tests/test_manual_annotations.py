from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from analysis_backend.manual_annotations import load_manual_annotations_result


def write_manual_annotations_csv(path: Path, rows: list[str]) -> None:
    path.write_text(
        "\n".join(
            [
                "target_type,target_id,label_namespace,label_key,label_value,tagged_by,tagged_at,confidence,note",
                *rows,
            ]
        ),
        encoding="utf-8",
    )


class ManualAnnotationsLoaderTest(unittest.TestCase):
    def test_missing_file_returns_empty_data_frames_without_issues(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "missing.csv"

            result = load_manual_annotations_result(csv_path)

            self.assertEqual(result.issues, [])
            self.assertTrue(result.raw_annotations_df.is_empty())
            self.assertTrue(result.paragraph_annotations_df.is_empty())
            self.assertTrue(result.normalized_paragraph_annotations_df.is_empty())
            self.assertEqual(
                list(result.paragraph_annotations_df.columns),
                [
                    "paragraph_id",
                    "manual_annotation_count",
                    "manual_annotation_pairs",
                    "manual_annotation_pairs_text",
                    "manual_annotation_namespaces",
                    "manual_annotation_namespaces_text",
                ],
            )

    def test_loader_deduplicates_exact_duplicates_and_keeps_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "annotations.csv"
            write_manual_annotations_csv(
                csv_path,
                [
                    "paragraph,3036,zoning,zone_strength,suppression,alice,2026-03-17,high,first",
                    "paragraph,3036,zoning,zone_strength,suppression,alice,2026-03-17,high,first",
                    "paragraph,3036,zoning,zone_strength,prohibition,alice,2026-03-17,high,conflict",
                    "document,doc-1,meta,reviewed,true,alice,2026-03-17,high,ignored",
                ],
            )

            result = load_manual_annotations_result(csv_path)

            self.assertEqual(
                [issue.code for issue in result.issues],
                ["annotation_duplicate_deduplicated", "annotation_value_conflict"],
            )
            self.assertEqual(
                result.paragraph_annotations_df.to_dicts(),
                [
                    {
                        "paragraph_id": 3036,
                        "manual_annotation_count": 2,
                        "manual_annotation_pairs": [
                            "zoning:zone_strength=prohibition",
                            "zoning:zone_strength=suppression",
                        ],
                        "manual_annotation_pairs_text": "zoning:zone_strength=prohibition\nzoning:zone_strength=suppression",
                        "manual_annotation_namespaces": ["zoning"],
                        "manual_annotation_namespaces_text": "zoning",
                    }
                ],
            )
            self.assertEqual(
                result.normalized_paragraph_annotations_df.to_dicts(),
                [
                    {
                        "paragraph_id": 3036,
                        "label_namespace": "zoning",
                        "label_key": "zone_strength",
                        "label_value": "suppression",
                        "tagged_by": "alice",
                        "tagged_at": "2026-03-17",
                        "confidence": "high",
                        "note": "first",
                    },
                    {
                        "paragraph_id": 3036,
                        "label_namespace": "zoning",
                        "label_key": "zone_strength",
                        "label_value": "suppression",
                        "tagged_by": "alice",
                        "tagged_at": "2026-03-17",
                        "confidence": "high",
                        "note": "first",
                    },
                    {
                        "paragraph_id": 3036,
                        "label_namespace": "zoning",
                        "label_key": "zone_strength",
                        "label_value": "prohibition",
                        "tagged_by": "alice",
                        "tagged_at": "2026-03-17",
                        "confidence": "high",
                        "note": "conflict",
                    },
                ],
            )

    def test_invalid_paragraph_target_id_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "annotations.csv"
            write_manual_annotations_csv(
                csv_path,
                [
                    "paragraph,not-a-number,zoning,zone_strength,suppression,alice,2026-03-17,high,invalid",
                ],
            )

            result = load_manual_annotations_result(csv_path)

            self.assertIsNone(result.paragraph_annotations_df)
            self.assertIsNone(result.normalized_paragraph_annotations_df)
            self.assertEqual(len(result.issues), 1)
            self.assertEqual(result.issues[0].code, "annotation_target_id_invalid")

    def test_unknown_target_type_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "annotations.csv"
            write_manual_annotations_csv(
                csv_path,
                [
                    "mystery,3036,zoning,zone_strength,suppression,alice,2026-03-17,high,invalid",
                ],
            )

            result = load_manual_annotations_result(csv_path)

            self.assertIsNone(result.paragraph_annotations_df)
            self.assertIsNone(result.normalized_paragraph_annotations_df)
            self.assertEqual(len(result.issues), 1)
            self.assertEqual(result.issues[0].code, "annotation_target_type_unknown")


if __name__ == "__main__":
    unittest.main()
