from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import polars as pl


AnnotationIssueSeverity = Literal["warning", "error"]

REQUIRED_ANNOTATION_COLUMNS = [
    "target_type",
    "target_id",
    "label_namespace",
    "label_key",
    "label_value",
]
OPTIONAL_ANNOTATION_COLUMNS = [
    "tagged_by",
    "tagged_at",
    "confidence",
    "note",
]
RAW_ANNOTATION_SCHEMA = {
    "target_type": pl.String,
    "target_id": pl.String,
    "label_namespace": pl.String,
    "label_key": pl.String,
    "label_value": pl.String,
    "tagged_by": pl.String,
    "tagged_at": pl.String,
    "confidence": pl.String,
    "note": pl.String,
}
PARAGRAPH_ANNOTATION_SUMMARY_SCHEMA = {
    "paragraph_id": pl.Int64,
    "manual_annotation_count": pl.UInt32,
    "manual_annotation_pairs": pl.List(pl.String),
    "manual_annotation_pairs_text": pl.String,
    "manual_annotation_namespaces": pl.List(pl.String),
    "manual_annotation_namespaces_text": pl.String,
}
KNOWN_TARGET_TYPES = {"paragraph", "document", "sentence"}


@dataclass(frozen=True)
class AnnotationIssue:
    code: str
    severity: AnnotationIssueSeverity
    message: str
    scope: str = "manual_annotation"
    field_name: str | None = None
    target_id: str | None = None
    row_number: int | None = None


@dataclass(frozen=True)
class LoadManualAnnotationsResult:
    raw_annotations_df: pl.DataFrame | None
    paragraph_annotations_df: pl.DataFrame | None
    issues: list[AnnotationIssue] = field(default_factory=list)


def _empty_raw_annotations_df() -> pl.DataFrame:
    return pl.DataFrame(schema=RAW_ANNOTATION_SCHEMA)


def _empty_paragraph_annotations_df() -> pl.DataFrame:
    return pl.DataFrame(schema=PARAGRAPH_ANNOTATION_SUMMARY_SCHEMA)


def _build_annotation_issue(
    *,
    code: str,
    severity: AnnotationIssueSeverity,
    message: str,
    field_name: str | None = None,
    target_id: str | None = None,
    row_number: int | None = None,
) -> AnnotationIssue:
    return AnnotationIssue(
        code=code,
        severity=severity,
        message=message,
        field_name=field_name,
        target_id=target_id,
        row_number=row_number,
    )


def _ensure_annotation_columns(raw_annotations_df: pl.DataFrame) -> pl.DataFrame:
    ensured_df = raw_annotations_df
    for column_name, dtype in RAW_ANNOTATION_SCHEMA.items():
        if column_name not in ensured_df.columns:
            ensured_df = ensured_df.with_columns(pl.lit("").cast(dtype).alias(column_name))
    return ensured_df.select(list(RAW_ANNOTATION_SCHEMA.keys()))


def _normalize_existing_annotation_columns(raw_annotations_df: pl.DataFrame) -> pl.DataFrame:
    string_columns = [
        column_name
        for column_name in RAW_ANNOTATION_SCHEMA.keys()
        if column_name in raw_annotations_df.columns
    ]
    return raw_annotations_df.with_columns([
        pl.col(column_name).cast(pl.String).fill_null("").str.strip_chars().alias(column_name)
        for column_name in string_columns
    ])


def _read_raw_annotations_df(annotation_csv_path: Path) -> pl.DataFrame:
    if not annotation_csv_path.exists():
        return _empty_raw_annotations_df()

    raw_annotations_df = pl.read_csv(
        annotation_csv_path,
        schema_overrides=RAW_ANNOTATION_SCHEMA,
        infer_schema=False,
        raise_if_empty=False,
    )
    return _normalize_existing_annotation_columns(raw_annotations_df)


def _validate_required_columns(raw_annotations_df: pl.DataFrame) -> list[AnnotationIssue]:
    missing_columns = [
        column_name
        for column_name in REQUIRED_ANNOTATION_COLUMNS
        if column_name not in raw_annotations_df.columns
    ]
    return [
        _build_annotation_issue(
            code="annotation_required_column_missing",
            severity="error",
            message=f"manual annotation CSV is missing required column: {column_name}",
            field_name=column_name,
        )
        for column_name in missing_columns
    ]


def _build_duplicate_and_conflict_issues(paragraph_annotations_df: pl.DataFrame) -> list[AnnotationIssue]:
    issues: list[AnnotationIssue] = []
    duplicate_counter: Counter[tuple[object, ...]] = Counter()
    value_sets: defaultdict[tuple[object, ...], set[str]] = defaultdict(set)
    first_row_numbers: dict[tuple[object, ...], int] = {}

    for row in paragraph_annotations_df.to_dicts():
        duplicate_key = (
            row["paragraph_id"],
            row["label_namespace"],
            row["label_key"],
            row["label_value"],
        )
        duplicate_counter[duplicate_key] += 1
        if duplicate_key not in first_row_numbers:
            first_row_numbers[duplicate_key] = int(row["row_number"])

        conflict_key = (
            row["paragraph_id"],
            row["label_namespace"],
            row["label_key"],
        )
        value_sets[conflict_key].add(str(row["label_value"]))

    for duplicate_key, duplicate_count in sorted(duplicate_counter.items()):
        if duplicate_count < 2:
            continue
        paragraph_id, label_namespace, label_key, label_value = duplicate_key
        issues.append(
            _build_annotation_issue(
                code="annotation_duplicate_deduplicated",
                severity="warning",
                message=(
                    "manual annotation duplicates were deduplicated: "
                    f"paragraph_id={paragraph_id}, namespace={label_namespace}, "
                    f"key={label_key}, value={label_value}, count={duplicate_count}"
                ),
                field_name="label_value",
                target_id=str(paragraph_id),
                row_number=first_row_numbers[duplicate_key],
            )
        )

    for conflict_key, values in sorted(value_sets.items()):
        if len(values) < 2:
            continue
        paragraph_id, label_namespace, label_key = conflict_key
        issues.append(
            _build_annotation_issue(
                code="annotation_value_conflict",
                severity="warning",
                message=(
                    "manual annotation key has multiple distinct values: "
                    f"paragraph_id={paragraph_id}, namespace={label_namespace}, "
                    f"key={label_key}, values={sorted(values)}"
                ),
                field_name="label_value",
                target_id=str(paragraph_id),
            )
        )

    return issues


def _build_paragraph_annotations_summary_df(
    paragraph_annotations_df: pl.DataFrame,
) -> pl.DataFrame:
    if paragraph_annotations_df.is_empty():
        return _empty_paragraph_annotations_df()

    deduplicated_df = (
        paragraph_annotations_df
        .unique(subset=["paragraph_id", "label_namespace", "label_key", "label_value"], keep="first")
        .with_columns([
            pl.concat_str([
                pl.col("label_namespace"),
                pl.lit(":"),
                pl.col("label_key"),
                pl.lit("="),
                pl.col("label_value"),
            ]).alias("manual_annotation_pair"),
            pl.col("label_namespace").alias("manual_annotation_namespace"),
        ])
    )

    return (
        deduplicated_df
        .group_by("paragraph_id")
        .agg([
            pl.len().cast(pl.UInt32).alias("manual_annotation_count"),
            pl.col("manual_annotation_pair")
            .sort()
            .alias("manual_annotation_pairs"),
            pl.col("manual_annotation_namespace")
            .unique()
            .sort()
            .alias("manual_annotation_namespaces"),
        ])
        .with_columns([
            pl.col("manual_annotation_pairs").list.join("\n").alias("manual_annotation_pairs_text"),
            pl.col("manual_annotation_namespaces").list.join(", ").alias("manual_annotation_namespaces_text"),
        ])
        .sort("paragraph_id")
        .select(list(PARAGRAPH_ANNOTATION_SUMMARY_SCHEMA.keys()))
    )


def load_manual_annotations_result(annotation_csv_path: Path) -> LoadManualAnnotationsResult:
    resolved_annotation_csv_path = annotation_csv_path.expanduser().resolve()
    if not resolved_annotation_csv_path.exists():
        return LoadManualAnnotationsResult(
            raw_annotations_df=_empty_raw_annotations_df(),
            paragraph_annotations_df=_empty_paragraph_annotations_df(),
            issues=[],
        )

    raw_annotations_df = _read_raw_annotations_df(resolved_annotation_csv_path)
    required_column_issues = _validate_required_columns(raw_annotations_df)
    if required_column_issues:
        return LoadManualAnnotationsResult(
            raw_annotations_df=raw_annotations_df,
            paragraph_annotations_df=None,
            issues=required_column_issues,
        )

    indexed_raw_annotations_df = _ensure_annotation_columns(raw_annotations_df).with_row_index(
        "row_number",
        offset=1,
    )
    normalized_target_type_df = indexed_raw_annotations_df.with_columns(
        pl.col("target_type").str.to_lowercase().alias("target_type")
    )

    unknown_target_type_rows = normalized_target_type_df.filter(
        ~pl.col("target_type").is_in(sorted(KNOWN_TARGET_TYPES))
    )
    if not unknown_target_type_rows.is_empty():
        issues = [
            _build_annotation_issue(
                code="annotation_target_type_unknown",
                severity="error",
                message=f"unknown target_type in manual annotation CSV: {row['target_type']}",
                field_name="target_type",
                target_id=str(row["target_id"]),
                row_number=int(row["row_number"]),
            )
            for row in unknown_target_type_rows.select(["row_number", "target_type", "target_id"]).to_dicts()
        ]
        return LoadManualAnnotationsResult(
            raw_annotations_df=raw_annotations_df,
            paragraph_annotations_df=None,
            issues=issues,
        )

    paragraph_candidate_df = (
        normalized_target_type_df
        .filter(pl.col("target_type") == "paragraph")
        .with_columns(
            pl.col("target_id").cast(pl.Int64, strict=False).alias("paragraph_id")
        )
    )

    invalid_target_id_rows = paragraph_candidate_df.filter(pl.col("paragraph_id").is_null())
    if not invalid_target_id_rows.is_empty():
        issues = [
            _build_annotation_issue(
                code="annotation_target_id_invalid",
                severity="error",
                message=f"paragraph target_id must be an integer: {row['target_id']}",
                field_name="target_id",
                target_id=str(row["target_id"]),
                row_number=int(row["row_number"]),
            )
            for row in invalid_target_id_rows.select(["row_number", "target_id"]).to_dicts()
        ]
        return LoadManualAnnotationsResult(
            raw_annotations_df=raw_annotations_df,
            paragraph_annotations_df=None,
            issues=issues,
        )

    paragraph_annotations_df = paragraph_candidate_df.select([
        "row_number",
        "paragraph_id",
        "label_namespace",
        "label_key",
        "label_value",
        "tagged_by",
        "tagged_at",
        "confidence",
        "note",
    ])
    issues = _build_duplicate_and_conflict_issues(paragraph_annotations_df)
    paragraph_annotations_summary_df = _build_paragraph_annotations_summary_df(
        paragraph_annotations_df=paragraph_annotations_df,
    )
    return LoadManualAnnotationsResult(
        raw_annotations_df=raw_annotations_df,
        paragraph_annotations_df=paragraph_annotations_summary_df,
        issues=issues,
    )
