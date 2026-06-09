from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pdf_converter.python_text_correction_model import (
    BlockMatch,
    CorrectionCandidate,
    ExtractedLine,
    InspectionCandidate,
    LineMatch,
    LineMatchingConfig,
    MarkdownLine,
    PdfTextExtractionMetadata,
    PythonTextCorrectionReportPaths,
    RecommendedBatch,
    ReviewCandidate,
    SuppressedCandidate,
    SuppressedCandidateRecord,
    TextBlock,
    dataclass_to_jsonable,
)


def build_report_paths(run_id: str, output_root: Path | None = None) -> PythonTextCorrectionReportPaths:
    if output_root is None:
        output_root = Path("runtime") / "pdf-text-correction"
    run_dir = output_root / run_id
    return PythonTextCorrectionReportPaths(
        run_dir=run_dir,
        manifest_path=run_dir / "manifest.json",
        extracted_lines_path=run_dir / "extracted-lines.jsonl",
        line_matches_path=run_dir / "line-matches.jsonl",
        correction_candidates_path=run_dir / "correction-candidates.jsonl",
        blocks_path=run_dir / "blocks.jsonl",
        block_matches_path=run_dir / "block-matches.jsonl",
        review_candidates_path=run_dir / "review-candidates.jsonl",
        table_review_candidates_path=run_dir / "table-review-candidates.jsonl",
        inspection_candidates_path=run_dir / "inspection-candidates.jsonl",
        suppressed_candidates_path=run_dir / "suppressed-candidates.jsonl",
        candidate_summary_md_path=run_dir / "candidate-summary.md",
        candidate_summary_json_path=run_dir / "candidate-summary.json",
        inspection_summary_md_path=run_dir / "inspection-summary.md",
        inspection_summary_json_path=run_dir / "inspection-summary.json",
        warnings_path=run_dir / "warnings.md",
    )


def write_python_text_correction_reports(
    *,
    report_paths: PythonTextCorrectionReportPaths,
    pdf_path: Path,
    markdown_path: Path,
    working_markdown_path: Path,
    extraction_metadata: PdfTextExtractionMetadata,
    matching_config: LineMatchingConfig,
    extracted_lines: tuple[ExtractedLine, ...],
    markdown_lines: tuple[MarkdownLine, ...],
    matches: tuple[LineMatch, ...],
    candidates: tuple[CorrectionCandidate, ...],
    warnings: list[str],
    blocks: tuple[TextBlock, ...] = (),
    block_matches: tuple[BlockMatch, ...] = (),
    review_candidates: tuple[ReviewCandidate, ...] = (),
    table_review_candidates: tuple[ReviewCandidate, ...] = (),
    suppressed_candidates: tuple[SuppressedCandidate, ...] = (),
    inspection_candidates: tuple[InspectionCandidate, ...] = (),
    suppressed_candidate_records: tuple[SuppressedCandidateRecord, ...] = (),
    suppressed_candidate_event_count: int = 0,
    resolved_match_count: int = 0,
    recommended_batches: tuple[RecommendedBatch, ...] = (),
) -> None:
    report_paths.run_dir.mkdir(parents=True, exist_ok=True)
    summary = _build_candidate_summary(
        candidates=candidates,
        review_candidates=review_candidates,
        table_review_candidates=table_review_candidates,
        suppressed_candidates=suppressed_candidates,
        inspection_candidates=inspection_candidates,
        suppressed_candidate_records=suppressed_candidate_records,
        suppressed_candidate_event_count=suppressed_candidate_event_count,
    )
    inspection_summary = _build_inspection_summary(
        inspection_candidates=inspection_candidates,
        suppressed_candidate_records=suppressed_candidate_records,
        suppressed_candidate_event_count=suppressed_candidate_event_count,
        resolved_match_count=resolved_match_count,
        recommended_batches=recommended_batches,
    )
    _write_json(
        report_paths.manifest_path,
        {
            "schemaVersion": 1,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "inputPdf": str(pdf_path),
            "inputMarkdown": str(markdown_path),
            "workingMarkdown": str(working_markdown_path),
            "extractor": dataclass_to_jsonable(extraction_metadata),
            "matchingConfig": dataclass_to_jsonable(matching_config),
            "counts": {
                "extractedLines": len(extracted_lines),
                "markdownLines": len(markdown_lines),
                "lineMatches": len(matches),
                "correctionCandidates": len(candidates),
                "safeCandidates": len(candidates),
                "reviewCandidates": len(review_candidates),
                "tableReviewCandidates": len(table_review_candidates),
                "suppressedCandidates": len(suppressed_candidates),
                "inspectionCandidates": len(inspection_candidates),
                "suppressedCandidateRecords": len(suppressed_candidate_records),
                "suppressedCandidateEvents": suppressed_candidate_event_count,
                "resolvedMatches": resolved_match_count,
                "blocks": len(blocks),
                "blockMatches": len(block_matches),
                "warnings": len(warnings),
            },
            "outputs": {
                "extractedLines": str(report_paths.extracted_lines_path),
                "lineMatches": str(report_paths.line_matches_path),
                "correctionCandidates": str(report_paths.correction_candidates_path),
                "blocks": str(report_paths.blocks_path),
                "blockMatches": str(report_paths.block_matches_path),
                "reviewCandidates": str(report_paths.review_candidates_path),
                "tableReviewCandidates": str(report_paths.table_review_candidates_path),
                "inspectionCandidates": str(report_paths.inspection_candidates_path),
                "suppressedCandidates": str(report_paths.suppressed_candidates_path),
                "candidateSummaryMarkdown": str(report_paths.candidate_summary_md_path),
                "candidateSummaryJson": str(report_paths.candidate_summary_json_path),
                "inspectionSummaryMarkdown": str(report_paths.inspection_summary_md_path),
                "inspectionSummaryJson": str(report_paths.inspection_summary_json_path),
                "warnings": str(report_paths.warnings_path),
            },
        },
    )
    _write_jsonl(report_paths.extracted_lines_path, extracted_lines)
    _write_jsonl(report_paths.line_matches_path, matches)
    _write_jsonl(report_paths.correction_candidates_path, candidates)
    _write_jsonl(report_paths.blocks_path, blocks)
    _write_jsonl(report_paths.block_matches_path, block_matches)
    _write_jsonl(report_paths.review_candidates_path, review_candidates)
    _write_jsonl(report_paths.table_review_candidates_path, table_review_candidates)
    _write_jsonl(report_paths.inspection_candidates_path, inspection_candidates)
    _write_jsonl(report_paths.suppressed_candidates_path, suppressed_candidate_records)
    _write_json(report_paths.candidate_summary_json_path, summary)
    _write_candidate_summary_markdown(report_paths.candidate_summary_md_path, summary)
    _write_json(report_paths.inspection_summary_json_path, inspection_summary)
    _write_inspection_summary_markdown(report_paths.inspection_summary_md_path, inspection_summary)
    _write_warnings(report_paths.warnings_path, warnings)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, records: tuple[Any, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output_file:
        for record in records:
            output_file.write(json.dumps(dataclass_to_jsonable(record), ensure_ascii=False))
            output_file.write("\n")


def _write_warnings(path: Path, warnings: list[str]) -> None:
    if not warnings:
        path.write_text("# Warnings\n\nNo warnings.\n", encoding="utf-8")
        return
    lines = ["# Warnings", ""]
    for warning in warnings:
        lines.append(f"- {warning}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_candidate_summary(
    *,
    candidates: tuple[CorrectionCandidate, ...],
    review_candidates: tuple[ReviewCandidate, ...],
    table_review_candidates: tuple[ReviewCandidate, ...],
    suppressed_candidates: tuple[SuppressedCandidate, ...],
    inspection_candidates: tuple[InspectionCandidate, ...],
    suppressed_candidate_records: tuple[SuppressedCandidateRecord, ...],
    suppressed_candidate_event_count: int,
) -> dict[str, Any]:
    priority_counts = Counter(candidate.priority for candidate in review_candidates + table_review_candidates)
    risk_flag_counts: Counter[str] = Counter()
    page_counts: Counter[str] = Counter()
    for candidate in review_candidates + table_review_candidates:
        page_key = "unknown" if candidate.page_index is None else str(candidate.page_index + 1)
        page_counts[page_key] += 1
        for risk_flag in candidate.risk_flags:
            risk_flag_counts[risk_flag] += 1
    suppressed_reason_counts = Counter(candidate.suppressed_reason for candidate in suppressed_candidates)
    return {
        "schemaVersion": 1,
        "counts": {
            "safeCandidates": len(candidates),
            "reviewCandidates": len(review_candidates),
            "tableReviewCandidates": len(table_review_candidates),
            "suppressedCandidates": len(suppressed_candidates),
            "inspectionCandidates": len(inspection_candidates),
            "suppressedCandidateEvents": suppressed_candidate_event_count,
            "suppressedCandidateRecords": len(suppressed_candidate_records),
        },
        "byPriority": dict(sorted(priority_counts.items())),
        "byPage": dict(sorted(page_counts.items())),
        "byRiskFlag": dict(sorted(risk_flag_counts.items())),
        "bySuppressedReason": dict(sorted(suppressed_reason_counts.items())),
        "reviewCandidateIds": [candidate.candidate_id for candidate in review_candidates],
        "tableReviewCandidateIds": [candidate.candidate_id for candidate in table_review_candidates],
    }


def _build_inspection_summary(
    *,
    inspection_candidates: tuple[InspectionCandidate, ...],
    suppressed_candidate_records: tuple[SuppressedCandidateRecord, ...],
    suppressed_candidate_event_count: int,
    resolved_match_count: int,
    recommended_batches: tuple[RecommendedBatch, ...],
) -> dict[str, Any]:
    priority_counts = Counter(candidate.inspection_priority for candidate in inspection_candidates)
    source_counts = Counter(candidate.source_method for candidate in inspection_candidates)
    page_counts = Counter(
        "unknown" if candidate.page_index is None else str(candidate.page_index + 1)
        for candidate in inspection_candidates
    )
    suppressed_counts = Counter(record.suppressed_reason for record in suppressed_candidate_records)
    risk_counts: Counter[str] = Counter()
    for candidate in inspection_candidates:
        for risk_flag in candidate.risk_flags:
            risk_counts[risk_flag] += 1
    return {
        "schemaVersion": 1,
        "counts": {
            "inspectionCandidates": len(inspection_candidates),
            "suppressedCandidateEvents": suppressed_candidate_event_count,
            "suppressedCandidateRecords": len(suppressed_candidate_records),
            "resolvedMatches": resolved_match_count,
            "dedupedInspectionCandidates": suppressed_counts.get("duplicate", 0),
        },
        "byPriority": dict(sorted(priority_counts.items())),
        "bySourceMethod": dict(sorted(source_counts.items())),
        "byPage": dict(sorted(page_counts.items())),
        "suppressedByReason": dict(sorted(suppressed_counts.items())),
        "dedupedCounts": {
            "duplicate": suppressed_counts.get("duplicate", 0),
        },
        "topRiskFlags": dict(risk_counts.most_common(10)),
        "recommendedBatches": dataclass_to_jsonable(recommended_batches),
        "ambiguousSourceCount": sum(
            1 for candidate in inspection_candidates
            if "ambiguous" in candidate.source_method
        ),
        "ambiguousInspectionCount": sum(
            1 for candidate in inspection_candidates
            if "ambiguous" in candidate.source_method
        ),
        "largeTableCellSuppressed": suppressed_counts.get("large_table_cell", 0),
    }


def _write_candidate_summary_markdown(path: Path, summary: dict[str, Any]) -> None:
    counts = summary["counts"]
    lines = [
        "# Candidate Summary",
        "",
        f"- safeCandidates: {counts['safeCandidates']}",
        f"- reviewCandidates: {counts['reviewCandidates']}",
        f"- tableReviewCandidates: {counts['tableReviewCandidates']}",
        f"- suppressedCandidates: {counts['suppressedCandidates']}",
        "",
        "## Priority",
        "",
    ]
    for key, value in summary["byPriority"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Suppressed Reasons", ""])
    if summary["bySuppressedReason"]:
        for key, value in summary["bySuppressedReason"].items():
            lines.append(f"- {key}: {value}")
    else:
        lines.append("- none: 0")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_inspection_summary_markdown(path: Path, summary: dict[str, Any]) -> None:
    counts = summary["counts"]
    lines = [
        "# Inspection Summary",
        "",
        f"- inspectionCandidates: {counts['inspectionCandidates']}",
        f"- suppressedCandidateEvents: {counts['suppressedCandidateEvents']}",
        f"- suppressedCandidateRecords: {counts['suppressedCandidateRecords']}",
        f"- resolvedMatches: {counts['resolvedMatches']}",
        "",
        "## Source Methods",
        "",
    ]
    if summary["bySourceMethod"]:
        for key, value in summary["bySourceMethod"].items():
            lines.append(f"- {key}: {value}")
    else:
        lines.append("- none: 0")
    lines.extend(["", "## Recommended Batches", ""])
    if summary["recommendedBatches"]:
        for batch in summary["recommendedBatches"]:
            lines.append(
                "- "
                f"{batch['batch_id']}: displayIndex {batch['display_index_start']}-{batch['display_index_end']} "
                f"({batch['count']} candidates, {batch['source_method']})"
            )
    else:
        lines.append("- none")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
