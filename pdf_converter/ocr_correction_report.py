from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pdf_converter.python_text_correction_model import (
    CorrectionCandidate,
    ExtractedLine,
    LineMatch,
    LineMatchingConfig,
    MarkdownLine,
    PdfTextExtractionMetadata,
    PythonTextCorrectionReportPaths,
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
) -> None:
    report_paths.run_dir.mkdir(parents=True, exist_ok=True)
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
                "warnings": len(warnings),
            },
            "outputs": {
                "extractedLines": str(report_paths.extracted_lines_path),
                "lineMatches": str(report_paths.line_matches_path),
                "correctionCandidates": str(report_paths.correction_candidates_path),
                "warnings": str(report_paths.warnings_path),
            },
        },
    )
    _write_jsonl(report_paths.extracted_lines_path, extracted_lines)
    _write_jsonl(report_paths.line_matches_path, matches)
    _write_jsonl(report_paths.correction_candidates_path, candidates)
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
