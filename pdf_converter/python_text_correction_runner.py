from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pdf_converter.ocr_correction_report import (
    build_report_paths,
    write_python_text_correction_reports,
)
from pdf_converter.ocr_line_matcher import (
    build_correction_candidates,
    low_confidence_ratio,
    match_lines,
)
from pdf_converter.ocr_paths import (
    MarkdownResolutionError,
    WorkingDirectoryError,
    WorkingMarkdownError,
    copy_markdown_to_working_directory,
    resolve_ocr_markdown_path,
    resolve_working_directory,
)
from pdf_converter.pdf_input import PdfValidationError, validate_pdf_path
from pdf_converter.pdf_text_extractor import PdfTextExtractor, PyMuPdfTextExtractor
from pdf_converter.pdf_text_normalizer import (
    classify_extracted_lines,
    classify_markdown_lines,
)
from pdf_converter.python_text_correction_model import (
    LINE_KIND_BODY,
    LineMatchingConfig,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_UNMATCHED,
    PythonTextCorrectionResult,
    WARNING_EMPTY_EXTRACTION,
    WARNING_LOW_CONFIDENCE_RUN,
)


PYTHON_TEXT_CORRECTION_TASK = "python-text-correct"


def run_python_text_correction(
    *,
    pdf_path: str,
    markdown_path: str | None,
    working_dir: str | None,
    extractor: PdfTextExtractor | None = None,
    matching_config: LineMatchingConfig | None = None,
    report_output_root: Path | None = None,
) -> PythonTextCorrectionResult:
    if matching_config is None:
        matching_config = LineMatchingConfig()
    if extractor is None:
        extractor = PyMuPdfTextExtractor()

    try:
        validated_pdf_path = validate_pdf_path(pdf_path)
    except PdfValidationError as exc:
        return _failure(f"PDF validation failed: {exc}")

    try:
        resolved_markdown_path = resolve_ocr_markdown_path(
            pdf_path=validated_pdf_path,
            markdown_path=markdown_path,
        )
    except MarkdownResolutionError as exc:
        return _failure(f"Markdown resolution failed: {exc}")

    try:
        resolved_working_dir = resolve_working_directory(working_dir)
    except WorkingDirectoryError as exc:
        return _failure(f"Working directory resolution failed: {exc}")

    try:
        working_markdown_path = copy_markdown_to_working_directory(
            source_markdown_path=resolved_markdown_path,
            working_dir=resolved_working_dir,
        )
    except WorkingMarkdownError as exc:
        return _failure(f"Working Markdown preparation failed: {exc}")

    extraction_result = extractor.extract(validated_pdf_path)
    extracted_lines = classify_extracted_lines(extraction_result.lines)
    warnings: list[str] = []
    if extraction_result.error is not None:
        warnings.append(f"{WARNING_EMPTY_EXTRACTION}: {extraction_result.error}")
        if not extracted_lines:
            return PythonTextCorrectionResult(
                ok=True,
                warning_only=True,
                message="PDF text extraction produced no comparable text.",
                working_markdown_path=working_markdown_path,
                candidate_count=0,
                warning_count=len(warnings),
                low_confidence_ratio=1.0,
            )

    try:
        markdown_text = resolved_markdown_path.read_text(encoding="utf-8")
    except OSError as exc:
        return _failure(f"Markdown read failed: {resolved_markdown_path}: {exc}")

    markdown_lines = classify_markdown_lines(markdown_text)
    matches = match_lines(markdown_lines, extracted_lines, matching_config)
    candidates = build_correction_candidates(markdown_lines, extracted_lines, matches)
    ratio = low_confidence_ratio(matches)

    for line in extracted_lines:
        for warning_code in line.warning_codes:
            warnings.append(
                f"{warning_code}: page={line.page_index + 1}, line={line.line_index + 1}, text={line.text!r}"
            )
    for match in matches:
        if match.match_kind in {MATCH_KIND_UNMATCHED, MATCH_KIND_AMBIGUOUS}:
            warnings.append(
                f"{match.match_kind}: markdown_lines={list(match.markdown_line_indexes)}, score={match.score:.3f}"
            )
        for warning_code in match.warning_codes:
            warnings.append(f"{warning_code}: markdown_lines={list(match.markdown_line_indexes)}")
    if ratio >= matching_config.low_confidence_match_ratio_threshold:
        warnings.append(f"{WARNING_LOW_CONFIDENCE_RUN}: ratio={ratio:.3f}")

    run_id = _build_run_id(resolved_markdown_path)
    report_paths = build_report_paths(run_id, report_output_root)
    try:
        write_python_text_correction_reports(
            report_paths=report_paths,
            pdf_path=validated_pdf_path,
            markdown_path=resolved_markdown_path,
            working_markdown_path=working_markdown_path,
            extraction_metadata=extraction_result.metadata,
            matching_config=matching_config,
            extracted_lines=extracted_lines,
            markdown_lines=markdown_lines,
            matches=matches,
            candidates=candidates,
            warnings=warnings,
        )
    except OSError as exc:
        return _failure(f"Report output failed: {exc}")

    comparable_count = sum(1 for line in extracted_lines if line.kind == LINE_KIND_BODY)
    message = (
        "Python PDF text correction report generated. "
        f"comparable_lines={comparable_count}, candidates={len(candidates)}, warnings={len(warnings)}"
    )
    return PythonTextCorrectionResult(
        ok=True,
        warning_only=bool(warnings),
        message=message,
        working_markdown_path=working_markdown_path,
        report_paths=report_paths,
        candidate_count=len(candidates),
        warning_count=len(warnings),
        low_confidence_ratio=ratio,
    )


def _failure(message: str) -> PythonTextCorrectionResult:
    return PythonTextCorrectionResult(ok=False, warning_only=False, message=message)


def _build_run_id(markdown_path: Path) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_stem = "".join(
        char if char.isalnum() or char in ("-", "_") else "_"
        for char in markdown_path.stem
    )
    return f"{safe_stem}-{timestamp}"
