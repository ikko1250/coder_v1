from __future__ import annotations

from datetime import datetime
from dataclasses import replace
from pathlib import Path
from typing import Iterable

from pdf_converter.ocr_correction_report import (
    build_report_paths,
    write_python_text_correction_reports,
)
from pdf_converter.ocr_block_matcher import (
    build_extracted_blocks,
    build_markdown_blocks,
    match_blocks,
)
from pdf_converter.ocr_candidate_deduplicator import deduplicate_review_candidates
from pdf_converter.ocr_known_patterns import find_known_ocr_pattern_candidates
from pdf_converter.ocr_line_matcher import (
    build_correction_candidates,
    low_confidence_ratio,
    match_lines,
)
from pdf_converter.ocr_ngram_diff import build_ngram_review_candidates
from pdf_converter.ocr_paths import (
    MarkdownResolutionError,
    WorkingDirectoryError,
    WorkingMarkdownError,
    copy_markdown_to_working_directory,
    resolve_ocr_markdown_path,
    resolve_working_directory,
)
from pdf_converter.ocr_table_candidates import find_table_review_candidates
from pdf_converter.pdf_input import PdfValidationError, validate_pdf_path
from pdf_converter.pdf_text_extractor import PdfTextExtractor, PyMuPdfTextExtractor
from pdf_converter.pdf_text_normalizer import (
    classify_extracted_lines,
    classify_markdown_lines,
)
from pdf_converter.python_text_correction_model import (
    CANDIDATE_KIND_REVIEW,
    CANDIDATE_KIND_TABLE_REVIEW,
    LINE_KIND_BODY,
    LineMatchingConfig,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_UNMATCHED,
    PythonTextCorrectionResult,
    ReviewCandidate,
    ReviewCandidateConfig,
    SUPPRESSED_LOW_SCORE,
    SuppressedCandidate,
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
    review_config: ReviewCandidateConfig | None = None,
    report_output_root: Path | None = None,
) -> PythonTextCorrectionResult:
    if matching_config is None:
        matching_config = LineMatchingConfig()
    if review_config is None:
        review_config = ReviewCandidateConfig()
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
    run_id = _build_run_id(resolved_markdown_path)
    source_document_id = resolved_markdown_path.stem

    markdown_blocks = build_markdown_blocks(markdown_lines, review_config)
    extracted_blocks = build_extracted_blocks(extracted_lines, review_config)
    block_matches = match_blocks(markdown_blocks, extracted_blocks, review_config)
    known_candidates = find_known_ocr_pattern_candidates(
        markdown_lines,
        source_document_id=source_document_id,
        run_id=run_id,
        max_context_chars=review_config.max_review_context_chars,
        include_body_lines=True,
        include_table_lines=False,
    )
    table_candidates = find_table_review_candidates(
        markdown_lines,
        source_document_id=source_document_id,
        run_id=run_id,
        max_context_chars=review_config.max_review_context_chars,
    )
    ngram_candidates, ngram_suppressed = build_ngram_review_candidates(
        markdown_blocks=markdown_blocks,
        extracted_blocks=extracted_blocks,
        block_matches=block_matches,
        source_document_id=source_document_id,
        run_id=run_id,
        config=review_config,
    )
    review_candidates, suppressed_candidates = _prepare_review_candidates(
        known_candidates + ngram_candidates + table_candidates,
        ngram_suppressed,
        review_config,
    )
    body_review_candidates = _reindex_display(
        candidate for candidate in review_candidates
        if candidate.candidate_kind == CANDIDATE_KIND_REVIEW
    )
    table_review_candidates = _reindex_display(
        candidate for candidate in review_candidates
        if candidate.candidate_kind == CANDIDATE_KIND_TABLE_REVIEW
    )

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
            blocks=markdown_blocks + extracted_blocks,
            block_matches=block_matches,
            review_candidates=body_review_candidates,
            table_review_candidates=table_review_candidates,
            suppressed_candidates=suppressed_candidates,
        )
    except OSError as exc:
        return _failure(f"Report output failed: {exc}")

    comparable_count = sum(1 for line in extracted_lines if line.kind == LINE_KIND_BODY)
    message = (
        "Python PDF text correction report generated. "
        f"comparable_lines={comparable_count}, candidates={len(candidates)}, "
        f"review_candidates={len(body_review_candidates)}, "
        f"table_review_candidates={len(table_review_candidates)}, "
        f"suppressed_candidates={len(suppressed_candidates)}, warnings={len(warnings)}"
    )
    return PythonTextCorrectionResult(
        ok=True,
        warning_only=bool(warnings),
        message=message,
        working_markdown_path=working_markdown_path,
        report_paths=report_paths,
        candidate_count=len(candidates),
        review_candidate_count=len(body_review_candidates),
        table_review_candidate_count=len(table_review_candidates),
        suppressed_candidate_count=len(suppressed_candidates),
        warning_count=len(warnings),
        low_confidence_ratio=ratio,
    )


def _failure(message: str) -> PythonTextCorrectionResult:
    return PythonTextCorrectionResult(ok=False, warning_only=False, message=message)


def _reindex_display(candidates: Iterable[ReviewCandidate]) -> tuple[ReviewCandidate, ...]:
    return tuple(
        replace(candidate, display_index=index)
        for index, candidate in enumerate(candidates, start=1)
    )


def _prepare_review_candidates(
    candidates: tuple[ReviewCandidate, ...],
    suppressed_candidates: tuple[SuppressedCandidate, ...],
    config: ReviewCandidateConfig,
) -> tuple[tuple[ReviewCandidate, ...], tuple[SuppressedCandidate, ...]]:
    passing: list[ReviewCandidate] = []
    suppressed = list(suppressed_candidates)
    for candidate in candidates:
        if candidate.score < config.min_review_score:
            suppressed.append(
                SuppressedCandidate(
                    source_document_id=candidate.source_document_id,
                    run_id=candidate.run_id,
                    candidate_kind=candidate.candidate_kind,
                    source_method=",".join(candidate.source_methods),
                    page_index=candidate.page_index,
                    markdown_line_range=candidate.markdown_line_range,
                    old_text=candidate.old_text,
                    suggested_text=candidate.suggested_text,
                    diff_span=candidate.diff_span,
                    suppressed_reason=SUPPRESSED_LOW_SCORE,
                    score=candidate.score,
                    risk_flags=candidate.risk_flags,
                )
            )
            continue
        passing.append(candidate)
    kept, extra_suppressed = deduplicate_review_candidates(tuple(passing), config)
    return kept, tuple(suppressed) + extra_suppressed


def _build_run_id(markdown_path: Path) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    safe_stem = "".join(
        char if char.isalnum() or char in ("-", "_") else "_"
        for char in markdown_path.stem
    )
    return f"{safe_stem}-{timestamp}"
