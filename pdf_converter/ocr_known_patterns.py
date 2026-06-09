from __future__ import annotations

import re
from dataclasses import dataclass

from pdf_converter.python_text_correction_model import (
    CANDIDATE_KIND_REVIEW,
    CANDIDATE_KIND_TABLE_REVIEW,
    LINE_KIND_BODY,
    LINE_KIND_TABLE,
    PRIORITY_LOW,
    PRIORITY_MEDIUM,
    ReviewCandidate,
    SOURCE_METHOD_KNOWN_OCR_PATTERN,
    MarkdownLine,
)


@dataclass(frozen=True)
class KnownOcrPattern:
    pattern_id: str
    old_text: str
    suggested_text: str
    reason: str
    risk_level: str = PRIORITY_MEDIUM
    context_regex: str | None = None
    negative_context_regex: str | None = None


KNOWN_OCR_PATTERNS: tuple[KnownOcrPattern, ...] = (
    KnownOcrPattern(
        pattern_id="jp-kanji-jouto-001",
        old_text="議渡",
        suggested_text="譲渡",
        reason="known_ocr_confusion_jouto",
    ),
    KnownOcrPattern(
        pattern_id="jp-kanji-taisho-001",
        old_text="对象",
        suggested_text="対象",
        reason="known_ocr_confusion_taisho",
    ),
    KnownOcrPattern(
        pattern_id="jp-kanji-shinkabu-001",
        old_text="新林予約権",
        suggested_text="新株予約権",
        reason="known_ocr_confusion_shinkabu_yoyakuken",
    ),
    KnownOcrPattern(
        pattern_id="jp-kanji-sanshiki-001",
        old_text="算え",
        suggested_text="算式",
        reason="known_ocr_confusion_sanshiki",
        risk_level=PRIORITY_LOW,
    ),
)


def find_known_ocr_pattern_candidates(
    markdown_lines: tuple[MarkdownLine, ...],
    *,
    source_document_id: str,
    run_id: str,
    max_context_chars: int,
    include_body_lines: bool = True,
    include_table_lines: bool = True,
) -> tuple[ReviewCandidate, ...]:
    candidates: list[ReviewCandidate] = []
    for line in markdown_lines:
        if line.kind == LINE_KIND_BODY and not include_body_lines:
            continue
        if line.kind == LINE_KIND_TABLE and not include_table_lines:
            continue
        if line.kind not in {LINE_KIND_BODY, LINE_KIND_TABLE}:
            continue
        candidate_kind = CANDIDATE_KIND_TABLE_REVIEW if line.kind == LINE_KIND_TABLE else CANDIDATE_KIND_REVIEW
        for pattern in KNOWN_OCR_PATTERNS:
            candidates.extend(
                _candidates_for_pattern(
                    line,
                    pattern,
                    source_document_id=source_document_id,
                    run_id=run_id,
                    candidate_kind=candidate_kind,
                    max_context_chars=max_context_chars,
                )
            )
    return tuple(candidates)


def _candidates_for_pattern(
    line: MarkdownLine,
    pattern: KnownOcrPattern,
    *,
    source_document_id: str,
    run_id: str,
    candidate_kind: str,
    max_context_chars: int,
) -> list[ReviewCandidate]:
    if pattern.context_regex and not re.search(pattern.context_regex, line.text):
        return []
    if pattern.negative_context_regex and re.search(pattern.negative_context_regex, line.text):
        return []

    candidates: list[ReviewCandidate] = []
    start = 0
    while True:
        index = line.text.find(pattern.old_text, start)
        if index < 0:
            break
        end = index + len(pattern.old_text)
        risk_flags = ("known_pattern",)
        table_id = None
        table_detection_reason = None
        if candidate_kind == CANDIDATE_KIND_TABLE_REVIEW:
            risk_flags = ("known_pattern", "table_context")
            table_id = f"md-table-line-{line.line_index}"
            table_detection_reason = "markdown_table_line"
        candidates.append(
            ReviewCandidate(
                candidate_id="",
                display_index=0,
                source_document_id=source_document_id,
                run_id=run_id,
                candidate_kind=candidate_kind,
                source_methods=(SOURCE_METHOD_KNOWN_OCR_PATTERN,),
                priority=pattern.risk_level,
                page_index=None,
                markdown_line_range=(line.line_index, line.line_index),
                extracted_line_range=(),
                old_text=pattern.old_text,
                suggested_text=pattern.suggested_text,
                diff_span=(index, end),
                context_before=line.text[max(0, index - max_context_chars):index],
                context_after=line.text[end:end + max_context_chars],
                reason=pattern.reason,
                score=0.80 if pattern.risk_level == PRIORITY_MEDIUM else 0.72,
                risk_flags=risk_flags,
                evidence=({"pattern_id": pattern.pattern_id},),
                table_id=table_id,
                cell_text=line.text if candidate_kind == CANDIDATE_KIND_TABLE_REVIEW else None,
                table_detection_reason=table_detection_reason,
            )
        )
        start = end
    return candidates
