from __future__ import annotations

from dataclasses import dataclass

from pdf_converter.python_text_correction_model import (
    ExtractedLine,
    InspectionCandidate,
    InspectionCandidateConfig,
    MarkdownLine,
    PRIORITY_LOW,
    REQUIRED_EVIDENCE_NONE,
    SOURCE_METHOD_CONTAINS_VALUE,
)


@dataclass(frozen=True)
class ResolvedContainsMatch:
    markdown_line_range: tuple[int, int]
    extracted_line_range: tuple[tuple[int, int], ...]
    markdown_text: str
    extracted_text: str
    page_index: int
    reason: str


def build_contains_value_inspections(
    *,
    markdown_lines: tuple[MarkdownLine, ...],
    extracted_lines: tuple[ExtractedLine, ...],
    source_document_id: str,
    run_id: str,
    config: InspectionCandidateConfig,
) -> tuple[tuple[InspectionCandidate, ...], tuple[ResolvedContainsMatch, ...]]:
    inspections: list[InspectionCandidate] = []
    resolved: list[ResolvedContainsMatch] = []
    body_markdown = [line for line in markdown_lines if line.normalized_text]
    body_extracted = [line for line in extracted_lines if line.normalized_text and line.confidence > 0]
    for previous, current in zip(body_markdown, body_markdown[1:]):
        if not _looks_like_label(previous.text):
            continue
        combined = f"{previous.normalized_text} {current.normalized_text}".strip()
        if len(current.normalized_text) > 120:
            continue
        for extracted in body_extracted:
            extracted_norm = extracted.normalized_text
            if current.normalized_text and current.normalized_text in extracted_norm:
                resolved.append(
                    ResolvedContainsMatch(
                        markdown_line_range=(previous.line_index, current.line_index),
                        extracted_line_range=((extracted.page_index, extracted.line_index),),
                        markdown_text=combined,
                        extracted_text=extracted.text,
                        page_index=extracted.page_index,
                        reason="markdown_value_contained_in_extracted_line",
                    )
                )
                if config.include_no_text_change_inspection:
                    inspections.append(
                        InspectionCandidate(
                            candidate_id="",
                            display_index=0,
                            source_document_id=source_document_id,
                            run_id=run_id,
                            candidate_kind="inspection",
                            source_method=SOURCE_METHOD_CONTAINS_VALUE,
                            inspection_priority=PRIORITY_LOW,
                            page_index=extracted.page_index,
                            markdown_line_range=(previous.line_index, current.line_index),
                            extracted_line_range=((extracted.page_index, extracted.line_index),),
                            markdown_text=combined,
                            extracted_text=extracted.text,
                            normalized_markdown_text=combined,
                            normalized_extracted_text=extracted_norm,
                            diff_preview=f"{combined[:80]} -> {extracted_norm[:80]}",
                            context_before=previous.text,
                            context_after="",
                            reason="contains_value_no_text_change",
                            score=1.0,
                            risk_flags=("contains_value",),
                            required_evidence=REQUIRED_EVIDENCE_NONE,
                        )
                    )
                break
    return tuple(inspections), tuple(resolved)


def _looks_like_label(text: str) -> bool:
    stripped = text.strip()
    return (
        stripped.startswith("【")
        or stripped.endswith("】")
        or stripped.endswith(":")
        or stripped.endswith("：")
    )
