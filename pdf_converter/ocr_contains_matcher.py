from __future__ import annotations

import re
import unicodedata
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

KNOWN_LABELS = (
    "提出先",
    "提出日",
    "会社名",
    "英訳名",
    "代表者の役職氏名",
    "本店の所在の場所",
    "電話番号",
    "事務連絡者氏名",
    "最寄りの連絡場所",
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
        label = _clean_label(previous.normalized_text)
        combined = f"{previous.normalized_text} {current.normalized_text}".strip()
        if len(current.normalized_text) > 120:
            continue
        for extracted in body_extracted:
            extracted_norm = extracted.normalized_text
            if not _is_label_value_match(label, current.normalized_text, extracted_norm):
                continue
            if current.normalized_text and _layout_normalize(current.normalized_text) in _layout_normalize(extracted_norm):
                resolved.append(
                    ResolvedContainsMatch(
                        markdown_line_range=(previous.line_index, current.line_index),
                        extracted_line_range=((extracted.page_index, extracted.line_index),),
                        markdown_text=combined,
                        extracted_text=extracted.text,
                        page_index=extracted.page_index,
                        reason="label_value_joined" if label in KNOWN_LABELS else "markdown_value_contained_in_extracted_line",
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
        _clean_label(stripped) in KNOWN_LABELS
        or stripped.startswith("【")
        or stripped.endswith("】")
        or stripped.endswith(":")
        or stripped.endswith("：")
    )


def _clean_label(text: str) -> str:
    stripped = text.strip()
    stripped = stripped.removeprefix("【").removesuffix("】")
    stripped = stripped.removesuffix(":").removesuffix("：")
    return stripped.strip()


def _is_label_value_match(label: str, value: str, extracted_text: str) -> bool:
    if not value:
        return False
    extracted_norm = _layout_normalize(extracted_text)
    value_norm = _layout_normalize(value)
    if value_norm not in extracted_norm:
        return False
    if label in KNOWN_LABELS:
        return _layout_normalize(label) in extracted_norm
    return True


def _layout_normalize(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()
