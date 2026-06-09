from __future__ import annotations

import re
import unicodedata
from collections import Counter

from pdf_converter.python_text_correction_model import (
    ExtractedLine,
    LINE_KIND_BODY,
    LINE_KIND_EMPTY,
    LINE_KIND_HEADER_FOOTER,
    LINE_KIND_MARKDOWN_MARKUP,
    LINE_KIND_PAGE_NUMBER,
    LINE_KIND_TABLE,
    MarkdownLine,
    WARNING_HEADER_FOOTER_CANDIDATE,
    WARNING_PAGE_NUMBER_CANDIDATE,
    WARNING_READING_ORDER_CANDIDATE,
    WARNING_TABLE_CANDIDATE,
)


PAGE_NUMBER_PATTERN = re.compile(r"^\s*(?:[-–—]\s*)?\d{1,4}(?:\s*[-–—])?\s*$")


def normalize_for_compare(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = unicodedata.normalize("NFC", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def is_table_like_line(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    lowered = stripped.lower()
    if "<table" in lowered or "<td" in lowered or "<th" in lowered:
        return True
    if stripped.count("|") >= 2:
        return True
    if "\t" in stripped and len([part for part in stripped.split("\t") if part.strip()]) >= 3:
        return True
    return len(re.findall(r"\S+\s{2,}\S+", stripped)) >= 2


def classify_markdown_lines(text: str) -> tuple[MarkdownLine, ...]:
    lines: list[MarkdownLine] = []
    for line_index, raw_line in enumerate(text.splitlines()):
        normalized = normalize_for_compare(raw_line)
        kind = LINE_KIND_BODY
        warnings: tuple[str, ...] = ()
        stripped = raw_line.strip()
        if not stripped:
            kind = LINE_KIND_EMPTY
        elif is_table_like_line(raw_line):
            kind = LINE_KIND_TABLE
            warnings = (WARNING_TABLE_CANDIDATE,)
        elif stripped.startswith(("#", "```", "![", "- ", "* ")):
            kind = LINE_KIND_MARKDOWN_MARKUP
        lines.append(
            MarkdownLine(
                line_index=line_index,
                text=raw_line,
                normalized_text=normalized,
                kind=kind,
                warning_codes=warnings,
            )
        )
    return tuple(lines)


def classify_extracted_lines(lines: tuple[ExtractedLine, ...]) -> tuple[ExtractedLine, ...]:
    edge_counter: Counter[str] = Counter()
    page_line_counts: dict[int, int] = {}
    for line in lines:
        page_line_counts[line.page_index] = max(page_line_counts.get(line.page_index, -1), line.line_index)
    for line in lines:
        normalized = normalize_for_compare(line.text)
        if line.line_index <= 1 or line.line_index >= page_line_counts.get(line.page_index, 0) - 1:
            if 0 < len(normalized) <= 80:
                edge_counter[normalized] += 1

    classified: list[ExtractedLine] = []
    previous_page = -1
    previous_line = -1
    for line in lines:
        normalized = normalize_for_compare(line.text)
        kind = LINE_KIND_BODY
        warnings: list[str] = []
        confidence = 1.0
        stripped = line.text.strip()
        if not stripped:
            kind = LINE_KIND_EMPTY
            confidence = 0.0
        elif PAGE_NUMBER_PATTERN.fullmatch(stripped):
            kind = LINE_KIND_PAGE_NUMBER
            warnings.append(WARNING_PAGE_NUMBER_CANDIDATE)
            confidence = 0.0
        elif normalized and edge_counter[normalized] >= 2:
            kind = LINE_KIND_HEADER_FOOTER
            warnings.append(WARNING_HEADER_FOOTER_CANDIDATE)
            confidence = 0.0
        elif is_table_like_line(line.text):
            kind = LINE_KIND_TABLE
            warnings.append(WARNING_TABLE_CANDIDATE)
            confidence = 0.0

        if line.page_index < previous_page or (
            line.page_index == previous_page and line.line_index < previous_line
        ):
            warnings.append(WARNING_READING_ORDER_CANDIDATE)
            confidence = min(confidence, 0.5)
        previous_page = line.page_index
        previous_line = line.line_index

        classified.append(
            ExtractedLine(
                page_index=line.page_index,
                line_index=line.line_index,
                text=line.text,
                normalized_text=normalized,
                kind=kind,
                confidence=confidence,
                warning_codes=tuple(warnings),
            )
        )
    return tuple(classified)
