from __future__ import annotations

from html.parser import HTMLParser

from pdf_converter.ocr_known_patterns import find_known_ocr_pattern_candidates
from pdf_converter.python_text_correction_model import (
    LINE_KIND_TABLE,
    MarkdownLine,
    ReviewCandidate,
)


class _HtmlTableCellParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.cells: list[tuple[int, int, str]] = []
        self._in_cell = False
        self._row_index = -1
        self._col_index = -1
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "tr":
            self._row_index += 1
            self._col_index = -1
        if tag.lower() in {"td", "th"}:
            self._in_cell = True
            self._col_index += 1
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"td", "th"} and self._in_cell:
            self.cells.append((self._row_index, self._col_index, "".join(self._buffer).strip()))
            self._in_cell = False
            self._buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._buffer.append(data)


def find_table_review_candidates(
    markdown_lines: tuple[MarkdownLine, ...],
    *,
    source_document_id: str,
    run_id: str,
    max_context_chars: int,
) -> tuple[ReviewCandidate, ...]:
    raw_candidates = find_known_ocr_pattern_candidates(
        markdown_lines,
        source_document_id=source_document_id,
        run_id=run_id,
        max_context_chars=max_context_chars,
        include_body_lines=False,
        include_table_lines=True,
    )
    enriched: list[ReviewCandidate] = []
    line_by_index = {line.line_index: line for line in markdown_lines}
    for candidate in raw_candidates:
        line = line_by_index.get(candidate.markdown_line_range[0])
        if line is None:
            enriched.append(candidate)
            continue
        row_index, col_index, cell_text = _locate_cell(line.text, candidate.diff_span[0])
        enriched.append(
            ReviewCandidate(
                **{
                    **candidate.__dict__,
                    "table_id": candidate.table_id or f"md-table-line-{line.line_index}",
                    "row_index": row_index,
                    "col_index": col_index,
                    "cell_text": cell_text or candidate.cell_text or line.text,
                    "cell_context": cell_text or line.text,
                    "table_detection_reason": _table_detection_reason(line),
                }
            )
        )
    return tuple(enriched)


def _locate_cell(text: str, diff_start: int) -> tuple[int | None, int | None, str | None]:
    if "<table" in text.lower():
        return _locate_html_cell(text, diff_start)
    if "|" in text:
        cursor = 0
        cells = text.split("|")
        for index, cell in enumerate(cells):
            start = cursor
            end = cursor + len(cell)
            if start <= diff_start <= end:
                return (0, max(0, index - 1), cell.strip())
            cursor = end + 1
    return (None, None, None)


def _locate_html_cell(text: str, diff_start: int) -> tuple[int | None, int | None, str | None]:
    parser = _HtmlTableCellParser()
    parser.feed(text)
    for row_index, col_index, cell_text in parser.cells:
        if cell_text and cell_text in text:
            start = text.find(cell_text)
            if start <= diff_start <= start + len(cell_text):
                return (row_index, col_index, cell_text)
    return (None, None, None)


def _table_detection_reason(line: MarkdownLine) -> str:
    if line.kind == LINE_KIND_TABLE:
        if "<table" in line.text.lower():
            return "html_table"
        return "markdown_table_line"
    return "table_candidate"
