from pdf_converter.ocr_table_candidates import find_table_review_candidates
from pdf_converter.pdf_text_normalizer import classify_markdown_lines


def test_find_table_review_candidates_keeps_table_context_separate():
    markdown = classify_markdown_lines("| 科目 | 株式の議渡制限 |")

    candidates = find_table_review_candidates(
        markdown,
        source_document_id="doc",
        run_id="run",
        max_context_chars=10,
    )

    assert len(candidates) == 1
    assert candidates[0].candidate_kind == "table_review"
    assert "table_context" in candidates[0].risk_flags
    assert candidates[0].table_detection_reason == "markdown_table_line"
    assert candidates[0].col_index == 1


def test_find_table_review_candidates_reads_html_table_cell():
    markdown = classify_markdown_lines("<table><tr><td>科目</td><td>株式の議渡制限</td></tr></table>")

    candidates = find_table_review_candidates(
        markdown,
        source_document_id="doc",
        run_id="run",
        max_context_chars=10,
    )

    assert len(candidates) == 1
    assert candidates[0].row_index == 0
    assert candidates[0].col_index == 1
    assert candidates[0].cell_text == "株式の議渡制限"
    assert candidates[0].table_detection_reason == "html_table"
