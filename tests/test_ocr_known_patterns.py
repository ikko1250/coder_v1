from pdf_converter.ocr_known_patterns import find_known_ocr_pattern_candidates
from pdf_converter.pdf_text_normalizer import classify_markdown_lines


def test_find_known_ocr_pattern_candidates_from_body_line():
    markdown = classify_markdown_lines("株式の議渡制限")

    candidates = find_known_ocr_pattern_candidates(
        markdown,
        source_document_id="doc",
        run_id="run",
        max_context_chars=10,
    )

    assert len(candidates) == 1
    assert candidates[0].old_text == "議渡"
    assert candidates[0].suggested_text == "譲渡"
    assert candidates[0].candidate_kind == "review"
    assert candidates[0].apply_policy == "never_auto_apply"


def test_find_known_ocr_pattern_candidates_can_skip_table_lines():
    markdown = classify_markdown_lines("| 株式の議渡制限 |")

    candidates = find_known_ocr_pattern_candidates(
        markdown,
        source_document_id="doc",
        run_id="run",
        max_context_chars=10,
        include_table_lines=False,
    )

    assert candidates == ()
