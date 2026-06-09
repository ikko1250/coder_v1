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


def test_find_known_ocr_pattern_candidates_detects_renova_apply_patterns():
    markdown = classify_markdown_lines("① 壳上收益 12,864 百万円\n2025年3月未現在\n单位：百万円")

    candidates = find_known_ocr_pattern_candidates(
        markdown,
        source_document_id="レノバ_report",
        run_id="run",
        max_context_chars=10,
    )

    assert {candidate.old_text for candidate in candidates} == {"壳上收益", "未現在", "单位"}
    assert all(candidate.priority == "high" for candidate in candidates)
    assert all("scope" in candidate.evidence[0] for candidate in candidates)


def test_document_specific_known_pattern_requires_document_context():
    markdown = classify_markdown_lines("労北風力合同会社")

    unrelated = find_known_ocr_pattern_candidates(
        markdown,
        source_document_id="other_report",
        run_id="run",
        max_context_chars=10,
    )
    renova = find_known_ocr_pattern_candidates(
        markdown,
        source_document_id="レノバ_report",
        run_id="run",
        max_context_chars=10,
    )

    assert unrelated == ()
    assert len(renova) == 1
    assert renova[0].suggested_text == "苓北"
