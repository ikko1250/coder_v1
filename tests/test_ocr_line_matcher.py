from pdf_converter.ocr_line_matcher import (
    build_correction_candidates,
    low_confidence_ratio,
    match_lines,
)
from pdf_converter.pdf_text_normalizer import classify_extracted_lines, classify_markdown_lines
from pdf_converter.python_text_correction_model import (
    ExtractedLine,
    LineMatchingConfig,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_NEAR,
    MATCH_KIND_NORMALIZED_EXACT,
)


def test_match_lines_normalized_exact():
    markdown = classify_markdown_lines("A   B")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "A B"),))

    matches = match_lines(markdown, extracted)

    assert matches[0].match_kind == MATCH_KIND_NORMALIZED_EXACT


def test_match_lines_near_generates_typo_candidate():
    markdown = classify_markdown_lines("solar powar rule")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "solar power rule"),))

    matches = match_lines(markdown, extracted)
    candidates = build_correction_candidates(markdown, extracted, matches)

    assert matches[0].match_kind == MATCH_KIND_NEAR
    assert len(candidates) == 1
    assert candidates[0].reason == "typo_candidate"


def test_correction_candidate_schema_stays_compatible():
    markdown = classify_markdown_lines("solar powar rule")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "solar power rule"),))

    matches = match_lines(markdown, extracted)
    candidate = build_correction_candidates(markdown, extracted, matches)[0]

    assert candidate.candidate_id == "C0001"
    assert candidate.markdown_line_indexes == (0,)
    assert candidate.extracted_line_refs[0].page_index == 0
    assert candidate.extracted_line_refs[0].line_index == 0
    assert candidate.old_text == "solar powar rule"
    assert candidate.suggested_text == "solar power rule"
    assert candidate.reason == "typo_candidate"
    assert candidate.requires_human_review is True


def test_table_lines_do_not_generate_candidates():
    markdown = classify_markdown_lines("| solar povver |")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "| solar power |"),))

    matches = match_lines(markdown, extracted)
    candidates = build_correction_candidates(markdown, extracted, matches)

    assert matches == ()
    assert candidates == ()


def test_ambiguous_match_is_low_confidence():
    markdown = classify_markdown_lines("solar powar rule")
    extracted = classify_extracted_lines((
        ExtractedLine(0, 0, "solar power rule"),
        ExtractedLine(0, 1, "solar pover rule"),
    ))

    matches = match_lines(markdown, extracted, LineMatchingConfig(ambiguous_score_margin=0.1))

    assert matches[0].match_kind == MATCH_KIND_AMBIGUOUS
    assert low_confidence_ratio(matches) == 1.0
