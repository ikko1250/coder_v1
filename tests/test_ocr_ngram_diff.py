from pdf_converter.ocr_block_matcher import build_extracted_blocks, build_markdown_blocks, match_blocks
from pdf_converter.ocr_ngram_diff import build_ngram_review_candidates
from pdf_converter.pdf_text_normalizer import classify_extracted_lines, classify_markdown_lines
from pdf_converter.python_text_correction_model import ExtractedLine, ReviewCandidateConfig


def test_build_ngram_review_candidates_detects_local_kanji_diff():
    config = ReviewCandidateConfig()
    markdown = classify_markdown_lines("株式の議渡制限")
    extracted = classify_extracted_lines((
        ExtractedLine(0, 0, "株式の譲渡"),
        ExtractedLine(0, 1, "制限"),
    ))
    markdown_blocks = build_markdown_blocks(markdown, config)
    extracted_blocks = build_extracted_blocks(extracted, config)
    block_matches = match_blocks(markdown_blocks, extracted_blocks, config)

    candidates, suppressed = build_ngram_review_candidates(
        markdown_blocks=markdown_blocks,
        extracted_blocks=extracted_blocks,
        block_matches=block_matches,
        source_document_id="doc",
        run_id="run",
        config=config,
    )

    assert len(candidates) == 1
    assert candidates[0].old_text == "議"
    assert candidates[0].suggested_text == "譲"
    assert candidates[0].source_methods == ("ngram_diff",)
    assert all(item.suppressed_reason == "width_or_symbol_only" for item in suppressed)


def test_build_ngram_review_candidates_suppresses_width_only_diff():
    config = ReviewCandidateConfig()
    markdown = classify_markdown_lines("比率は50%です")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "比率は50％です"),))
    markdown_blocks = build_markdown_blocks(markdown, config)
    extracted_blocks = build_extracted_blocks(extracted, config)
    block_matches = match_blocks(markdown_blocks, extracted_blocks, config)

    candidates, suppressed = build_ngram_review_candidates(
        markdown_blocks=markdown_blocks,
        extracted_blocks=extracted_blocks,
        block_matches=block_matches,
        source_document_id="doc",
        run_id="run",
        config=config,
    )

    assert candidates == ()
    assert suppressed[0].suppressed_reason == "width_or_symbol_only"
