from pdf_converter.ocr_block_matcher import build_extracted_blocks, build_markdown_blocks, match_blocks
from pdf_converter.pdf_text_normalizer import classify_extracted_lines, classify_markdown_lines
from pdf_converter.python_text_correction_model import ExtractedLine, ReviewCandidateConfig


def test_match_blocks_handles_wrapped_extracted_lines():
    config = ReviewCandidateConfig()
    markdown = classify_markdown_lines("株式の議渡制限")
    extracted = classify_extracted_lines((
        ExtractedLine(0, 0, "株式の譲渡"),
        ExtractedLine(0, 1, "制限"),
    ))

    markdown_blocks = build_markdown_blocks(markdown, config)
    extracted_blocks = build_extracted_blocks(extracted, config)
    matches = match_blocks(markdown_blocks, extracted_blocks, config)

    assert len(markdown_blocks) == 1
    assert len(extracted_blocks) == 1
    assert matches[0].match_kind in {"near", "weak_near"}
    assert matches[0].extracted_block_id == extracted_blocks[0].block_id
