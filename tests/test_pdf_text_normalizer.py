from pdf_converter.pdf_text_normalizer import (
    classify_extracted_lines,
    classify_markdown_lines,
    normalize_for_compare,
)
from pdf_converter.python_text_correction_model import (
    ExtractedLine,
    LINE_KIND_HEADER_FOOTER,
    LINE_KIND_PAGE_NUMBER,
    LINE_KIND_TABLE,
)


def test_normalize_for_compare_preserves_comparable_text():
    assert normalize_for_compare(" A\r\n  B\tC ") == "A B C"


def test_classify_markdown_table_line():
    lines = classify_markdown_lines("| A | B |\nplain")

    assert lines[0].kind == LINE_KIND_TABLE
    assert lines[1].normalized_text == "plain"


def test_classify_extracted_page_number_and_repeated_edge():
    lines = (
        ExtractedLine(0, 0, "Document Header"),
        ExtractedLine(0, 1, "Body A"),
        ExtractedLine(0, 2, "- 1 -"),
        ExtractedLine(1, 0, "Document Header"),
        ExtractedLine(1, 1, "Body B"),
        ExtractedLine(1, 2, "2"),
    )

    classified = classify_extracted_lines(lines)

    assert classified[0].kind == LINE_KIND_HEADER_FOOTER
    assert classified[2].kind == LINE_KIND_PAGE_NUMBER
    assert classified[5].kind == LINE_KIND_PAGE_NUMBER
