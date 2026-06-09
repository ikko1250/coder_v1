from pdf_converter.ocr_contains_matcher import build_contains_value_inspections
from pdf_converter.pdf_text_normalizer import classify_extracted_lines, classify_markdown_lines
from pdf_converter.python_text_correction_model import ExtractedLine, InspectionCandidateConfig


def test_contains_value_records_resolved_match_without_default_inspection():
    markdown = classify_markdown_lines("【提出先】\n関東財務局長")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "【提出先】 関東財務局長"),))

    inspections, resolved = build_contains_value_inspections(
        markdown_lines=markdown,
        extracted_lines=extracted,
        source_document_id="doc",
        run_id="run",
        config=InspectionCandidateConfig(),
    )

    assert inspections == ()
    assert len(resolved) == 1
    assert resolved[0].reason == "label_value_joined"


def test_contains_value_can_emit_no_text_change_inspection_when_enabled():
    markdown = classify_markdown_lines("【提出先】\n関東財務局長")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "【提出先】 関東財務局長"),))

    inspections, resolved = build_contains_value_inspections(
        markdown_lines=markdown,
        extracted_lines=extracted,
        source_document_id="doc",
        run_id="run",
        config=InspectionCandidateConfig(include_no_text_change_inspection=True),
    )

    assert len(resolved) == 1
    assert len(inspections) == 1
    assert inspections[0].source_method == "contains_value"
    assert inspections[0].apply_policy == "never_auto_apply"


def test_contains_value_requires_label_for_known_cover_fields():
    markdown = classify_markdown_lines("電話番号\n03-1234-5678")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "住所 03-1234-5678"),))

    inspections, resolved = build_contains_value_inspections(
        markdown_lines=markdown,
        extracted_lines=extracted,
        source_document_id="doc",
        run_id="run",
        config=InspectionCandidateConfig(),
    )

    assert inspections == ()
    assert resolved == ()


def test_contains_value_does_not_resolve_value_kanji_difference():
    markdown = classify_markdown_lines("会社名\n单位")
    extracted = classify_extracted_lines((ExtractedLine(0, 0, "会社名 単位"),))

    inspections, resolved = build_contains_value_inspections(
        markdown_lines=markdown,
        extracted_lines=extracted,
        source_document_id="doc",
        run_id="run",
        config=InspectionCandidateConfig(),
    )

    assert inspections == ()
    assert resolved == ()
