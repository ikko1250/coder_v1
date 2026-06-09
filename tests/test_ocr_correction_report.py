import json

from pdf_converter.ocr_correction_report import build_report_paths, write_python_text_correction_reports
from pdf_converter.python_text_correction_model import (
    CorrectionCandidate,
    ExtractedLine,
    ExtractedLineRef,
    LineMatch,
    LineMatchingConfig,
    MarkdownLine,
    MATCH_KIND_NEAR,
    PdfTextExtractionMetadata,
    ReviewCandidate,
)


def test_write_python_text_correction_reports(tmp_path):
    report_paths = build_report_paths("run-1", tmp_path)
    extracted = (ExtractedLine(0, 0, "solar power"),)
    markdown = (MarkdownLine(0, "solar povver"),)
    matches = (
        LineMatch(
            markdown_line_indexes=(0,),
            extracted_line_refs=(ExtractedLineRef(0, 0),),
            match_kind=MATCH_KIND_NEAR,
            score=0.95,
        ),
    )
    candidates = (
        CorrectionCandidate(
            candidate_id="C0001",
            markdown_line_indexes=(0,),
            extracted_line_refs=(ExtractedLineRef(0, 0),),
            old_text="solar povver",
            suggested_text="solar power",
            reason="typo_candidate",
            score=0.95,
        ),
    )
    review_candidates = (
        ReviewCandidate(
            candidate_id="RC-test",
            display_index=1,
            source_document_id="sample",
            run_id="run-1",
            candidate_kind="review",
            source_methods=("known_ocr_pattern",),
            priority="medium",
            page_index=None,
            markdown_line_range=(0, 0),
            extracted_line_range=(),
            old_text="議渡",
            suggested_text="譲渡",
            diff_span=(0, 2),
            context_before="",
            context_after="",
            reason="known_ocr_confusion_jouto",
            score=0.8,
            risk_flags=("known_pattern",),
        ),
    )

    write_python_text_correction_reports(
        report_paths=report_paths,
        pdf_path=tmp_path / "sample.pdf",
        markdown_path=tmp_path / "sample.md",
        working_markdown_path=tmp_path / "work.md",
        extraction_metadata=PdfTextExtractionMetadata("fake", "test", 1, {}),
        matching_config=LineMatchingConfig(),
        extracted_lines=extracted,
        markdown_lines=markdown,
        matches=matches,
        candidates=candidates,
        review_candidates=review_candidates,
        warnings=["table_candidate: sample"],
    )

    manifest = json.loads(report_paths.manifest_path.read_text(encoding="utf-8"))
    candidate_lines = report_paths.correction_candidates_path.read_text(encoding="utf-8").splitlines()
    review_lines = report_paths.review_candidates_path.read_text(encoding="utf-8").splitlines()
    summary = json.loads(report_paths.candidate_summary_json_path.read_text(encoding="utf-8"))

    assert manifest["counts"]["correctionCandidates"] == 1
    assert manifest["counts"]["safeCandidates"] == 1
    assert manifest["counts"]["reviewCandidates"] == 1
    assert manifest["counts"]["tableReviewCandidates"] == 0
    assert manifest["counts"]["suppressedCandidates"] == 0
    assert json.loads(candidate_lines[0])["candidate_id"] == "C0001"
    assert json.loads(review_lines[0])["candidate_id"] == "RC-test"
    assert summary["counts"]["reviewCandidates"] == 1
    assert report_paths.table_review_candidates_path.exists()
    assert report_paths.table_review_candidates_path.read_text(encoding="utf-8") == ""
    assert "table_candidate" in report_paths.warnings_path.read_text(encoding="utf-8")
