import json

from pdf_converter.ocr_correction_report import build_report_paths, write_python_text_correction_reports
from pdf_converter.python_text_correction_model import (
    CorrectionCandidate,
    ExtractedLine,
    ExtractedLineRef,
    InspectionCandidate,
    LineMatch,
    LineMatchingConfig,
    MarkdownLine,
    MATCH_KIND_NEAR,
    PdfTextExtractionMetadata,
    RecommendedBatch,
    ReviewCandidate,
    SuppressedCandidateRecord,
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
    inspection_candidates = (
        InspectionCandidate(
            candidate_id="IC-test",
            display_index=1,
            source_document_id="sample",
            run_id="run-1",
            candidate_kind="inspection",
            source_method="line_unmatched",
            inspection_priority="medium",
            page_index=0,
            markdown_line_range=(0, 0),
            extracted_line_range=((0, 0),),
            markdown_text="solar povver",
            extracted_text="solar power",
            normalized_markdown_text="solar povver",
            normalized_extracted_text="solar power",
            diff_preview="solar povver -> solar power",
            context_before="",
            context_after="",
            reason="unmatched",
            score=0.7,
            risk_flags=("semantic_symbol_diff",),
        ),
    )
    suppressed_records = (
        SuppressedCandidateRecord(
            record_id="SC-test",
            source_document_id="sample",
            run_id="run-1",
            source_method="ngram_diff",
            page_index=0,
            markdown_line_range=(0, 0),
            extracted_line_range=((0, 0),),
            old_text="%",
            suggested_text="％",
            diff_span=(1, 2),
            suppressed_reason="width_or_symbol_only",
            score=0.9,
        ),
        SuppressedCandidateRecord(
            record_id="SC-dup",
            source_document_id="sample",
            run_id="run-1",
            source_method="line_ambiguous",
            page_index=0,
            markdown_line_range=(1, 1),
            extracted_line_range=((0, 1),),
            old_text="same",
            suggested_text="same same",
            diff_span=(0, 0),
            suppressed_reason="duplicate_extracted_text",
            score=0.9,
        ),
    )
    recommended_batches = (
        RecommendedBatch(
            batch_id="IB0001",
            display_index_start=1,
            display_index_end=1,
            candidate_ids=("IC-test",),
            count=1,
            priority="medium",
            source_method="line_unmatched",
            recommended_assignee="inspection-review",
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
        inspection_candidates=inspection_candidates,
        suppressed_candidate_records=suppressed_records,
        suppressed_candidate_event_count=2,
        resolved_match_count=1,
        recommended_batches=recommended_batches,
        warnings=["table_candidate: sample"],
    )

    manifest = json.loads(report_paths.manifest_path.read_text(encoding="utf-8"))
    candidate_lines = report_paths.correction_candidates_path.read_text(encoding="utf-8").splitlines()
    review_lines = report_paths.review_candidates_path.read_text(encoding="utf-8").splitlines()
    summary = json.loads(report_paths.candidate_summary_json_path.read_text(encoding="utf-8"))
    inspection_lines = report_paths.inspection_candidates_path.read_text(encoding="utf-8").splitlines()
    suppressed_lines = report_paths.suppressed_candidates_path.read_text(encoding="utf-8").splitlines()
    inspection_summary = json.loads(report_paths.inspection_summary_json_path.read_text(encoding="utf-8"))

    assert manifest["counts"]["correctionCandidates"] == 1
    assert manifest["counts"]["safeCandidates"] == 1
    assert manifest["counts"]["reviewCandidates"] == 1
    assert manifest["counts"]["tableReviewCandidates"] == 0
    assert manifest["counts"]["suppressedCandidates"] == 0
    assert manifest["counts"]["inspectionCandidates"] == 1
    assert manifest["counts"]["suppressedCandidateRecords"] == 2
    assert manifest["counts"]["suppressedCandidateEvents"] == 2
    assert manifest["counts"]["resolvedMatches"] == 1
    assert json.loads(candidate_lines[0])["candidate_id"] == "C0001"
    assert json.loads(review_lines[0])["candidate_id"] == "RC-test"
    assert json.loads(inspection_lines[0])["candidate_id"] == "IC-test"
    assert json.loads(suppressed_lines[0])["record_id"] == "SC-test"
    assert summary["counts"]["reviewCandidates"] == 1
    assert summary["counts"]["inspectionCandidates"] == 1
    assert inspection_summary["counts"]["inspectionCandidates"] == 1
    assert inspection_summary["counts"]["suppressedCandidateEvents"] == 2
    assert inspection_summary["counts"]["suppressedCandidateRecords"] == 2
    assert inspection_summary["qualityFilters"]["duplicateExtractedText"] == 1
    assert inspection_summary["qualityFilters"]["labelValueJoined"] == {"resolved": 1, "suppressed": 0}
    assert inspection_summary["qualityFilters"]["semanticSymbolDiff"] == 1
    assert inspection_summary["qualityFilters"]["formatSymbolDiff"] == 0
    assert inspection_summary["promotionHints"]["semanticSymbolReview"] == 1
    assert inspection_summary["discardReductionEstimate"] is None
    assert inspection_summary["recommendedBatches"][0]["candidate_ids"] == ["IC-test"]
    assert report_paths.table_review_candidates_path.exists()
    assert report_paths.table_review_candidates_path.read_text(encoding="utf-8") == ""
    assert "table_candidate" in report_paths.warnings_path.read_text(encoding="utf-8")
