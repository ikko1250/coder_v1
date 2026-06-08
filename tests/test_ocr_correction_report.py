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
        warnings=["table_candidate: sample"],
    )

    manifest = json.loads(report_paths.manifest_path.read_text(encoding="utf-8"))
    candidate_lines = report_paths.correction_candidates_path.read_text(encoding="utf-8").splitlines()

    assert manifest["counts"]["correctionCandidates"] == 1
    assert json.loads(candidate_lines[0])["candidate_id"] == "C0001"
    assert "table_candidate" in report_paths.warnings_path.read_text(encoding="utf-8")
