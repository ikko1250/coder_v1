import shutil
import json

import pdf_converter.ocr_paths as ocr_paths
from pdf_converter.pdf_text_extractor import FakePdfTextExtractor
from pdf_converter.python_text_correction_model import ExtractedLine
from pdf_converter.python_text_correction_runner import run_python_text_correction


def test_run_python_text_correction_generates_reports(tmp_path):
    manual_root = tmp_path / "manual"
    pdf_dir = manual_root / "pdf"
    markdown_dir = manual_root / "md"
    work_dir = manual_root / "work"
    pdf_dir.mkdir(parents=True)
    markdown_dir.mkdir()
    work_dir.mkdir()

    pdf_path = pdf_dir / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    markdown_path = markdown_dir / "sample-2026-06-09_01-00-00.md"
    markdown_path.write_text("solar powar rule\n| table povver |\n", encoding="utf-8")

    original_values = (
        ocr_paths.DEFAULT_MANUAL_ROOT,
        ocr_paths.DEFAULT_MANUAL_PDF_DIR,
        ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR,
        ocr_paths.DEFAULT_MANUAL_WORK_DIR,
    )
    ocr_paths.DEFAULT_MANUAL_ROOT = manual_root
    ocr_paths.DEFAULT_MANUAL_PDF_DIR = pdf_dir
    ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR = markdown_dir
    ocr_paths.DEFAULT_MANUAL_WORK_DIR = work_dir
    try:
        result = run_python_text_correction(
            pdf_path=str(pdf_path),
            markdown_path=str(markdown_path),
            working_dir=str(work_dir),
            extractor=FakePdfTextExtractor((
                ExtractedLine(0, 0, "solar power rule"),
                ExtractedLine(0, 1, "| table power |"),
            )),
            report_output_root=tmp_path / "runtime",
        )
    finally:
        (
            ocr_paths.DEFAULT_MANUAL_ROOT,
            ocr_paths.DEFAULT_MANUAL_PDF_DIR,
            ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR,
            ocr_paths.DEFAULT_MANUAL_WORK_DIR,
        ) = original_values

    assert result.ok is True
    assert result.working_markdown_path is not None
    assert result.working_markdown_path.exists()
    assert result.report_paths is not None
    assert result.report_paths.manifest_path.exists()
    assert result.candidate_count == 1
    assert result.review_candidate_count == 1
    assert result.table_review_candidate_count == 0
    assert result.report_paths.review_candidates_path.exists()
    assert result.report_paths.table_review_candidates_path.exists()
    assert result.report_paths.inspection_candidates_path.exists()
    assert result.report_paths.suppressed_candidates_path.exists()
    assert result.report_paths.inspection_summary_json_path.exists()
    assert result.report_paths.candidate_summary_json_path.exists()
    inspection_summary = json.loads(result.report_paths.inspection_summary_json_path.read_text(encoding="utf-8"))
    assert inspection_summary["counts"]["inspectionCandidates"] == result.inspection_candidate_count
    assert inspection_summary["counts"]["suppressedCandidateRecords"] == result.suppressed_candidate_record_count
    assert "table_candidate" in result.report_paths.warnings_path.read_text(encoding="utf-8")
    shutil.rmtree(manual_root, ignore_errors=True)


def test_run_python_text_correction_generates_known_and_table_review_candidates(tmp_path):
    manual_root = tmp_path / "manual"
    pdf_dir = manual_root / "pdf"
    markdown_dir = manual_root / "md"
    work_dir = manual_root / "work"
    pdf_dir.mkdir(parents=True)
    markdown_dir.mkdir()
    work_dir.mkdir()

    pdf_path = pdf_dir / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n")
    markdown_path = markdown_dir / "sample-2026-06-09_01-00-00.md"
    markdown_path.write_text("株式の議渡制限\n| 科目 | 新林予約権 |\n", encoding="utf-8")

    original_values = (
        ocr_paths.DEFAULT_MANUAL_ROOT,
        ocr_paths.DEFAULT_MANUAL_PDF_DIR,
        ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR,
        ocr_paths.DEFAULT_MANUAL_WORK_DIR,
    )
    ocr_paths.DEFAULT_MANUAL_ROOT = manual_root
    ocr_paths.DEFAULT_MANUAL_PDF_DIR = pdf_dir
    ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR = markdown_dir
    ocr_paths.DEFAULT_MANUAL_WORK_DIR = work_dir
    try:
        result = run_python_text_correction(
            pdf_path=str(pdf_path),
            markdown_path=str(markdown_path),
            working_dir=str(work_dir),
            extractor=FakePdfTextExtractor((
                ExtractedLine(0, 0, "株式の譲渡制限"),
                ExtractedLine(0, 1, "| 科目 | 新株予約権 |"),
            )),
            report_output_root=tmp_path / "runtime",
        )
    finally:
        (
            ocr_paths.DEFAULT_MANUAL_ROOT,
            ocr_paths.DEFAULT_MANUAL_PDF_DIR,
            ocr_paths.DEFAULT_MANUAL_MARKDOWN_DIR,
            ocr_paths.DEFAULT_MANUAL_WORK_DIR,
        ) = original_values

    assert result.ok is True
    assert result.review_candidate_count >= 1
    assert result.table_review_candidate_count == 1
    assert result.report_paths.inspection_candidates_path.exists()
    assert result.report_paths.suppressed_candidates_path.exists()
    assert "議渡" in result.report_paths.review_candidates_path.read_text(encoding="utf-8")
    assert "新林予約権" in result.report_paths.table_review_candidates_path.read_text(encoding="utf-8")
    shutil.rmtree(manual_root, ignore_errors=True)
