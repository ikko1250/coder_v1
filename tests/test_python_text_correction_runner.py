import shutil

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
    assert "table_candidate" in result.report_paths.warnings_path.read_text(encoding="utf-8")
    shutil.rmtree(manual_root, ignore_errors=True)
