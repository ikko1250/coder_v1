from __future__ import annotations

from pathlib import Path
from typing import Protocol

from pdf_converter.python_text_correction_model import (
    ExtractedLine,
    PdfTextExtractionMetadata,
    PdfTextExtractionResult,
)


class PdfTextExtractor(Protocol):
    def extract(self, pdf_path: Path) -> PdfTextExtractionResult:
        """Extract page lines from a PDF without mutating project files."""


class FakePdfTextExtractor:
    def __init__(
        self,
        lines: tuple[ExtractedLine, ...],
        *,
        error: str | None = None,
    ) -> None:
        self._lines = lines
        self._error = error

    def extract(self, pdf_path: Path) -> PdfTextExtractionResult:
        page_count = 0
        if self._lines:
            page_count = max(line.page_index for line in self._lines) + 1
        return PdfTextExtractionResult(
            lines=self._lines,
            metadata=PdfTextExtractionMetadata(
                extractor_name="fake",
                extractor_version="test",
                page_count=page_count,
                settings={},
            ),
            error=self._error,
        )


class PyMuPdfTextExtractor:
    def __init__(self, *, sort: bool = True) -> None:
        self._sort = sort

    def extract(self, pdf_path: Path) -> PdfTextExtractionResult:
        try:
            import pymupdf
        except ImportError as exc:
            return self._error_result(f"PyMuPDF is not installed: {exc}")

        try:
            with pymupdf.open(pdf_path) as doc:
                lines: list[ExtractedLine] = []
                for page_index, page in enumerate(doc):
                    text = page.get_text("text", sort=self._sort)
                    for line_index, raw_line in enumerate(text.splitlines()):
                        lines.append(
                            ExtractedLine(
                                page_index=page_index,
                                line_index=line_index,
                                text=raw_line,
                            )
                        )
                metadata = PdfTextExtractionMetadata(
                    extractor_name="pymupdf",
                    extractor_version=getattr(pymupdf, "__version__", "unknown"),
                    page_count=len(doc),
                    settings={"sort": self._sort},
                )
        except Exception as exc:
            return self._error_result(f"PDF text extraction failed: {exc}")

        if not any(line.text.strip() for line in lines):
            return PdfTextExtractionResult(
                lines=tuple(lines),
                metadata=metadata,
                error="PDF text extraction produced no text.",
            )

        return PdfTextExtractionResult(lines=tuple(lines), metadata=metadata)

    def _error_result(self, message: str) -> PdfTextExtractionResult:
        return PdfTextExtractionResult(
            lines=(),
            metadata=PdfTextExtractionMetadata(
                extractor_name="pymupdf",
                extractor_version="unknown",
                page_count=0,
                settings={"sort": self._sort},
            ),
            error=message,
        )
