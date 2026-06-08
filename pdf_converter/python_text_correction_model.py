from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


LINE_KIND_BODY = "body"
LINE_KIND_EMPTY = "empty"
LINE_KIND_HEADER_FOOTER = "header_footer_candidate"
LINE_KIND_MARKDOWN_MARKUP = "markdown_markup"
LINE_KIND_PAGE_NUMBER = "page_number"
LINE_KIND_TABLE = "table_candidate"

MATCH_KIND_EXACT = "exact"
MATCH_KIND_NORMALIZED_EXACT = "normalized_exact"
MATCH_KIND_NEAR = "near"
MATCH_KIND_UNMATCHED = "unmatched"
MATCH_KIND_AMBIGUOUS = "ambiguous"

WARNING_EMPTY_EXTRACTION = "empty_extraction"
WARNING_HEADER_FOOTER_CANDIDATE = "header_footer_candidate"
WARNING_LOW_CONFIDENCE_MATCH = "low_confidence_match"
WARNING_LOW_CONFIDENCE_RUN = "low_confidence_run"
WARNING_PAGE_NUMBER_CANDIDATE = "page_number_candidate"
WARNING_READING_ORDER_CANDIDATE = "reading_order_candidate"
WARNING_TABLE_CANDIDATE = "table_candidate"


@dataclass(frozen=True)
class ExtractedLine:
    page_index: int
    line_index: int
    text: str
    normalized_text: str = ""
    kind: str = LINE_KIND_BODY
    confidence: float = 1.0
    warning_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class MarkdownLine:
    line_index: int
    text: str
    normalized_text: str = ""
    kind: str = LINE_KIND_BODY
    warning_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExtractedLineRef:
    page_index: int
    line_index: int


@dataclass(frozen=True)
class LineMatch:
    markdown_line_indexes: tuple[int, ...]
    extracted_line_refs: tuple[ExtractedLineRef, ...]
    match_kind: str
    score: float
    warning_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CorrectionCandidate:
    candidate_id: str
    markdown_line_indexes: tuple[int, ...]
    extracted_line_refs: tuple[ExtractedLineRef, ...]
    old_text: str
    suggested_text: str
    reason: str
    score: float
    requires_human_review: bool = True


@dataclass(frozen=True)
class LineMatchingConfig:
    near_score_threshold: float = 0.92
    ambiguous_score_margin: float = 0.03
    near_window_size: int = 8
    max_merged_markdown_lines: int = 3
    max_merged_extracted_lines: int = 3
    low_confidence_match_ratio_threshold: float = 0.35


@dataclass(frozen=True)
class PdfTextExtractionMetadata:
    extractor_name: str
    extractor_version: str
    page_count: int
    settings: dict[str, Any]


@dataclass(frozen=True)
class PdfTextExtractionResult:
    lines: tuple[ExtractedLine, ...]
    metadata: PdfTextExtractionMetadata
    error: str | None = None

    @property
    def is_ok(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class PythonTextCorrectionReportPaths:
    run_dir: Path
    manifest_path: Path
    extracted_lines_path: Path
    line_matches_path: Path
    correction_candidates_path: Path
    warnings_path: Path


@dataclass(frozen=True)
class PythonTextCorrectionResult:
    ok: bool
    warning_only: bool
    message: str
    working_markdown_path: Path | None = None
    report_paths: PythonTextCorrectionReportPaths | None = None
    candidate_count: int = 0
    warning_count: int = 0
    low_confidence_ratio: float = 0.0


def dataclass_to_jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "__dataclass_fields__"):
        return {
            key: dataclass_to_jsonable(item)
            for key, item in asdict(value).items()
        }
    if isinstance(value, tuple):
        return [dataclass_to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [dataclass_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): dataclass_to_jsonable(item)
            for key, item in value.items()
        }
    return value
