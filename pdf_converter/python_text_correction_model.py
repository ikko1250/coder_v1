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
MATCH_KIND_WEAK_NEAR = "weak_near"
MATCH_KIND_UNMATCHED = "unmatched"
MATCH_KIND_AMBIGUOUS = "ambiguous"

WARNING_EMPTY_EXTRACTION = "empty_extraction"
WARNING_HEADER_FOOTER_CANDIDATE = "header_footer_candidate"
WARNING_LOW_CONFIDENCE_MATCH = "low_confidence_match"
WARNING_LOW_CONFIDENCE_RUN = "low_confidence_run"
WARNING_PAGE_NUMBER_CANDIDATE = "page_number_candidate"
WARNING_READING_ORDER_CANDIDATE = "reading_order_candidate"
WARNING_TABLE_CANDIDATE = "table_candidate"

CANDIDATE_KIND_REVIEW = "review"
CANDIDATE_KIND_TABLE_REVIEW = "table_review"

SOURCE_METHOD_BLOCK_NEAR = "block_near"
SOURCE_METHOD_KNOWN_OCR_PATTERN = "known_ocr_pattern"
SOURCE_METHOD_NGRAM_DIFF = "ngram_diff"
SOURCE_METHOD_BLOCK_AMBIGUOUS = "block_ambiguous"
SOURCE_METHOD_BLOCK_UNMATCHED = "block_unmatched"
SOURCE_METHOD_CONTAINS_VALUE = "contains_value"
SOURCE_METHOD_LINE_AMBIGUOUS = "line_ambiguous"
SOURCE_METHOD_LINE_UNMATCHED = "line_unmatched"
SOURCE_METHOD_SUPPRESSED_DIFF = "suppressed_diff"
SOURCE_METHOD_TABLE_CELL_DIFF = "table_cell_diff"

PRIORITY_HIGH = "high"
PRIORITY_MEDIUM = "medium"
PRIORITY_LOW = "low"

APPLY_POLICY_NEVER_AUTO_APPLY = "never_auto_apply"

REVIEW_STATUS_UNREVIEWED = "unreviewed"

CLASSIFICATION_STATUS_UNREVIEWED = "unreviewed"

REQUIRED_EVIDENCE_NONE = "none"
REQUIRED_EVIDENCE_ADJACENT_PAGES = "adjacent_pages"
REQUIRED_EVIDENCE_FULL_TABLE = "full_table"
REQUIRED_EVIDENCE_ORIGINAL_PDF = "original_pdf"
REQUIRED_EVIDENCE_PDF_PAGE_IMAGE = "pdf_page_image"

SUPPRESSED_AMBIGUOUS_ANCHOR = "ambiguous_anchor"
SUPPRESSED_DUPLICATE = "duplicate"
SUPPRESSED_LARGE_TABLE_CELL = "large_table_cell"
SUPPRESSED_LARGE_DIFF = "large_diff"
SUPPRESSED_LOW_SCORE = "low_score"
SUPPRESSED_NO_TEXT_CHANGE = "no_text_change"
SUPPRESSED_NUMERIC_ONLY = "numeric_only"
SUPPRESSED_TABLE_CONTEXT = "table_context"
SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES = "too_many_inspection_candidates"
SUPPRESSED_TOO_MANY_CANDIDATES_ON_PAGE = "too_many_candidates_on_page"
SUPPRESSED_WIDTH_OR_SYMBOL_ONLY = "width_or_symbol_only"


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
class ReviewCandidate:
    candidate_id: str
    display_index: int
    source_document_id: str
    run_id: str
    candidate_kind: str
    source_methods: tuple[str, ...]
    priority: str
    page_index: int | None
    markdown_line_range: tuple[int, int]
    extracted_line_range: tuple[tuple[int, int], ...]
    old_text: str
    suggested_text: str
    diff_span: tuple[int, int]
    context_before: str
    context_after: str
    reason: str
    score: float
    risk_flags: tuple[str, ...] = ()
    suppressed_reason: str | None = None
    apply_policy: str = APPLY_POLICY_NEVER_AUTO_APPLY
    review_status: str = REVIEW_STATUS_UNREVIEWED
    review_decision: str | None = None
    reviewer_note: str | None = None
    requires_human_review: bool = True
    evidence: tuple[dict[str, Any], ...] = ()
    table_id: str | None = None
    row_index: int | None = None
    col_index: int | None = None
    cell_text: str | None = None
    cell_context: str | None = None
    table_detection_reason: str | None = None


@dataclass(frozen=True)
class SuppressedCandidate:
    source_document_id: str
    run_id: str
    candidate_kind: str
    source_method: str
    page_index: int | None
    markdown_line_range: tuple[int, int]
    old_text: str
    suggested_text: str
    diff_span: tuple[int, int]
    suppressed_reason: str
    score: float
    risk_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class InspectionCandidate:
    candidate_id: str
    display_index: int
    source_document_id: str
    run_id: str
    candidate_kind: str
    source_method: str
    inspection_priority: str
    page_index: int | None
    markdown_line_range: tuple[int, int]
    extracted_line_range: tuple[tuple[int, int], ...]
    markdown_text: str
    extracted_text: str
    normalized_markdown_text: str
    normalized_extracted_text: str
    diff_preview: str
    context_before: str
    context_after: str
    reason: str
    score: float
    risk_flags: tuple[str, ...] = ()
    suppressed_reason: str | None = None
    classification_status: str = CLASSIFICATION_STATUS_UNREVIEWED
    classification_decision: str | None = None
    reviewer_note: str | None = None
    apply_policy: str = APPLY_POLICY_NEVER_AUTO_APPLY
    duplicate_of_candidate_id: str | None = None
    duplicate_of_candidate_file: str | None = None
    dedupe_reason: str | None = None
    required_evidence: str = REQUIRED_EVIDENCE_NONE
    table_id: str | None = None
    row_index: int | None = None
    col_index: int | None = None
    cell_text: str | None = None
    cell_context: str | None = None
    table_detection_reason: str | None = None


@dataclass(frozen=True)
class SuppressedCandidateRecord:
    record_id: str
    source_document_id: str
    run_id: str
    source_method: str
    page_index: int | None
    markdown_line_range: tuple[int, int]
    extracted_line_range: tuple[tuple[int, int], ...]
    old_text: str
    suggested_text: str
    diff_span: tuple[int, int]
    suppressed_reason: str
    score: float
    risk_flags: tuple[str, ...] = ()
    promoted_to_inspection: bool = False
    duplicate_of_candidate_id: str | None = None
    duplicate_of_candidate_file: str | None = None
    dedupe_reason: str | None = None


@dataclass(frozen=True)
class RecommendedBatch:
    batch_id: str
    display_index_start: int
    display_index_end: int
    candidate_ids: tuple[str, ...]
    count: int
    priority: str
    source_method: str
    recommended_assignee: str


@dataclass(frozen=True)
class TextBlock:
    block_id: str
    source: str
    page_range: tuple[int, int] | None
    line_refs: tuple[int | tuple[int, int], ...]
    text: str
    normalized_text: str
    kind: str
    confidence: float = 1.0
    warning_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class BlockMatch:
    block_match_id: str
    markdown_block_id: str
    extracted_block_id: str | None
    match_kind: str
    score: float
    alignment_confidence: str
    ambiguity_count: int = 0
    warning_codes: tuple[str, ...] = ()
    candidate_suppression_reason: str | None = None


@dataclass(frozen=True)
class LineMatchingConfig:
    near_score_threshold: float = 0.92
    ambiguous_score_margin: float = 0.03
    near_window_size: int = 8
    max_merged_markdown_lines: int = 3
    max_merged_extracted_lines: int = 3
    low_confidence_match_ratio_threshold: float = 0.35


@dataclass(frozen=True)
class ReviewCandidateConfig:
    max_review_candidates_per_page: int = 25
    min_review_score: float = 0.72
    max_diff_chars: int = 3
    max_block_chars: int = 1200
    max_review_context_chars: int = 30
    block_near_score_threshold: float = 0.70
    block_weak_near_score_threshold: float = 0.58
    block_ambiguous_score_margin: float = 0.03


@dataclass(frozen=True)
class InspectionCandidateConfig:
    max_inspection_candidates_total: int = 300
    max_high_priority_inspection_candidates: int = 200
    max_inspection_candidates_per_page: int = 40
    max_inspection_candidates_per_source_method: int = 120
    min_inspection_score: float = 0.45
    include_percent_width_diff: bool = False
    include_whitespace_diff: bool = False
    include_punctuation_diff: bool = False
    include_ascii_width_diff: bool = False
    include_no_text_change_inspection: bool = False
    max_ambiguous_alternatives: int = 3
    max_inspection_context_chars: int = 80
    max_table_inspection_candidates_per_table: int = 20
    max_table_inspection_candidates_per_page: int = 40
    numeric_only_suppressed: bool = True


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
    blocks_path: Path
    block_matches_path: Path
    review_candidates_path: Path
    table_review_candidates_path: Path
    inspection_candidates_path: Path
    suppressed_candidates_path: Path
    candidate_summary_md_path: Path
    candidate_summary_json_path: Path
    inspection_summary_md_path: Path
    inspection_summary_json_path: Path
    warnings_path: Path


@dataclass(frozen=True)
class PythonTextCorrectionResult:
    ok: bool
    warning_only: bool
    message: str
    working_markdown_path: Path | None = None
    report_paths: PythonTextCorrectionReportPaths | None = None
    candidate_count: int = 0
    review_candidate_count: int = 0
    table_review_candidate_count: int = 0
    suppressed_candidate_count: int = 0
    inspection_candidate_count: int = 0
    suppressed_candidate_record_count: int = 0
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
