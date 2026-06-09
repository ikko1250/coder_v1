from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import replace

from pdf_converter.ocr_suppressed_candidates import SuppressedCandidateCollector
from pdf_converter.python_text_correction_model import (
    APPLY_POLICY_NEVER_AUTO_APPLY,
    BlockMatch,
    CANDIDATE_KIND_REVIEW,
    CorrectionCandidate,
    ExtractedLine,
    InspectionCandidate,
    InspectionCandidateConfig,
    LineMatch,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_UNMATCHED,
    MarkdownLine,
    PRIORITY_HIGH,
    PRIORITY_LOW,
    PRIORITY_MEDIUM,
    RecommendedBatch,
    REQUIRED_EVIDENCE_ADJACENT_PAGES,
    REQUIRED_EVIDENCE_NONE,
    ReviewCandidate,
    SOURCE_METHOD_BLOCK_AMBIGUOUS,
    SOURCE_METHOD_BLOCK_UNMATCHED,
    SOURCE_METHOD_LINE_AMBIGUOUS,
    SOURCE_METHOD_LINE_UNMATCHED,
    SUPPRESSED_DUPLICATE,
    SUPPRESSED_LOW_SCORE,
    SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES,
    TextBlock,
)


PRIORITY_RANK = {
    PRIORITY_HIGH: 0,
    PRIORITY_MEDIUM: 1,
    PRIORITY_LOW: 2,
}


def build_match_inspection_candidates(
    *,
    markdown_lines: tuple[MarkdownLine, ...],
    extracted_lines: tuple[ExtractedLine, ...],
    line_matches: tuple[LineMatch, ...],
    markdown_blocks: tuple[TextBlock, ...],
    extracted_blocks: tuple[TextBlock, ...],
    block_matches: tuple[BlockMatch, ...],
    existing_correction_candidates: tuple[CorrectionCandidate, ...],
    existing_review_candidates: tuple[ReviewCandidate, ...],
    existing_table_review_candidates: tuple[ReviewCandidate, ...],
    source_document_id: str,
    run_id: str,
    config: InspectionCandidateConfig,
    suppressed_collector: SuppressedCandidateCollector,
) -> tuple[InspectionCandidate, ...]:
    existing_ranges = _existing_markdown_ranges(
        existing_correction_candidates,
        existing_review_candidates,
        existing_table_review_candidates,
    )
    candidates: list[InspectionCandidate] = []
    candidates.extend(
        _line_inspection_candidates(
            markdown_lines=markdown_lines,
            extracted_lines=extracted_lines,
            line_matches=line_matches,
            source_document_id=source_document_id,
            run_id=run_id,
            config=config,
            suppressed_collector=suppressed_collector,
        )
    )
    candidates.extend(
        _block_inspection_candidates(
            markdown_blocks=markdown_blocks,
            extracted_blocks=extracted_blocks,
            block_matches=block_matches,
            source_document_id=source_document_id,
            run_id=run_id,
            config=config,
            suppressed_collector=suppressed_collector,
        )
    )
    return apply_inspection_limits(
        tuple(candidates),
        existing_ranges=existing_ranges,
        config=config,
        suppressed_collector=suppressed_collector,
    )


def apply_inspection_limits(
    candidates: tuple[InspectionCandidate, ...],
    *,
    existing_ranges: set[tuple[int, int]],
    config: InspectionCandidateConfig,
    suppressed_collector: SuppressedCandidateCollector,
) -> tuple[InspectionCandidate, ...]:
    deduped: dict[tuple[object, ...], InspectionCandidate] = {}
    for candidate in candidates:
        if candidate.score < config.min_inspection_score:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_LOW_SCORE)
            continue
        if candidate.markdown_line_range in existing_ranges:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_DUPLICATE)
            continue
        key = (
            candidate.markdown_line_range,
            candidate.extracted_line_range,
            candidate.diff_preview,
            candidate.source_method,
        )
        existing = deduped.get(key)
        if existing is None or _candidate_sort_key(candidate) < _candidate_sort_key(existing):
            if existing is not None:
                _suppress_candidate(existing, suppressed_collector, SUPPRESSED_DUPLICATE)
            deduped[key] = candidate
        else:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_DUPLICATE)

    ranked = sorted(deduped.values(), key=_candidate_sort_key)
    kept: list[InspectionCandidate] = []
    source_counts: Counter[str] = Counter()
    page_counts: Counter[int | None] = Counter()
    high_count = 0
    for candidate in ranked:
        if len(kept) >= config.max_inspection_candidates_total:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES)
            continue
        if candidate.inspection_priority == PRIORITY_HIGH and high_count >= config.max_high_priority_inspection_candidates:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES)
            continue
        if source_counts[candidate.source_method] >= config.max_inspection_candidates_per_source_method:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES)
            continue
        if page_counts[candidate.page_index] >= config.max_inspection_candidates_per_page:
            _suppress_candidate(candidate, suppressed_collector, SUPPRESSED_TOO_MANY_INSPECTION_CANDIDATES)
            continue
        display_index = len(kept) + 1
        candidate_id = stable_inspection_candidate_id(candidate)
        kept_candidate = replace(candidate, candidate_id=candidate_id, display_index=display_index)
        kept.append(kept_candidate)
        source_counts[candidate.source_method] += 1
        page_counts[candidate.page_index] += 1
        if candidate.inspection_priority == PRIORITY_HIGH:
            high_count += 1
    return tuple(kept)


def build_recommended_batches(
    candidates: tuple[InspectionCandidate, ...],
    *,
    max_batch_size: int = 50,
) -> tuple[RecommendedBatch, ...]:
    if not candidates:
        return ()
    batches: list[RecommendedBatch] = []
    sorted_candidates = sorted(candidates, key=lambda candidate: candidate.display_index)
    start = 0
    while start < len(sorted_candidates):
        chunk = sorted_candidates[start:start + max_batch_size]
        source_method = _dominant([candidate.source_method for candidate in chunk])
        priority = _dominant([candidate.inspection_priority for candidate in chunk])
        batches.append(
            RecommendedBatch(
                batch_id=f"IB{len(batches) + 1:04d}",
                display_index_start=chunk[0].display_index,
                display_index_end=chunk[-1].display_index,
                candidate_ids=tuple(candidate.candidate_id for candidate in chunk),
                count=len(chunk),
                priority=priority,
                source_method=source_method,
                recommended_assignee=_recommended_assignee(source_method),
            )
        )
        start += max_batch_size
    return tuple(batches)


def stable_inspection_candidate_id(candidate: InspectionCandidate) -> str:
    text_digest = hashlib.sha1(
        "|".join((
            _normalize(candidate.normalized_markdown_text),
            _normalize(candidate.normalized_extracted_text),
        )).encode("utf-8")
    ).hexdigest()[:6]
    payload = "|".join((
        _normalize(candidate.source_document_id),
        candidate.source_method,
        "" if candidate.page_index is None else str(candidate.page_index),
        f"{candidate.markdown_line_range[0]}-{candidate.markdown_line_range[1]}",
        ",".join(f"{page}:{line}" for page, line in candidate.extracted_line_range),
        candidate.diff_preview[:80],
        text_digest,
    ))
    return f"IC-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


def _line_inspection_candidates(
    *,
    markdown_lines: tuple[MarkdownLine, ...],
    extracted_lines: tuple[ExtractedLine, ...],
    line_matches: tuple[LineMatch, ...],
    source_document_id: str,
    run_id: str,
    config: InspectionCandidateConfig,
    suppressed_collector: SuppressedCandidateCollector,
) -> list[InspectionCandidate]:
    markdown_by_index = {line.line_index: line for line in markdown_lines}
    extracted_by_ref = {(line.page_index, line.line_index): line for line in extracted_lines}
    candidates: list[InspectionCandidate] = []
    for match in line_matches:
        if match.match_kind not in {MATCH_KIND_AMBIGUOUS, MATCH_KIND_UNMATCHED}:
            continue
        if not match.extracted_line_refs:
            continue
        markdown_text = "\n".join(
            markdown_by_index[index].text
            for index in match.markdown_line_indexes
            if index in markdown_by_index
        )
        extracted = [
            extracted_by_ref[(ref.page_index, ref.line_index)]
            for ref in match.extracted_line_refs
            if (ref.page_index, ref.line_index) in extracted_by_ref
        ][:config.max_ambiguous_alternatives]
        if not markdown_text or not extracted:
            continue
        extracted_text = "\n".join(line.text for line in extracted)
        page_index = extracted[0].page_index
        method = SOURCE_METHOD_LINE_AMBIGUOUS if match.match_kind == MATCH_KIND_AMBIGUOUS else SOURCE_METHOD_LINE_UNMATCHED
        candidates.append(
            _candidate(
                source_document_id=source_document_id,
                run_id=run_id,
                source_method=method,
                page_index=page_index,
                markdown_line_range=(min(match.markdown_line_indexes), max(match.markdown_line_indexes)),
                extracted_line_range=tuple((line.page_index, line.line_index) for line in extracted),
                markdown_text=markdown_text,
                extracted_text=extracted_text,
                score=match.score,
                reason=match.match_kind,
                risk_flags=("low_confidence_match",),
                config=config,
            )
        )
    return candidates


def _block_inspection_candidates(
    *,
    markdown_blocks: tuple[TextBlock, ...],
    extracted_blocks: tuple[TextBlock, ...],
    block_matches: tuple[BlockMatch, ...],
    source_document_id: str,
    run_id: str,
    config: InspectionCandidateConfig,
    suppressed_collector: SuppressedCandidateCollector,
) -> list[InspectionCandidate]:
    markdown_by_id = {block.block_id: block for block in markdown_blocks}
    extracted_by_id = {block.block_id: block for block in extracted_blocks}
    candidates: list[InspectionCandidate] = []
    for match in block_matches:
        if match.match_kind not in {MATCH_KIND_AMBIGUOUS, MATCH_KIND_UNMATCHED}:
            continue
        if match.extracted_block_id is None:
            continue
        markdown_block = markdown_by_id.get(match.markdown_block_id)
        extracted_block = extracted_by_id.get(match.extracted_block_id)
        if markdown_block is None or extracted_block is None:
            continue
        method = SOURCE_METHOD_BLOCK_AMBIGUOUS if match.match_kind == MATCH_KIND_AMBIGUOUS else SOURCE_METHOD_BLOCK_UNMATCHED
        candidates.append(
            _candidate(
                source_document_id=source_document_id,
                run_id=run_id,
                source_method=method,
                page_index=extracted_block.page_range[0] if extracted_block.page_range else None,
                markdown_line_range=_markdown_line_range(markdown_block),
                extracted_line_range=_extracted_line_range(extracted_block),
                markdown_text=markdown_block.text,
                extracted_text=extracted_block.text,
                score=match.score,
                reason=match.match_kind,
                risk_flags=("block_level_only", "low_confidence_match"),
                config=config,
            )
        )
    return candidates


def _candidate(
    *,
    source_document_id: str,
    run_id: str,
    source_method: str,
    page_index: int | None,
    markdown_line_range: tuple[int, int],
    extracted_line_range: tuple[tuple[int, int], ...],
    markdown_text: str,
    extracted_text: str,
    score: float,
    reason: str,
    risk_flags: tuple[str, ...],
    config: InspectionCandidateConfig,
) -> InspectionCandidate:
    normalized_markdown_text = _normalize(markdown_text)
    normalized_extracted_text = _normalize(extracted_text)
    priority = _priority(score, normalized_markdown_text, normalized_extracted_text)
    return InspectionCandidate(
        candidate_id="",
        display_index=0,
        source_document_id=source_document_id,
        run_id=run_id,
        candidate_kind="inspection",
        source_method=source_method,
        inspection_priority=priority,
        page_index=page_index,
        markdown_line_range=markdown_line_range,
        extracted_line_range=extracted_line_range,
        markdown_text=markdown_text,
        extracted_text=extracted_text,
        normalized_markdown_text=normalized_markdown_text,
        normalized_extracted_text=normalized_extracted_text,
        diff_preview=_diff_preview(normalized_markdown_text, normalized_extracted_text),
        context_before="",
        context_after="",
        reason=reason,
        score=score,
        risk_flags=risk_flags,
        apply_policy=APPLY_POLICY_NEVER_AUTO_APPLY,
        required_evidence=REQUIRED_EVIDENCE_ADJACENT_PAGES if priority == PRIORITY_LOW else REQUIRED_EVIDENCE_NONE,
    )


def _suppress_candidate(
    candidate: InspectionCandidate,
    suppressed_collector: SuppressedCandidateCollector,
    reason: str,
) -> None:
    suppressed_collector.add(
        source_method=candidate.source_method,
        page_index=candidate.page_index,
        markdown_line_range=candidate.markdown_line_range,
        extracted_line_range=candidate.extracted_line_range,
        old_text=candidate.markdown_text,
        suggested_text=candidate.extracted_text,
        diff_span=(0, 0),
        suppressed_reason=reason,
        score=candidate.score,
        risk_flags=candidate.risk_flags,
        duplicate_of_candidate_id=candidate.duplicate_of_candidate_id,
        duplicate_of_candidate_file=candidate.duplicate_of_candidate_file,
        dedupe_reason=candidate.dedupe_reason,
    )


def _existing_markdown_ranges(
    correction_candidates: tuple[CorrectionCandidate, ...],
    review_candidates: tuple[ReviewCandidate, ...],
    table_review_candidates: tuple[ReviewCandidate, ...],
) -> set[tuple[int, int]]:
    ranges: set[tuple[int, int]] = set()
    for candidate in correction_candidates:
        if candidate.markdown_line_indexes:
            ranges.add((min(candidate.markdown_line_indexes), max(candidate.markdown_line_indexes)))
    for candidate in review_candidates + table_review_candidates:
        ranges.add(candidate.markdown_line_range)
    return ranges


def _candidate_sort_key(candidate: InspectionCandidate) -> tuple[object, ...]:
    return (
        PRIORITY_RANK.get(candidate.inspection_priority, 99),
        -candidate.score,
        candidate.page_index if candidate.page_index is not None else 10**9,
        candidate.source_method,
        candidate.markdown_line_range,
    )


def _priority(score: float, markdown_text: str, extracted_text: str) -> str:
    if score >= 0.78 or markdown_text in extracted_text or extracted_text in markdown_text:
        return PRIORITY_HIGH
    if score >= 0.60:
        return PRIORITY_MEDIUM
    return PRIORITY_LOW


def _markdown_line_range(block: TextBlock) -> tuple[int, int]:
    line_indexes = [ref for ref in block.line_refs if isinstance(ref, int)]
    if not line_indexes:
        return (0, 0)
    return (min(line_indexes), max(line_indexes))


def _extracted_line_range(block: TextBlock) -> tuple[tuple[int, int], ...]:
    refs: list[tuple[int, int]] = []
    for ref in block.line_refs:
        if isinstance(ref, tuple) and len(ref) == 2:
            refs.append((int(ref[0]), int(ref[1])))
    return tuple(refs)


def _diff_preview(markdown_text: str, extracted_text: str) -> str:
    return f"{markdown_text[:80]} -> {extracted_text[:80]}"


def _dominant(values: list[str]) -> str:
    if not values:
        return ""
    return Counter(values).most_common(1)[0][0]


def _recommended_assignee(source_method: str) -> str:
    if "table" in source_method:
        return "table-review"
    if "ambiguous" in source_method:
        return "ambiguous-review"
    return "inspection-review"


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
