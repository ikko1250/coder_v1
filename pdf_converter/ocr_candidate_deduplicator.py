from __future__ import annotations

import hashlib
import re
from dataclasses import replace

from pdf_converter.python_text_correction_model import (
    CANDIDATE_KIND_TABLE_REVIEW,
    PRIORITY_HIGH,
    PRIORITY_LOW,
    PRIORITY_MEDIUM,
    ReviewCandidate,
    ReviewCandidateConfig,
    SUPPRESSED_TOO_MANY_CANDIDATES_ON_PAGE,
    SuppressedCandidate,
)


PRIORITY_RANK = {
    PRIORITY_HIGH: 0,
    PRIORITY_MEDIUM: 1,
    PRIORITY_LOW: 2,
}


def stable_candidate_id(
    *,
    prefix: str,
    document_key: str,
    page_index: int | None,
    markdown_line_range: tuple[int, int],
    diff_span: tuple[int, int],
    source_methods: tuple[str, ...],
    old_text: str,
    suggested_text: str,
) -> str:
    payload = "|".join((
        _normalize_key_part(document_key),
        "" if page_index is None else str(page_index),
        f"{markdown_line_range[0]}-{markdown_line_range[1]}",
        f"{diff_span[0]}-{diff_span[1]}",
        ",".join(sorted(source_methods)),
        _normalize_key_part(old_text),
        _normalize_key_part(suggested_text),
    ))
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}-{digest}"


def deduplicate_review_candidates(
    candidates: tuple[ReviewCandidate, ...],
    config: ReviewCandidateConfig,
) -> tuple[tuple[ReviewCandidate, ...], tuple[SuppressedCandidate, ...]]:
    grouped: dict[tuple[object, ...], ReviewCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.candidate_kind,
            candidate.page_index,
            candidate.markdown_line_range,
            candidate.diff_span,
            _normalize_key_part(candidate.old_text),
            _normalize_key_part(candidate.suggested_text),
        )
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = candidate
            continue
        grouped[key] = _merge_candidate(existing, candidate)

    ranked = sorted(grouped.values(), key=_candidate_sort_key)
    kept: list[ReviewCandidate] = []
    suppressed: list[SuppressedCandidate] = []
    per_page_counts: dict[tuple[str, int | None], int] = {}
    for candidate in ranked:
        page_key = (candidate.candidate_kind, candidate.page_index)
        next_count = per_page_counts.get(page_key, 0) + 1
        if next_count > config.max_review_candidates_per_page:
            suppressed.append(_to_suppressed(candidate, SUPPRESSED_TOO_MANY_CANDIDATES_ON_PAGE))
            continue
        per_page_counts[page_key] = next_count
        display_index = len(kept) + 1
        prefix = "TRC" if candidate.candidate_kind == CANDIDATE_KIND_TABLE_REVIEW else "RC"
        candidate_id = stable_candidate_id(
            prefix=prefix,
            document_key=candidate.source_document_id,
            page_index=candidate.page_index,
            markdown_line_range=candidate.markdown_line_range,
            diff_span=candidate.diff_span,
            source_methods=candidate.source_methods,
            old_text=candidate.old_text,
            suggested_text=candidate.suggested_text,
        )
        kept.append(replace(candidate, candidate_id=candidate_id, display_index=display_index))
    return tuple(kept), tuple(suppressed)


def _merge_candidate(left: ReviewCandidate, right: ReviewCandidate) -> ReviewCandidate:
    source_methods = tuple(sorted(set(left.source_methods) | set(right.source_methods)))
    risk_flags = tuple(sorted(set(left.risk_flags) | set(right.risk_flags)))
    evidence = left.evidence + right.evidence
    score = max(left.score, right.score)
    priority = min((left.priority, right.priority), key=lambda priority: PRIORITY_RANK.get(priority, 99))
    reason = left.reason if left.score >= right.score else right.reason
    return replace(
        left,
        source_methods=source_methods,
        risk_flags=risk_flags,
        evidence=evidence,
        score=score,
        priority=priority,
        reason=reason,
    )


def _to_suppressed(candidate: ReviewCandidate, reason: str) -> SuppressedCandidate:
    return SuppressedCandidate(
        source_document_id=candidate.source_document_id,
        run_id=candidate.run_id,
        candidate_kind=candidate.candidate_kind,
        source_method=",".join(candidate.source_methods),
        page_index=candidate.page_index,
        markdown_line_range=candidate.markdown_line_range,
        old_text=candidate.old_text,
        suggested_text=candidate.suggested_text,
        diff_span=candidate.diff_span,
        suppressed_reason=reason,
        score=candidate.score,
        risk_flags=candidate.risk_flags,
    )


def _candidate_sort_key(candidate: ReviewCandidate) -> tuple[object, ...]:
    return (
        candidate.page_index if candidate.page_index is not None else 10**9,
        PRIORITY_RANK.get(candidate.priority, 99),
        -candidate.score,
        candidate.markdown_line_range,
        candidate.diff_span,
        candidate.old_text,
    )


def _normalize_key_part(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
