from __future__ import annotations

import hashlib
import re
from dataclasses import replace

from pdf_converter.python_text_correction_model import (
    SuppressedCandidate,
    SuppressedCandidateRecord,
)


def stable_suppressed_record_id(
    *,
    source_document_id: str,
    source_method: str,
    page_index: int | None,
    markdown_line_range: tuple[int, int],
    extracted_line_range: tuple[tuple[int, int], ...],
    diff_span: tuple[int, int],
    old_text: str,
    suggested_text: str,
    suppressed_reason: str,
) -> str:
    payload = "|".join((
        _normalize(source_document_id),
        source_method,
        "" if page_index is None else str(page_index),
        f"{markdown_line_range[0]}-{markdown_line_range[1]}",
        ",".join(f"{page}:{line}" for page, line in extracted_line_range),
        f"{diff_span[0]}-{diff_span[1]}",
        _normalize(old_text),
        _normalize(suggested_text),
        suppressed_reason,
    ))
    return f"SC-{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:12]}"


class SuppressedCandidateCollector:
    def __init__(self, *, source_document_id: str, run_id: str) -> None:
        self.source_document_id = source_document_id
        self.run_id = run_id
        self.event_count = 0
        self._records: dict[tuple[object, ...], SuppressedCandidateRecord] = {}

    def add(
        self,
        *,
        source_method: str,
        page_index: int | None,
        markdown_line_range: tuple[int, int],
        extracted_line_range: tuple[tuple[int, int], ...] = (),
        old_text: str,
        suggested_text: str,
        diff_span: tuple[int, int] = (0, 0),
        suppressed_reason: str,
        score: float,
        risk_flags: tuple[str, ...] = (),
        promoted_to_inspection: bool = False,
        duplicate_of_candidate_id: str | None = None,
        duplicate_of_candidate_file: str | None = None,
        dedupe_reason: str | None = None,
    ) -> SuppressedCandidateRecord:
        self.event_count += 1
        key = (
            source_method,
            page_index,
            markdown_line_range,
            extracted_line_range,
            _normalize(old_text),
            _normalize(suggested_text),
            diff_span,
            suppressed_reason,
        )
        existing = self._records.get(key)
        if existing is not None:
            if promoted_to_inspection and not existing.promoted_to_inspection:
                existing = replace(existing, promoted_to_inspection=True)
                self._records[key] = existing
            return existing
        record = SuppressedCandidateRecord(
            record_id=stable_suppressed_record_id(
                source_document_id=self.source_document_id,
                source_method=source_method,
                page_index=page_index,
                markdown_line_range=markdown_line_range,
                extracted_line_range=extracted_line_range,
                diff_span=diff_span,
                old_text=old_text,
                suggested_text=suggested_text,
                suppressed_reason=suppressed_reason,
            ),
            source_document_id=self.source_document_id,
            run_id=self.run_id,
            source_method=source_method,
            page_index=page_index,
            markdown_line_range=markdown_line_range,
            extracted_line_range=extracted_line_range,
            old_text=old_text,
            suggested_text=suggested_text,
            diff_span=diff_span,
            suppressed_reason=suppressed_reason,
            score=score,
            risk_flags=risk_flags,
            promoted_to_inspection=promoted_to_inspection,
            duplicate_of_candidate_id=duplicate_of_candidate_id,
            duplicate_of_candidate_file=duplicate_of_candidate_file,
            dedupe_reason=dedupe_reason,
        )
        self._records[key] = record
        return record

    def add_legacy(self, candidate: SuppressedCandidate) -> SuppressedCandidateRecord:
        return self.add(
            source_method=candidate.source_method,
            page_index=candidate.page_index,
            markdown_line_range=candidate.markdown_line_range,
            old_text=candidate.old_text,
            suggested_text=candidate.suggested_text,
            diff_span=candidate.diff_span,
            suppressed_reason=candidate.suppressed_reason,
            score=candidate.score,
            risk_flags=candidate.risk_flags,
        )

    def records(self) -> tuple[SuppressedCandidateRecord, ...]:
        return tuple(sorted(self._records.values(), key=lambda record: record.record_id))


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()
