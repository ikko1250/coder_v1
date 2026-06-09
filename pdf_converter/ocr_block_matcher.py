from __future__ import annotations

import difflib

from pdf_converter.python_text_correction_model import (
    BlockMatch,
    ExtractedLine,
    LINE_KIND_BODY,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_NEAR,
    MATCH_KIND_UNMATCHED,
    MATCH_KIND_WEAK_NEAR,
    MarkdownLine,
    ReviewCandidateConfig,
    SUPPRESSED_LARGE_DIFF,
    SUPPRESSED_LOW_SCORE,
    TextBlock,
)


def build_markdown_blocks(
    lines: tuple[MarkdownLine, ...],
    config: ReviewCandidateConfig,
) -> tuple[TextBlock, ...]:
    blocks: list[TextBlock] = []
    current: list[MarkdownLine] = []
    for line in lines:
        if line.kind != LINE_KIND_BODY or not line.normalized_text:
            _flush_markdown_block(blocks, current, config)
            current = []
            continue
        current.append(line)
        if sum(len(item.normalized_text) for item in current) >= config.max_block_chars:
            _flush_markdown_block(blocks, current, config, forced_warning=SUPPRESSED_LARGE_DIFF)
            current = []
    _flush_markdown_block(blocks, current, config)
    return tuple(blocks)


def build_extracted_blocks(
    lines: tuple[ExtractedLine, ...],
    config: ReviewCandidateConfig,
) -> tuple[TextBlock, ...]:
    blocks: list[TextBlock] = []
    current: list[ExtractedLine] = []
    for line in lines:
        if line.kind != LINE_KIND_BODY or not line.normalized_text or line.confidence <= 0:
            _flush_extracted_block(blocks, current, config)
            current = []
            continue
        current.append(line)
        if sum(len(item.normalized_text) for item in current) >= config.max_block_chars:
            _flush_extracted_block(blocks, current, config, forced_warning=SUPPRESSED_LARGE_DIFF)
            current = []
    _flush_extracted_block(blocks, current, config)
    return tuple(blocks)


def match_blocks(
    markdown_blocks: tuple[TextBlock, ...],
    extracted_blocks: tuple[TextBlock, ...],
    config: ReviewCandidateConfig,
) -> tuple[BlockMatch, ...]:
    matches: list[BlockMatch] = []
    used_extracted_indexes: set[int] = set()
    cursor = 0
    for markdown_index, markdown_block in enumerate(markdown_blocks):
        candidates: list[tuple[float, int, TextBlock]] = []
        search_start = max(0, cursor - 2)
        search_end = min(len(extracted_blocks), cursor + 4)
        for extracted_index in range(search_start, search_end):
            if extracted_index in used_extracted_indexes:
                continue
            extracted_block = extracted_blocks[extracted_index]
            score = difflib.SequenceMatcher(
                None,
                markdown_block.normalized_text,
                extracted_block.normalized_text,
            ).ratio()
            candidates.append((score, extracted_index, extracted_block))
        if not candidates:
            matches.append(_unmatched(markdown_index, markdown_block))
            continue

        candidates.sort(key=lambda item: item[0], reverse=True)
        best_score, best_index, best_block = candidates[0]
        ambiguity_count = sum(
            1 for score, _, _ in candidates[1:]
            if best_score - score <= config.block_ambiguous_score_margin
        )
        if ambiguity_count:
            matches.append(
                BlockMatch(
                    block_match_id=f"BM{markdown_index + 1:04d}",
                    markdown_block_id=markdown_block.block_id,
                    extracted_block_id=best_block.block_id,
                    match_kind=MATCH_KIND_AMBIGUOUS,
                    score=best_score,
                    alignment_confidence="low",
                    ambiguity_count=ambiguity_count,
                    candidate_suppression_reason=SUPPRESSED_LOW_SCORE,
                )
            )
            continue
        if best_score >= config.block_near_score_threshold:
            match_kind = MATCH_KIND_NEAR
            alignment_confidence = "high" if best_score >= 0.86 else "medium"
            suppression_reason = None
            used_extracted_indexes.add(best_index)
            cursor = max(cursor, best_index + 1)
        elif best_score >= config.block_weak_near_score_threshold:
            match_kind = MATCH_KIND_WEAK_NEAR
            alignment_confidence = "medium"
            suppression_reason = None
            used_extracted_indexes.add(best_index)
            cursor = max(cursor, best_index + 1)
        else:
            match_kind = MATCH_KIND_UNMATCHED
            alignment_confidence = "low"
            suppression_reason = SUPPRESSED_LOW_SCORE
        matches.append(
            BlockMatch(
                block_match_id=f"BM{markdown_index + 1:04d}",
                markdown_block_id=markdown_block.block_id,
                extracted_block_id=best_block.block_id,
                match_kind=match_kind,
                score=best_score,
                alignment_confidence=alignment_confidence,
                ambiguity_count=0,
                candidate_suppression_reason=suppression_reason,
            )
        )
    return tuple(matches)


def _flush_markdown_block(
    blocks: list[TextBlock],
    lines: list[MarkdownLine],
    config: ReviewCandidateConfig,
    forced_warning: str | None = None,
) -> None:
    if not lines:
        return
    block_index = len(blocks) + 1
    text = "\n".join(line.text for line in lines)
    normalized = " ".join(line.normalized_text for line in lines)
    warning_codes = (forced_warning,) if forced_warning else ()
    blocks.append(
        TextBlock(
            block_id=f"MB{block_index:04d}",
            source="markdown",
            page_range=None,
            line_refs=tuple(line.line_index for line in lines),
            text=text,
            normalized_text=normalized[:config.max_block_chars],
            kind=LINE_KIND_BODY,
            warning_codes=warning_codes,
        )
    )


def _flush_extracted_block(
    blocks: list[TextBlock],
    lines: list[ExtractedLine],
    config: ReviewCandidateConfig,
    forced_warning: str | None = None,
) -> None:
    if not lines:
        return
    block_index = len(blocks) + 1
    text = "\n".join(line.text for line in lines)
    normalized = " ".join(line.normalized_text for line in lines)
    page_indexes = [line.page_index for line in lines]
    warning_codes = (forced_warning,) if forced_warning else ()
    blocks.append(
        TextBlock(
            block_id=f"EB{block_index:04d}",
            source="pdf_text",
            page_range=(min(page_indexes), max(page_indexes)),
            line_refs=tuple((line.page_index, line.line_index) for line in lines),
            text=text,
            normalized_text=normalized[:config.max_block_chars],
            kind=LINE_KIND_BODY,
            confidence=min(line.confidence for line in lines),
            warning_codes=warning_codes,
        )
    )


def _unmatched(markdown_index: int, markdown_block: TextBlock) -> BlockMatch:
    return BlockMatch(
        block_match_id=f"BM{markdown_index + 1:04d}",
        markdown_block_id=markdown_block.block_id,
        extracted_block_id=None,
        match_kind=MATCH_KIND_UNMATCHED,
        score=0.0,
        alignment_confidence="low",
        candidate_suppression_reason=SUPPRESSED_LOW_SCORE,
    )
