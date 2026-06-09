from __future__ import annotations

import difflib
import re
import string

from pdf_converter.python_text_correction_model import (
    CANDIDATE_KIND_REVIEW,
    BlockMatch,
    PRIORITY_HIGH,
    PRIORITY_MEDIUM,
    ReviewCandidate,
    ReviewCandidateConfig,
    SOURCE_METHOD_NGRAM_DIFF,
    SUPPRESSED_AMBIGUOUS_ANCHOR,
    SUPPRESSED_LARGE_DIFF,
    SUPPRESSED_LOW_SCORE,
    SUPPRESSED_WIDTH_OR_SYMBOL_ONLY,
    SuppressedCandidate,
    TextBlock,
)


PUNCTUATION_CHARS = set(string.punctuation) | set("、。，．・：；！？（）［］【】「」『』〈〉《》〔〕％￥")


def build_ngram_review_candidates(
    *,
    markdown_blocks: tuple[TextBlock, ...],
    extracted_blocks: tuple[TextBlock, ...],
    block_matches: tuple[BlockMatch, ...],
    source_document_id: str,
    run_id: str,
    config: ReviewCandidateConfig,
) -> tuple[tuple[ReviewCandidate, ...], tuple[SuppressedCandidate, ...]]:
    markdown_by_id = {block.block_id: block for block in markdown_blocks}
    extracted_by_id = {block.block_id: block for block in extracted_blocks}
    candidates: list[ReviewCandidate] = []
    suppressed: list[SuppressedCandidate] = []
    for match in block_matches:
        if match.extracted_block_id is None:
            continue
        if match.alignment_confidence == "low" or match.score < config.block_weak_near_score_threshold:
            continue
        markdown_block = markdown_by_id[match.markdown_block_id]
        extracted_block = extracted_by_id[match.extracted_block_id]
        block_candidates, block_suppressed = _diff_block(
            markdown_block=markdown_block,
            extracted_block=extracted_block,
            block_match=match,
            source_document_id=source_document_id,
            run_id=run_id,
            config=config,
        )
        candidates.extend(block_candidates)
        suppressed.extend(block_suppressed)
    return tuple(candidates), tuple(suppressed)


def _diff_block(
    *,
    markdown_block: TextBlock,
    extracted_block: TextBlock,
    block_match: BlockMatch,
    source_document_id: str,
    run_id: str,
    config: ReviewCandidateConfig,
) -> tuple[list[ReviewCandidate], list[SuppressedCandidate]]:
    matcher = difflib.SequenceMatcher(None, markdown_block.normalized_text, extracted_block.normalized_text)
    candidates: list[ReviewCandidate] = []
    suppressed: list[SuppressedCandidate] = []
    markdown_line_range = _markdown_line_range(markdown_block)
    extracted_line_range = _extracted_line_range(extracted_block)
    page_index = extracted_block.page_range[0] if extracted_block.page_range else None
    for tag, left_start, left_end, right_start, right_end in matcher.get_opcodes():
        if tag == "equal":
            continue
        old_text = markdown_block.normalized_text[left_start:left_end]
        suggested_text = extracted_block.normalized_text[right_start:right_end]
        reason = _suppression_reason(old_text, suggested_text, config)
        if reason is not None:
            suppressed.append(
                SuppressedCandidate(
                    source_document_id=source_document_id,
                    run_id=run_id,
                    candidate_kind=CANDIDATE_KIND_REVIEW,
                    source_method=SOURCE_METHOD_NGRAM_DIFF,
                    page_index=page_index,
                    markdown_line_range=markdown_line_range,
                    old_text=old_text,
                    suggested_text=suggested_text,
                    diff_span=(left_start, left_end),
                    suppressed_reason=reason,
                    score=block_match.score,
                    risk_flags=("block_level_only",),
                )
            )
            continue
        if _anchor_is_ambiguous(markdown_block.normalized_text, left_start, left_end):
            suppressed.append(
                SuppressedCandidate(
                    source_document_id=source_document_id,
                    run_id=run_id,
                    candidate_kind=CANDIDATE_KIND_REVIEW,
                    source_method=SOURCE_METHOD_NGRAM_DIFF,
                    page_index=page_index,
                    markdown_line_range=markdown_line_range,
                    old_text=old_text,
                    suggested_text=suggested_text,
                    diff_span=(left_start, left_end),
                    suppressed_reason=SUPPRESSED_AMBIGUOUS_ANCHOR,
                    score=block_match.score,
                    risk_flags=("block_level_only",),
                )
            )
            continue
        context_before = markdown_block.normalized_text[max(0, left_start - config.max_review_context_chars):left_start]
        context_after = markdown_block.normalized_text[left_end:left_end + config.max_review_context_chars]
        priority = PRIORITY_HIGH if block_match.score >= 0.86 else PRIORITY_MEDIUM
        candidates.append(
            ReviewCandidate(
                candidate_id="",
                display_index=0,
                source_document_id=source_document_id,
                run_id=run_id,
                candidate_kind=CANDIDATE_KIND_REVIEW,
                source_methods=(SOURCE_METHOD_NGRAM_DIFF,),
                priority=priority,
                page_index=page_index,
                markdown_line_range=markdown_line_range,
                extracted_line_range=extracted_line_range,
                old_text=old_text,
                suggested_text=suggested_text,
                diff_span=(left_start, left_end),
                context_before=context_before,
                context_after=context_after,
                reason="ngram_local_diff",
                score=block_match.score,
                risk_flags=("block_level_only",),
                evidence=({"block_match_id": block_match.block_match_id},),
            )
        )
    return candidates, suppressed


def _suppression_reason(old_text: str, suggested_text: str, config: ReviewCandidateConfig) -> str | None:
    if not old_text.strip() or not suggested_text.strip():
        return SUPPRESSED_WIDTH_OR_SYMBOL_ONLY
    if old_text == suggested_text:
        return SUPPRESSED_WIDTH_OR_SYMBOL_ONLY
    if len(old_text) > config.max_diff_chars or len(suggested_text) > config.max_diff_chars:
        return SUPPRESSED_LARGE_DIFF
    if _strip_spaces(old_text) == _strip_spaces(suggested_text):
        return SUPPRESSED_WIDTH_OR_SYMBOL_ONLY
    if _strip_punctuation(old_text) == _strip_punctuation(suggested_text):
        return SUPPRESSED_WIDTH_OR_SYMBOL_ONLY
    if _is_symbol_only(old_text) or _is_symbol_only(suggested_text):
        return SUPPRESSED_WIDTH_OR_SYMBOL_ONLY
    return None


def _anchor_is_ambiguous(text: str, start: int, end: int) -> bool:
    left_anchor = text[max(0, start - 4):start]
    right_anchor = text[end:end + 4]
    if left_anchor and text.count(left_anchor) > 3:
        return True
    if right_anchor and text.count(right_anchor) > 3:
        return True
    return False


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


def _strip_spaces(text: str) -> str:
    return re.sub(r"\s+", "", text)


def _strip_punctuation(text: str) -> str:
    return "".join(char for char in _strip_spaces(text) if char not in PUNCTUATION_CHARS)


def _is_symbol_only(text: str) -> bool:
    stripped = _strip_spaces(text)
    return bool(stripped) and all(char in PUNCTUATION_CHARS for char in stripped)
