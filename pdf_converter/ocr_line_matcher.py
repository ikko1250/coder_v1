from __future__ import annotations

import difflib
import re
import string

from pdf_converter.python_text_correction_model import (
    CorrectionCandidate,
    ExtractedLine,
    ExtractedLineRef,
    LINE_KIND_BODY,
    LineMatch,
    LineMatchingConfig,
    MATCH_KIND_AMBIGUOUS,
    MATCH_KIND_EXACT,
    MATCH_KIND_NEAR,
    MATCH_KIND_NORMALIZED_EXACT,
    MATCH_KIND_UNMATCHED,
    MarkdownLine,
    WARNING_LOW_CONFIDENCE_MATCH,
)


PUNCTUATION_CHARS = set(string.punctuation) | set("、。，．・：；！？（）［］【】「」『』〈〉《》〔〕")


def _body_markdown_lines(lines: tuple[MarkdownLine, ...]) -> list[MarkdownLine]:
    return [
        line for line in lines
        if line.kind == LINE_KIND_BODY and line.normalized_text
    ]


def _body_extracted_lines(lines: tuple[ExtractedLine, ...]) -> list[ExtractedLine]:
    return [
        line for line in lines
        if line.kind == LINE_KIND_BODY and line.normalized_text and line.confidence > 0.0
    ]


def _score(left: str, right: str) -> float:
    return difflib.SequenceMatcher(None, left, right).ratio()


def _line_ref(line: ExtractedLine) -> ExtractedLineRef:
    return ExtractedLineRef(page_index=line.page_index, line_index=line.line_index)


def match_lines(
    markdown_lines: tuple[MarkdownLine, ...],
    extracted_lines: tuple[ExtractedLine, ...],
    config: LineMatchingConfig | None = None,
) -> tuple[LineMatch, ...]:
    if config is None:
        config = LineMatchingConfig()

    body_extracted = _body_extracted_lines(extracted_lines)
    extracted_by_normalized: dict[str, list[ExtractedLine]] = {}
    extracted_indexes: dict[tuple[int, int], int] = {}
    for extracted_index, extracted in enumerate(body_extracted):
        extracted_by_normalized.setdefault(extracted.normalized_text, []).append(extracted)
        extracted_indexes[(extracted.page_index, extracted.line_index)] = extracted_index

    used_indexes: set[int] = set()
    matches: list[LineMatch] = []
    cursor_index = 0
    for markdown in _body_markdown_lines(markdown_lines):
        exact_candidates = [
            candidate for candidate in extracted_by_normalized.get(markdown.normalized_text, [])
            if extracted_indexes[(candidate.page_index, candidate.line_index)] not in used_indexes
        ]
        if len(exact_candidates) == 1:
            candidate = exact_candidates[0]
            candidate_index = extracted_indexes[(candidate.page_index, candidate.line_index)]
            used_indexes.add(candidate_index)
            cursor_index = max(cursor_index, candidate_index + 1)
            match_kind = MATCH_KIND_EXACT if markdown.text == candidate.text else MATCH_KIND_NORMALIZED_EXACT
            matches.append(
                LineMatch(
                    markdown_line_indexes=(markdown.line_index,),
                    extracted_line_refs=(_line_ref(candidate),),
                    match_kind=match_kind,
                    score=1.0,
                )
            )
            continue
        if len(exact_candidates) > 1:
            nearby_candidates = [
                candidate for candidate in exact_candidates
                if abs(extracted_indexes[(candidate.page_index, candidate.line_index)] - cursor_index)
                <= config.near_window_size
            ]
            if len(nearby_candidates) == 1:
                candidate = nearby_candidates[0]
                candidate_index = extracted_indexes[(candidate.page_index, candidate.line_index)]
                used_indexes.add(candidate_index)
                cursor_index = max(cursor_index, candidate_index + 1)
                matches.append(
                    LineMatch(
                        markdown_line_indexes=(markdown.line_index,),
                        extracted_line_refs=(_line_ref(candidate),),
                        match_kind=MATCH_KIND_EXACT if markdown.text == candidate.text else MATCH_KIND_NORMALIZED_EXACT,
                        score=1.0,
                    )
                )
                continue
            matches.append(
                LineMatch(
                    markdown_line_indexes=(markdown.line_index,),
                    extracted_line_refs=tuple(_line_ref(candidate) for candidate in exact_candidates[:3]),
                    match_kind=MATCH_KIND_AMBIGUOUS,
                    score=1.0,
                    warning_codes=(WARNING_LOW_CONFIDENCE_MATCH,),
                )
            )
            continue

        near_match = _find_near_match(markdown, body_extracted, used_indexes, cursor_index, config)
        matches.append(near_match)
        if near_match.match_kind in {MATCH_KIND_NEAR, MATCH_KIND_NORMALIZED_EXACT, MATCH_KIND_EXACT}:
            for ref in near_match.extracted_line_refs:
                matched_index = extracted_indexes.get((ref.page_index, ref.line_index))
                if matched_index is not None:
                    used_indexes.add(matched_index)
                    cursor_index = max(cursor_index, matched_index + 1)
        else:
            cursor_index = min(len(body_extracted), cursor_index + 1)

    return tuple(matches)


def _find_near_match(
    markdown: MarkdownLine,
    extracted_lines: list[ExtractedLine],
    used_indexes: set[int],
    cursor_index: int,
    config: LineMatchingConfig,
) -> LineMatch:
    search_start = max(0, cursor_index - config.near_window_size)
    search_end = min(len(extracted_lines), cursor_index + config.near_window_size + 1)
    candidates: list[tuple[float, tuple[ExtractedLine, ...]]] = []
    for index in range(search_start, search_end):
        if index in used_indexes:
            continue
        extracted = extracted_lines[index]
        candidates.append((_score(markdown.normalized_text, extracted.normalized_text), (extracted,)))
        for merge_count in range(2, config.max_merged_extracted_lines + 1):
            merged_indexes = list(range(index, min(index + merge_count, len(extracted_lines))))
            if any(merged_index in used_indexes for merged_index in merged_indexes):
                continue
            merged = [extracted_lines[merged_index] for merged_index in merged_indexes]
            if len(merged) != merge_count:
                continue
            merged_text = " ".join(line.normalized_text for line in merged)
            candidates.append((_score(markdown.normalized_text, merged_text), tuple(merged)))

    if not candidates:
        return LineMatch(
            markdown_line_indexes=(markdown.line_index,),
            extracted_line_refs=(),
            match_kind=MATCH_KIND_UNMATCHED,
            score=0.0,
            warning_codes=(WARNING_LOW_CONFIDENCE_MATCH,),
        )

    candidates.sort(key=lambda item: item[0], reverse=True)
    best_score, best_lines = candidates[0]
    if best_score < config.near_score_threshold:
        return LineMatch(
            markdown_line_indexes=(markdown.line_index,),
            extracted_line_refs=tuple(_line_ref(line) for line in best_lines),
            match_kind=MATCH_KIND_UNMATCHED,
            score=best_score,
            warning_codes=(WARNING_LOW_CONFIDENCE_MATCH,),
        )

    if len(candidates) > 1 and best_score - candidates[1][0] <= config.ambiguous_score_margin:
        candidate_lines = best_lines + candidates[1][1]
        return LineMatch(
            markdown_line_indexes=(markdown.line_index,),
            extracted_line_refs=tuple(_line_ref(line) for line in candidate_lines),
            match_kind=MATCH_KIND_AMBIGUOUS,
            score=best_score,
            warning_codes=(WARNING_LOW_CONFIDENCE_MATCH,),
        )

    return LineMatch(
        markdown_line_indexes=(markdown.line_index,),
        extracted_line_refs=tuple(_line_ref(line) for line in best_lines),
        match_kind=MATCH_KIND_NEAR,
        score=best_score,
    )


def build_correction_candidates(
    markdown_lines: tuple[MarkdownLine, ...],
    extracted_lines: tuple[ExtractedLine, ...],
    matches: tuple[LineMatch, ...],
) -> tuple[CorrectionCandidate, ...]:
    markdown_by_index = {line.line_index: line for line in markdown_lines}
    extracted_by_ref = {
        (line.page_index, line.line_index): line
        for line in extracted_lines
    }
    candidates: list[CorrectionCandidate] = []
    for match in matches:
        if match.match_kind != MATCH_KIND_NEAR:
            continue
        if match.warning_codes:
            continue
        old_text = "\n".join(markdown_by_index[index].text for index in match.markdown_line_indexes)
        suggested_text = "\n".join(
            extracted_by_ref[(ref.page_index, ref.line_index)].text
            for ref in match.extracted_line_refs
            if (ref.page_index, ref.line_index) in extracted_by_ref
        )
        if not _is_typo_candidate(old_text, suggested_text):
            continue
        candidate_index = len(candidates) + 1
        candidates.append(
            CorrectionCandidate(
                candidate_id=f"C{candidate_index:04d}",
                markdown_line_indexes=match.markdown_line_indexes,
                extracted_line_refs=match.extracted_line_refs,
                old_text=old_text,
                suggested_text=suggested_text,
                reason="typo_candidate",
                score=match.score,
            )
        )
    return tuple(candidates)


def low_confidence_ratio(matches: tuple[LineMatch, ...]) -> float:
    if not matches:
        return 0.0
    low_count = sum(
        1 for match in matches
        if match.match_kind in {MATCH_KIND_UNMATCHED, MATCH_KIND_AMBIGUOUS} or match.warning_codes
    )
    return low_count / len(matches)


def _is_typo_candidate(old_text: str, suggested_text: str) -> bool:
    if not old_text.strip() or not suggested_text.strip():
        return False
    if old_text == suggested_text:
        return False
    if _strip_spaces(old_text) == _strip_spaces(suggested_text):
        return False
    if _strip_punctuation(old_text) == _strip_punctuation(suggested_text):
        return False
    if "\n" in old_text or "\n" in suggested_text:
        return False
    old_len = len(old_text)
    new_len = len(suggested_text)
    if max(old_len, new_len) > 160:
        return False
    if abs(old_len - new_len) > max(3, int(max(old_len, new_len) * 0.12)):
        return False
    return True


def _strip_spaces(text: str) -> str:
    return re.sub(r"\s+", "", text)


def _strip_punctuation(text: str) -> str:
    return "".join(char for char in _strip_spaces(text) if char not in PUNCTUATION_CHARS)
