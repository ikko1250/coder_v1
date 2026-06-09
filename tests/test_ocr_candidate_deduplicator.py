from pdf_converter.ocr_candidate_deduplicator import deduplicate_review_candidates
from pdf_converter.python_text_correction_model import ReviewCandidate, ReviewCandidateConfig


def _candidate(source_method: str, score: float = 0.8) -> ReviewCandidate:
    return ReviewCandidate(
        candidate_id="",
        display_index=0,
        source_document_id="doc",
        run_id="run",
        candidate_kind="review",
        source_methods=(source_method,),
        priority="medium",
        page_index=0,
        markdown_line_range=(1, 1),
        extracted_line_range=((0, 1),),
        old_text="議渡",
        suggested_text="譲渡",
        diff_span=(4, 6),
        context_before="株式の",
        context_after="制限",
        reason=source_method,
        score=score,
        risk_flags=("known_pattern",),
    )


def test_deduplicate_review_candidates_merges_same_span():
    kept, suppressed = deduplicate_review_candidates(
        (_candidate("known_ocr_pattern"), _candidate("ngram_diff", 0.9)),
        ReviewCandidateConfig(),
    )

    assert len(kept) == 1
    assert suppressed == ()
    assert kept[0].candidate_id.startswith("RC-")
    assert kept[0].source_methods == ("known_ocr_pattern", "ngram_diff")
    assert kept[0].score == 0.9


def test_deduplicate_review_candidates_suppresses_after_page_limit():
    kept, suppressed = deduplicate_review_candidates(
        (_candidate("known_ocr_pattern"),),
        ReviewCandidateConfig(max_review_candidates_per_page=0),
    )

    assert kept == ()
    assert suppressed[0].suppressed_reason == "too_many_candidates_on_page"
