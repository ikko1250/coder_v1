from pdf_converter.ocr_inspection_candidates import (
    apply_inspection_limits,
    build_recommended_batches,
    stable_inspection_candidate_id,
)
from pdf_converter.ocr_suppressed_candidates import SuppressedCandidateCollector
from pdf_converter.python_text_correction_model import InspectionCandidate, InspectionCandidateConfig


def _candidate(index: int, *, source_method: str = "line_unmatched", score: float = 0.8) -> InspectionCandidate:
    return InspectionCandidate(
        candidate_id="",
        display_index=0,
        source_document_id="doc",
        run_id="run",
        candidate_kind="inspection",
        source_method=source_method,
        inspection_priority="high",
        page_index=0,
        markdown_line_range=(index, index),
        extracted_line_range=((0, index),),
        markdown_text=f"old {index}",
        extracted_text=f"new {index}",
        normalized_markdown_text=f"old {index}",
        normalized_extracted_text=f"new {index}",
        diff_preview=f"old {index} -> new {index}",
        context_before="",
        context_after="",
        reason="test",
        score=score,
    )


def test_apply_inspection_limits_assigns_stable_ids_and_limits_total():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")
    candidates = tuple(_candidate(index) for index in range(3))

    kept = apply_inspection_limits(
        candidates,
        existing_ranges=set(),
        config=InspectionCandidateConfig(max_inspection_candidates_total=2),
        suppressed_collector=collector,
    )

    assert len(kept) == 2
    assert kept[0].candidate_id.startswith("IC-")
    assert collector.records()[0].suppressed_reason == "too_many_inspection_candidates"


def test_apply_inspection_limits_suppresses_existing_candidate_ranges():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")

    kept = apply_inspection_limits(
        (_candidate(1),),
        existing_ranges={(1, 1)},
        config=InspectionCandidateConfig(),
        suppressed_collector=collector,
    )

    assert kept == ()
    assert collector.records()[0].suppressed_reason == "duplicate"


def test_build_recommended_batches_uses_display_index_and_candidate_ids():
    first = _candidate(1)
    second = _candidate(2)
    first = first.__class__(**{**first.__dict__, "candidate_id": stable_inspection_candidate_id(first), "display_index": 1})
    second = second.__class__(**{**second.__dict__, "candidate_id": stable_inspection_candidate_id(second), "display_index": 2})

    batches = build_recommended_batches((first, second), max_batch_size=1)

    assert len(batches) == 2
    assert batches[0].display_index_start == 1
    assert batches[0].candidate_ids == (first.candidate_id,)
