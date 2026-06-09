from pdf_converter.ocr_inspection_candidates import (
    apply_inspection_limits,
    build_recommended_batches,
    stable_inspection_candidate_id,
)
from pdf_converter.ocr_suppressed_candidates import SuppressedCandidateCollector
from dataclasses import replace

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


def test_apply_inspection_limits_suppresses_duplicate_extracted_text():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")
    candidate = replace(
        _candidate(1),
        markdown_text="同じ注記です。",
        extracted_text="同じ注記です。\n同じ注記です。",
        normalized_markdown_text="同じ注記です。",
        normalized_extracted_text="同じ注記です。 同じ注記です。",
        extracted_line_range=((0, 1), (0, 2)),
    )

    kept = apply_inspection_limits(
        (candidate,),
        existing_ranges=set(),
        config=InspectionCandidateConfig(),
        suppressed_collector=collector,
    )

    assert kept == ()
    assert collector.records()[0].suppressed_reason == "duplicate_extracted_text"


def test_apply_inspection_limits_keeps_semantic_symbol_diff():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")
    candidate = replace(
        _candidate(1, score=0.9),
        markdown_text="· 木南代表取締役社長",
        extracted_text="・木南代表取締役社長",
        normalized_markdown_text="· 木南代表取締役社長",
        normalized_extracted_text="・木南代表取締役社長",
        inspection_priority="low",
    )

    kept = apply_inspection_limits(
        (candidate,),
        existing_ranges=set(),
        config=InspectionCandidateConfig(),
        suppressed_collector=collector,
    )

    assert len(kept) == 1
    assert kept[0].inspection_priority == "medium"
    assert "semantic_symbol_diff" in kept[0].risk_flags
    assert collector.records() == ()


def test_apply_inspection_limits_suppresses_format_symbol_diff():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")
    candidate = replace(
        _candidate(1, score=0.9),
        markdown_text="9/9回中(100%)",
        extracted_text="9/9回中（100%）",
        normalized_markdown_text="9/9回中(100%)",
        normalized_extracted_text="9/9回中（100%）",
    )

    kept = apply_inspection_limits(
        (candidate,),
        existing_ranges=set(),
        config=InspectionCandidateConfig(),
        suppressed_collector=collector,
    )

    assert kept == ()
    assert collector.records()[0].suppressed_reason == "format_symbol_diff"


def test_apply_inspection_limits_suppresses_partial_segment_without_guard():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")
    markdown_text = "これは長い段落です。" * 12
    extracted_text = "これは長い段落です。" * 3
    candidate = replace(
        _candidate(1, score=0.5),
        markdown_text=markdown_text,
        extracted_text=extracted_text,
        normalized_markdown_text=markdown_text,
        normalized_extracted_text=extracted_text,
    )

    kept = apply_inspection_limits(
        (candidate,),
        existing_ranges=set(),
        config=InspectionCandidateConfig(),
        suppressed_collector=collector,
    )

    assert kept == ()
    assert collector.records()[0].suppressed_reason == "partial_segment_match"
