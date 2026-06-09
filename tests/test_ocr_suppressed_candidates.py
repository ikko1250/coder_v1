from pdf_converter.ocr_suppressed_candidates import SuppressedCandidateCollector


def test_suppressed_candidate_collector_deduplicates_records_but_counts_events():
    collector = SuppressedCandidateCollector(source_document_id="doc", run_id="run")

    first = collector.add(
        source_method="ngram_diff",
        page_index=0,
        markdown_line_range=(1, 1),
        old_text="%",
        suggested_text="％",
        diff_span=(3, 4),
        suppressed_reason="width_or_symbol_only",
        score=0.9,
    )
    second = collector.add(
        source_method="ngram_diff",
        page_index=0,
        markdown_line_range=(1, 1),
        old_text="%",
        suggested_text="％",
        diff_span=(3, 4),
        suppressed_reason="width_or_symbol_only",
        score=0.9,
    )

    assert first.record_id == second.record_id
    assert collector.event_count == 2
    assert len(collector.records()) == 1
    assert collector.records()[0].record_id.startswith("SC-")
