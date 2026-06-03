# Spike 003: Paragraph Number Candidate Collection

## Purpose

Collect and classify likely paragraph-number markers such as `２事業者は` or `２条例...` from an existing analysis DB without changing sentence splitting behavior.

This is Phase 1 report-only work. It does not update:

- `analysis_sentences`
- `analysis_paragraphs`
- existing DB files
- `split_into_sentences()` defaults
- production legal-item splitting

## Command

```sh
"/f/program_2026/csv_viewer_v2/coder_v1/.venv/Scripts/python.exe" \
  spikes/003-paragraph-number-candidates/collect_paragraph_number_candidates.py \
  --analysis-db asset/texts_2nd/analysis_a1.2.db \
  --out-dir spikes/003-paragraph-number-candidates/out \
  --context-len 80
```

For a quick smoke check:

```sh
"/f/program_2026/csv_viewer_v2/coder_v1/.venv/Scripts/python.exe" \
  spikes/003-paragraph-number-candidates/collect_paragraph_number_candidates.py \
  --analysis-db asset/texts_2nd/analysis_a1.2.db \
  --out-dir spikes/003-paragraph-number-candidates/out-smoke \
  --limit 100
```

`--run-id` is optional. When omitted, the collector uses the latest `analysis_runs.status = 'completed'` run if `analysis_runs` exists, otherwise it falls back to the maximum `analysis_sentences.run_id`. If the DB schema has no `run_id` columns, the collector runs without run scoping for minimal fixtures only.

The selected run is recorded in `paragraph_number_candidates.summary.json` as `selected_run_id`, and each output row includes `run_id`.

`--context-len` controls the number of characters kept before and after the marker in CSV context fields. The default is `80`.

## Outputs

- `paragraph_number_candidates.all.csv`: all collected rows, including rejects.
- `paragraph_number_candidates.review.csv`: high, medium, and low candidates for manual review.
- `paragraph_number_candidates.all.jsonl`: all rows with full paragraph text.
- `paragraph_number_candidates.summary.json`: counters and stratified sample data.
- `paragraph_number_candidates.samples.md`: readable samples by confidence and reject reason.

`split_decision` is always `report_only`.

## Review Procedure

1. Inspect totals in `paragraph_number_candidates.summary.json`.
2. Compare `total_broad_candidates`, `total_targeted_opener_candidates`, and `total_broad_only_candidates`.
3. Review samples for high, medium, low, broad-only, and reject buckets.
4. Check `sample_marker_value_2_without_1` and `sample_later_marker_without_prior_1` for implicit-first paragraph cases.
5. Inspect reject samples by reason to confirm marker-local negative guards are not overrejecting.

## Negative Guard Notes

Marker-local reject reasons are tied to the numeric span being classified. A legal reference elsewhere in the same sentence should be recorded as nearby context, not as a marker-local reject for a different span.

Additional negative guard categories and demotion reasons include:

- `revision_note_or_metadata`: reject reason and candidate type for revision metadata such as `(令７規則９・一部改正)` and `旧第２項`.
- `ocr_or_formatting_noise`: reject reason and candidate type for clear OCR or formatting intrusions such as `に１より`.
- `ocr_or_formatting_noise_demoted`: non-reject low-confidence demotion reason for boundary-like but incomplete cases such as `事項２。` or an empty after-context.

Guard precedence keeps these cases stable:

- In `第10条第2項`, `10` remains `multi_digit_numeric_span`, while `2` is `article_subnumber_or_citation`.
- In `別表第２(第4条関係)`, `２` is `table_or_appendix_noise`, while `4` is `article_subnumber_or_citation`.
- Protected positives such as `事項２事業者は`, `区域２周辺の者は`, and `基準２条例...` must remain in `review.csv`.
- Bracket or quote depth alone is not a reject reason; non-metadata bracket/quote candidates remain low-confidence review rows.

For before/after comparisons, write new outputs to a separate directory, for example:

```sh
"/f/program_2026/csv_viewer_v2/coder_v1/.venv/Scripts/python.exe" \
  spikes/003-paragraph-number-candidates/collect_paragraph_number_candidates.py \
  --analysis-db asset/texts_2nd/analysis_a1.2.db \
  --out-dir spikes/003-paragraph-number-candidates/out-negative-guard-check
```

Do not overwrite the baseline `out` directory when comparing summary counters.

## Non-Goals

- No automatic sentence splitting.
- No DB migration.
- No Sudachi/token table changes.
- No production integration.
- No broadening to kanji numerals or multi-digit paragraph markers beyond audit output.

Any future splitter change requires a separate design, second opinion, and explicit approval.
