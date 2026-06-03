from __future__ import annotations

import importlib.util
import csv
import json
import shutil
import sqlite3
import sys
import tempfile
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path


def _load_collector_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "spikes"
        / "003-paragraph-number-candidates"
        / "collect_paragraph_number_candidates.py"
    )
    spec = importlib.util.spec_from_file_location("paragraph_number_candidate_collector", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


collector = _load_collector_module()
PROJECT_ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def temporaryWorkspaceDirectory():
    root = PROJECT_ROOT / ".test_tmp"
    root.mkdir(exist_ok=True)
    path = root / f"paragraph-number-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield str(path)
    finally:
        shutil.rmtree(path, ignore_errors=True)


class ParagraphNumberCandidateCollectorTest(unittest.TestCase):
    def _row(self, sentence: str, paragraph: str | None = None, isTable: int = 0):
        return collector.SentenceRow(
            runId=1,
            sentenceId=1,
            paragraphId=2,
            documentId=3,
            fileName="fixture.txt",
            sourceFilePath="fixture.txt",
            isTableParagraph=isTable,
            sentenceNoInDocument=1,
            sentenceNoInParagraph=1,
            sentenceText=sentence,
            paragraphText=paragraph or sentence,
            sentenceCharOffsetInParagraph=0,
            offsetResolutionReason="test",
        )

    def _assertSelfMatchedReject(self, candidate) -> None:
        self.assertEqual(candidate.confidence, "reject")
        self.assertEqual(candidate.negative_span_id, str(candidate.numeric_span_id))
        self.assertEqual(candidate.matched_negative_span_start, str(candidate.numeric_span_start))
        self.assertEqual(candidate.matched_negative_span_end, str(candidate.numeric_span_end))

    def test_tokenizer_keeps_multi_digit_and_formatted_spans_whole(self) -> None:
        spans = collector.tokenizeNumericSpans("第12条 ２０年 10,000 １／１０，０００ ３．５ ２事業者")
        self.assertEqual([span.rawText for span in spans], ["12", "２０", "10,000", "１／１０，０００", "３．５", "２"])
        self.assertEqual([span.normalizedText for span in spans], ["12", "20", "10,000", "1／10，000", "3．5", "2"])
        self.assertEqual(spans[0].spanShape, "multi_digit_numeric_span")
        self.assertEqual(spans[1].spanShape, "multi_digit_numeric_span")
        self.assertEqual(spans[2].spanShape, "formatted_numeric_span")
        self.assertEqual(spans[3].spanShape, "formatted_numeric_span")
        self.assertEqual(spans[4].spanShape, "formatted_numeric_span")
        self.assertEqual(spans[5].spanShape, "single_digit")

    def test_multi_digit_and_formatted_spans_emit_single_audit_candidates(self) -> None:
        row = self._row("第12条 1/10,000 3.5 ２事業者")
        candidates = collector.collectCandidatesFromRow(row)
        self.assertEqual([candidate.numeric_span_text for candidate in candidates], ["12", "1/10,000", "3.5", "２"])
        self.assertEqual([candidate.numeric_span_normalized_text for candidate in candidates], ["12", "1/10,000", "3.5", "2"])
        self.assertEqual(candidates[0].candidate_type, "multi_digit_numeric_span")
        self.assertEqual(candidates[1].candidate_type, "multi_digit_numeric_span")
        self.assertEqual(candidates[2].candidate_type, "multi_digit_numeric_span")

    def test_marker_local_negative_guard_does_not_reject_nearby_later_citation(self) -> None:
        row = self._row("基準２条例第12条第２項の適用")
        candidates = collector.collectCandidatesFromRow(row)
        firstTwo = next(candidate for candidate in candidates if candidate.marker == "２" and candidate.offset == 2)
        self.assertNotEqual(firstTwo.confidence, "reject")
        self.assertNotEqual(firstTwo.candidate_type, "article_subnumber_or_citation")
        self.assertIn("legal_reference_nearby_but_not_marker_local", firstTwo.negative_reasons)
        articleNumber = next(candidate for candidate in candidates if candidate.marker == "12")
        self.assertEqual(articleNumber.candidate_type, "multi_digit_numeric_span")
        self._assertSelfMatchedReject(articleNumber)
        citationTwo = next(candidate for candidate in candidates if candidate.marker == "２" and candidate.offset > 2)
        self.assertEqual(citationTwo.candidate_type, "article_subnumber_or_citation")
        self._assertSelfMatchedReject(citationTwo)

    def test_article_citation_precedence_marks_each_numeric_span(self) -> None:
        row = self._row("第10条第2項")
        candidates = collector.collectCandidatesFromRow(row)
        self.assertEqual(
            [(candidate.marker, candidate.candidate_type) for candidate in candidates],
            [
                ("10", "multi_digit_numeric_span"),
                ("2", "article_subnumber_or_citation"),
            ],
        )
        self._assertSelfMatchedReject(candidates[0])
        self._assertSelfMatchedReject(candidates[1])

    def test_revision_note_prefix_precedence_uses_future_reason(self) -> None:
        row = self._row("旧第２項 旧第２号")
        candidates = collector.collectCandidatesFromRow(row)
        self.assertEqual(
            [(candidate.marker, candidate.candidate_type) for candidate in candidates],
            [
                ("２", "revision_note_or_metadata"),
                ("２", "revision_note_or_metadata"),
            ],
        )
        for candidate in candidates:
            self.assertNotEqual(candidate.candidate_type, "article_subnumber_or_citation")
            self._assertSelfMatchedReject(candidate)

    def test_expanded_legal_reference_guards_are_marker_local(self) -> None:
        cases = [
            ("前２項の規定", "２"),
            ("前2号に掲げるもの", "2"),
            ("同２項の規定", "２"),
            ("第 ２ 条の規定", "２"),
            ("第1項第5号", "1"),
            ("第1項第5号", "5"),
        ]
        for sentence, marker in cases:
            with self.subTest(sentence=sentence, marker=marker):
                candidate = next(
                    candidate
                    for candidate in collector.collectCandidatesFromRow(self._row(sentence))
                    if candidate.marker == marker
                )
                self.assertEqual(candidate.candidate_type, "article_subnumber_or_citation")
                self._assertSelfMatchedReject(candidate)

    def test_table_form_and_layout_guards_are_rejected(self) -> None:
        cases = [
            ("別表第２", "２", "table_or_appendix_noise"),
            ("別記２", "２", "table_or_appendix_noise"),
            ("第２号様式", "２", "attachment_form_number"),
            ("第２面", "２", "table_or_appendix_noise"),
            ("A4", "4", "table_or_appendix_noise"),
            ("注２", "２", "page_or_table_note_marker"),
            ("表２", "２", "table_or_appendix_noise"),
        ]
        for sentence, marker, expectedType in cases:
            with self.subTest(sentence=sentence):
                candidate = next(
                    candidate
                    for candidate in collector.collectCandidatesFromRow(self._row(sentence))
                    if candidate.marker == marker
                )
                self.assertEqual(candidate.candidate_type, expectedType)
                self._assertSelfMatchedReject(candidate)

    def test_table_form_parenthetical_reference_classifies_each_span(self) -> None:
        row = self._row("別表第２(第4条関係)")
        candidates = collector.collectCandidatesFromRow(row)
        tableNumber = next(candidate for candidate in candidates if candidate.marker == "２")
        articleNumber = next(candidate for candidate in candidates if candidate.marker == "4")
        self.assertEqual(tableNumber.candidate_type, "table_or_appendix_noise")
        self.assertEqual(articleNumber.candidate_type, "article_subnumber_or_citation")
        self._assertSelfMatchedReject(tableNumber)
        self._assertSelfMatchedReject(articleNumber)

    def test_revision_note_and_date_guards_do_not_conflict(self) -> None:
        revisionCandidates = collector.collectCandidatesFromRow(self._row("(令７規則９・一部改正)"))
        self.assertEqual([candidate.candidate_type for candidate in revisionCandidates], ["revision_note_or_metadata", "revision_note_or_metadata"])
        for candidate in revisionCandidates:
            self._assertSelfMatchedReject(candidate)

        dateCandidates = collector.collectCandidatesFromRow(self._row("令和７年４月１日施行"))
        self.assertEqual([candidate.candidate_type for candidate in dateCandidates], ["era_date_or_effective_date"] * 3)
        for candidate in dateCandidates:
            self._assertSelfMatchedReject(candidate)

    def test_quantity_and_specification_guards_are_marker_local(self) -> None:
        cases = [
            ("１キロワット", "１"),
            ("年１回", "１"),
            ("６ヶ月", "６"),
        ]
        for sentence, marker in cases:
            with self.subTest(sentence=sentence):
                candidate = next(
                    candidate
                    for candidate in collector.collectCandidatesFromRow(self._row(sentence))
                    if candidate.marker == marker
                )
                self.assertEqual(candidate.candidate_type, "quantity_date_unit")
                self._assertSelfMatchedReject(candidate)

    def test_ocr_noise_rejects_only_clear_intrusions_and_demotes_boundary_cases(self) -> None:
        for sentence in ["に１より", "又４は"]:
            with self.subTest(sentence=sentence):
                candidate = collector.collectCandidatesFromRow(self._row(sentence))[0]
                self.assertEqual(candidate.candidate_type, "ocr_or_formatting_noise")
                self._assertSelfMatchedReject(candidate)

        for sentence in ["事項２。", "事項２"]:
            with self.subTest(sentence=sentence):
                candidate = collector.collectCandidatesFromRow(self._row(sentence))[0]
                self.assertEqual(candidate.candidate_type, "ambiguous")
                self.assertEqual(candidate.confidence, "low")
                self.assertIn("ocr_or_formatting_noise_demoted", candidate.negative_reasons)
                self.assertNotEqual(candidate.confidence, "reject")

    def test_protected_positive_fixtures_are_not_rejected(self) -> None:
        cases = [
            "事項２事業者は届け出る。",
            "事項2 市長は届け出る。",
            "区域２周辺の者は対応する。",
            "本文<br>事項２事業者は届け出る。",
            "令和７年条例により事項２事業者は届け出る。",
            "令和７年の条例について区域２周辺の者は対応する。",
            "令和７年条例改正により事項２事業者は届け出る。",
            "基準２条例に定める事項。",
        ]
        for sentence in cases:
            with self.subTest(sentence=sentence):
                candidates = collector.collectCandidatesFromRow(self._row(sentence))
                candidate = next(candidate for candidate in candidates if candidate.marker_value == "2")
                self.assertNotEqual(candidate.confidence, "reject")
                self.assertNotEqual(candidate.candidate_type, "article_subnumber_or_citation")
                self.assertEqual(candidate.split_decision, "report_only")

    def test_table_form_words_elsewhere_do_not_reject_protected_positive(self) -> None:
        paragraph = "別表第２に定める。事項２事業者は届け出る。"
        row = self._row("事項２事業者は届け出る。", paragraph=paragraph)
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertNotEqual(candidate.confidence, "reject")
        self.assertEqual(candidate.candidate_type, "implicit_first_paragraph")

    def test_marker_local_negative_reason_categories(self) -> None:
        cases = [
            ("令和２年施行", "era_date_or_effective_date"),
            ("正副２通", "quantity_date_unit"),
            ("様式第２号", "attachment_form_number"),
            ("※２ 注意", "page_or_table_note_marker"),
            ("(２) 添付書類", "parenthetical_or_enumeration_noise"),
            ("2. 添付書類", "numeric_sequence_or_index"),
        ]
        for sentence, expectedType in cases:
            with self.subTest(sentence=sentence):
                candidate = collector.collectCandidatesFromRow(self._row(sentence))[0]
                self.assertEqual(candidate.candidate_type, expectedType)
                self._assertSelfMatchedReject(candidate)

    def test_sentence_initial_numbered_paragraph_is_report_only(self) -> None:
        row = self._row("２　前項の規定により届け出る。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "sentence_initial_numbered_paragraph")
        self.assertEqual(candidate.actionability, "already_split_or_sentence_initial")
        self.assertEqual(candidate.split_decision, "report_only")

    def test_explicit_run_has_run_members(self) -> None:
        row = self._row("１　目的。２　定義。３　手続。")
        candidates = collector.collectCandidatesFromRow(row)
        second = candidates[1]
        self.assertEqual(second.candidate_type, "explicit_run")
        self.assertEqual(second.confidence, "high")
        self.assertIn('"value": 2', second.run_members)

    def test_preceding_period_is_positive_for_implicit_paragraph(self) -> None:
        row = self._row("前文。２地域の者は対応する。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "implicit_first_paragraph")
        self.assertIn("preceding_boundary", candidate.positive_reasons)
        self.assertEqual(candidate.split_decision, "report_only")

    def test_completed_legal_item_body_is_positive_for_implicit_paragraph(self) -> None:
        row = self._row("⑴ A⑵ B２周辺の者は対応する。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "implicit_first_paragraph")
        self.assertIn("preceding_boundary", candidate.positive_reasons)
        self.assertEqual(candidate.preceding_pattern, "completed_legal_item_body")

    def test_ascii_two_with_space_and_opener_is_implicit_paragraph(self) -> None:
        row = self._row("事項2 前項の規定により届け出る。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "implicit_first_paragraph")
        self.assertIn("narrow_opener", candidate.positive_reasons)
        self.assertEqual(candidate.marker, "2")

    def test_weak_single_digit_without_positive_evidence_is_ambiguous_low(self) -> None:
        row = self._row("本文2関連。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "ambiguous")
        self.assertEqual(candidate.confidence, "low")
        self.assertEqual(candidate.actionability, "review_candidate")

    def test_broad_only_weak_subject_is_preserved(self) -> None:
        row = self._row("区域２周辺の者は対応する。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.collection_stage, "broad_only")
        self.assertEqual(candidate.candidate_type, "implicit_first_paragraph")
        self.assertIn("short_subject_ending_wa", candidate.positive_reasons)
        self.assertEqual(candidate.narrow_opener_matched, 0)
        self.assertEqual(candidate.broad_rule_matched, 1)
        self.assertEqual(candidate.would_be_in_targeted_135, 0)
        self.assertIn("no_narrow_opener", candidate.targeted_exclusion_reasons)

    def test_targeted_opener_candidate_is_marked_separately(self) -> None:
        row = self._row("事項２事業者は届け出る。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.collection_stage, "targeted_opener")
        self.assertEqual(candidate.narrow_opener_matched, 1)
        self.assertEqual(candidate.would_be_in_targeted_135, 1)
        self.assertEqual(candidate.audit_group, "targeted_high")

    def test_table_candidate_is_collected_as_noise(self) -> None:
        row = self._row("| 区分 | ２事業者 |", isTable=1)
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.collection_stage, "table_noise")
        self.assertEqual(candidate.candidate_type, "table_or_appendix_noise")
        self.assertEqual(candidate.confidence, "reject")
        self.assertIn("table_or_appendix_noise", candidate.negative_reasons)

    def test_bracket_depth_candidate_is_collected(self) -> None:
        row = self._row("本文（２前項の規定）後文。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertIn("inside_bracket_or_quote", candidate.negative_reasons)
        self.assertNotEqual(candidate.confidence, "reject")
        self.assertEqual(candidate.actionability, "inside_bracket_or_quote_review")
        self.assertEqual(candidate.split_decision, "report_only")

    def test_ascii_quote_depth_candidate_is_collected(self) -> None:
        row = self._row('"２前項の規定" 後文。')
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertIn("inside_bracket_or_quote", candidate.negative_reasons)
        self.assertNotEqual(candidate.confidence, "reject")
        self.assertEqual(candidate.audit_group, "inside_bracket_or_quote")
        self.assertEqual(candidate.split_decision, "report_only")

    def test_japanese_quote_depth_candidate_is_collected(self) -> None:
        row = self._row("「２前項の規定」後文。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertIn("inside_bracket_or_quote", candidate.negative_reasons)
        self.assertNotEqual(candidate.confidence, "reject")
        self.assertEqual(candidate.audit_group, "inside_bracket_or_quote")
        self.assertEqual(candidate.split_decision, "report_only")

    def test_sentence_initial_opener_is_not_targeted_embedded_135_case(self) -> None:
        row = self._row("２事業者は届け出る。")
        candidate = collector.collectCandidatesFromRow(row)[0]
        self.assertEqual(candidate.candidate_type, "sentence_initial_numbered_paragraph")
        self.assertEqual(candidate.narrow_opener_matched, 1)
        self.assertEqual(candidate.would_be_in_targeted_135, 0)

    def test_report_writer_keeps_rejects_in_all_and_not_review(self) -> None:
        high = collector.collectCandidatesFromRow(self._row("２　前項の規定。"))[0]
        reject = collector.collectCandidatesFromRow(self._row("第２項の規定。"))[0]
        with temporaryWorkspaceDirectory() as tempDir:
            outDir = Path(tempDir)
            collector.writeReports(outDir, [high, reject])
            allCsv = (outDir / "paragraph_number_candidates.all.csv").read_text(encoding="utf-8")
            reviewCsv = (outDir / "paragraph_number_candidates.review.csv").read_text(encoding="utf-8")
            summary = (outDir / "paragraph_number_candidates.summary.json").read_text(encoding="utf-8")
            jsonl = (outDir / "paragraph_number_candidates.all.jsonl").read_text(encoding="utf-8")
            samples = (outDir / "paragraph_number_candidates.samples.md").read_text(encoding="utf-8")
        self.assertIn("article_subnumber_or_citation", allCsv)
        self.assertNotIn("article_subnumber_or_citation", reviewCsv)
        self.assertIn("numeric_span_normalized_text", allCsv)
        self.assertIn("paragraph_full_text", jsonl)
        self.assertIn("sample_reject_by_reason", samples)
        self.assertIn("sample_reject_by_reason", summary)

    def test_report_writer_keeps_protected_positives_in_review(self) -> None:
        protected = [
            collector.collectCandidatesFromRow(self._row("事項２事業者は届け出る。"))[0],
            collector.collectCandidatesFromRow(self._row("事項2 市長は届け出る。"))[0],
            collector.collectCandidatesFromRow(self._row("区域２周辺の者は対応する。"))[0],
        ]
        with temporaryWorkspaceDirectory() as tempDir:
            outDir = Path(tempDir)
            collector.writeReports(outDir, protected)
            with (outDir / "paragraph_number_candidates.review.csv").open(encoding="utf-8", newline="") as inputFile:
                reviewRows = list(csv.DictReader(inputFile))
        self.assertEqual(len(reviewRows), 3)
        self.assertTrue(all(row["confidence"] in {"high", "medium", "low"} for row in reviewRows))

    def test_summary_and_samples_include_new_reject_reasons(self) -> None:
        revision = collector.collectCandidatesFromRow(self._row("(令７規則９・一部改正)"))[0]
        ocrNoise = collector.collectCandidatesFromRow(self._row("に１より"))[0]
        with temporaryWorkspaceDirectory() as tempDir:
            outDir = Path(tempDir)
            collector.writeReports(outDir, [revision, ocrNoise], selectedRunId=1)
            summary = json.loads((outDir / "paragraph_number_candidates.summary.json").read_text(encoding="utf-8"))
            samples = (outDir / "paragraph_number_candidates.samples.md").read_text(encoding="utf-8")
        self.assertIn("revision_note_or_metadata", summary["by_negative_reason"])
        self.assertIn("ocr_or_formatting_noise", summary["by_negative_reason"])
        self.assertIn("revision_note_or_metadata", summary["sample_reject_by_reason"])
        self.assertIn("ocr_or_formatting_noise", summary["sample_reject_by_reason"])
        self.assertIn("revision_note_or_metadata", samples)
        self.assertIn("ocr_or_formatting_noise", samples)

    def test_summary_includes_run_and_broad_only_audit_counts(self) -> None:
        high = collector.collectCandidatesFromRow(self._row("事項２事業者は届け出る。"))[0]
        broadOnly = collector.collectCandidatesFromRow(self._row("区域２周辺の者は対応する。"))[0]
        reject = collector.collectCandidatesFromRow(self._row("第２項の規定。"))[0]
        summary = collector.buildSummary([high, broadOnly, reject], selectedRunId=1)
        self.assertEqual(summary["selected_run_id"], 1)
        self.assertEqual(summary["candidate_run_ids"], [1])
        self.assertEqual(summary["total_candidates"], 3)
        self.assertIn("total_broad_only_candidates", summary)
        self.assertIn("sample_marker_value_2_without_1", summary)
        self.assertIn("sample_later_marker_without_prior_1", summary)

    def test_report_schema_is_parseable_and_contains_required_keys(self) -> None:
        candidates = [
            collector.collectCandidatesFromRow(self._row("事項２事業者は届け出る。"))[0],
            collector.collectCandidatesFromRow(self._row("区域２周辺の者は対応する。"))[0],
            collector.collectCandidatesFromRow(self._row("第２項の規定。"))[0],
        ]
        with temporaryWorkspaceDirectory() as tempDir:
            outDir = Path(tempDir)
            collector.writeReports(outDir, candidates, selectedRunId=1)
            with (outDir / "paragraph_number_candidates.all.csv").open(encoding="utf-8", newline="") as inputFile:
                allRows = list(csv.DictReader(inputFile))
            with (outDir / "paragraph_number_candidates.review.csv").open(encoding="utf-8", newline="") as inputFile:
                reviewRows = list(csv.DictReader(inputFile))
            jsonlRows = [
                json.loads(line)
                for line in (outDir / "paragraph_number_candidates.all.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            summary = json.loads((outDir / "paragraph_number_candidates.summary.json").read_text(encoding="utf-8"))
            samples = (outDir / "paragraph_number_candidates.samples.md").read_text(encoding="utf-8")

        requiredCsvFields = {
            "candidate_id",
            "run_id",
            "confidence",
            "split_decision",
            "before_context",
            "after_context",
            "numeric_span_normalized_text",
        }
        self.assertTrue(requiredCsvFields.issubset(allRows[0].keys()))
        self.assertTrue(all(row["split_decision"] == "report_only" for row in allRows))
        self.assertTrue(all(row["confidence"] in {"high", "medium", "low"} for row in reviewRows))
        self.assertTrue(all("paragraph_full_text" in row for row in jsonlRows))

        requiredSummaryKeys = {
            "selected_run_id",
            "candidate_run_ids",
            "total_candidates",
            "total_broad_candidates",
            "total_targeted_opener_candidates",
            "total_broad_only_candidates",
            "by_candidate_type",
            "by_confidence",
            "by_negative_reason",
            "by_positive_reason",
            "top_files_by_candidate_count",
            "top_files_by_high_medium_count",
            "sample_high",
            "sample_medium",
            "sample_low",
            "sample_reject_by_reason",
            "sample_broad_only_high",
            "sample_broad_only_medium",
            "sample_broad_only_low",
            "sample_broad_only_reject",
            "sample_marker_value_2_without_1",
            "sample_later_marker_without_prior_1",
        }
        self.assertTrue(requiredSummaryKeys.issubset(summary.keys()))
        for heading in [
            "## sample_high",
            "## sample_medium",
            "## sample_low",
            "## sample_broad_only_high",
            "## sample_broad_only_medium",
            "## sample_broad_only_low",
            "## sample_broad_only_reject",
            "## sample_marker_value_2_without_1",
            "## sample_later_marker_without_prior_1",
            "## sample_reject_by_reason",
        ]:
            self.assertIn(heading, samples)

    def test_load_rows_resolves_duplicate_sentence_offsets_with_reason(self) -> None:
        with temporaryWorkspaceDirectory() as tempDir:
            dbPath = Path(tempDir) / "analysis.db"
            with sqlite3.connect(dbPath) as conn:
                conn.executescript(
                    """
                    CREATE TABLE analysis_documents (
                        document_id INTEGER PRIMARY KEY,
                        file_name TEXT NOT NULL,
                        source_file_path TEXT NOT NULL
                    );
                    CREATE TABLE analysis_paragraphs (
                        paragraph_id INTEGER PRIMARY KEY,
                        document_id INTEGER NOT NULL,
                        paragraph_text TEXT NOT NULL,
                        is_table_paragraph INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE analysis_sentences (
                        sentence_id INTEGER PRIMARY KEY,
                        paragraph_id INTEGER NOT NULL,
                        document_id INTEGER NOT NULL,
                        sentence_no_in_document INTEGER NOT NULL,
                        sentence_no_in_paragraph INTEGER NOT NULL,
                        sentence_text TEXT NOT NULL
                    );
                    INSERT INTO analysis_documents VALUES (1, 'fixture.txt', 'fixture.txt');
                    INSERT INTO analysis_paragraphs VALUES (10, 1, '２　前項。２　前項。', 0);
                    INSERT INTO analysis_sentences VALUES (100, 10, 1, 1, 1, '２　前項。');
                    INSERT INTO analysis_sentences VALUES (101, 10, 1, 2, 2, '２　前項。');
                    """
                )
                rows = collector.loadSentenceRows(conn)
        self.assertEqual(rows[0].sentenceCharOffsetInParagraph, 0)
        self.assertEqual(rows[1].sentenceCharOffsetInParagraph, 5)
        self.assertEqual(rows[0].offsetResolutionReason, "ordered_search_duplicate_text")
        self.assertEqual(rows[1].offsetResolutionReason, "ordered_search_duplicate_text")

    def test_load_rows_scopes_to_selected_run_id(self) -> None:
        with temporaryWorkspaceDirectory() as tempDir:
            dbPath = Path(tempDir) / "analysis.db"
            with sqlite3.connect(dbPath) as conn:
                conn.executescript(
                    """
                    CREATE TABLE analysis_runs (
                        run_id INTEGER PRIMARY KEY,
                        status TEXT NOT NULL
                    );
                    CREATE TABLE analysis_documents (
                        document_id INTEGER PRIMARY KEY,
                        run_id INTEGER NOT NULL,
                        file_name TEXT NOT NULL,
                        source_file_path TEXT NOT NULL
                    );
                    CREATE TABLE analysis_paragraphs (
                        paragraph_id INTEGER PRIMARY KEY,
                        run_id INTEGER NOT NULL,
                        document_id INTEGER NOT NULL,
                        paragraph_text TEXT NOT NULL,
                        is_table_paragraph INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE analysis_sentences (
                        sentence_id INTEGER PRIMARY KEY,
                        run_id INTEGER NOT NULL,
                        paragraph_id INTEGER NOT NULL,
                        document_id INTEGER NOT NULL,
                        sentence_no_in_document INTEGER NOT NULL,
                        sentence_no_in_paragraph INTEGER NOT NULL,
                        sentence_text TEXT NOT NULL
                    );
                    INSERT INTO analysis_runs VALUES (1, 'completed');
                    INSERT INTO analysis_runs VALUES (2, 'completed');
                    INSERT INTO analysis_documents VALUES (1, 1, 'old.txt', 'old.txt');
                    INSERT INTO analysis_documents VALUES (2, 2, 'new.txt', 'new.txt');
                    INSERT INTO analysis_paragraphs VALUES (10, 1, 1, '１　旧。', 0);
                    INSERT INTO analysis_paragraphs VALUES (20, 2, 2, '２　新。', 0);
                    INSERT INTO analysis_sentences VALUES (100, 1, 10, 1, 1, 1, '１　旧。');
                    INSERT INTO analysis_sentences VALUES (200, 2, 20, 2, 1, 1, '２　新。');
                    """
                )
                self.assertEqual(collector.resolveRunId(conn, None), 2)
                rows = collector.loadSentenceRows(conn, runId=2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].runId, 2)
        self.assertEqual(rows[0].fileName, "new.txt")

    def test_load_rows_defaults_to_latest_run_and_keeps_context_inside_run(self) -> None:
        with temporaryWorkspaceDirectory() as tempDir:
            dbPath = Path(tempDir) / "analysis.db"
            with sqlite3.connect(dbPath) as conn:
                conn.executescript(
                    """
                    CREATE TABLE analysis_runs (
                        run_id INTEGER PRIMARY KEY,
                        status TEXT NOT NULL
                    );
                    CREATE TABLE analysis_documents (
                        document_id INTEGER,
                        run_id INTEGER NOT NULL,
                        file_name TEXT NOT NULL,
                        source_file_path TEXT NOT NULL
                    );
                    CREATE TABLE analysis_paragraphs (
                        paragraph_id INTEGER,
                        run_id INTEGER NOT NULL,
                        document_id INTEGER NOT NULL,
                        paragraph_text TEXT NOT NULL,
                        is_table_paragraph INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE analysis_sentences (
                        sentence_id INTEGER,
                        run_id INTEGER NOT NULL,
                        paragraph_id INTEGER NOT NULL,
                        document_id INTEGER NOT NULL,
                        sentence_no_in_document INTEGER NOT NULL,
                        sentence_no_in_paragraph INTEGER NOT NULL,
                        sentence_text TEXT NOT NULL
                    );
                    INSERT INTO analysis_runs VALUES (1, 'completed');
                    INSERT INTO analysis_runs VALUES (2, 'completed');
                    INSERT INTO analysis_documents VALUES (1, 1, 'old.txt', 'old.txt');
                    INSERT INTO analysis_documents VALUES (1, 2, 'new.txt', 'new.txt');
                    INSERT INTO analysis_paragraphs VALUES (10, 1, 1, 'ï¼‘ã€€æ—§ã€‚ï¼‘ã€€æ—§å¾Œã€‚', 0);
                    INSERT INTO analysis_paragraphs VALUES (10, 2, 1, 'ï¼’ã€€æ–°ã€‚ï¼’ã€€æ–°å¾Œã€‚', 0);
                    INSERT INTO analysis_sentences VALUES (100, 1, 10, 1, 1, 1, 'ï¼‘ã€€æ—§ã€‚');
                    INSERT INTO analysis_sentences VALUES (101, 1, 10, 1, 2, 2, 'ï¼‘ã€€æ—§å¾Œã€‚');
                    INSERT INTO analysis_sentences VALUES (200, 2, 10, 1, 1, 1, 'ï¼’ã€€æ–°ã€‚');
                    INSERT INTO analysis_sentences VALUES (201, 2, 10, 1, 2, 2, 'ï¼’ã€€æ–°å¾Œã€‚');
                    """
                )
                rows = collector.loadSentenceRows(conn)
        self.assertEqual([row.runId for row in rows], [2, 2])
        self.assertEqual(rows[0].previousSentenceText, "")
        self.assertEqual(rows[0].nextSentenceText, "ï¼’ã€€æ–°å¾Œã€‚")
        self.assertEqual(rows[0].sentenceCharOffsetInParagraph, 0)
        self.assertEqual(rows[1].sentenceCharOffsetInParagraph, len("ï¼’ã€€æ–°ã€‚"))

    def test_missing_required_tables_lists_specific_names(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("CREATE TABLE analysis_sentences (sentence_id INTEGER)")
            missing = collector.missingRequiredTables(conn)
        self.assertEqual(missing, ["analysis_paragraphs", "analysis_documents"])


if __name__ == "__main__":
    unittest.main()
