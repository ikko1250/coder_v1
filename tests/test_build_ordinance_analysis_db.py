from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path


def _load_builder_module():
    module_path = Path(__file__).resolve().parents[1] / "docs" / "build_ordinance_analysis_db.py"
    spec = importlib.util.spec_from_file_location("build_ordinance_analysis_db", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _load_builder_module()


class SentenceSplitTest(unittest.TestCase):
    def test_halfwidth_ideographic_full_stop_is_sentence_end(self) -> None:
        sentences = builder.split_into_sentences("前文｡後文。")
        self.assertEqual(sentences, ["前文｡", "後文。"])

    def test_halfwidth_ideographic_full_stop_inside_parentheses_is_not_split(self) -> None:
        paragraph = "本文（以下同じ｡）の続き。次文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["本文（以下同じ｡）の続き。", "次文。"])

    def test_mixed_fullwidth_open_ascii_close_parenthesis_recovers_boundary(self) -> None:
        paragraph = "第１条 本文（以下同じ｡)。第２条 次文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["第１条 本文（以下同じ｡)。", "第２条 次文。"])

    def test_mixed_ascii_open_fullwidth_close_parenthesis_recovers_boundary(self) -> None:
        paragraph = "第１条 本文(以下同じ｡）。第２条 次文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["第１条 本文(以下同じ｡）。", "第２条 次文。"])

    def test_normal_parenthetical_period_is_not_split_by_default(self) -> None:
        paragraph = "この規則は、条例（令和５年条例第20号。以下「条例」という。）の施行に関し定める。次文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(
            sentences,
            [
                "この規則は、条例（令和５年条例第20号。以下「条例」という。）の施行に関し定める。",
                "次文。",
            ],
        )

    def test_enumeration_after_mixed_parenthesis_is_split(self) -> None:
        paragraph = (
            "(4) 農地法（営農型太陽光発電設備の設置予定地を除く。第７号において同じ｡)。"
            "(5) 地すべり等防止法の地すべり防止区域。"
            "(6) 急傾斜地崩壊危険区域。"
        )
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(
            sentences,
            [
                "(4) 農地法（営農型太陽光発電設備の設置予定地を除く。第７号において同じ｡)。",
                "(5) 地すべり等防止法の地すべり防止区域。",
                "(6) 急傾斜地崩壊危険区域。",
            ],
        )

    def test_enumeration_fallback_after_trailing_ascii_close(self) -> None:
        paragraph = "前文（説明。)(2) 次号。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明。)", "(2) 次号。"])

    def test_enumeration_fallback_after_trailing_ascii_close_and_halfwidth_stop(self) -> None:
        paragraph = "前文（説明｡)(2) 次号。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明｡)", "(2) 次号。"])

    def test_ascii_enumeration_reference_inside_parenthetical_is_not_split(self) -> None:
        paragraph = "前文（説明。(2)に掲げる事項を含む。）後文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明。(2)に掲げる事項を含む。）後文。"])

    def test_ascii_enumeration_reference_with_space_inside_parenthetical_is_not_split(self) -> None:
        paragraph = "前文（説明。 (2) に掲げる事項を含む。）後文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明。 (2) に掲げる事項を含む。）後文。"])

    def test_ascii_enumeration_reference_with_newline_inside_parenthetical_is_not_split(self) -> None:
        paragraph = "前文（説明。\n(2) に掲げる事項を含む。）後文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明。\n(2) に掲げる事項を含む。）後文。"])

    def test_fullwidth_enumeration_inside_parenthetical_is_not_split(self) -> None:
        paragraph = "前文（説明。（2）次号。）後文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["前文（説明。（2）次号。）後文。"])

    def test_non_round_brackets_are_not_compatibly_closed(self) -> None:
        paragraph = "本文【説明）。次文。"
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(sentences, ["本文【説明）。次文。"])

    def test_split_inside_parentheses_true_still_splits_inside_parentheses(self) -> None:
        paragraph = "本文（説明。次文）。後文。"
        sentences = builder.split_into_sentences(paragraph, split_inside_parentheses=True)
        self.assertEqual(sentences, ["本文（説明。", "次文）。", "後文。"])

    def test_split_inside_parentheses_true_handles_halfwidth_stop_without_normalizing(self) -> None:
        paragraph = "本文（以下同じ｡）の続き。次文。"
        sentences = builder.split_into_sentences(paragraph, split_inside_parentheses=True)
        self.assertEqual(sentences, ["本文（以下同じ｡）", "の続き。", "次文。"])

    def test_split_circled_legal_items_after_punctuation_split(self) -> None:
        paragraph = "指定する。⑴ A⑵ B⑶ C"
        self.assertEqual(builder.split_into_sentences(paragraph), ["指定する。", "⑴ A", "⑵ B", "⑶ C"])

    def test_split_circled_legal_items_starting_at_two_and_two_digit_numbers(self) -> None:
        self.assertEqual(builder.split_into_sentences("⑵ A⑶ B⑷ C"), ["⑵ A", "⑶ B", "⑷ C"])
        self.assertEqual(builder.split_into_sentences("⑼ A⑽ B⑾ C"), ["⑼ A", "⑽ B", "⑾ C"])

    def test_split_upper_range_circled_legal_items(self) -> None:
        self.assertEqual(builder.split_into_sentences("⒅ A⒆ B⒇ C"), ["⒅ A", "⒆ B", "⒇ C"])

    def test_split_circled_legal_items_with_fullwidth_space_and_newline(self) -> None:
        self.assertEqual(builder.split_into_sentences("⑴　A⑵　B⑶　C"), ["⑴　A", "⑵　B", "⑶　C"])
        self.assertEqual(builder.split_into_sentences("⑴\nA⑵\nB"), ["⑴\nA", "⑵\nB"])

    def test_circled_legal_items_do_not_split_single_or_no_space_or_nonconsecutive(self) -> None:
        self.assertEqual(builder.split_into_sentences("本文⑴ A。"), ["本文⑴ A。"])
        self.assertEqual(builder.split_into_sentences("本文⑴に掲げる事項⑵に掲げる事項。"), ["本文⑴に掲げる事項⑵に掲げる事項。"])
        self.assertEqual(builder.split_into_sentences("⑴ A⑶ C"), ["⑴ A⑶ C"])

    def test_split_katakana_legal_items_without_spaces(self) -> None:
        paragraph = "ア再生可能エネルギー発電事業イ精神の機能ウ破産者"
        self.assertEqual(
            builder.split_into_sentences(paragraph),
            ["ア再生可能エネルギー発電事業", "イ精神の機能", "ウ破産者"],
        )

    def test_split_katakana_legal_items_starting_at_later_marker(self) -> None:
        paragraph = "イ地すべり区域ウ急傾斜地区域エ土砂災害区域"
        self.assertEqual(builder.split_into_sentences(paragraph), ["イ地すべり区域", "ウ急傾斜地区域", "エ土砂災害区域"])

    def test_katakana_legal_items_do_not_split_words_references_or_nonconsecutive(self) -> None:
        self.assertEqual(builder.split_into_sentences("アプリケーションとエネルギーの説明。"), ["アプリケーションとエネルギーの説明。"])
        self.assertEqual(builder.split_into_sentences("アからカまでのいずれかに該当する場合。"), ["アからカまでのいずれかに該当する場合。"])
        self.assertEqual(builder.split_into_sentences("アAウC"), ["アAウC"])
        self.assertEqual(builder.split_into_sentences("アAイBウC"), ["アAイBウC"])
        self.assertEqual(builder.split_into_sentences("ア1イ2ウ3"), ["ア1イ2ウ3"])

    def test_legal_item_post_split_does_not_apply_to_markdown_table_rows(self) -> None:
        wrapped = "| 区分 | ⑴ A ⑵ B ⑶ C | アAイB | 備考 |"
        unwrapped = "区分 | ⑴ A ⑵ B ⑶ C | アAイB | 備考"
        self.assertEqual(builder.split_into_sentences(wrapped), [wrapped])
        self.assertEqual(builder.split_into_sentences(unwrapped), [unwrapped])

    def test_header_without_punctuation_keeps_prefix_on_first_item(self) -> None:
        paragraph = "次に掲げるもの⑴ A⑵ B"
        self.assertEqual(builder.split_into_sentences(paragraph), ["次に掲げるもの⑴ A", "⑵ B"])

    def test_nishiwaki_appendix_like_non_table_case_splits(self) -> None:
        paragraph = "別表第１（第５条、第８条関係）１位置図 1/10,000以上 ⑴ 方位⑵ 事業区域の位置"
        self.assertEqual(
            builder.split_into_sentences(paragraph),
            ["別表第１（第５条、第８条関係）１位置図 1/10,000以上 ⑴ 方位", "⑵ 事業区域の位置"],
        )

    def test_markdown_table_rows_with_sentence_punctuation_are_not_split(self) -> None:
        wrapped = "| 区分 | 本文。次文。 | 備考 |"
        unwrapped = "区分 | 本文。次文。 | 備考"
        self.assertEqual(builder.split_into_sentences(wrapped), [wrapped])
        self.assertEqual(builder.split_into_sentences(unwrapped), [unwrapped])

    def test_legal_item_post_split_disabled_when_split_inside_parentheses_true(self) -> None:
        paragraph = "本文（説明。⑴ A⑵ B。アAイB）。後文。"
        sentences = builder.split_into_sentences(paragraph, split_inside_parentheses=True)
        self.assertNotIn("⑵ B。", sentences)
        self.assertFalse(any(sentence == "イB）。" for sentence in sentences))


    def test_legal_item_markers_inside_parentheses_or_quotes_do_not_split(self) -> None:
        self.assertEqual(builder.split_into_sentences("本文（⑴ A⑵ B）後文。"), ["本文（⑴ A⑵ B）後文。"])
        self.assertEqual(builder.split_into_sentences("本文（アAイB）後文。"), ["本文（アAイB）後文。"])
        self.assertEqual(builder.split_into_sentences("本文「アAイB」「⑴ A⑵ B」後文。"), ["本文「アAイB」「⑴ A⑵ B」後文。"])

    def test_circled_legal_item_markers_inside_ascii_double_quotes_do_not_split(self) -> None:
        self.assertEqual(builder.split_into_sentences('"⑴ A⑵ B"'), ['"⑴ A⑵ B"'])

    def test_circled_legal_item_markers_inside_ascii_single_quotes_do_not_split(self) -> None:
        self.assertEqual(builder.split_into_sentences("'⑴ A⑵ B'"), ["'⑴ A⑵ B'"])

    def test_split_mixed_katakana_and_circled_runs(self) -> None:
        paragraph = "ア甲イ乙ウ丙⑵ D⑶ E⑷ F２条例本文"
        self.assertEqual(builder.split_into_sentences(paragraph), ["ア甲", "イ乙", "ウ丙", "⑵ D", "⑶ E", "⑷ F２条例本文"])

    def test_numazu_like_katakana_run_ignores_internal_item_range_reference(self) -> None:
        paragraph = (
            "ア再生可能エネルギー発電事業を実施するために必要な資力及び信用があると認められない場合"
            "イ精神の機能の障害により事業を適正に行うに当たって必要な認知、判断及び意思疎通を適切に行うことができない場合"
            "ウ破産者で復権を得ないものである場合"
            "エ拘禁刑以上の刑に処せられた場合"
            "オ関係法令の規定に違反して刑に処せられた場合"
            "カ沼津市暴力団排除条例に規定する暴力団員等である場合"
            "キ許可申請者等が法人である場合において、その役員がアからカまでのいずれかに該当する場合"
            "⑵ 沼津市土地利用事業指導要綱に基づく基準"
            "⑶ 沼津市景観計画に定める基準"
            "⑷ 次条第１項各号の規定により市長が別に定める基準２条例第12条第２項第１号及び第２号に規定する事業区域の面積の適用については、第４条第１項の例による。"
        )
        sentences = builder.split_into_sentences(paragraph)
        self.assertEqual(len(sentences), 10)
        self.assertEqual(sentences[0][:1], "ア")
        self.assertEqual(sentences[6][:1], "キ")
        self.assertIn("アからカまで", sentences[6])
        self.assertEqual(sentences[7][:1], "⑵")
        self.assertEqual(sentences[8][:1], "⑶")
        self.assertTrue(sentences[9].startswith("⑷"))
        self.assertIn("２条例第12条", sentences[9])

    def test_restarted_same_family_sequences_do_not_split(self) -> None:
        self.assertEqual(builder.split_into_sentences("⑴ A⑵ B⑶ C⑴ D⑵ E⑶ F"), ["⑴ A⑵ B⑶ C⑴ D⑵ E⑶ F"])
        self.assertEqual(builder.split_into_sentences("ア甲イ乙ア丙イ丁"), ["ア甲イ乙ア丙イ丁"])

    def test_high_value_izu_source_or_reduced_fixture_splits_circled_items_1_to_11(self) -> None:
        source_path = (
            Path(__file__).resolve().parents[1]
            / "asset"
            / "texts_2nd"
            / "out_pdfplumber_processed"
            / "68_伊豆市_条例.md"
        )
        if source_path.exists():
            paragraph = next(
                line.strip()
                for line in source_path.read_text(encoding="utf-8").splitlines()
                if "⑴ 伊豆市景観まちづくり条例" in line and "⑾ 砂防法" in line
            )
        else:
            # Source-derived reduced fixture from 伊豆市 第７条（抑制区域）.
            paragraph = (
                "次の各号に掲げる区域について指定する。"
                "⑴ 伊豆市景観まちづくり条例により定めた景観まちづくり重点地区"
                "⑵ 森林法に規定する森林地区及び保安林"
                "⑶ 自然公園法により指定された特別地域等"
                "⑷ 鳥獣保護区"
                "⑸ 海岸保全区域"
                "⑹ 史跡、名勝又は天然記念物の指定地"
                "⑺ 周知の埋蔵文化財包蔵地"
                "⑻ 地すべり防止区域"
                "⑼ 急傾斜地崩壊危険区域"
                "⑽ 土砂災害特別警戒区域"
                "⑾ 砂防指定地"
            )
        expected_markers = ["⑴", "⑵", "⑶", "⑷", "⑸", "⑹", "⑺", "⑻", "⑼", "⑽", "⑾"]

        sentences = builder.split_into_sentences(paragraph)
        item_units = [sentence for sentence in sentences if any(sentence.startswith(marker) for marker in expected_markers)]

        self.assertEqual(len(item_units), 11)
        self.assertTrue(item_units[0].startswith("⑴"))
        self.assertTrue(item_units[-1].startswith("⑾"))
        self.assertEqual([unit[:1] for unit in item_units], expected_markers)

    def test_high_value_numazu_tricky_fixture_preserves_katakana_range_and_trailing_paragraph(self) -> None:
        paragraph = (
            "ア再生可能エネルギー発電事業を実施するために必要な資力及び信用があると認められない場合"
            "イ精神の機能の障害により事業を適正に行うに当たって必要な認知、判断及び意思疎通を適切に行うことができない場合"
            "ウ破産者で復権を得ないものである場合"
            "エ拘禁刑以上の刑に処せられた場合"
            "オ関係法令の規定に違反して刑に処せられた場合"
            "カ沼津市暴力団排除条例に規定する暴力団員等である場合"
            "キ許可申請者等が法人である場合において、その役員がアからカまでのいずれかに該当する場合"
            "⑵ 沼津市土地利用事業指導要綱に基づく基準"
            "⑶ 沼津市景観計画に定める基準"
            "⑷ 次条第１項各号の規定により市長が別に定める基準２条例第12条第２項第１号及び第２号に規定する事業区域の面積の適用については、第４条第１項の例による。"
        )
        expected_markers = ["ア", "イ", "ウ", "エ", "オ", "カ", "キ", "⑵", "⑶", "⑷"]

        sentences = builder.split_into_sentences(paragraph)

        self.assertEqual(len(sentences), 10)
        self.assertEqual([sentence[:1] for sentence in sentences], expected_markers)
        self.assertIn("アからカまで", sentences[6])
        self.assertIn("２条例第12条", sentences[9])

    def test_high_value_nishiwaki_appendix_fixture_splits_two_circled_items(self) -> None:
        paragraph = "別表第１（第５条、第８条関係）１位置図 1/10,000以上 ⑴ 方位⑵ 事業区域の位置"
        sentences = builder.split_into_sentences(paragraph)

        self.assertEqual(len(sentences), 2)
        self.assertTrue(sentences[0].startswith("別表第１"))
        self.assertIn("⑴ 方位", sentences[0])
        self.assertTrue(sentences[1].startswith("⑵ 事業区域の位置"))


class FolderInputBuildTest(unittest.TestCase):
    def test_folder_input_builds_analysis_db_with_category_columns(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()

            (input_dir / "札幌市_条例.txt").write_text("第1条 本文です。", encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            self.assertTrue(analysis_db_path.exists())
            with sqlite3.connect(analysis_db_path) as conn:
                row = conn.execute(
                    """
                    SELECT file_name, category1, category2, raw_text
                    FROM analysis_documents
                    """
                ).fetchone()
            self.assertEqual(
                row,
                ("札幌市_条例.txt", "札幌市", "条例", "第1条 本文です。"),
            )

    def test_numeric_id_prefix_maps_to_category1_category2(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "100_かすみがうら市_施行規則.md").write_text(
                "第1条 本文です。", encoding="utf-8"
            )
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                row = conn.execute(
                    """
                    SELECT file_name, category1, category2, raw_text
                    FROM analysis_documents
                    """
                ).fetchone()
            self.assertEqual(
                row,
                (
                    "100_かすみがうら市_施行規則.md",
                    "かすみがうら市",
                    "施行規則",
                    "第1条 本文です。",
                ),
            )

    def test_invalid_file_name_fails_before_output_db_update(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "invalid-name.txt").write_text("本文", encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 1)
            self.assertFalse(analysis_db_path.exists())
            report_path = analysis_db_path.with_name(f"{analysis_db_path.name}.report.json")
            self.assertTrue(report_path.exists())
            self.assertIn("invalid_file_name", report_path.read_text(encoding="utf-8"))

    def test_bom_prefixed_utf8_is_read_without_bom_character(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "カテゴリ1_カテゴリ2.txt").write_bytes(b"\xef\xbb\xbfBOM text")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                raw_text = conn.execute(
                    "SELECT raw_text FROM analysis_documents"
                ).fetchone()[0]
            self.assertEqual(raw_text, "BOM text")

    def test_builder_splits_circled_legal_items_and_preserves_raw_text(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            input_text = "指定する。⑴ A⑵ B⑶ C"
            (input_dir / "小規模市_条例.txt").write_text(input_text, encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT sentence_text
                    FROM analysis_sentences
                    ORDER BY sentence_no_in_document
                    """
                ).fetchall()
                raw_text = conn.execute("SELECT raw_text FROM analysis_documents").fetchone()[0]

            self.assertEqual([row[0] for row in rows], ["指定する。", "⑴ A", "⑵ B", "⑶ C"])
            self.assertEqual(raw_text, input_text)

    def test_builder_splits_katakana_legal_items_with_non_ascii_payload_and_preserves_raw_text(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            input_text = "ア甲イ乙ウ丙"
            (input_dir / "小規模市_規則.txt").write_text(input_text, encoding="utf-8")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT sentence_text
                    FROM analysis_sentences
                    ORDER BY sentence_no_in_document
                    """
                ).fetchall()
                raw_text = conn.execute("SELECT raw_text FROM analysis_documents").fetchone()[0]

            self.assertEqual([row[0] for row in rows], ["ア甲", "イ乙", "ウ丙"])
            self.assertEqual(raw_text, input_text)

    def test_builder_table_paragraphs_are_not_split_by_legal_item_post_split(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            input_text = "\n".join(
                [
                    "| 区分 | 内容 | 備考 |",
                    "| --- | --- | --- |",
                    "| A | ⑴ A ⑵ B ⑶ C | ア甲イ乙 |",
                ]
            )
            (input_dir / "小規模市_別表.md").write_text(input_text, encoding="utf-8", newline="\n")
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
                "--merge-table-lines",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                paragraph_rows = conn.execute(
                    """
                    SELECT paragraph_text, is_table_paragraph, table_column_count, table_parse_error
                    FROM analysis_paragraphs
                    ORDER BY paragraph_no
                    """
                ).fetchall()
                sentence_rows = conn.execute(
                    """
                    SELECT s.sentence_text, p.is_table_paragraph
                    FROM analysis_sentences AS s
                    JOIN analysis_paragraphs AS p ON p.paragraph_id = s.paragraph_id
                    ORDER BY s.sentence_no_in_document
                    """
                ).fetchall()
                raw_text = conn.execute("SELECT raw_text FROM analysis_documents").fetchone()[0]

            self.assertEqual(raw_text, input_text)
            self.assertEqual(paragraph_rows, [(input_text, 1, 3, 0)])
            self.assertEqual(
                sentence_rows,
                [
                    ("| 区分 | 内容 | 備考 |", 1),
                    ("| A | ⑴ A ⑵ B ⑶ C | ア甲イ乙 |", 1),
                ],
            )
            sentence_texts = [row[0] for row in sentence_rows]
            self.assertNotIn("⑵ B", sentence_texts)
            self.assertNotIn("⑶ C", sentence_texts)
            self.assertNotIn("イ乙", sentence_texts)

    def test_builder_splits_enumeration_after_mixed_parenthesis(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            input_text = (
                "(4) 農地法（営農型太陽光発電設備の設置予定地を除く。第７号において同じ｡)。"
                "(5) 地すべり等防止法の地すべり防止区域。"
                "(6) 急傾斜地崩壊危険区域。"
            )
            (input_dir / "掛川市_施行規則.md").write_text(
                input_text,
                encoding="utf-8",
            )
            analysis_db_path = temp_path / "analysis.db"

            exit_code = builder.main([
                "--input-dir",
                str(input_dir),
                "--analysis-db",
                str(analysis_db_path),
                "--skip-tokenize",
            ])

            self.assertEqual(exit_code, 0)
            with sqlite3.connect(analysis_db_path) as conn:
                rows = conn.execute(
                    """
                    SELECT sentence_no_in_paragraph, sentence_text
                    FROM analysis_sentences
                    ORDER BY sentence_no_in_paragraph
                    """
                ).fetchall()
                raw_text = conn.execute("SELECT raw_text FROM analysis_documents").fetchone()[0]

            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][1], "(4) 農地法（営農型太陽光発電設備の設置予定地を除く。第７号において同じ｡)。")
            self.assertEqual(rows[1][1], "(5) 地すべり等防止法の地すべり防止区域。")
            self.assertEqual(rows[2][1], "(6) 急傾斜地崩壊危険区域。")
            self.assertEqual(raw_text, input_text)
            self.assertIn("同じ｡", rows[0][1])


class ForbiddenDirTest(unittest.TestCase):
    def test_exact_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            (forbidden / "cat1_cat2.txt").write_text("test", encoding="utf-8")

            rows, issues = builder.load_source_rows_from_dir(forbidden, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_parent_of_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            parent = temp_path / "parent"
            parent.mkdir()
            forbidden = parent / "forbidden"
            forbidden.mkdir()

            rows, issues = builder.load_source_rows_from_dir(parent, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_child_of_forbidden_dir_returns_preflight_failure(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            child = forbidden / "child"
            child.mkdir()

            rows, issues = builder.load_source_rows_from_dir(child, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

    def test_prune_excludes_forbidden_subtree(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "valid1_cat2.txt").write_text("test", encoding="utf-8")

            forbidden = input_dir / "forbidden"
            forbidden.mkdir()
            (forbidden / "valid2_cat2.txt").write_text("test", encoding="utf-8")

            # input_dir is parent of forbidden, so preflight blocks it
            rows, issues = builder.load_source_rows_from_dir(input_dir, None, [forbidden])
            self.assertEqual(rows, [])
            self.assertTrue(
                any(i.severity == "error" and i.code == "forbidden_input_dir" for i in issues)
            )

            # Verify the prune logic itself (same / child dirs are dropped)
            dirs = ["forbidden", "allowed"]
            pruned = []
            for d in dirs:
                dir_path = (input_dir / d).resolve()
                is_forbidden = False
                for fdir in [forbidden]:
                    rel = builder.classify_forbidden_input_relation(dir_path, fdir)
                    if rel in ("same", "child"):
                        is_forbidden = True
                        break
                if not is_forbidden:
                    pruned.append(d)
            self.assertEqual(pruned, ["allowed"])

    def test_limit_counts_only_valid_candidates(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "input"
            input_dir.mkdir()
            (input_dir / "a_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "b_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "c_cat2.txt").write_text("test", encoding="utf-8")
            (input_dir / "invalid.txt").write_text("test", encoding="utf-8")
            (input_dir / "bad-name.md").write_text("test", encoding="utf-8")

            # Forbidden dir as a sibling; walk never reaches it, but limit should
            # still apply only to valid rows.
            forbidden = temp_path / "forbidden"
            forbidden.mkdir()
            (forbidden / "d_cat2.txt").write_text("test", encoding="utf-8")

            rows, issues = builder.load_source_rows_from_dir(input_dir, 2, [forbidden])
            self.assertEqual(len(rows), 2)
            self.assertEqual(len([i for i in issues if i.code == "invalid_file_name"]), 2)

    def test_resolve_forbidden_dirs_excludes_texts_2nd_manual(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            project_root = temp_path / "project"
            project_root.mkdir()
            forbidden_dirs = builder.resolve_forbidden_dirs(project_root)
            self.assertNotIn(project_root / "asset" / "texts_2nd" / "manual", forbidden_dirs)
            self.assertIn(project_root / "asset" / "ocr_manual", forbidden_dirs)

    def test_texts_2nd_style_dir_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            input_dir = temp_path / "texts_2nd"
            input_dir.mkdir()
            (input_dir / "cat1_cat2.txt").write_text("test", encoding="utf-8")

            forbidden_dirs = builder.resolve_forbidden_dirs(temp_path)
            rows, issues = builder.load_source_rows_from_dir(input_dir, None, forbidden_dirs)
            self.assertEqual(len(rows), 1)
            self.assertFalse(
                any(i.code == "forbidden_input_dir" for i in issues)
            )


if __name__ == "__main__":
    unittest.main()
