# 2026-03-25 sentence モードで「高度条件の説明」が表示されない問題の修正

## 概要

`analysis_unit: "sentence"` で実行した場合に、GUI上で「高度条件の説明」CollapsingHeader が表示されなかった問題を修正。

## 原因

sentence モードのデータパイプラインにおいて、paragraph-level の form group 説明に関する4カラムが出力スキーマ/カラムリストから欠落していた。

- `matched_form_group_ids_text`
- `matched_form_group_logics_text`
- `form_group_explanations_text`
- `mixed_scope_warning_text`

Rust側の `AnalysisJsonRecord` は `#[serde(default)]` によりフィールド欠落時に空文字列となり、`draw_form_group_explanations_panel` で空チェックにより早期リターン → パネル非表示。

## 修正内容

### rendering.py
- `RENDERED_SENTENCE_SCHEMA` に上記4カラムを追加
- `build_rendered_sentences_df()` に `paragraph_match_summary_df` パラメータを追加
- `_merge_sentence_paragraph_match_summary()` ヘルパー関数を新設し、paragraph-level の説明を sentence 行にマージ

### analysis_core.py
- `build_rendered_sentences_df()` ラッパー関数に `paragraph_match_summary_df` パラメータを追加・転送

### cli.py
- `build_rendered_sentences_df()` 呼び出し時に `paragraph_match_summary_df` を渡すように変更

### export_formatter.py
- `SENTENCE_GUI_RECORD_COLUMNS` に4カラムを追加
- `enrich_reconstructed_sentences_result()` の `.select()` に4カラムを追加
- `build_reconstructed_sentences_export_df()` の `.select()` に4カラムを追加
- `build_sentence_gui_records_df()` の cast/fill_null に4カラムを追加

## 検証

- Python モジュール import: OK
- `cargo check`: OK (既存 warning のみ)
