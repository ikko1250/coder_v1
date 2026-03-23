# P1-11: 公開 API の見直し（`pub(crate)` / `pub(super)` / 警告整理）

設計書 §9.1 P1-11 の完了条件に沿い、**バイナリクレート内の可視性**と **`dead_code` 警告の解消**を記録する。

---

## 1. クレート構造の前提

- ルートは **`main.rs` の単一バイナリ**（`lib.rs` なし）。`pub(crate)` はクレート内のモジュール間共有、`pub(super)` は親（`app`）からのみ参照、という使い分けが可能。
- `app.rs` の子モジュール（`app_*`）は、**`App` への委譲エントリ**を `pub(super) fn` に揃え、**兄弟サブモジュールから直接呼ばない**（`app_analysis_job` ↔ `app_condition_editor` の横断は `App` または共通の `crate::` モジュール経由）。

---

## 2. `app` 子モジュールの公開エントリ（`pub(super) fn`）

| ファイル | 公開関数（概要） |
|----------|------------------|
| `app_toolbar.rs` | `draw_toolbar` |
| `app_db_viewer.rs` | `draw_db_viewer_button`, `open_db_viewer_for_selected_record`, `load_db_viewer_context`, `load_db_viewer_context_for_location`, `previous_db_viewer_location`, `next_db_viewer_location`, `draw_db_viewer_window` |
| `app_analysis_settings.rs` | `draw_analysis_settings_window` |
| `app_analysis_job.rs` | `try_cleanup_analysis_jobs`, `refresh_analysis_runtime`, `resolved_filter_config_path`, `start_analysis_job`, `start_export_job`, `poll_analysis_job`, `draw_warning_details_window`, `guard_root_close_with_dirty_editor` |
| `app_main_layout.rs` | `draw_body` |
| `app_error_dialog.rs` | `draw_error_dialog_if_any` |
| `app_condition_editor.rs` | `focus_condition_editor_viewport`, `open_condition_editor`, `sync_condition_editor_with_runtime_path`, `draw_condition_editor_window` |
| `app_lifecycle.rs` | `run_update_prelude` |

上記以外の関数・型は **`pub(super)` なし**（モジュール内 private）とし、P1 分割の境界を維持する。

---

## 3. トップレベルモジュールの `pub(crate)`

`filter`, `model`, `analysis_runner`, `condition_editor`, `db` 等は **`main` / `app` / ビュー**から共有されるため、`pub(crate) struct` / `fn` を継続利用する。将来コアクレートを分離する際は、**ここを `pub` に絞った上で crate 境界を切る**想定（P2 以降）。

---

## 4. `dead_code` 整理（本変更）

| 対象 | 対応 |
|------|------|
| `analysis_runner.rs` の `AnalysisJsonResponse` / `read_meta_json` / `read_json_response` | **テスト専用**だったが非 test ビルドでもコンパイルされ `dead_code` になっていた。`#[cfg(test)] mod json_response_tests` に移し、単体テストから `super::json_response_tests::…` 経由で利用。 |

**結果**: `cargo check` で **警告 0 件**（既存の `analysis_runner` 以外の警告は解消済み）。

---

## 5. 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P1-11 初版（可視性方針 + `analysis_runner` の test 専用コードを `cfg(test)` に集約） |
