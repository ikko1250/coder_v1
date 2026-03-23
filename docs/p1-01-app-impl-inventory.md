# P1-01: `impl App` 機能別一覧と切り出し候補モジュール

本書は `src/app.rs` 内の **`impl App` に属するメソッド**を列挙し、P1（論理分割）向けの**移動候補モジュール名**を付与したものである。  
`app.rs` には `impl App` が**二つのブロック**に分かれている（その間に `impl eframe::App for App` が挟まる）。

## `impl App` のブロック構成

| ブロック | おおよその行範囲 | 内容 |
|----------|------------------|------|
| 第 1 ブロック | 337 行付近 〜 1300 行付近 | コンストラクタ・データ・フィルタ・選択・DB/注釈・条件エディタ状態・分析ジョブ・警告ウィンドウ・終了ガード等 |
| `impl eframe::App` | 1302 〜 1348 | `update` のみ |
| 第 2 ブロック | 1352 行付近 〜 2775 行付近 | DB Viewer ウィンドウ・ツールバー・分析設定・条件エディタ描画・中央ペイン（フィルタ/ツリー/詳細）等 |

※ 行番号は作成時点の参照用。リファクタ後はずれる。

---

## 切り出し候補モジュールとメソッド対応

モジュール名は **スネークケース**（Rust の `app_toolbar.rs` 等）を想定。責務が重いものはさらに分割可能。

### A. `app_data` — データソース・レコード置換

| メソッド | 備考 |
|----------|------|
| `App::new` | エントリ。分割時は `App` 組み立てのまま残すか、`App::default` + `init` に分ける判断は後続 |
| `load_csv` | |
| `replace_records` | フィルタ・キャッシュ・選択をまとめて更新 |

### B. `app_selection` — 一覧インデックス・キーボード選択

| メソッド | 備考 |
|----------|------|
| `apply_selection_change` | |
| `select_first_filtered_row` | |
| `move_selection_up` | |
| `move_selection_down` | |
| `handle_keyboard_navigation` | `egui::Context` 依存（ホスト寄り） |
| `selected_record` | |
| `selected_record_index` | |
| `selected_record_mut` | |

### C. `app_filter` — フィルタ状態と `filtered_indices` 更新

| メソッド | 備考 |
|----------|------|
| `apply_filters` | |
| `record_matches_filters` | |
| `clear_filters_for_column` | |
| `clear_all_filters` | |
| `toggle_filter_value` | |

### D. `app_segments` — 本文セグメント（メモ化キャッシュ）

| メソッド | 備考 |
|----------|------|
| `get_segments` | `cached_segments` 無効化ロジックの要 |

### E. `app_annotation` — 手動アノテーション追記

| メソッド | 備考 |
|----------|------|
| `resolved_annotation_csv_path` | |
| `annotation_save_enabled` | |
| `clear_annotation_editor_status` | |
| `clear_annotation_editor_inputs` | |
| `build_annotation_append_row` | |
| `apply_saved_annotation_to_selected_record` | |
| `save_annotation_for_selected_record` | |

### F. `app_db_viewer` — DB 参照ウィンドウ・状態準備

| メソッド | 備考 |
|----------|------|
| `draw_db_viewer_button` | **実装済**: `src/app_db_viewer.rs`。`impl App` は `draw_db_viewer_button` / `draw_db_viewer_window` のみ委譲。 |
| `selected_paragraph_id_for_db` 等 | 同上モジュール内の非公開関数（`prepare_db_viewer_state`、`open_db_viewer_for_selected_record`、`load_db_viewer_context` 等）。 |
| `draw_db_viewer_window` | 同上。`parent::DB_VIEWER_VIEWPORT_ID` を参照。 |

### G. `app_analysis_job` — Python 分析・エクスポート・ランタイム

| メソッド | 備考 |
|----------|------|
| `try_cleanup_analysis_jobs` 〜 `guard_root_close_with_dirty_editor` | **実装済**: `src/app_analysis_job.rs`。`handle_*` / `warning_*` はモジュール内の非公開関数。 |
| `resolved_filter_config_path` | 条件エディタ側は `app_analysis_job::resolved_filter_config_path(self)` を直接呼ぶ。 |
| `poll_analysis_job` | `egui::Context` 依存。 |
| `draw_warning_details_window` | |
| `guard_root_close_with_dirty_editor` | 終了ガード（未保存の条件エディタ）。 |

### H. `app_condition_editor` — 条件 JSON エディタ（状態・コマンド・描画）

第 1 ブロック・第 2 ブロックにまたがるメソッドが多い。

**状態・パス同期**

| メソッド |
|----------|
| `focus_condition_editor_viewport` |
| `open_condition_editor` |
| `load_condition_editor_from_path` |
| `clamp_condition_editor_selection` |
| `clamp_condition_editor_group_selection` |
| `mark_condition_editor_dirty` |
| `condition_editor_selection_draft` |
| `condition_editor_window_inputs` |
| `apply_condition_editor_selection_draft` |
| `reload_condition_editor` |
| `request_condition_editor_reload` |
| `save_condition_editor_document` |
| `sync_condition_editor_with_runtime_path` |

**描画（サブパネル）**

| メソッド |
|----------|
| `draw_condition_editor_body_panel` |
| `draw_condition_editor_detail_panel` |
| `draw_condition_editor_detail_contents` |
| `condition_editor_status_message` |
| `condition_editor_save_enabled` |
| `condition_editor_confirm_message` |
| `draw_condition_editor_embedded_window` |
| `draw_condition_editor_viewport_panels` |
| `draw_condition_editor_window` |

**レスポンス適用（イベントハンドラ）**

| メソッド |
|----------|
| `apply_condition_editor_close_request` |
| `apply_condition_editor_footer_response` |
| `apply_condition_editor_confirm_overlay_response` |
| `apply_condition_editor_list_response` |
| `apply_condition_editor_detail_response` |
| `apply_condition_editor_add_request` |
| `apply_condition_editor_delete_request` |
| `apply_condition_editor_reload_request` |
| `apply_condition_editor_modal_response` |
| `apply_condition_editor_command_draft` |

### I. `app_toolbar` — トップツールバー

| メソッド | 備考 |
|----------|------|
| `draw_toolbar` | **P1-02 済**: `src/app_toolbar.rs`（`app` の子モジュール）に実装。`impl App` は `app_toolbar::draw_toolbar` に委譲。 |

### J. `app_analysis_settings` — 分析設定オーバーレイ

| メソッド | 備考 |
|----------|------|
| `draw_analysis_settings_window` | **実装済**: `src/app_analysis_settings.rs`。`draw_analysis_path_override_row` は同ファイル内の非公開関数。 |

### K. `app_warning` — 分析警告詳細ウィンドウ

| メソッド | 備考 |
|----------|------|
| `draw_warning_details_window` | **実装場所**: `app_analysis_job.rs`（§G と同じファイル）に集約。 |

### L. `app_lifecycle` — フレーム単位の統合・終了ガード

| メソッド | 備考 |
|----------|------|
| `guard_root_close_with_dirty_editor` | **実装場所**: `app_analysis_job.rs`。 |
| `update` | `impl eframe::App` だが、論理上は「フレームオーケストレーション」。ファイル上は現状どおり `app.rs` に残すか、`app_lifecycle.rs` に移すかは実装時判断 |

### M. `app_main_layout` — 中央ペイン（フィルタ・ツリー・詳細・注釈 UI）

| メソッド | 備考 |
|----------|------|
| `draw_body` | **P1-03 済**: `src/app_main_layout.rs`。`impl App` は委譲のみ。 |
| `record_list_panel_width_range` 〜 `draw_annotation_editor_panel` | 同上。`build_record_text_layout_job` / `editor_status_color` も同ファイルへ移動。 |
| `draw_filters` | `filter_panel_view` 呼び出し |
| `draw_tree` | `TREE_COLUMN_SPECS` は親 `app.rs` の定数を参照 |

---

## `impl App` に含まれない（ファイル末尾の自由関数）

以下は **`impl App` 外**のヘルパであり、切り出し時は `app_ui_helpers.rs` や既存 `ui_helpers` との整理対象になる。

- `build_record_text_layout_job`（→ `app_main_layout.rs`）
- `analysis_status_color`（`app_toolbar.rs`）、`editor_status_color`（`app_main_layout.rs`）
- `draw_analysis_path_override_row`（→ `app_analysis_settings.rs`）
- `build_tree_*_column` / `tree_*_value` 系
- `clamp_condition_index` 等（条件エディタ用の自由関数）

---

## P1-02 以降へのメモ

- **`app_condition_editor`** と **`app_main_layout`** は行数が大きいため、**最初に切り出すなら `app_toolbar` または `app_db_viewer`** のように境界が明瞭なものから着手すると差分が追いやすい。
- `poll_analysis_job` / `handle_keyboard_navigation` / `guard_root_close_with_dirty_editor` は **`egui::Context` 依存**が強い。P2 のコア分離時はホスト側に残す想定（設計書 §5）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P1-01 初版（`feature/p1-01-app-impl-inventory`） |
| 2026-03-23 | P1-02: `draw_toolbar` を `src/app_toolbar.rs` へ切り出し（子モジュール `#[path]`） |
| 2026-03-23 | DB Viewer 系を `src/app_db_viewer.rs` へ切り出し（`app` 子モジュール） |
| 2026-03-23 | 分析設定を `app_analysis_settings.rs`、分析ジョブ・警告・終了ガードを `app_analysis_job.rs` へ切り出し |
| 2026-03-23 | P1-03: 中央ペインを `app_main_layout.rs` へ切り出し（`TreeScrollRequest` を `pub(super)`） |
