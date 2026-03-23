# P2-02: レコード・フィルタ・選択のドメイン状態を `ViewerCoreState` へ集約

設計書 §9.1 P2-02 の完了条件に基づき、**一覧・フィルタ・行選択**に関する状態を `App` の直下フィールドから [`crate::viewer_core::ViewerCoreState`]（`src/viewer_core.rs`）へ移した。`ViewerCoreState` は **egui / eframe を持たない**。

## 移したフィールド（`App` → `App::core`）

| フィールド | 型（要点） |
|------------|------------|
| `all_records` | `Vec<AnalysisRecord>` |
| `filtered_indices` | `Vec<usize>` |
| `filter_options` | `HashMap<FilterColumn, Vec<FilterOption>>` |
| `selected_filter_values` | `HashMap<FilterColumn, BTreeSet<String>>` |
| `filter_candidate_queries` | `HashMap<FilterColumn, String>` |
| `active_filter_column` | `FilterColumn` |
| `selected_row` | `Option<usize>` |

## その他

- **`clamp_selected_row`** は `app.rs` から `viewer_core` に集約し、選択行のクランプはコア側の純関数として再利用する。
- UI サブモジュール（`app_main_layout` / `app_toolbar` / `app_lifecycle` 等）は **`app.core.*`** 経由で参照する。

## 非目的（P2-02 ではやらないこと）

- 分析ランタイム・条件エディタ・キャッシュ等の残りの `App` 状態のコア化（後続フェーズ）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-02 初版 |
