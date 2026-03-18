# 2026-03-18 Rust condition JSON editor Stage 1 実装

## 既存挙動

- Rust 側は `filter_config_path` の override を `分析設定` で切り替えられるだけで、condition JSON 本体の編集 UI は無かった
- condition JSON の読込・validation は Python 側が担当しており、Rust 側は JSON 内容を保持していなかった
- `analysis settings` は `current_job.is_none()` を使って disable 制御していた

## 今回の実装

- 新規 [src/condition_editor.rs](/mnt/f/program_2026/csv_viewer/src/condition_editor.rs) を追加
  - condition JSON の struct model
  - unknown field 保持用 `extra_fields`
  - `max_token_distance` などの型揺れ吸収
  - sanitize helper
  - atomic write helper
- [src/main.rs](/mnt/f/program_2026/csv_viewer/src/main.rs) に `mod condition_editor;` を追加
- [src/analysis_runner.rs](/mnt/f/program_2026/csv_viewer/src/analysis_runner.rs) の `resolve_filter_config_path(...)` を `pub(crate)` 化
- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) に condition editor state / UI を追加
  - ツールバー `条件編集` ボタン
  - 別ウィンドウ editor
  - condition 一覧
  - 詳細フォーム
  - list editor / annotation filter editor helper
  - dirty close / reload ガード
  - settings の path 変更時の同期
  - job 実行中の save / reload disable

## 検証

- `git diff --check -- src/app.rs src/analysis_runner.rs src/main.rs src/condition_editor.rs`
  - 成功
- `cargo check`
  - 実施不可
  - この環境に `cargo` が存在しないため

## 備考

- 既存の `.gitignore` 差分に trailing whitespace があるため、`git diff --check` 全体実行は今回の変更範囲外で失敗した
- Rust compile / test / fmt は未実施
