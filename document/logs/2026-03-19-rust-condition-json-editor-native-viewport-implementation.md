# 2026-03-19 Rust condition JSON editor native viewport 化 実装

## 既存挙動

- condition editor は [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の `egui::Window` としてメインウィンドウ内に描かれていた
- DB viewer には `show_viewport_immediate(...)` を使った native viewport の前例があった
- condition editor の JSON load/save, sanitize, atomic write は [src/condition_editor.rs](/mnt/f/program_2026/csv_viewer/src/condition_editor.rs) に分離済みだった

## 今回の実装

- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の condition editor を `egui::Window` から `show_viewport_immediate(...)` ベースへ変更
- condition editor 用 fixed viewport id を追加
- `条件編集` 再押下時に
  - `ViewportCommand::Minimized(false)`
  - `ViewportCommand::Focus`
  を送るようにした
- editor UI を
  - header panel
  - body panel
  - footer panel
  へ分けた
- dirty confirm を viewport 内 overlay へ移した
- root viewport close 時に、condition editor が dirty なら `CancelClose` して終了を止めるガードを追加した

## 検証

- `git diff --check -- src/app.rs`
  - 成功
- `cargo check`
  - 実施不可
  - この環境に `cargo` が存在しないため

## 備考

- Rust compile / test / fmt は未実施
- 手元確認はユーザー環境で実施前提
