# 2026-03-19 Rust condition JSON editor 右詳細ペイン right margin 64 調整

## 既存挙動

- 右詳細ペインの inner margin は `left = 32`, `right = 32` だった

## 今回の修正

- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の右詳細ペイン inner margin を
  - `left = 32`
  - `right = 64`
  に変更した

## 検証

- `git diff --check -- src/app.rs`
  - 成功

## 備考

- `cargo check` はこの環境に `cargo` が無いため未実施
