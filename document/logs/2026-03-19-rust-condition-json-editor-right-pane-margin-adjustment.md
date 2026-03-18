# 2026-03-19 Rust condition JSON editor 右詳細ペイン margin 再調整

## 既存挙動

- condition editor body には中央 `16px` のガター列が入っていた
- 右詳細ペインは `Frame` の inner margin が `left/right = 14` だった

## 今回の修正

- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の condition editor body から中央ガター列を削除した
- 右詳細ペインの inner margin を
  - `left = 32`
  - `right = 32`
  に拡大した

## 検証

- `git diff --check -- src/app.rs`
  - 成功

## 備考

- `cargo check` はこの環境に `cargo` が無いため未実施
