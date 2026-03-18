# 2026-03-19 Rust condition JSON editor 右詳細ペイン padding 修正

## 既存挙動

- viewport 化後の condition editor は、左右ペインの間に十分なガターが無かった
- 右詳細ペインも `ScrollArea` をそのまま置いていたため、内側余白が不足し、右端が詰まって見えた

## 今回の修正

- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の `draw_condition_editor_body_panel(...)` を更新した
- `StripBuilder` に 16px の中央ガター列を追加した
- 右詳細ペインを `Frame` で包み、`left/right/top/bottom` の inner margin を明示した

## 検証

- `git diff --check -- src/app.rs`
  - 成功

## 備考

- `cargo check` はこの環境に `cargo` が無いため未実施
