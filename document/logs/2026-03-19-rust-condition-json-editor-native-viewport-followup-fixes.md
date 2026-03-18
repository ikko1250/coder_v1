# 2026-03-19 Rust condition JSON editor native viewport follow-up fixes

## 既存挙動

- viewport 化後の condition editor は panel の `Frame` に fill が無く、OS 背景色が見えて黒く見える状態だった
- body は `ui.columns(2)` の均等割りだったため、左一覧が広すぎる一方で右詳細が窮屈になり、右側 UI が切れやすかった

## 今回の修正

- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の viewport header / body / footer panel に `panel_fill` を明示的に入れた
- [src/app.rs](/mnt/f/program_2026/csv_viewer/src/app.rs) の condition editor body を `ui.columns(2)` から `StripBuilder` に変更した
  - 左: `340px` 固定
  - 右: `remainder`
- 右詳細側を `ScrollArea` に包み、残り幅を使って編集できるようにした

## 検証

- `git diff --check -- src/app.rs`
  - 成功

## 備考

- `cargo check` はこの環境に `cargo` が無いため未実施
