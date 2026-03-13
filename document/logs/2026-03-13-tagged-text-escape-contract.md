# 2026-03-13 tagged text escape 契約整理ログ

- 対象: `analysis_backend/rendering.py`, `src/tagged_text.rs`
- 目的: `[[HIT ...]]` 属性 escape が HTML 用ではなく、Rust 側 parser との独自契約であることを明示する

## 既存挙動

- Python 側 `rendering.py` は `[[HIT ...]]` 形式の独自タグを出力する
- 属性値の escape は backslash と double quote のみを対象としていた
- Rust 側 `src/tagged_text.rs` は regex と `unescape_attribute(...)` で、その形式を復元している
- つまりここは HTML 属性 escape ではなく、Python/Rust 間の tagged-text 契約だった

## 実施内容

1. `rendering.py` にコメントを追加
   - `_escape_tag_attribute(...)` が HTML ではなく custom tagged text 用であること
   - `src/tagged_text.rs::unescape_attribute` と揃える必要があること
2. `src/tagged_text.rs` に unit test を追加
   - escaped quote / backslash を含む属性値が復元されること
   - HIT マーカーが無い場合は plain text 1 segment のままであること

## 検証

- Python 側:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
  - 23 tests passed
- Rust 側:
  - この環境には `cargo` が無いため未実行

## 備考

- 今回は escape 方式そのものは変更していない
- 変更すると Rust parser 側と同時に直す必要があるため、まず契約を明文化した
