# 2026-03-13 CLI limit_rows 整合修正ログ

- 対象: `analysis_backend/cli.py`, `tests/test_cli.py`
- 目的: `limit_rows` 指定時に `analysis_tokens` と `analysis_sentences` の扱いを整合させる

## 既存挙動

- CLI は `read_analysis_tokens(..., limit_rows=args.limit_rows)` を使っていた
- 一方で `read_analysis_sentences(..., limit_rows=None)` は固定で全文読込だった
- 単純に同じ `limit_rows` を sentences に渡すと、token 件数と sentence 件数の単位差で join を壊す可能性がある

## 実施内容

1. `cli.py` に `_filter_sentences_for_tokens(...)` を追加
   - `analysis_tokens_df` に含まれる `sentence_id + paragraph_id` の組だけ残す
   - token が空、または sentence が空なら空 DataFrame を返す
2. `run_analysis_job(...)` を更新
   - `limit_rows is not None` の場合だけ、全文読込した `analysis_sentences_df` を helper で絞り込む
3. `tests/test_cli.py` を更新
   - helper が limit 済み token に対応する sentence だけを残すことを確認するテストを追加

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 21 tests passed

## 備考

- この修正は `limit_rows` の意味を「token を制限したときに必要な sentence だけ使う」へ揃えるもの
- sentence 側に同じ件数 limit を適用する実装は採っていない
