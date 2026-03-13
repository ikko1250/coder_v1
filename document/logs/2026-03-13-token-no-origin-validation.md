# 2026-03-13 token_no 起点確認ログ

- 対象: `analysis_backend/token_position.py`, `tests/test_analysis_core.py`, `asset/ordinance_analysis3.db`, `asset/ordinance_analysis4.db`
- 目的: `token_no` をそのまま position に使っている実装が実データ契約と整合しているか確認する

## 既存挙動

- `token_position.py` は `sentence_token_position = token_no` を採用している
- テスト fixture は 0 始まり `token_no` が多く、実 DB の起点とは未照合だった

## 実データ確認

- `asset/ordinance_analysis3.db`
  - `analysis_tokens.token_no` 全体最小値: `1`
  - 先頭 10 sentence の `min(token_no)` はすべて `1`
- `asset/ordinance_analysis4.db`
  - `analysis_tokens.token_no` 全体最小値: `1`
  - 先頭 10 sentence の `min(token_no)` はすべて `1`

## 判断

- 実 DB の `token_no` は 1 始まり
- 現行実装は source の `token_no` をそのまま保持するため、実データ契約とは整合している
- downstream で使っているのは主に順序と span 差分なので、0/1 始まりの違い自体では距離判定は壊れない
- ただし、実 DB とテスト fixture の起点がずれていたため、1 始まり契約を保護するテストを追加した

## 実施内容

1. `token_position.py` に意図コメントを追加
   - source token numbering を保持することを明示
2. `tests/test_analysis_core.py` に 1 始まり fixture のテストを追加
   - `sentence_token_position` が `token_no` を保持する
   - `paragraph_token_position` が sentence 境界をまたいで連続する

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 22 tests passed

## 備考

- この確認により、レビュー指摘の「0 始まりでないと不正」は採用しない
- 代わりに「実データは 1 始まりであり、その契約を保持している」と整理する
