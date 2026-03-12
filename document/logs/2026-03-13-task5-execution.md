# 2026-03-13 Task 5 実施ログ

- 対象: `analysis_backend/data_access.py`, `analysis_backend/token_position.py`, `analysis_backend/analysis_core.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`
- 目的: SQLite 読込と token 位置計算の責務を `analysis_core.py` から分離する

## 既存挙動

- `analysis_core.py` は以下を直接持っていた
  - `read_analysis_tokens(...)`
  - `read_analysis_sentences(...)`
  - `read_paragraph_document_metadata(...)`
  - `build_tokens_with_position_df(...)`
  - candidate token 用の private helper
- CLI / marimo / package API はこれらを `analysis_core` 経由で利用していた

## 実施内容

1. `analysis_backend/data_access.py` を追加
   - `read_analysis_tokens(...)`
   - `read_analysis_sentences(...)`
   - `read_paragraph_document_metadata(...)`
2. `analysis_backend/token_position.py` を追加
   - `build_tokens_with_position_df(...)`
   - `build_candidate_tokens_with_position_df(...)`
3. `analysis_core.py` は上記モジュールを import する形へ変更
   - 既存の public 関数名は維持
   - 内部の candidate token 呼び出しも新 helper へ切替
4. `analysis_backend/__init__.py` の package API を新モジュールへ向けた
5. テスト更新
   - package API が `data_access.py` と `token_position.py` を向くことを確認
   - 既存の sentence order 契約テストは維持

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 13 tests passed

## Web 調査

- Polars 公式 docs を確認
  - `read_database`
  - `DataFrame.join`
  - `DataFrame.select`
  - `DataFrame.with_columns`
- 参考:
  - `https://docs.pola.rs/user-guide/io/database/`
  - `https://docs.pola.rs/py-polars/html/reference/api/polars.read_database.html`
  - `https://docs.pola.rs/api/python/dev/reference/dataframe/api/polars.DataFrame.join.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.select.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.with_columns.html`

## 備考

- `reconstruct_sentences_by_ids(...)` と `reconstruct_paragraphs_by_ids(...)` は今回は移していない
- この段階では「DB 読込」と「位置計算」だけを先に分離し、再構成処理は後続タスクへ残す
