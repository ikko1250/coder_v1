# 2026-03-13 DataAccessResult 導入ログ

- 対象: `analysis_backend/condition_model.py`, `analysis_backend/data_access.py`, `analysis_backend/analysis_core.py`, `analysis_backend/cli.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`, `tests/test_cli.py`
- 目的: SQLite 読込失敗を Result 型で扱えるようにし、CLI が data access failure を構造化して meta JSON へ出せるようにする

## 既存挙動

- `data_access.py` は SQLite 読込失敗を `RuntimeError` で投げていた
- CLI は `except Exception` でまとめて失敗扱いにしていた
- DB 読込失敗の種別は `errorSummary` 文字列でしか識別できなかった

## 実施内容

1. `condition_model.py` を更新
   - `DataAccessIssue`
   - `DataAccessResult`
   を追加
2. `data_access.py` を更新
   - `read_analysis_tokens_result(...)`
   - `read_analysis_sentences_result(...)`
   - `read_paragraph_document_metadata_result(...)`
   を追加
   - legacy API は façade として Result を unwrap して例外へ変換
3. `analysis_core.py` / `analysis_backend.__init__` を更新
   - 新 Result API を export
4. `cli.py` を更新
   - token / sentence 読込は Result API を使う
   - data access error 時は `warningMessages` に issue を載せつつ failure meta を返す
   - `queryName`, `dbPath` を serialize
5. テスト追加
   - missing table 時に `read_analysis_tokens_result(...)` が structured issue を返す
   - CLI が data access failure を meta JSON に書くこと

## issue code

- `sqlite_read_failed`
- `sqlite_metadata_read_failed`

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 29 tests passed

## 備考

- この段階では CLI の `except Exception` はまだ残っている
- ただし filter config と data access については Result API を持てたため、次段で例外依存をさらに縮小できる
