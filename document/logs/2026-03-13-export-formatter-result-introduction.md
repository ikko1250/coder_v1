# 2026-03-13 export formatter Result 導入ログ

- 対象: `analysis_backend/export_formatter.py`, `analysis_backend/analysis_core.py`, `analysis_backend/__init__.py`, `analysis_backend/cli.py`, `tests/test_analysis_core.py`, `tests/test_cli.py`
- 目的: paragraph metadata 読込失敗を Result 型で CLI まで伝播し、`except Exception` 依存をさらに減らす

## 既存挙動

- `read_paragraph_document_metadata_result(...)` は存在していた
- ただし `enrich_reconstructed_paragraphs_df(...)` は legacy 例外 API に依存していた
- そのため metadata 読込失敗は export formatter の内部例外としてしか扱えなかった

## 実施内容

1. `export_formatter.py` を更新
   - `enrich_reconstructed_paragraphs_result(...)` を追加
   - metadata 読込失敗時は `DataAccessResult` の issue をそのまま返す
   - legacy `enrich_reconstructed_paragraphs_df(...)` は façade として例外へ戻す
2. `analysis_core.py` / `analysis_backend.__init__` を更新
   - `enrich_reconstructed_paragraphs_result(...)` を export
3. `cli.py` を更新
   - formatter の Result API を利用
   - metadata failure 時も structured issue を `warningMessages` に載せて failure meta を返す
4. テスト追加
   - export formatter Result が `sqlite_metadata_read_failed` を返すこと
   - CLI が metadata failure を `meta.json` に書くこと

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 31 tests passed

## 備考

- この段階で CLI は filter config, token read, sentence read, metadata enrich について Result 型経由になった
- まだ top-level の `except Exception` は残るが、範囲はかなり縮小できている
