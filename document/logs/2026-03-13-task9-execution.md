# 2026-03-13 Task 9 実施ログ

- 対象: `analysis_backend/rendering.py`, `analysis_backend/export_formatter.py`, `analysis_backend/analysis_core.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`
- 目的: rendering と export formatting を `analysis_core.py` から分離する

## 既存挙動

- `analysis_core.py` はまだ以下を直接持っていた
  - `render_tagged_token(...)`
  - `build_token_annotations_df(...)`
  - `build_rendered_paragraphs_df(...)`
  - `enrich_reconstructed_paragraphs_df(...)`
  - `build_reconstructed_paragraphs_export_df(...)`
- HTML escape 順序と CSV 列順は既存契約としてすでにテストで守られていた

## 実施内容

1. `analysis_backend/rendering.py` を追加
   - `render_tagged_token(...)`
   - `build_token_annotations_df(...)`
   - `build_rendered_paragraphs_df(...)`
   - rendering 用 helper と schema
2. `analysis_backend/export_formatter.py` を追加
   - `enrich_reconstructed_paragraphs_df(...)`
   - `build_reconstructed_paragraphs_export_df(...)`
3. `analysis_core.py` は wrapper 化
   - moved function は `_impl` alias で import
   - façade 側は legacy symbol を維持
4. `analysis_backend/__init__.py` の package API も新 module を向くよう更新
5. テスト更新
   - package API が `rendering.py` / `export_formatter.py` を向く
   - `analysis_core` には legacy function symbol が残る
   - 既存の escape / export 列順 / dtype 契約は継続

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 17 tests passed

## Web 調査

- Python 公式 docs を確認
  - `html.escape`
- Polars 公式 docs を確認
  - `list.join`
  - `DataFrame.sort`
  - `DataFrame.with_columns`
- 参考:
  - `https://docs.python.org/3.12/library/html.html#html.escape`
  - `https://docs.pola.rs/api/python/stable/reference/expressions/api/polars.Expr.list.join.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.sort.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.with_columns.html`

## 備考

- `analysis_core.py` にはまだ reconstruct 系関数が残っている
- ただし rendering/export の本体は分離できたため、今後は façade の外から実装を個別 module 単位で追える状態になった
