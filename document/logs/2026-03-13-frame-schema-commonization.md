# 2026-03-13 frame schema 共通化ログ

- 対象: `analysis_backend/frame_schema.py`, `analysis_backend/analysis_core.py`, `analysis_backend/token_position.py`, `analysis_backend/distance_matcher.py`, `analysis_backend/data_access.py`, `analysis_backend/condition_evaluator.py`, `analysis_backend/rendering.py`, `tests/test_analysis_core.py`
- 目的: 分割後に散った `_empty_df()` と shared schema 定義を 1 か所へ寄せる

## 既存挙動

- `POSITIONED_TOKEN_SCHEMA` は `analysis_core.py`, `token_position.py`, `rendering.py` に重複していた
- `CONDITION_HIT_SCHEMA` は `analysis_core.py` と `distance_matcher.py` に重複していた
- `PARAGRAPH_METADATA_SCHEMA` は `data_access.py` に個別定義されていた
- `empty DataFrame` 生成 helper も複数 module に分散していた

## 実施内容

1. `analysis_backend/frame_schema.py` を追加
   - `POSITIONED_TOKEN_SCHEMA`
   - `CONDITION_HIT_SCHEMA`
   - `PARAGRAPH_METADATA_SCHEMA`
   - `empty_df(...)`
2. 各 module を更新
   - shared schema を新 module から import
   - local `_empty_df()` を削除
   - 空 DataFrame 生成は `empty_df(...)` に統一
3. `tests/test_analysis_core.py` を更新
   - shared schema が各 module で再利用されていることを確認
   - `empty_df(...)` が空 DataFrame を返すことを確認

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 23 tests passed

## Web 調査

- Polars 公式 docs を確認
  - `polars.DataFrame`
- 参考:
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.html`

## 備考

- 今回は shared schema の一本化に限定しており、`TOKEN_ANNOTATION_SCHEMA` や `RENDERED_PARAGRAPH_SCHEMA` は local のまま残している
- 次段で export/render もさらに集約する場合は、この module を起点に広げられる
