# 2026-03-13 Task 8 実施ログ

- 対象: `analysis_backend/analysis_core.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`
- 目的: `analysis_core.py` を明示的な compatibility façade として整える

## 既存挙動

- `analysis_core.py` は一部 wrapper 化されていたが、どの関数が互換レイヤーで、どの関数が本体実装かがコード上で分かりにくかった
- package root の `analysis_backend` には `__getattr__` があるが、`dir()` では export 面が明確でなかった
- 互換性を守るべき public symbol は存在していたが、その意図がテストで明示されていなかった

## 実施内容

1. `analysis_core.py` に `__all__` を追加し、legacy public API を明示
2. moved function は `_impl` alias で import し、`analysis_core.py` 側に wrapper 関数を定義
   - `load_filter_config(...)`
   - `read_analysis_tokens(...)`
   - `read_analysis_sentences(...)`
   - `read_paragraph_document_metadata(...)`
   - `build_tokens_with_position_df(...)`
3. `build_condition_hit_tokens_df(...)` と `select_target_ids_by_cooccurrence_conditions(...)` に
   - `Legacy facade` コメントを追加
   - structured result から DataFrame / 5 要素 tuple へ戻す責務を明示
4. `analysis_backend/__init__.py` に `__dir__()` を追加し、export 面を introspection 可能にした
5. テスト追加
   - `dir(analysis_backend)` に主要 export が見える
   - `analysis_core` の legacy function symbol が module 上に残っている

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 17 tests passed

## Web 調査

- Python 公式 docs を確認
  - module `__getattr__`
  - module `__dir__`
- 参考:
  - `https://docs.python.org/3/reference/datamodel.html#customizing-module-attribute-access`

## 備考

- この段階では package root の一部 symbol は引き続き新 submodule を直接向いている
- ただし `analysis_core.py` 自体は、legacy import path を維持する compatibility façade として意図を明示できた
