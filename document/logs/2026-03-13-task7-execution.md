# 2026-03-13 Task 7 実施ログ

- 対象: `analysis_backend/condition_evaluator.py`, `analysis_backend/analysis_core.py`, `tests/test_analysis_core.py`
- 目的: 条件正規化、条件評価、target selection を `analysis_core.py` から分離する

## 既存挙動

- `analysis_core.py` は以下を直接持っていた
  - `_normalize_condition_categories(...)`
  - `_clean_cooccurrence_conditions(...)`
  - `select_target_ids_by_cooccurrence_conditions(...)` の本体
- 条件の正規化後表現は dict ベースだった
- 公開 API は 5 要素 tuple を返していた

## 実施内容

1. `analysis_backend/condition_evaluator.py` を追加
   - `normalize_cooccurrence_conditions(...)`
   - `select_target_ids_by_conditions_result(...)`
2. 条件正規化は `NormalizedCondition` dataclass を返す形へ変更
3. target selection の本体を `TargetSelectionResult` ベースで新 module に移設
4. `analysis_core.py` は wrapper 化
   - `build_condition_hit_tokens_df(...)` では `NormalizedCondition` を dict へ変換して matcher へ渡す
   - `select_target_ids_by_cooccurrence_conditions(...)` は `TargetSelectionResult` を 5 要素 tuple へ戻して返す
5. `analysis_core.py` から evaluator 用 schema / helper を削減
6. テスト追加
   - `normalize_cooccurrence_conditions(...)` が `NormalizedCondition` を返す
   - 条件 ID 重複補正と category/forms 正規化が維持される
   - 既存の 5 要素 tuple 契約テストは継続

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 16 tests passed

## Web 調査

- Polars 公式 docs を確認
  - `group_by`
  - `agg`
  - `unique`
- 参考:
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.group_by.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.dataframe.group_by.GroupBy.agg.html`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.unique.html`

## 備考

- public 互換性維持のため、`analysis_core.select_target_ids_by_cooccurrence_conditions(...)` はまだ tuple return を維持
- internal では `NormalizedCondition` と `TargetSelectionResult` を使える状態になったため、次段の façade 整理がしやすくなった
