# 2026-03-13 NormalizeConditionsResult 導入ログ

- 対象: `analysis_backend/condition_model.py`, `analysis_backend/condition_evaluator.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`
- 目的: 条件正規化時の auto-fix / warning / error を観測できる Result 型を導入しつつ、legacy API の戻り値は維持する

## 既存挙動

- `normalize_cooccurrence_conditions(...)` は `list[NormalizedCondition]` だけを返していた
- invalid condition の skip や `condition_id` 自動補完、未知値の default 化が外から見えなかった
- legacy API を壊さずに issue 情報だけ増やせる入口が無かった

## 実施内容

1. `condition_model.py` を更新
   - `ConfigIssue`
   - `NormalizeConditionsResult`
   を追加
2. `condition_evaluator.py` を更新
   - `normalize_cooccurrence_conditions_result(...)` を追加
   - issue code を付けて現行の skip / default / auto-fix を収集するようにした
   - `normalize_cooccurrence_conditions(...)` は façade として `normalized_conditions` だけ返す形を維持
3. package export を更新
   - `analysis_backend.ConfigIssue`
   - `analysis_backend.NormalizeConditionsResult`
4. `tests/test_analysis_core.py` を更新
   - 新 dataclass export を確認
   - result API が issue を返すこと
   - legacy API が同じ入力に対して同じ normalized_conditions を返すこと
   を確認

## issue code

- `condition_not_object`
- `forms_not_list`
- `forms_empty`
- `condition_id_generated`
- `condition_id_deduplicated`
- `form_match_logic_defaulted`
- `search_scope_defaulted`
- `max_token_distance_ignored`
- `max_token_distance_disabled`
- `categories_defaulted`

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 25 tests passed

## 備考

- この段階では issue の `severity` は観測用であり、legacy API はまだそれを例外へ変換しない
- 次段で `LoadFilterConfigResult` を導入し、CLI meta へ config warning を流すのが自然
