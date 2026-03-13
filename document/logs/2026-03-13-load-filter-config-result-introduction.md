# 2026-03-13 LoadFilterConfigResult 導入ログ

- 対象: `analysis_backend/condition_model.py`, `analysis_backend/filter_config.py`, `analysis_backend/analysis_core.py`, `analysis_backend/cli.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`, `tests/test_cli.py`
- 目的: filter config 読込時の default 化を Result 型で観測できるようにし、CLI meta JSON へ config warning を伝播する

## 既存挙動

- `load_filter_config(...)` は `FilterConfig` だけを返していた
- unknown `condition_match_logic`, `distance_matching_mode`, 数値設定の invalid 値は default へ丸められていた
- その丸め込みは利用者に見えず、CLI meta JSON の `warningMessages` にも出ていなかった

## 実施内容

1. `condition_model.py` を更新
   - `LoadFilterConfigResult` を追加
2. `filter_config.py` を更新
   - `load_filter_config_result(...)` を追加
   - filter config default 化を `ConfigIssue` として収集
   - legacy `load_filter_config(...)` は façade として `FilterConfig` だけ返す
3. `analysis_core.py` / `analysis_backend.__init__` を更新
   - `load_filter_config_result(...)` を export
4. `cli.py` を更新
   - `load_filter_config_result(...)` を使用
   - config issues を既存 matching warning と同じ `warningMessages` 配列へ合流
   - `severity`, `scope`, `fieldName` を serialize
5. テスト追加
   - filter config Result が issue を返すこと
   - CLI success meta に config warning が出ること

## issue code

- `condition_match_logic_defaulted`
- `max_reconstructed_paragraphs_defaulted`
- `distance_matching_mode_defaulted`
- `distance_match_combination_cap_defaulted`
- `distance_match_strict_safety_limit_defaulted`

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 27 tests passed

## 備考

- file 不在や JSON 破損などの hard failure は引き続き例外
- 次段では data access 側の Result 化へ進めると、CLI の `except Exception` 依存をさらに減らせる
