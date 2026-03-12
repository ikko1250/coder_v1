# 2026-03-13 Task 4 実施ログ

- 対象: `analysis_backend/filter_config.py`, `analysis_backend/analysis_core.py`, `analysis_backend/__init__.py`, `tests/test_analysis_core.py`
- 目的: filter config 読込責務を分離し、matching mode 関連設定を正式に読める状態にする

## 既存挙動

- `load_filter_config(...)` は `analysis_core.py` に直接定義されていた
- `distance_matching_mode`, `distance_match_combination_cap`, `distance_match_strict_safety_limit` は model 側 default はあったが、JSON からはまだ正式に読んでいなかった
- package API も `load_filter_config` を `analysis_core` 経由で引いていた

## 実施内容

1. `analysis_backend/filter_config.py` を追加
   - `load_filter_config(...)` を移設
   - 整数設定の default 補正を `_read_int_with_default(...)` に集約
2. JSON 読込項目を拡張
   - `distance_matching_mode`
   - `distance_match_combination_cap`
   - `distance_match_strict_safety_limit`
3. `analysis_core.py` は `filter_config.py` から `load_filter_config` を import する形へ変更
4. `analysis_backend/__init__.py` の `load_filter_config` 再 export 先を `filter_config.py` に変更
5. テスト追加
   - package API が `filter_config.load_filter_config` を向くこと
   - explicit 設定値の読込
   - 不正値時の default 補正

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 13 tests passed

## Web 調査

- Python 公式 docs を確認
  - `json`
  - `pathlib`
- 参考:
  - `https://docs.python.org/3.12/library/json.html`
  - `https://docs.python.org/3.12/library/pathlib.html`

## 備考

- この段階では CLI 側の引数や meta JSON はまだ拡張していない
- 目的は config 読込責務の分離と、後続 matcher 実装の入力契約の固定
