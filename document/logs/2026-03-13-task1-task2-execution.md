# 2026-03-13 Task 1 + Task 2 実施ログ

- 対象: `analysis_backend/analysis_core.py`, `analysis_backend/cli.py`, `tests/test_analysis_core.py`, `tests/test_cli.py`
- 目的: matching 方式 contract 固定と、分割前 characterization test の追加

## 実施内容

1. `distance matching` の契約ログを追加
   - mode: `strict / auto-approx / approx`
   - `distance_match_combination_cap`
   - `distance_match_strict_safety_limit`
   - warning / error / meta JSON / façade 契約
2. `tests/test_analysis_core.py` に characterization test を追加
   - `select_target_ids_by_cooccurrence_conditions(...)` の 5 要素 tuple 契約
   - export DataFrame の scalar dtype 契約
3. `tests/test_cli.py` に meta JSON の `warningMessages == []` 契約を追加

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 9 tests passed

## 備考

- `uv run` 実行時、sandbox 内では依存取得に失敗したため、ネットワーク付き実行で確認した。
- `.venv` は `uv run` 実行時に再作成された。
