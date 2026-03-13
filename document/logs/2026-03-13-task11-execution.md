# 2026-03-13 Task 11 実施ログ

- 対象: `tests/test_analysis_core.py`, `tests/test_cli.py`
- 目的: matching mode と façade 互換、および warning/error meta の回帰テストを拡張する

## 既存挙動

- Task 10 までで distance matching の mode と warning 伝播は実装済みだった
- ただし、`strict` と `approx` の意味差そのものを固定するテストは不足していた
- また、`analysis_backend` package root から `build_condition_hit_result(...)` を辿れることと、`strict` safety limit 超過時の CLI failure meta 契約は未保護だった

## 実施内容

1. `tests/test_analysis_core.py` を更新
   - `analysis_backend.build_condition_hit_result` が façade API から参照できることを確認
   - `dir(analysis_backend)` に `build_condition_hit_result` が含まれることを確認
   - 小さな `A/B/A/B` データで `strict` と `approx` の group 数差を固定
   - `strict` は 4 group、`approx` は 2 group になることを確認
2. `tests/test_cli.py` を更新
   - `strict` safety limit を強制的に超える filter config helper を追加
   - CLI 実行失敗時に `meta.json` が `failed` になり、`errorSummary` に `distance_match_strict_limit_exceeded` を含むことを確認
   - strict limit failure 時は `warningMessages == []` を維持することを確認

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 20 tests passed

## Web 調査

- Python 公式 docs を確認
  - `unittest`
- 参考:
  - `https://docs.python.org/3/library/unittest.html`

## 備考

- 今回の追加で、mode 差分は「実装上そうなっている」ではなく「契約として守る」状態になった
- strict failure は warning ではなく error として扱う方針も、CLI meta 契約として固定できた
