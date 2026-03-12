# 2026-03-13 Task 6 実施ログ

- 対象: `analysis_backend/distance_matcher.py`, `analysis_backend/analysis_core.py`, `analysis_backend/cli.py`, `tests/test_analysis_core.py`
- 目的: distance matcher を `analysis_core.py` から分離し、`strict / auto-approx / approx` と safety limit を実装する

## 既存挙動

- `build_condition_hit_tokens_df(...)` は DataFrame だけを返していた
- `max_token_distance` 条件では、候補組合せ数が `10000` を超えると silently に greedy fallback していた
- 呼び出し側は `used_mode` や warning を受け取れなかった
- `strict` という明示 mode も safety limit も存在しなかった

## 実施内容

1. `analysis_backend/distance_matcher.py` を追加
   - `evaluate_distance_matches_by_unit(...)`
   - `build_condition_hit_result(...)`
   - `DistanceMatchLimitExceededError`
   - strict / auto-approx / approx の切替
   - safety limit 超過時の失敗
   - fallback warning 生成
2. `analysis_core.py` の距離 matcher 実装本体を削除し、新 module を呼ぶ wrapper に変更
   - `build_condition_hit_tokens_df(...)` は引き続き DataFrame を返す
   - 追加引数:
     - `distance_matching_mode`
     - `distance_match_combination_cap`
     - `distance_match_strict_safety_limit`
3. `select_target_ids_by_cooccurrence_conditions(...)` の距離判定 helper も新 module を利用する形へ変更
4. `cli.py` から `FilterConfig` の mode / cap / safety limit を `build_condition_hit_tokens_df(...)` に渡すよう変更
5. matcher 側でも mode / int 設定の最終正規化を追加

## テスト

- 追加した観点:
  - `auto-approx` で fallback warning が返る
  - `strict` で safety limit 超過時に `DistanceMatchLimitExceededError` を投げる
- 既存の大組合せテストも継続し、旧 DataFrame 契約が維持されていることを確認

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 15 tests passed

## Web 調査

- Python 公式 docs を確認
  - `itertools.product`
- Polars 公式 docs を確認
  - `partition_by`
  - `concat`
- 参考:
  - `https://docs.python.org/3.12/library/itertools.html#itertools.product`
  - `https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.partition_by.html`
  - `https://docs.pola.rs/api/python/version/0.18/reference/api/polars.concat.html`

## 備考

- この段階では CLI meta JSON へ warning を載せていない
- ただし内部では `ConditionHitResult.warning_messages` が取れる状態になったため、Task 10 の warning 伝播へ繋げられる
- public 互換性のため、`analysis_core.build_condition_hit_tokens_df(...)` は当面 DataFrame return を維持した
