# 2026-03-13 distance matching contract 固定

- 目的: `analysis_backend/analysis_core.py` 分割前に、distance matching の方式・安全装置・warning 契約を固定する
- 対象: `analysis_backend/analysis_core.py`, `analysis_backend/cli.py`
- 前提: 現在は `MAX_DISTANCE_MATCH_COMBINATIONS = 10000` 超過時に silently に greedy fallback する

## 既存挙動

- `max_token_distance` は `form_match_logic == "all"` の条件でのみ有効。
- 距離制約付きヒット抽出では、各 form 候補数の積が `10000` 以下なら全組合せ探索、超えると greedy fallback を使う。
- 呼び出し側は fallback の有無や使用方式を受け取れない。
- CLI meta JSON は `warningMessages` を持つが、現状は常に空配列。

## 今回固定する契約

### 1. matching mode

- `strict`
  - 厳密探索を要求する
  - ただし safety limit 超過時は approx に落とさず失敗させる
- `auto-approx`
  - cap 以下では strict 相当
  - cap 超過時のみ approx へ切替
- `approx`
  - 常に greedy 近似を使う

### 2. 設定キー

- `distance_matching_mode`
  - default: `auto-approx`
- `distance_match_combination_cap`
  - default: `10000`
  - 用途: `auto-approx` で strict から approx へ切り替える閾値
- `distance_match_strict_safety_limit`
  - default: `1000000`
  - 用途: `strict` 実行時に、これを超える場合は失敗させる上限

## error / warning 契約

### warning

- code: `distance_match_fallback`
- 発生条件:
  - requested mode が `auto-approx`
  - combination count が `distance_match_combination_cap` を超えた
- 最低保持項目:
  - `code`
  - `message`
  - `conditionId`
  - `unitId`
  - `requestedMode`
  - `usedMode`
  - `combinationCount`
  - `combinationCap`

### error

- code: `distance_match_strict_limit_exceeded`
- 発生条件:
  - requested mode が `strict`
  - combination count が `distance_match_strict_safety_limit` を超えた
- 最低保持項目:
  - `code`
  - `message`
  - `conditionId`
  - `unitId`
  - `requestedMode`
  - `combinationCount`
  - `safetyLimit`

## used mode 契約

- requested mode が `strict` のとき:
  - 成功時 `usedMode = "strict"`
  - safety limit 超過時は失敗
- requested mode が `auto-approx` のとき:
  - cap 以下 `usedMode = "strict"`
  - cap 超過 `usedMode = "approx"`
- requested mode が `approx` のとき:
  - 常に `usedMode = "approx"`

## meta JSON 契約

- `warningMessages` は引き続き meta JSON に保持する
- 追加実装時は、少なくとも fallback warning をこの配列へ伝播させる
- 当面は CSV に warning を載せない
- UI 表示は将来課題だが、まずは CLI meta JSON まで確実に出す

## façade 契約

- `analysis_core.py` は段階移行中、既存公開関数名を維持する
- 新内部実装で dataclass / result 型を使っても、façade 側で旧 tuple / DataFrame 契約へ変換する
- 後方互換は、少なくとも CLI と現行テストが壊れないレベルを維持対象とする

## 補足

- `strict` を無制限全探索にしないのは、OOM や極端なタイムアウトを避けるため
- `auto-approx` を default にするのは、現行運用と最も連続性が高いため
- 分析再現が必要なときは `strict` を明示選択する前提とする
