# analysis_backend 条件正規化 / Result 型移行 設計案

- 日付: 2026-03-13
- 対象: `analysis_backend/condition_evaluator.py`, `analysis_backend/filter_config.py`, `analysis_backend/data_access.py`, `analysis_backend/cli.py`, `analysis_backend/analysis_core.py`
- 目的: 条件正規化の hard fail / warning / auto-fix 方針と、例外中心実装から Result 型中心実装への段階移行方針を定義する

## 既存挙動

- `normalize_cooccurrence_conditions(...)` は以下を行う
  - 非 dict condition を silently skip
  - `forms` が list でなければ silently skip
  - 空文字 form を除外
  - 重複 form を除外
  - `condition_id` 未指定時は `condition_{idx}` を補完
  - `condition_id` 重複時は `_2`, `_3` を付けて補正
  - `form_match_logic` の未知値は `all` へ丸める
  - `search_scope` の未知値は `paragraph` へ丸める
  - `categories` が無ければ `未分類` を補完
  - `max_token_distance` が不正なら `None` に落とす
  - `form_match_logic != "all"` のとき距離条件を無効化
- `load_filter_config(...)` は以下を行う
  - file 不在、JSON 破損、root 型不正、`cooccurrence_conditions` 型不正は例外
  - `condition_match_logic` 未知値は `any` に丸める
  - `distance_matching_mode` 未知値は `auto-approx` に丸める
  - 数値設定は不正時に default へ丸める
- `cli.py` は上記例外を `except Exception` でまとめて failure meta へ落としている

## 問題

- 正規化時の救済処理が多いが、利用者に見えない
- 何を hard fail にするかの基準が未定義
- AGENTS 方針の「例外より Result 型を優先」と現実装がずれている
- façade が残っているため、Result 型移行は一気に壊すと危険

## 判定基準

### Hard Fail

- 利用者が明示的に指定した値が、意味を確定できない場合
- 処理継続すると「別条件として解釈される」危険がある場合
- 例:
  - JSON root が object ではない
  - `cooccurrence_conditions` が list ではない
  - condition が dict ではない
  - `forms` が list ではない
  - 正規化後の `forms` が空
  - `form_match_logic` が未知値
  - `search_scope` が未知値

### Warning

- 処理継続はできるが、利用者の期待とズレる可能性が高い場合
- 例:
  - `condition_id` 未指定で自動採番した
  - `condition_id` 重複に suffix を付与した
  - `categories` を `未分類` で補完した
  - `max_token_distance` が不正で無効化された
  - `form_match_logic != "all"` のため距離条件を無効化した
  - `condition_match_logic` を default に丸めた
  - `distance_matching_mode` を default に丸めた
  - 数値設定を default に丸めた

### Auto-Fix を許す範囲

- 空白除去
- form の重複除去
- category の重複除去

これらは意味の変更ではなく正規化とみなす。

## 推奨データ構造

```python
@dataclass(frozen=True)
class ConfigIssue:
    code: str
    severity: Literal["warning", "error"]
    scope: Literal["filter_config", "condition"]
    condition_index: int | None
    condition_id: str | None
    field_name: str | None
    message: str
```

```python
@dataclass(frozen=True)
class NormalizeConditionsResult:
    normalized_conditions: list[NormalizedCondition]
    issues: list[ConfigIssue]
```

```python
@dataclass(frozen=True)
class LoadFilterConfigResult:
    filter_config: FilterConfig | None
    issues: list[ConfigIssue]
```

## 実装方針

### Phase 1. characterization

- 現行の自動補正一覧をテストで固定
- warning / error code を文書化

### Phase 2. 条件正規化 Result 化

- `normalize_cooccurrence_conditions(...)` を新設 Result API へ分離
  - 例: `normalize_cooccurrence_conditions_result(...)`
- 既存 API は façade として `normalized_conditions` だけ返す
- error issue がある場合の扱い
  - 新 API は issues に保持
  - legacy API は当面 `ValueError` へ変換

### Phase 3. filter config 読込 Result 化

- `load_filter_config_result(...)` を追加
- `load_filter_config(...)` は legacy façade として残す
- CLI は新 API を使い、warning を meta JSON へ出す

### Phase 4. data access Result 化

- DB read 関数に `DataAccessResult[T]` 相当を導入
- SQLite error を typed issue / error code へ寄せる
- CLI は failure payload へ serialize する

### Phase 5. façade 縮退

- 新 API 直利用箇所数を追う
- 旧 façade 利用箇所を列挙して移行

## façade 縮退の完了条件

- `analysis_backend.analysis_core` 経由でしか使えない関数が 0 件
- `analysis_backend.__getattr__` で `.analysis_core` へフォールバックする export が 0 件、または legacy と明記された最小集合のみ
- CLI が legacy 例外 API に依存しない

## 依存関係

- schema / 列定義の変更は CSV 契約テストと Rust loader 契約を同時確認する
- 条件正規化 Result 化は CLI meta warning 伝播と同時に進める
- façade 縮退は Result 型の出口が揃った後でないと着手しない

## 推奨実行順

1. 条件正規化の issue taxonomy を確定
2. `NormalizeConditionsResult` を導入
3. `LoadFilterConfigResult` を導入
4. CLI meta へ config warning を伝播
5. data access の Result 化
6. façade 利用箇所を削減
