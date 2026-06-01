# AIファースト条件フォーマット 設計・実装承認案

> **For Hermes:** 実装時は `subagent-driven-development` skill を使い、task ごとに実装者・仕様レビュー・品質レビューを分離する。

**Goal:** 既存の `cooccurrence_conditions` 実行フォーマットを壊さず、AI/人間が読み書きしやすい authoring フォーマットを追加し、compiler で現行 JSON へ変換できるようにする。

**Architecture:** 現行 JSON は Python/Rust 実行系の安定 runtime schema として維持する。新たに `analysis_backend.condition_authoring` を追加し、JSON authoring schema を `FilterConfig` 互換 raw config へコンパイルする。初版では GUI 編集統合は行わず、CLI/テスト/仕様書で安全に導入する。

**Tech Stack:** Python 3.12+, stdlib `json`, dataclass, pytest, existing `ConfigIssue` / `FilterConfig` / condition normalizer. YAML は初版対象外とし、v1.1 以降で検討する。

**Review Status:** 2026-06-01 に delegate_task セカンドオピニオンを2回実施。設計レビューとタスク分解レビューはいずれも `REQUEST_CHANGES`。Critical/Important 指摘は本版に反映済み。

---

## 1. 背景と現状

現行の条件ファイルは `asset/cooccurrence-conditions.json` で、Python 側では以下の流れで処理される。

1. `analysis_backend/filter_config.py`
   - JSON root object を読む。
   - `condition_match_logic`, `analysis_unit`, `max_reconstructed_paragraphs`, `distance_matching_mode` などのグローバル設定を解釈する。
   - `cooccurrence_conditions` を取り出し、`skip` 条件を除外する。
2. `analysis_backend/condition_evaluator.py`
   - `normalize_cooccurrence_conditions_result()` で `NormalizedCondition` に正規化する。
   - `form_groups`, `text_groups`, `annotation_filters`, 参照条件、除外条件、循環参照を検証する。
3. `analysis_backend/condition_model.py`
   - 正規化後 dataclass を定義する。

確認時点の `asset/cooccurrence-conditions.json`:

- 条件数: 164
- 正規化成功: 164
- error: 0
- warning: 1 (`legacy_schema_migrated`)
- `text_groups`: 139件
- `excluded_condition_ids_any`: 152件
- `required_condition_ids_all`: 126件
- `form_groups`: 24件

現行 schema は動作しているが、AI が一次ソースとして大量編集するには冗長性と暗黙ルールが多い。

---

## 2. 問題点

### 2.1 order-sensitive な group 構造

現行 `form_groups` / `text_groups` は、2個目以降の group に `combine_logic` を書く方式である。

```json
"text_groups": [
  {"texts": ["A"], "match_logic": "or"},
  {"texts": ["B"], "match_logic": "and", "combine_logic": "and"}
]
```

この方式は実装しやすいが、AI が生成する場合は以下を間違えやすい。

- group 1 に `combine_logic` を書いてはいけない。
- group 2+ には `combine_logic` が必要。
- group 1 では `not` が禁止される。
- `anchor_form` と `max_token_distance` の有効条件が複雑。

### 2.2 共通除外条件の重複

`excluded_condition_ids_any` が多くの条件に重複している。除外セットに変更があると、AI/人間のどちらでも更新漏れが起きやすい。

### 2.3 scope の階層が多い

`analysis_unit`, `overall_search_scope`, condition-level `search_scope`, group-level `search_scope` があり、sentence/paragraph 昇格の意味も絡む。AI 向けにはデフォルトと上書き箇所を明確にする必要がある。

### 2.4 runtime schema と authoring schema が同一

現行 schema は評価器に近い内部表現であり、authoring 意図を表しにくい。AI には「何を抽出したいか」を書かせ、機械的に runtime schema へ落とす方が安全である。

---

## 3. 設計方針

### 3.1 非破壊導入

- 既存 `cooccurrence_conditions` schema は変更しない。
- 既存 `load_filter_config_result()` の JSON 読込挙動も原則変更しない。
- 新 authoring schema は別ファイルで opt-in とする。
- compiler の出力は既存 `FilterConfig` と互換の raw config dict とする。

### 3.2 二層構成

1. Runtime schema
   - 現行 `cooccurrence_conditions`。
   - Python/Rust GUI/既存テストが依存する安定形式。

2. Authoring schema
   - AI/人間が編集する上位形式。
   - `rules`, `sets`, `defaults`, `settings` を持つ。
   - compiler が runtime schema に展開する。

### 3.3 小さく始める

初版は以下に限定する。

- `text_any`, `text_all`
- `token_window`
- `match.any`, `match.all` は1段のみ
- `requires_all`, `requires_any`
- `exclude_any`
- reusable `sets`
- `defaults.exclude_any`
- JSON authoring 入力
- YAML は初版対象外。PyYAML を新規依存に追加しない。

初版では以下を対象外とする。

- Rust 条件エディタの authoring schema 編集 UI
- 既存 runtime JSON から authoring JSON/YAML への完全逆変換
- 任意の深い boolean expression tree
- annotation filter の alias
- standalone negative rule (`text_not_any` だけで成立する条件)
- `match.any` 内の negative clause

---

## 4. Authoring schema 案

### 4.1 ファイル全体

```yaml
format: condition-authoring/v1

settings:
  condition_match_logic: any
  analysis_unit: sentence
  max_reconstructed_paragraphs: 10000
  distance_matching_mode: auto-approx

defaults:
  # runtime の overall_search_scope に対応。analysis_unit とは別概念。
  scope: sentence
  exclude_any:
    - common_exclusions

sets:
  common_exclusions:
    - exclude_lead
    - exclude_procedure
    - exclude_reference_only
    - exclude_definition
    - exclude_sanction
    - exclude_approval_criteria
    - exclude_zone_definition
    - exclude_catch_all

rules:
  - id: area_lv1
    label: 抑制区域
    scope: sentence
    match:
      text_any:
        - 抑制区域
        - 抑制地区

  - id: area_lv2
    label: 禁止区域
    scope: paragraph
    match:
      any:
        - token_window:
            terms: [禁止, 区域]
            anchor: 禁止
            distance: 3
        - token_window:
            terms: [禁止, 地区]
            anchor: 禁止
            distance: 3
```

JSON authoring の例:

```json
{
  "format": "condition-authoring/v1",
  "settings": {"condition_match_logic": "any", "analysis_unit": "sentence"},
  "defaults": {"scope": "sentence", "exclude_any": ["common_exclusions"]},
  "sets": {"common_exclusions": ["exclude_lead", "exclude_definition"]},
  "rules": [
    {
      "id": "area_lv1",
      "label": "抑制区域",
      "match": {"text_any": ["抑制区域", "抑制地区"]}
    }
  ]
}
```

### 4.2 Runtime への対応例

#### text_any

```yaml
match:
  text_any: [抑制区域, 抑制地区]
```

```json
"text_groups": [
  {"texts": ["抑制区域", "抑制地区"], "match_logic": "or"}
]
```

#### text_all

```yaml
match:
  text_all: [景観, 区域]
```

```json
"text_groups": [
  {"texts": ["景観", "区域"], "match_logic": "and"}
]
```

`text_all` は単一 group 内の `match_logic: "and"` として展開する。複数 group を `combine_logic: "and"` で連結する意味ではない。

#### token_window

```yaml
match:
  token_window:
    terms: [禁止, 区域]
    anchor: 禁止
    distance: 3
```

```json
"form_groups": [
  {
    "forms": ["禁止", "区域"],
    "match_logic": "and",
    "anchor_form": "禁止",
    "max_token_distance": 3
  }
]
```

`anchor` は初版では必須とする。省略時に `terms[0]` を暗黙採用する案は AI には便利だが、意図しない距離窓を作るリスクがあるため後続拡張に回す。

#### match.any

```yaml
match:
  any:
    - token_window:
        terms: [禁止, 区域]
        anchor: 禁止
        distance: 3
    - token_window:
        terms: [禁止, 地区]
        anchor: 禁止
        distance: 3
```

```json
"form_groups": [
  {
    "forms": ["禁止", "区域"],
    "match_logic": "and",
    "anchor_form": "禁止",
    "max_token_distance": 3
  },
  {
    "forms": ["禁止", "地区"],
    "match_logic": "and",
    "anchor_form": "禁止",
    "max_token_distance": 3,
    "combine_logic": "or"
  }
]
```

#### match.all

```yaml
match:
  all:
    - text_any: [抑制区域]
    - text_all: [太陽光, 設備]
```

```json
"text_groups": [
  {"texts": ["抑制区域"], "match_logic": "or"},
  {"texts": ["太陽光", "設備"], "match_logic": "and", "combine_logic": "and"}
]
```

#### negative clause の扱い

現行 evaluator は `text_groups` / `form_groups` の group 1 で `match_logic: "not"` を禁止している。したがって初版 authoring schema では `text_not_any` / `not` shorthand を採用しない。

将来、positive clause と組み合わせる `exclude_text_any` を追加する場合は、必ず group 2+ に展開できる構造に限定する。

---

## 5. Authoring schema 詳細

### 5.1 top-level fields

- `format`: 必須。初版は `condition-authoring/v1`。未知 version は hard error。将来 v2 を追加する場合も v1 compiler は維持する。
- `settings`: 任意。runtime JSON の top-level settings に対応する。
- `defaults`: 任意。rule のデフォルト値。
- `sets`: 任意。文字列 ID の再利用セット。
- `rules`: 必須。rule object の配列。

### 5.2 settings fields

許容する field:

- `condition_match_logic`: `any` / `all`。未指定時 `any`。
- `analysis_unit`: `paragraph` / `sentence`。未指定時 `paragraph`。
- `max_reconstructed_paragraphs`: positive int。未指定時 10000。
- `distance_matching_mode`: `strict` / `auto-approx` / `approx`。未指定時 `auto-approx`。
- `distance_match_combination_cap`: positive int。未指定時 10000。
- `distance_match_strict_safety_limit`: positive int。未指定時 1000000。

### 5.3 defaults fields

- `scope`: 任意。`sentence` / `paragraph`。runtime の `overall_search_scope` に対応する。未指定時 `paragraph`。
- `exclude_any`: 任意。全 rule に適用する除外条件または set 参照。

`defaults.scope` は `analysis_unit` とは別概念である。`analysis_unit=sentence` は出力粒度、`defaults.scope=sentence` は rule の判定 scope 初期値を意味する。

### 5.4 rule fields

- `id`: 必須。runtime `condition_id`。
- `label` または `labels`: 必須。
  - `label`: 単一 string。runtime `categories: [label]`。
  - `labels`: string list。runtime `categories`。
  - 両方ある場合は `labels` を優先し、`label_ignored` warning を出す。
- `scope`: 任意。`sentence` / `paragraph`。未指定時は `defaults.scope`、さらに未指定なら `paragraph`。
- `match`: 任意。ただし `match`, `requires_*`, `exclude_any` のいずれかは必要。
- `requires_all`: 任意。runtime `required_condition_ids_all`。
- `requires_any`: 任意。runtime `required_condition_ids_any`。
- `exclude_any`: 任意。runtime `excluded_condition_ids_any`。set 名も許容する。
- `skip`: 任意。`true` の rule は compiler 出力から除外する。authoring 時の一時無効化用途。
- `description`: 任意。compiler は runtime へ出さない。AI/人間向けメモ。

Duplicate rule id は authoring compiler では hard error とする。runtime normalizer の suffix 補正に任せない。

### 5.5 match shorthand

許容 shorthand:

```yaml
match:
  text_any: [抑制区域, 抑制地区]
```

```yaml
match:
  text_all: [景観, 区域]
```

```yaml
match:
  token_window:
    terms: [抑制, 区域]
    anchor: 抑制
    distance: 3
```

```yaml
match:
  any:
    - text_any: [抑制区域, 抑制地区]
    - token_window:
        terms: [抑制, 区域]
        anchor: 抑制
        distance: 3
```

```yaml
match:
  all:
    - text_any: [抑制区域]
    - text_all: [太陽光, 設備]
```

制約:

- nested `any` / `all` は1段まで。
- `any` / `all` の中には `text_any`, `text_all`, `token_window` のみ許可。
- `any` / `all` の中で `text_groups` と `form_groups` が混在する場合は、runtime では token clause と text clause が condition 内で AND されるため、authoring の `any` 意図を保持できない。したがって初版では mixed `any` を hard error とする。
- mixed `all` は許可する。runtime でも token clause AND text clause として自然に対応できる。

### 5.6 set 展開

`sets` は `exclude_any` で参照できる。

```yaml
sets:
  common_exclusions: [exclude_a, exclude_b]

rules:
  - id: x
    label: X
    exclude_any: common_exclusions
```

配列で set 名と condition id を混在可能にする。

```yaml
exclude_any:
  - common_exclusions
  - special_exclusion
```

名前解決:

- `@common_exclusions` 形式を推奨する。
- bare name が `sets` に存在すれば set として展開する。
- bare name が `sets` と rule id の両方に一致する場合は `ambiguous_set_reference` hard error。
- bare name が `sets` に存在しなければ condition id として扱う。
- set の循環参照は `set_reference_cycle` hard error。

### 5.7 defaults.exclude_any の適用

`exclude_any` は以下の規則にする。

1. rule 側で `exclude_any` を省略した場合:
   - `defaults.exclude_any` を適用する。
2. rule 側で `exclude_any` に非空配列または string を明示した場合:
   - `defaults.exclude_any + rule.exclude_any` を union する。
3. rule 側で `exclude_any: []` を明示した場合:
   - defaults を無視し、除外なしとして扱う。

重複は順序維持で除去する。

---

## 6. Compiler 設計

### 6.1 追加モジュール

Create: `analysis_backend/condition_authoring.py`

責務:

- authoring document の構造検証
- `settings` 正規化
- `sets` 解決
- `defaults` 適用
- `match` shorthand の runtime `text_groups` / `form_groups` への展開
- compiler 出力を既存 `normalize_cooccurrence_conditions_result()` に通して最終検証する helper

### 6.2 dataclass 案

```python
@dataclass(frozen=True)
class CompileAuthoringResult:
    raw_config: dict[str, object] | None
    filter_config: FilterConfig | None = None
    issues: list[ConfigIssue] = field(default_factory=list)
```

`raw_config` は runtime JSON と同じ top-level 形式。

```python
{
    "condition_match_logic": "any",
    "analysis_unit": "sentence",
    "max_reconstructed_paragraphs": 10000,
    "distance_matching_mode": "auto-approx",
    "distance_match_combination_cap": 10000,
    "distance_match_strict_safety_limit": 1000000,
    "cooccurrence_conditions": [...],
}
```

`filter_config` は `FilterConfig` dataclass。`loaded_condition_count` は compiler が skip 除外前の rule 数として設定する。

### 6.3 public API 案

```python
def compile_authoring_config(raw_document: dict[str, object]) -> CompileAuthoringResult:
    ...


def load_authoring_config_result(path: Path) -> CompileAuthoringResult:
    ...


def compile_authoring_file_to_runtime_json(input_path: Path, output_path: Path) -> CompileAuthoringResult:
    ...
```

`load_authoring_config_result()` は `.json` のみを読む。YAML は初版対象外であり、`.yaml` / `.yml` が指定された場合は `yaml_not_supported_in_v1` error を返す。

`FilterConfig` 生成は existing `load_filter_config_result(Path)` には渡さない。同関数はファイル path 前提のため、compiler 内では settings 正規化済み値と compiled conditions から `FilterConfig` を直接構築し、conditions の semantic validation は `normalize_cooccurrence_conditions_result()` で行う。

### 6.4 CLI 追加案

Create: `analysis_backend/condition_authoring_cli.py`

```bash
uv run python -m analysis_backend.condition_authoring_cli \
  --input asset/conditions.ai.json \
  --output asset/cooccurrence-conditions.generated.json \
  --validate
```

挙動:

- compiled JSON を write
- validation issue を stderr に出す
- `--issues-json path` が指定された場合は issue summary を JSON 出力する
- error issue があれば non-zero exit
- `--validate` 指定時は compiled conditions を `normalize_cooccurrence_conditions_result()` に通す

---

## 7. エラー方針

### 7.1 hard error

- root が object ではない。
- `format` が未知。
- `settings` の値が許容値外。
- `settings` の positive int field が整数化不能または 1 未満。
- `.yaml` / `.yml` が指定された (`yaml_not_supported_in_v1`)。
- `rules` が list ではない。
- rule が object ではない。
- rule `id` が空。
- duplicate rule id。
- rule `label` / `labels` が空。
- `scope` が `sentence` / `paragraph` 以外。
- `match` の形式が未知。
- nested `any` / `all`。
- mixed `any` が text clause と token clause を混在させる。
- `token_window.terms` が空または list ではない。
- `token_window.anchor` がない。
- `token_window.anchor` が `terms` に含まれない。
- `token_window.distance` が負数または整数化不能。
- bare set reference が set と condition id の両方に一致する (`ambiguous_set_reference`)。
- set が循環参照する。

### 7.2 warning / auto-fix

- `label` と `labels` が両方ある場合は `labels` を優先し `label_ignored` warning。
- 重複 exclude は順序維持で重複除去し warning なし。
- `description` など authoring-only field は出力から落とし warning なし。
- unknown field は typo 検出のため warning。
- unknown `settings` field は `unknown_settings_field` warning。
- `skip: true` rule は出力から除外し warning なし。

---

## 8. テスト方針

### 8.1 Python unit tests

Create: `tests/test_condition_authoring.py`

最低限のテスト:

1. `text_any` rule が `text_groups` `match_logic="or"` に展開される。
2. `text_all` rule が `text_groups` `match_logic="and"` に展開される。
3. `token_window` が `form_groups` に展開される。
4. `match.any` の複数 `token_window` が `combine_logic="or"` に展開される。
5. `match.all` の複数 `text_*` が `combine_logic="and"` に展開される。
6. mixed `match.any` が hard error になる。
7. mixed `match.all` が token clause と text clause の AND として展開される。
8. `defaults.exclude_any` と rule `exclude_any` が union される。
9. rule `exclude_any: []` が defaults を無視する。
10. `sets` が展開される。
11. `@set` 参照が展開される。
12. set/rule id 衝突時の bare reference が `ambiguous_set_reference` error になる。
13. unknown `format` が error になる。
14. invalid `token_window.anchor` が error になる。
15. duplicate rule id が error になる。
16. `label` / `labels` の優先順位が期待通りになる。
17. `skip: true` rule が出力から除外される。
18. compiler 出力が `normalize_cooccurrence_conditions_result()` で error なしになる。
19. CLI が JSON 入力から output file を作る。
20. `.yaml` / `.yml` 入力が `yaml_not_supported_in_v1` error になる。

### 8.2 既存回帰

ターゲット確認:

```bash
uv run python -m pytest tests/test_analysis_core.py tests/test_condition_authoring.py -q
```

最終確認:

```bash
uv run python -m pytest tests/ -q
```

---

## 9. ドキュメント方針

Create: `docs/condition-authoring-format.md`

内容:

- runtime schema と authoring schema の違い
- authoring schema の完全仕様
- `settings` / `defaults.scope` / `analysis_unit` の違い
- よく使う例
- compiler CLI の使い方
- AI に渡すための短いプロンプト例
- 既存 `cooccurrence_conditions` と共存する運用
- Rust 条件エディタは初版では runtime JSON 編集のみであること

AI へのプロンプト例:

```text
次の条例テキストから、condition-authoring/v1 形式の rules を作成してください。
各 rule は id, label, scope, match を持たせてください。
原則として文言そのものを検出する場合は text_any/text_all、形態素距離が重要な場合だけ token_window を使ってください。
共通除外は exclude_any に @common_exclusions を指定してください。
standalone negative rule は作らないでください。
```

---

## 10. 採用しない案

### 10.1 現行 runtime schema を直接置き換える

却下理由:

- Rust UI / Python / tests への影響が大きい。
- 既存 asset が正常に動いている。
- 移行リスクが高い。

### 10.2 完全な boolean expression tree を初版で実装する

却下理由:

- 現行 runtime schema への変換が複雑になる。
- sentence/paragraph mixed scope の意味が難しい。
- 初版は既存条件の主要パターンを簡潔に書けることを優先する。

### 10.3 YAML を必須依存にする

却下理由:

- Windows/uv 環境での依存追加は検証が必要。
- JSON authoring だけでも AI 生成は可能。

### 10.4 standalone negative shorthand を初版に入れる

却下理由:

- 現行 evaluator は group 1 `not` を禁止している。
- negative-only 条件は評価 universe の設計が難しい。
- 除外系は既存の `excluded_condition_ids_any` と positive exclude condition で表現する。

---

## 11. 確認質問

実装前にユーザー確認したい点:

1. authoring ファイルの初版は JSON のみ対応、YAML は v1.1 以降へ送る方針でよいか。
2. duplicate rule id は runtime のように suffix 補正せず、authoring では error にしてよいか。
3. Rust 条件エディタ統合は今回スコープ外でよいか。
4. standalone negative shorthand (`text_not_any`) は初版対象外でよいか。

---

## 12. 細分タスク案

### Task 1: authoring compiler の空モジュールと result 型を追加

**Objective:** `analysis_backend.condition_authoring` の public API 土台を作る。

**Files:**
- Create: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Steps:**
1. `CompileAuthoringResult` dataclass を追加。
2. `compile_authoring_config(raw_document)` stub を追加。
3. root が object でない場合の error test を書く。
4. test を失敗確認。
5. 最小実装で pass。

**Verification:**

```bash
uv run python -m pytest tests/test_condition_authoring.py -q
```

### Task 2: settings 正規化を実装

**Objective:** `settings` を runtime top-level config に変換する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `condition_match_logic`, `analysis_unit`, `max_reconstructed_paragraphs`, `distance_matching_mode`, cap/safety limit を受ける。
- 未指定時は現行 `filter_config.py` と同じ default。
- unknown settings field は `unknown_settings_field` warning。
- unknown/invalid settings value は `invalid_settings_value` error。

### Task 3: rules の基本検証を実装

**Objective:** `format`, `rules`, `id`, `label/labels`, duplicate id, `skip` を処理する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `format != condition-authoring/v1` は error。
- `rules` non-list は error。
- duplicate rule id は error。
- `label` / `labels` が runtime `categories` に展開される。
- `skip: true` は出力から除外される。
- `description` など authoring-only field は compiler 出力から除外される。
- `requires_all` / `requires_any` はこの task では存在検証だけ行い、runtime 展開は Task 8 で行う。
- `match` キーなしで `requires_*` または `exclude_any` がある rule は valid として残す。

### Task 4: defaults.scope と rule.scope を実装

**Objective:** rule の `overall_search_scope` を決定する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- rule `scope` > `defaults.scope` > `paragraph` の順で決まる。
- invalid scope は error。
- `analysis_unit: sentence` + `defaults.scope: paragraph` の場合、runtime top-level `analysis_unit` は `sentence`、rule の `overall_search_scope` は `paragraph` になる。
- `analysis_unit: paragraph` + rule `scope: sentence` の場合、runtime top-level `analysis_unit` は `paragraph`、rule の `overall_search_scope` は `sentence` になる。

### Task 5: text_any / text_all 展開を実装

**Objective:** text shorthand を runtime `text_groups` に変換する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `text_any` -> `match_logic: or`。
- `text_all` -> `match_logic: and`。
- 空 list / non-list は error。
- `match` キーが存在するが空 object `{}` または `null` の場合は error。
- `match` キーなしで `requires_*` または `exclude_any` がある rule は valid。
- compiler 出力が existing normalizer で error なし。

### Task 6: token_window 展開を実装

**Objective:** token shorthand を runtime `form_groups` に変換する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `terms` -> `forms`。
- `anchor` -> `anchor_form`。
- `distance` -> `max_token_distance`。
- anchor 未指定、anchor が terms にない、負 distance は error。

### Task 7: match.any / match.all の1段展開を実装

**Objective:** 複数 shorthand を runtime group list に変換する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- token-only `any` は `form_groups` with `combine_logic: or`。
- text-only `all` は `text_groups` with `combine_logic: and`。
- mixed `all` は form/text を別 clause として AND 展開する。例: `match.all: [text_any: [A], token_window: {terms: [B, C], anchor: B, distance: 3}]` は同一 condition 内に `text_groups` と `form_groups` の両方を出力する。
- 事前確認済み: 現行 `normalize_cooccurrence_conditions_result()` は `text_groups` と `form_groups` が両方ある condition を issue なしで許容する。
- mixed `any` は runtime schema では OR 意図を保持できないため error。
- nested any/all は error。
- `match` 内に `text_any`, `text_all`, `token_window`, `any`, `all` 以外のキーがある場合は error。

### Task 8: sets と exclude_any 展開を実装

**Objective:** 共通除外セットを展開し、defaults/rule の適用規則を実装する。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `sets` を展開できる。
- `@set` を展開できる。
- bare set name を展開できる。
- set/rule id 衝突 bare reference は `ambiguous_set_reference` error。
- `exclude_any: []` は defaults を無視する。
- 重複は順序維持で除去。
- `requires_all` / `requires_any` を runtime `required_condition_ids_all` / `required_condition_ids_any` に展開できる。
- set 展開後の最終 condition id list に重複があっても error にしない。順序維持で重複除去する。

### Task 9: FilterConfig 互換 result と validation helper を実装

**Objective:** compiler 出力を既存 pipeline に安全に渡せるようにする。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- compiler は raw_config dict を生成する。
- `CompileAuthoringResult.filter_config` が成功時に入る。
- `FilterConfig` は `load_filter_config_result(Path)` には渡さず、compiler 内で settings 正規化済み値と compiled conditions から直接構築する。
- `loaded_condition_count` は skip 除外前 rule 数。
- `normalize_cooccurrence_conditions_result()` に通した error を result issues に統合できる。

### Task 10: authoring JSON file loader を実装

**Objective:** JSON file を読み込む。YAML は初版対象外として明示 error にする。

**Files:**
- Modify: `analysis_backend/condition_authoring.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `.json` を読める。
- `.yaml` / `.yml` は `yaml_not_supported_in_v1` error。
- file not found / invalid JSON は明確な error。

### Task 11: condition_authoring_cli を追加

**Objective:** authoring config を runtime JSON へ変換する CLI を提供する。

**Files:**
- Create: `analysis_backend/condition_authoring_cli.py`
- Test: `tests/test_condition_authoring.py`

**Acceptance Criteria:**
- `--input`, `--output`, `--validate`, `--issues-json` を持つ。
- 成功時 output JSON を書く。
- error issue があれば non-zero exit。
- pytest では `subprocess.run([sys.executable, "-m", "analysis_backend.condition_authoring_cli", ...])` で exit code と output file を検証する。

### Task 12: docs/condition-authoring-format.md を作成

**Objective:** AI/人間向け authoring schema 仕様を公開する。

**Files:**
- Create: `docs/condition-authoring-format.md`

**Acceptance Criteria:**
- runtime schema との違いを説明する。
- JSON/YAML 例を載せる。
- settings/defaults/sets/rules/match の仕様を載せる。
- AI プロンプト例を載せる。
- 初版対象外を明記する。

### Task 13: 回帰テストと最終確認

**Objective:** 既存 analysis_backend の挙動を壊していないことを確認する。

**Files:**
- No source change expected unless failures require test/spec adjustment.

**Commands:**

```bash
uv run python -m pytest tests/test_analysis_core.py tests/test_condition_authoring.py -q
uv run python -m pytest tests/ -q
```

**Acceptance Criteria:**
- targeted tests pass。
- full tests pass、または既知の Windows SQLite cleanup 問題のみ明示して記録する。

---

## 13. タスクレビュー反映メモ

delegate_task によるタスク分解レビューでは `REQUEST_CHANGES` を受けた。以下を反映済み。

1. mixed `all` の runtime 展開例を Task 7 に明記し、現行 normalizer が `text_groups` + `form_groups` 混在を許容することを実コマンドで確認した。
2. 空 `match` / `match` 省略時の扱いを Task 3 / Task 5 に追加した。
3. `requires_all` / `requires_any` の展開を Task 8 に追加した。
4. `FilterConfig` 生成は `load_filter_config_result(Path)` ではなく compiler 内で直接構築する方針に統一した。
5. YAML optional 対応は初版から外し、`.yaml` / `.yml` は `yaml_not_supported_in_v1` error にした。
6. CLI テストは subprocess 実行で exit code と output file を確認する方針に統一した。

---

## 14. 実装開始条件

- 本承認案へのユーザー承認。
- 確認質問 4点の回答。
- 実装時は Task 1 から順に、各 task ごとに TDD、仕様レビュー、品質レビューを行う。
