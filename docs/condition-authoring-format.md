# Condition Authoring Format v1

AI/人間が条件（rule）を記述し、runtime JSON に変換するためのオーサリングスキーマ仕様です。

---

## 1. 概要

### 1.1 フォーマット識別子

```json
"format": "condition-authoring/v1"
```

`format` は必須です。`condition-authoring/v1` 以外の値は `authoring_format_invalid` エラーになります。

### 1.2 ファイル形式

- **JSON のみ対応**（`.json`）
- **YAML は v1 では非対応**です。`.yaml` / `.yml` を読み込もうとすると `yaml_not_supported_in_v1` エラーになります。

### 1.3 Authoring schema と Runtime schema の違い

| 項目 | Authoring schema | Runtime schema |
|------|-----------------|----------------|
| 目的 | 人間/AI が読み書きしやすい短縮記法 | エンジンが直接解釈する正規化済み構造 |
| キー名 | `label`, `labels`, `match`, `requires_all` など | `condition_id`, `categories`, `text_groups`, `required_condition_ids_all` など |
| match の表現 | `text_any`, `text_all`, `token_window`, `any`/`all` | `text_groups`（`match_logic`/`combine_logic`）, `form_groups` |
| sets（語彙セット） | `@set_name` または bare name で展開 | 展開済みのリテラル配列 |
| ルール間参照 | `requires_all`, `requires_any`, `exclude_any` | `required_condition_ids_all`, `required_condition_ids_any`, `excluded_condition_ids_any` |

Authoring config は `compile_authoring_config()` で runtime JSON に変換されます。変換時に構造が展開・正規化されます。

---

## 2. トップレベル構造

```json
{
  "format": "condition-authoring/v1",
  "settings": { ... },
  "defaults": { ... },
  "sets": { ... },
  "rules": [ ... ]
}
```

| キー | 必須 | 型 | 説明 |
|------|------|-----|------|
| `format` | ✅ | string | `condition-authoring/v1` |
| `settings` | | object | グローバル動作設定 |
| `defaults` | | object | 全ルール共通のデフォルト値 |
| `sets` | | object | 語彙セット定義 |
| `rules` | ✅ | array | 条件ルールのリスト |

---

## 3. settings

```json
{
  "settings": {
    "condition_match_logic": "any",
    "analysis_unit": "paragraph",
    "max_reconstructed_paragraphs": 10000,
    "distance_matching_mode": "auto-approx",
    "distance_match_combination_cap": 10000,
    "distance_match_strict_safety_limit": 1000000
  }
}
```

| キー | 型 | デフォルト | 有効値 | 説明 |
|------|-----|-----------|--------|------|
| `condition_match_logic` | string | `"any"` | `"any"`, `"all"` | 複数条件のマッチ論理 |
| `analysis_unit` | string | `"paragraph"` | `"paragraph"`, `"sentence"` | 解析単位 |
| `max_reconstructed_paragraphs` | integer | `10000` | `>= 1` | 再構築段落数上限 |
| `distance_matching_mode` | string | `"auto-approx"` | `"strict"`, `"auto-approx"`, `"approx"` | 距離マッチングモード |
| `distance_match_combination_cap` | integer | `10000` | `>= 1` | 距離マッチ組み合わせ上限 |
| `distance_match_strict_safety_limit` | integer | `1000000` | `>= 1` | strict モード安全上限 |

- 未知の `settings` フィールドは `unknown_settings_field` **warning** を出しますが、処理は継続します。
- 型や値が不正な場合は `invalid_settings_value` **error** で停止します。
- 整数フィールドは文字列 `"500"` なども受け付けます（coerce）。

---

## 4. defaults

```json
{
  "defaults": {
    "scope": "paragraph",
    "exclude_any": ["other_rule_id"]
  }
}
```

| キー | 型 | デフォルト | 有効値 | 説明 |
|------|-----|-----------|--------|------|
| `scope` | string | `"paragraph"` | `"paragraph"`, `"sentence"` | 検索スコープ |
| `exclude_any` | array | `null` | ルールIDの配列 | 全ルールに適用する除外条件 |

- `defaults.exclude_any` は全ルールの `exclude_any` と **和集合**（union）になります。
- ルール側で `exclude_any: []`（空配列）を明示すると、`defaults.exclude_any` を**上書き無効化**できます。
- `exclude_any` で参照できるのは **skip されていないルールのID** のみです。skip 済みルールや存在しないIDを参照すると `unknown_condition_reference` エラーになります。

---

## 5. sets（語彙セット）

```json
{
  "sets": {
    "regional_terms": ["区域", "地区", "地域"],
    "action_verbs": ["禁止", "許可", "制限"]
  }
}
```

| 項目 | 仕様 |
|------|------|
| 型 | `string -> string[]` の object |
| 空配列 | 禁止（`authoring_set_invalid`） |
| 空文字/空白のみ | 禁止（`authoring_set_invalid`） |
| 重複 | 展開時に**順序を保ったまま重複除去**（dedupe）されます |

### 5.1 セット参照構文

| 構文 | 意味 | 例 |
|------|------|-----|
| `@set_name` | 明示的なセット参照 | `"@regional_terms"` |
| `bare_name` | bare set 参照（セット名がそのまま記述） | `"regional_terms"` |

- bare set 参照は、同名の **rule id と衝突**する場合に `ambiguous_set_reference` エラーになります。
- `@unknown_set` は `unknown_set_reference` エラーになります。
- bare name で未知のセット名は **リテラル文字列**として扱われます（エラーになりません）。

### 5.2 展開順序と重複除去

複数のセットやリテラルを混在させた場合、出現順を保ちながら重複を除去します：

```json
{"text_any": ["@set1", "@set2", "foo"]}
// set1: ["a", "b"], set2: ["b", "c"]
// -> ["a", "b", "c", "foo"]
```

---

## 6. rules

```json
{
  "rules": [
    {
      "id": "rule_1",
      "label": "建築基準法",
      "labels": ["建築", "規制"],
      "description": "建築基準法に関する記述を検出",
      "skip": false,
      "scope": "paragraph",
      "match": { ... },
      "requires_all": ["other_rule"],
      "requires_any": ["rule_a", "rule_b"],
      "exclude_any": ["excluded_rule"]
    }
  ]
}
```

### 6.1 ルールフィールド

| キー | 必須 | 型 | 説明 |
|------|------|-----|------|
| `id` | ✅ | string | ルール識別子。空・空白のみは不可。重複は不可。 |
| `label` | | string | 単一カテゴリ。`labels` がある場合は無視されます（`label_ignored` warning）。 |
| `labels` | | string[] | 複数カテゴリ。runtime の `categories` に展開されます。 |
| `description` | | string | **authoring only**。runtime には含まれません。 |
| `skip` | | boolean | `true` の場合、このルールは出力から除外されます。 |
| `scope` | | string | 検索スコープ。`defaults.scope` を上書きします。 |
| `match` | | object | マッチ条件。詳細は後述。 |
| `requires_all` | | string[] | すべての指定ルールがマッチしている必要あり |
| `requires_any` | | string[] | いずれかの指定ルールがマッチしている必要あり |
| `exclude_any` | | string[] | いずれかの指定ルールがマッチしている場合除外 |

### 6.2 ルール間参照の制約

- `requires_all` / `requires_any` / `exclude_any` で参照できるのは **skip されていないルールのID** のみです。
- skip 済みルールや存在しないIDを参照すると `unknown_condition_reference` エラーになります。
- `match` がなくても `requires_all` / `requires_any` / `exclude_any` のみのルールは有効です。

---

## 7. match

### 7.1 Direct match（単一マッチ）

```json
{
  "match": {
    "text_any": ["区域", "地区"]
  }
}
```

| キー | 型 | 説明 |
|------|-----|------|
| `text_any` | string[] | いずれかにマッチ（OR） |
| `text_all` | string[] | すべてにマッチ（AND） |
| `token_window` | object | トークン距離マッチ（詳細後述） |

制約：
- `text_any` と `text_all` を同時に指定できません。
- `token_window` と `text_any`/`text_all` を同時に指定できません。
- 空配列、空オブジェクト、`null` はエラー（`authoring_match_invalid`）。
- 空文字/空白のみの要素はスキップされます。すべて空になる場合はエラー。

### 7.2 text_not_any

**v1 では非対応**です。`text_not_any` は `match` 内で許可されていないキーとして扱われ、`authoring_match_invalid` エラーになります。

### 7.3 token_window

```json
{
  "match": {
    "token_window": {
      "terms": ["禁止", "区域"],
      "anchor": "禁止",
      "distance": 3
    }
  }
}
```

| キー | 必須 | 型 | 説明 |
|------|------|-----|------|
| `terms` | ✅ | string[] | 検索対象トークン。セット参照可。 |
| `anchor` | ✅ | string | アンカートークン。`terms` のいずれかである必要あり。 |
| `distance` | ✅ | integer | `>= 0`。アンカーからの最大トークン距離。 |

- `anchor` は `terms` に含まれている必要があります（含まれていない場合はエラー）。
- `anchor` はセット参照を展開しません（リテラルとして扱われます）。

### 7.4 match.any / match.all（複合マッチ）

```json
{
  "match": {
    "any": [
      {"text_any": ["区域", "地区"]},
      {"text_any": ["foo", "bar"]}
    ]
  }
}
```

```json
{
  "match": {
    "all": [
      {"text_all": ["foo", "bar"]},
      {"token_window": {"terms": ["禁止", "区域"], "anchor": "禁止", "distance": 3}}
    ]
  }
}
```

| キー | 型 | 説明 |
|------|-----|------|
| `any` | object[] | いずれかの条件にマッチ（OR）。各要素は `text_any`/`text_all`/`token_window` のいずれか1つのみ。 |
| `all` | object[] | すべての条件にマッチ（AND）。各要素は `text_any`/`text_all`/`token_window` のいずれか1つのみ。 |

制約：
- `any` / `all` は **1階層のみ**。ネストは禁止（`authoring_match_invalid`）。
- `any` 内で `text` 系と `token_window` を混在させることは**できません**（`authoring_match_invalid`）。
  - 理由：runtime schema が text/form families 間の OR 意図を保持できないため。
- `all` 内では `text` 系と `token_window` の混在が**許可されます**。
  - 理由：text/form families の両方が同一 condition に存在すると AND として扱えるため。
- 各要素は**1つのキーのみ**（`text_any` または `text_all` または `token_window`）。複数キーはエラー。
- 空配列、非リスト、非オブジェクト要素はエラー。

---

## 8. JSON 完全例

```json
{
  "format": "condition-authoring/v1",
  "settings": {
    "condition_match_logic": "any",
    "analysis_unit": "paragraph",
    "max_reconstructed_paragraphs": 5000,
    "distance_matching_mode": "auto-approx",
    "distance_match_combination_cap": 5000,
    "distance_match_strict_safety_limit": 500000
  },
  "defaults": {
    "scope": "paragraph",
    "exclude_any": ["noise_filter"]
  },
  "sets": {
    "regional_terms": ["区域", "地区", "地域"],
    "action_verbs": ["禁止", "許可", "制限"]
  },
  "rules": [
    {
      "id": "building_standard",
      "label": "建築基準法",
      "description": "建築基準法に関する規制記述",
      "match": {
        "text_any": ["@regional_terms", "建築基準法"]
      }
    },
    {
      "id": "restriction_zone",
      "labels": ["規制", "区域"],
      "match": {
        "all": [
          {"text_any": ["@action_verbs"]},
          {"token_window": {"terms": ["区域", "地区"], "anchor": "区域", "distance": 5}}
        ]
      },
      "requires_any": ["building_standard"]
    },
    {
      "id": "noise_filter",
      "label": "ノイズ除外",
      "skip": true,
      "match": {
        "text_any": ["お知らせ", "広告"]
      }
    }
  ]
}
```

---

## 9. YAML について（将来検討 / 非対応）

YAML は **v1 では非対応**です。将来的な拡張で検討される可能性がありますが、現時点では `.yaml` / `.yml` ファイルを読み込もうとすると `yaml_not_supported_in_v1` エラーになります。

以下は将来の参考例です（**現時点では動作しません**）：

```yaml
# ⚠️ このYAMLはv1では使用できません
format: condition-authoring/v1
settings:
  condition_match_logic: any
rules:
  - id: example
    label: 例
    match:
      text_any:
        - "foo"
        - "bar"
```

---

## 10. CLI 使用方法

```bash
# 変換（正常時に runtime JSON を出力）
python -m analysis_backend.condition_authoring_cli \
  --input authoring_config.json \
  --output runtime_config.json

# 検証のみ（--output は不要）
python -m analysis_backend.condition_authoring_cli \
  --input authoring_config.json \
  --validate

# issues を JSON で出力
python -m analysis_backend.condition_authoring_cli \
  --input authoring_config.json \
  --output runtime_config.json \
  --issues-json issues.json
```

| オプション | 説明 |
|-----------|------|
| `--input` | 入力の authoring config JSON（必須） |
| `--output` | 出力の runtime JSON（`--validate` 未指定時は必須） |
| `--validate` | 検証のみ。エラーがなければ exit 0 |
| `--issues-json` | issues リストを指定パスに JSON 出力 |

- エラーがある場合は exit code 1、エラーメッセージを stderr に出力します。
- `--issues-json` は成功時に空配列 `[]`、エラー時に issues リストを書き出します。

---

## 11. エラーコード一覧

| コード | 重大度 | 説明 |
|--------|--------|------|
| `authoring_root_invalid` | error | ルートが object でない |
| `authoring_format_missing` | error | `format` フィールドが欠落 |
| `authoring_format_invalid` | error | `format` の値が不正 |
| `invalid_settings_value` | error | settings の値が型・範囲外 |
| `unknown_settings_field` | warning | 未知の settings フィールド |
| `authoring_rules_missing` | error | `rules` フィールドが欠落 |
| `authoring_rules_invalid` | error | `rules` が配列でない |
| `authoring_rule_invalid` | error | ルールが object でない |
| `authoring_rule_id_invalid` | error | `id` が空または欠落 |
| `authoring_rule_id_duplicate` | error | `id` が重複 |
| `authoring_match_invalid` | error | `match` の構造が不正 |
| `authoring_sets_invalid` | error | `sets` が object でない |
| `authoring_set_invalid` | error | セットの値が不正（空配列、空文字など） |
| `authoring_defaults_invalid` | error | `defaults` が object でない |
| `authoring_scope_invalid` | error | `scope` の値が不正 |
| `unknown_set_reference` | error | `@set_name` で未知のセットを参照 |
| `ambiguous_set_reference` | error | bare set 参照が rule id と衝突 |
| `unknown_condition_reference` | error | `requires_all`/`requires_any`/`exclude_any`/`defaults.exclude_any` で未知または skip 済みのルールを参照 |
| `label_ignored` | warning | `label` と `labels` が両方ある場合、`label` を無視 |
| `yaml_not_supported_in_v1` | error | `.yaml` / `.yml` ファイルを読み込もうとした |
| `authoring_file_not_found` | error | 入力ファイルが存在しない |
| `authoring_json_invalid` | error | JSON のパースエラー |

---

## 12. AI プロンプト例

以下は、AI にこのフォーマットで条件を生成させる際のプロンプト例です：

```
以下の要件に従い、condition-authoring/v1 形式の JSON を生成してください。

- format は必ず "condition-authoring/v1"
- rules の各要素には必ず "id" を含める（"label" / "labels" は推奨・任意）
- match には text_any, text_all, token_window, any, all のいずれかを使用
- token_window を使う場合は terms, anchor, distance を必ず含める
- anchor は terms に含まれる必要がある
- any 内では text 系と token_window を混在させない
- sets を使う場合は @set_name 形式で参照する
- YAML は使用しない（JSON のみ）
- description は人間向けの説明として自由に追加してよい

出力は有効な JSON のみを返してください。
```

---

## 13. Rust GUI からの利用（MVP）

Rust GUI（`csv_highlight_viewer`）では、authoring JSON を条件ファイルとして選択し、分析実行時に自動的に runtime JSON へ compile できます。

### 13.1 できること

- **分析設定での条件ファイル選択**：分析設定では `.json` 条件ファイルを選択できます。`format: "condition-authoring/v1"` を持つ JSON は authoring として認識され、分析実行時に runtime JSON へ compile されます。
- **分析実行**：「分析実行」「再分析」ボタンで、authoring JSON が自動的に runtime JSON に compile されて Python worker へ渡されます。
- **警告の確認**：compile 時に発生した warning は、分析完了後の警告ウィンドウで worker 由来の警告と併せて確認できます。

### 13.2 できないこと

- **条件エディタ**：条件エディタのファイル選択は runtime JSON 専用です。authoring JSON は条件エディタでは開けません。authoring JSON を編集する場合は、外部エディタで JSON を直接変更してください。
- **YAML**：`.yaml` / `.yml` ファイルは条件ファイルとして選択できません（v1 非対応）。

### 13.3 生成された runtime JSON の保存先

分析実行時に生成された runtime JSON は以下のパスに保存されます：

```
runtime/compiled-conditions/<stable-key>.runtime.json
runtime/compiled-conditions/<stable-key>.issues.json
```

- `<stable-key>` は authoring ファイルの canonicalized 絶対パス＋ファイル内容＋bridge バージョンの SHA-256 ハッシュです。
- 同一ファイル・同一内容であれば常に同じファイル名が生成されます（content が変わると key も変わります）。
- このディレクトリは分析ジョブのクリーンアップ対象外であり、セッションを超えて残ります。

### 13.4 制限の再掲

Rust GUI 経由での authoring JSON 利用でも、以下の v1 制限は変わりません：

- **YAML 非対応**：`.yaml` / `.yml` はエラーになります。
- **text_not_any 非対応**：否定マッチは使用できません。
- **ネストした any/all 非対応**：1階層のみ。

---

## 14. 初版（v1）対象外

以下は将来のバージョンで検討される可能性がありますが、**v1 では対象外**です：

- **YAML サポート**：`.yaml` / `.yml` は読み込めません。
- **text_not_any**：否定マッチはサポートされていません。
- **ネストした any/all**：1階層の `any`/`all` のみ対応。
- **正規表現マッチ**：リテラル文字列・トークンマッチのみ。
- **動的セット定義**：ファイル参照や外部辞書連携はありません。
- **条件の継承・テンプレート**：ルール間の継承構文はありません。
