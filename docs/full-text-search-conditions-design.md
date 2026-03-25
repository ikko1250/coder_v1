# 共起条件へのフルテキスト検索（`text_groups`）設計書

## 1. 目的と背景

- **現状**: `analysis_backend` のトークン条件は `normalized_form`（形態素解析結果の語形）に基づき、段落または文単位で評価される。トークン境界に依存するため、表記ゆれ・複合語のまたぎ・解析辞書に載らない表記は拾いにくい。
- **目的**: 条件 JSON に **トークン条件とは独立したフィールド** で、**生テキスト（原文）上の部分文字列一致**による制約を追加する。
- **結合方針（要件）**:
  - フルテキスト側は **現行の `form_groups` と同様の「グループ内 match_logic」「グループ間 combine_logic」「search_scope」** を持つ。
  - **既存のトークン条件（`forms` / `form_groups`）とは論理 AND**。どちらか一方だけが定義されている場合は、その片方のみで成立とする（もう一方は「clause なし」として常に真）。

## 2. 用語

| 用語 | 意味 |
|------|------|
| 評価単位 | `search_scope` が `paragraph` なら **1 段落**、`sentence` なら **1 文**。 |
| フルテキスト | `analysis_sentences` 等から得られる **表層形の連結テキスト**（トークン `surface` の結合、または既存の文/段落テキスト列）。実装時に「どの列を正とするか」を固定する（後述）。 |
| clause | 条件オブジェクト内の大きなブロック。トークン clause / 注釈 clause / 参照 clause に加え、**テキスト clause** を追加する。 |

## 3. 条件 JSON スキーマ（提案）

### 3.1 トップレベル（`cooccurrence_conditions[]` の各要素）

新規フィールド（任意）:

- **`text_groups`**: `array` of **text group object**。未指定または空配列のとき、テキスト clause は「なし」とみなす。

既存フィールドとの関係:

- `form_groups` / レガシー `forms` は従来どおり。**`text_groups` と同時指定可能**。その場合の最終判定は **トークン側の成立 AND テキスト側の成立**（後述の式）。

### 3.2 text group object（`form_groups` の対応関係）

| フィールド | 型 | 必須 | 説明 |
|------------|-----|------|------|
| `texts` | `string[]` | 各グループで 1 個以上 | 空文字は正規化時に除外。重複は安定化のためユニーク化してよい。 |
| `match_logic` | `string` | 省略時 `and` | `and` / `or` / `not`（意味は form 側と同型）。 |
| `combine_logic` | `string` | 2 グループ目以降で使用 | `and` / `or`。1 つ目のグループでは無視。 |
| `search_scope` | `string` | 省略時は条件の `overall_search_scope` に従う | `paragraph` / `sentence`。 |

**意図的に `form_groups` にないもの（初版）**

- `anchor_form` / `max_token_distance` / `exclude_forms_any` に相当するフィールドは **設けない**（フルテキストは「部分文字列の有無」が中心のため）。将来必要なら別設計で拡張する。

### 3.3 `match_logic` の意味（評価単位ごと）

評価対象の文字列を \(T\)（段落全文または文全文）、`texts` を \(s_1,\ldots,s_k\) とする。

- **`and`**: すべての \(s_i\) が \(T\) に **部分文字列として**含まれる（順不同）。
- **`or`**: いずれかの \(s_i\) が \(T\) に含まれる。
- **`not`**: すべての \(s_i\) が \(T\) に **含まれない**（いわゆる「これらの語句が単位内に存在しない」）。

大文字小文字・Unicode 正規化は §5 で固定する。

### 3.4 グループ間の結合（`combine_logic`）

`form_groups` と同様、左から順に畳み込む:

- 2 グループ目以降の `combine_logic` が `and` なら **両方のグループ条件を満たす単位**のみ残す。
- `or` なら **いずれかを満たす単位**の和集合。

**スコープがグループ間で異なる場合**（例: g1 が `sentence`、g2 が `paragraph`）は、現行の form 側と同様に **段落への正規化（プロモート）** 方針を踏襲する（`condition_evaluator` 内の `_combine_group_matches_df` と同じパターン）。設計上は「単位のキーを `paragraph_id` に揃えた上で、最終的に段落ごとに `text_is_match` を決める」でよい。

### 3.5 記述例

トークン（「抑制」「区域」がともに出現）**かつ** フルテキスト（「第3条」と「別表」の**両方**が段落に含まれる）:

```json
{
  "condition_id": "example_text_and_forms",
  "categories": ["例"],
  "overall_search_scope": "paragraph",
  "form_groups": [
    {
      "forms": ["抑制", "区域"],
      "match_logic": "and",
      "search_scope": "paragraph"
    }
  ],
  "text_groups": [
    {
      "texts": ["第3条", "別表"],
      "match_logic": "and",
      "search_scope": "paragraph"
    }
  ]
}
```

複数グループの例（g1 **または** g2 を満たす、かつトークン条件は別途 AND）:

```json
"text_groups": [
  { "texts": ["第3条"], "match_logic": "or", "search_scope": "paragraph" },
  { "texts": ["別表"], "match_logic": "or", "combine_logic": "or", "search_scope": "paragraph" }
]
```

## 4. 正規化モデル（`NormalizedCondition` 拡張）

`analysis_backend/condition_model.py` に以下を追加する想定:

- **`NormalizedTextGroup`**: `NormalizedFormGroup` と対になる dataclass（`texts`, `match_logic`, `combine_logic`, `search_scope`）。
- **`NormalizedCondition`**: `text_groups: list[NormalizedTextGroup]`（デフォルト空）。

`normalize_cooccurrence_conditions_result` で:

- `text_groups` のバリデーション（リストか、各要素が object か、`texts` が非空か、`match_logic` / `combine_logic` / `search_scope` の許容値）。
- **最低限の clause 要件の更新**: 現行は「`forms` 系・注釈・参照のいずれか必須」。これを **「`text_groups` が非ならテキストのみでも可」** に拡張する。

## 5. マッチング仕様（文字列の比較）

### 5.1 初版（推奨）

- 各文字列は **前後 `strip` のみ**。比較は **そのまま部分文字列検索**（Polars `str.contains` 相当、**リテラル**扱い。正規表現メタ文字はエスケープする）。
- **大文字小文字**: 初版は **区別する**（日本語が主のため）。必要なら後続で `case_fold: true` のようなオプションを text group または条件全体に追加可能。

### 5.2 Unicode（任意・後続）

- 全角半角統一・NFKC 等が必要な場合は **`normalize_fulltext_for_match(s: str) -> str`** を一本化し、**テキスト列とクエリの両方**に適用する方針とする（片側だけだと漏れ・誤検出の原因）。

## 6. データソース（テキスト列）

### 6.1 現状コードとの差分（セカンドオピニオン確認）

- **`read_analysis_sentences_result`（`analysis_backend/data_access.py`）** は、現状 **`sentence_id` / `paragraph_id` / `sentence_no_in_paragraph` / `is_table_paragraph`** のみを返し、**文本文（`sentence_text` 等）は含まない**。設計初版の「`sentences_df` の文テキスト列」前提だけでは **文スコープの substring 評価は実装不能**。
- 実装の先に、DB から本文を読む **SELECT の拡張**、`SENTENCE_METADATA_SCHEMA`（または別スキーマ）の更新、および `select_target_ids_by_conditions_result` への受け渡しを定義する。

### 6.2 方針（canonical）

- **文スコープ**: 上記で追加する **文本文列**を正とする（トークン `surface` 再構成は表示経路と混同しない）。
- **段落スコープ**: 同一段落に属する文を **既存ルールで連結した段落テキスト**、または DB の段落テキスト列があればそれを使用。`is_table_paragraph` による **文間セパレータ（`""` / `"\n"`）の規則**を evaluator と renderer で揃える（§12「段落連結規則」）。

**実際の列名・結合規則**は `frame_schema` と `read_analysis_sentences` の契約として固定し、§8 の表示本文と **同一の canonical ルール**に寄せる。

## 7. 評価パイプラインへの組み込み（`condition_evaluator.py`）

### 7.1 現行の構造（要約）

段落モードでは概ね次の流れ:

1. `global_candidate_paragraphs_df` … トークン候補と注釈から **段落 ID の和集合**。
2. `token_paragraph_eval_df` … トークン / form_groups 評価 → `token_is_match`。
3. `annotation_paragraph_eval_df` … `annotation_is_match`。
4. `_build_base_condition_eval_df` …  
   `has_base_clause` ありのとき `base_is_match = token_is_match & annotation_is_match`。

### 7.2 変更案

1. **`text_paragraph_eval_df` を新設**  
   - 入力: `sentences_df`（および必要ならトークンから見た `paragraph_id` 集合）、`global_candidate_paragraphs_df` または **拡張後の universe**。
   - 出力列（段落集約後）: 少なくとも `paragraph_id`, `text_is_match`（中間として `matched_text_count` 等を form 側に揃えてもよい）。

2. **`base_is_match` の定義を拡張**  

   - `has_token_clause` … 従来どおり（`forms` が空でなく、かつ実質的にトークン評価が有効、または form_groups 由来で常に評価する現行ロジックに合わせる）。
   - `has_text_clause` … `text_groups` が非空。
   - `has_annotation_clause` … 従来どおり。

   推奨式（段落レベル）:

   \[
   \begin{aligned}
   token\_ok &= \begin{cases}
     token\_is\_match & (has\_token\_clause) \\
     true & (!has\_token\_clause)
   \end{cases} \\
   text\_ok &= \begin{cases}
     text\_is\_match & (has\_text\_clause) \\
     true & (!has\_text\_clause)
   \end{cases} \\
   annot\_ok &= \text{（現行どおり）} \\
   base\_is\_match &= token\_ok \land text\_ok \land annot\_ok
   \end{aligned}
   \]

   これにより **「トークン AND テキスト AND 注釈」** が要件どおり。`required_categories_*` による **参照 clause** は現行どおり `reference_is_match` で別レイヤ（既存の `_apply_category_reference_eval`）を維持。

3. **`CONDITION_EVAL_SCHEMA` の拡張（推奨）**  
   - デバッグ・エクスポート用に `text_groups` の要約文字列、`text_is_match`、`has_text_clause` 等を列として追加するかは実装フェーズで決定。最低限 `text_is_match` を保持するとトレースしやすい。

### 7.3 `global_candidate_paragraphs_df` の拡張（重要）

現状、`global_candidate_paragraphs_df` は **トークン候補と注釈段落の和集合**のみ（`condition_evaluator._build_global_candidate_paragraphs_df`）。

**問題**: 全条件の `forms` が空で **`text_groups` のみ**の条件がある場合、トークン絞り込みが空になり **候補段落が 0 件**になりうる。

**対策**: 正規化済み条件のうち **いずれかが非空の `text_groups` を持つ**とき、

- `sentences_df` から **全 `paragraph_id` のユニーク**（または `tokens_df` / DB スキーマで定義される全文書の段落集合）を `global_candidate_paragraphs_df` に **和集合**で追加する。

トークン条件が存在する場合は従来の `build_candidate_tokens_with_position_df` はそのまま活かし、**テキストのみの条件でも段落 universe が空にならない**ことを保証する。

- **sentence モード**: `selected_displayable_sentence_keys_df` 等、**表示可能な文への絞り**が別層にある。 paragraph universe 拡張だけでは足りず、**selection の真偽と displayable の真偽が二重化**しないよう sentence パスでも同様の前提整理が必要（§13）。

### 7.4 `analysis_unit == "sentence"` 分岐

`select_target_ids_by_conditions_result` の文単位モードでは、文スコープ条件と段落スコープ条件が分離されている。`text_groups` の `search_scope` も **sentence / paragraph** を取りうるため、

- **文スコープのテキスト条件**は文単位真理値に組み込み、
- **段落スコープ**は既存の段落要約へのマージ方針（form と同様）に従う。

**必須（§13 反映）**: 現行の sentence 側要約は **`build_condition_hit_result` の token hit**に依存する。`text_groups` のみの条件は token hit を出さないため、**sentence モード専用の text truth パス**（text 評価結果から `sentence_truth_df` / 要約へ合流）を paragraph モード対応と**同じリリース単位**で設計する。「実装時に整合」ではなく本設計のスコープに含める。

`sentence_truth_df` 構築と mixed-scope の **段落プロモート**は、**form_groups と同型**の思想とする（`not` と段落スコープの混在時はユーザー向け具体例をユーザーガイド等に追記するとよい）。

## 8. ヒットトークン列・可視化（`distance_matcher` / `build_condition_hit_result`）

- トークンヒットは **従来どおり `forms` / `form_groups` からのみ**生成する（変更しない）。
- **`text_groups` だけの条件**では、`condition_hit_tokens_df` は空になりうる。UI・CSV 上は「条件に合致だがトークンヒットなし」として扱う想定を文書化する。
- **トークン条件 AND テキスト条件**では、テキストはフィルタのみ、ハイライトはトークン側に限定（初版）。

### 8.1 評価テキストと表示テキストの一致（§13 反映）

- `rendering.build_rendered_paragraphs_df` / `build_rendered_sentences_df` および `analysis_core.reconstruct_*` は、表示本文を **token `surface` 再構成**で作っている経路がある。evaluator が **DB 生テキスト**でマッチさせ、UI が **再構成本文**を出すと、「ヒットした語句が画面に見えない」状態になりうる。
- **対策**: §6 の canonical と同一の本文源に寄せるか、意図的に異なる場合は **仕様で明示**し、CSV・詳細ペインに **text match の説明列・バッジ**を付ける（§14.3）。

将来、文书中で文字オフセットが取れるなら **疑似ヒット行**を追加する拡張は別タスクとする。

## 9. `analysis_core`・CLI・外部 I/F

- `_normalized_conditions_to_dicts` に `text_groups` を含めるかは、**Rust 側の `analysis_runner` や distance_matcher が dict を読む場合**に同期が必要。`build_condition_hit_result` が text を見ないなら、**必須ではない**が、デバッグ一貫性のため **含めることを推奨**。
- `filter_config` ローダはトップレベル変更なし（条件は `cooccurrence_conditions[]` 内）。

## 10. テスト方針

- 正規化: 無効な `match_logic`、空 `texts`、型エラーで **error issue** が出ること。
- 評価:  
  - テキストのみ / トークンのみ / 両方 AND で期待どおり `is_match` が変わること。  
  - `not` で部分文字列が無い単位のみマッチすること。  
  - `global_candidate_paragraphs_df` がテキストのみ条件で空にならないこと。
- 回帰: 既存の `form_groups` 条件のスナップショットが変わらないこと。
- **§13 掲載のチェックリスト**（editor 保存、一覧表示、text-only + paragraph/sentence、`limit_rows`、参照 clause、token 無し unit の件数検証、DB 本文 vs 再構成表示）をテスト計画に含める。

## 11. 実装タスク分割（参考）

1. `condition_model`: `NormalizedTextGroup` / `NormalizedCondition.text_groups`。
2. `condition_evaluator`: `_normalize_text_groups`、clause 必須条件の更新、`text_paragraph_eval_df` 構築、`_build_base_condition_eval_df` の AND 結合、`global_candidate_paragraphs_df` 拡張。
3. `analysis_unit=sentence` パスでの真理値合成（必要なら段落プロモート）。
4. テスト追加（`tests/test_analysis_core.py` 等）。
5. **必須寄り**: Rust `condition_editor`（`sanitize_document_for_save`・一覧・詳細）で `text_groups` を扱う。未対応なら保存不能・誤認（§13）。
6. `data_access` / `frame_schema`: 文本文列の取得とスキーマ。
7. `cli` / export: text-only 選択時の件数検証・空トークン描画。
8. `rendering` / `export_formatter`: canonical 本文と説明列の整合。

## 12. 未決事項・リスク

| 項目 | 内容 |
|------|------|
| 文/段落テキストの正 | DB 列とトークン再構成のどちらを正とするかで結果が変わりうる。**評価・表示の canonical source を 1 つに固定**しないとヒットと画面本文が不一致になりうる（§8・§14）。 |
| `sentences_df` 現状 | `read_analysis_sentences_result` は **文本文列を返していない**（メタのみ）。文スコープ text 評価には **データ取得とスキーマ拡張が先行**（§6・§13）。 |
| sentence モード | token hit ベースの要約だけでは **text-only 条件が偽のまま**残りうる。§7.4 を後回しにしない（§13・§14）。 |
| condition editor | `text_groups` を保存検証・UI が無視すると **保存不能・誤認**（§13）。 |
| CLI 件数検証 | text-only 選択後に token 行が無いと **selected count mismatch** になりうる（§13）。 |
| 段落 universe | paragraph だけでなく **sentence モードの displayable 絞り**とも整合が必要（§13）。 |
| `limit_rows` | text match の対象は **token 読込に合わせて制限**されうる。利用者向けに明示が必要（§13）。 |
| 参照 clause | `required_categories_*` が **analysis_unit によって text-only の categories を参照できない**可能性（§13）。 |
| 性能 | 全文書段落を universe に入れると Polars の `str.contains` が重くなりうる。必要なら段落 ID を事前絞り（トークン条件との積）を維持。 |
| 正規表現 | 初版はリテラルのみ。ユーザー拡張で regex を許す場合は **別フィールド**（例: `text_regex`）で明示し、ReDoS 対策を別設計とする。 |
| 段落連結規則 | `is_table_paragraph` により改行の有無が変わる。**段落テキストの結合規則を source 間で固定**しないと substring 結果がブレる（§13）。 |

## 13. セカンドオピニオン（実装確認ベース・批判的レビュー・原文）

確認対象:

- `analysis_backend/condition_evaluator.py`
- `analysis_backend/data_access.py`
- `analysis_backend/analysis_core.py`
- `analysis_backend/rendering.py`
- `analysis_backend/export_formatter.py`
- `analysis_backend/cli.py`
- `src/condition_editor.rs`
- `src/condition_editor_view.rs`
- `src/app_main_layout.rs`

総評:

- 方向性自体は妥当だが、**現状コードに対しては「条件 evaluator に text clause を足すだけ」で済む規模ではない**。
- 特に **condition editor の保存条件**, **`sentences_df` の契約**, **sentence モードの真理値合成**, **表示テキストと評価テキストの不一致** は、設計書の現状の書き方だと過小評価されている。
- このまま実装に入ると、**保存できない condition**, **ヒットしているのに画面上で理由が見えない結果**, **analysis_unit によって挙動が変わる reference clause**, **selected count mismatch** が起きる可能性が高い。

### 1. 高確率で実バグ化する点

- [重大] **text-only condition を condition editor で保存できない可能性が高い。**  
  `src/condition_editor.rs` の `sanitize_document_for_save` は clause 判定を `forms` / `form_groups` / `annotation_filters` / `required_categories_*` だけで見ており、`text_groups` を考慮していない。  
  そのため「`text_groups` のみを持つ正当な condition」を読み込めても、保存時に「clause が空」と判定される設計不整合が起きる。

- [重大] **設計 §6 の `sentences_df` 前提が、現状コードの契約と一致していない。**  
  `analysis_backend/data_access.py::read_analysis_sentences_result` が返しているのは、現状 `sentence_id` / `paragraph_id` / `sentence_no_in_paragraph` / `is_table_paragraph` であり、評価に使える `sentence_text` を持っていない。  
  つまり `select_target_ids_by_conditions_result(..., sentences_df=...)` の現行シグネチャのままでは、設計書が想定する「文テキストに対する substring 評価」は実装できない。`frame_schema` と呼び出し契約の変更が先に必要。

- [重大] **sentence モードでは text-only 条件が無視される実装になりやすい。**  
  `analysis_backend/condition_evaluator.py` の sentence 分岐は、`sentence_match_summary_df_sentence_scope` を `build_condition_hit_result` の token hit から組み立てている。  
  text-only condition は token hit を生成しないため、paragraph モード側だけを直しても sentence モードでは `false` 扱いのまま残る危険がある。  
  §7.4 は「実装時に整合させる」では弱く、**sentence モード専用の text truth パスが必要**と明記した方がよい。

- [重大] **表示・CSV の本文と、text match に使う本文がズレる可能性が高い。**  
  `analysis_backend/rendering.py::build_rendered_paragraphs_df` / `build_rendered_sentences_df` と `analysis_backend/analysis_core.py::reconstruct_paragraphs_by_ids` は、表示本文を token `surface` の再構成で作っている。  
  `analysis_backend/export_formatter.py::enrich_reconstructed_sentences_result` も metadata 側の `sentence_text` ではなく、左側の再構成 `sentence_text` をそのまま残す。  
  そのため evaluator が DB の生テキストを使い、UI/CSV が token 再構成本文を出すと、**「ヒットしたはずの語句が画面本文に見えない」** という説明困難な挙動が起きる。

- [重大] **text-only で選ばれた unit が、最終出力では消える可能性がある。**  
  `analysis_backend/cli.py` は選択後の描画を `build_tokens_with_position_df(... target_forms=None ...)` ベースで行い、さらに `selected count` をレコード件数で検証している。  
  text 条件で選ばれた paragraph / sentence に token row が無い場合、summary では selected でも描画対象が空になり、`selectedParagraphCount mismatch` / `selectedSentenceCount mismatch` でジョブ失敗になる可能性がある。

### 2. UI 崩れ・UI 誤認の可能性

- [重大] **condition editor 上で `text_groups` が不可視になりやすい。**  
  `ConditionEditorItem` は unknown key を `extra_fields` に保持するので、推測上は `text_groups` は round-trip されうる。  
  ただし `src/condition_editor_view.rs` の一覧表示 (`draw_condition_editor_list_panel`) と詳細編集 UI は `form_groups` / filters / refs しか見ていない。  
  その結果、**実際には text 条件を持つ condition が UI 上は「groups:0 forms:0 filters:0 refs:0」に見える**可能性がある。これは単なる未対応表示ではなく、誤編集を誘発する。

- [中] **condition editor は固定幅・横並び前提が強く、`text_groups` をそのまま足すと密度過多になりやすい。**  
  `src/condition_editor_view.rs` は `CONDITION_EDITOR_FIELD_LABEL_WIDTH` / `CONDITION_EDITOR_TEXT_INPUT_WIDTH` などの固定幅と `ui.horizontal(...)` を多用している。  
  `form_groups` 相当の編集セクションをもう 1 セット増やすと、ウィンドウ幅が狭い環境で詰まりやすく、特に一覧ラベルと詳細パネルが読みづらくなる。  
  既存 UI の延長で増築するより、`Token clauses` / `Text clauses` の折りたたみ分離やタブ分離を先に検討した方が安全。

- [中] **text-only match は「ヒット理由が見えない」UI になりやすい。**  
  `src/app_main_layout.rs` では既に「本文強調は直接ヒット token のみ」と明示している。  
  text clause を入れると、この注意文が「paragraph 昇格の一部」ではなく **text-only 条件の全件**に当てはまる。  
  つまり初版のままだと、**選択されたのに本文強調がゼロ**という結果が大量に出るため、専用バッジや explanation を追加しないと false positive に見えやすい。

### 3. 計算・件数不整合の可能性

- [重大] **paragraph universe 問題は paragraph モードだけの話ではない。**  
  設計書は `global_candidate_paragraphs_df` の拡張を主に paragraph モード文脈で述べているが、実際には sentence モードにも `selected_displayable_sentence_keys_df` という別の絞り込みがある。  
  ここは token 表示可能な sentence のみを残すため、text-only 条件を入れると **selection truth と displayable truth が二重化**し、結果が analysis unit 依存でズレやすい。

- [中] **`limit_rows` 併用時の全文検索範囲が直感とズレる。**  
  `analysis_backend/cli.py::_filter_sentences_for_tokens` は `analysis_tokens` の制限結果に合わせて `analysis_sentences_df` も inner join している。  
  そのため text match を sentences ベースで実装しても、`limit_rows` があると **全文検索対象は「DB の全文」ではなく「読み込めた token のある sentence」だけ**になる。  
  現行実装の一貫性としては理解できるが、設計書で明示しないと利用者にはかなり分かりにくい。

- [中] **reference clause の category 計算が `analysis_unit` でズレる危険がある。**  
  paragraph モードでは `_build_category_reference_eval_df` が `base_condition_eval_df` 由来の summary を使う。  
  しかし sentence モードの `_filter_sentence_hit_tokens_by_reference_clauses` は `condition_hit_tokens_df` から category を作っており、token hit を持たない text-only 条件はここに乗らない。  
  その結果、**text-only 条件の categories が他条件の `required_categories_*` を満たすかどうかが、analysis_unit によって変わる**可能性がある。

- [中] **既存の diagnostic 列は token 前提なので、text 条件の件数が説明されない。**  
  `CONDITION_EVAL_SCHEMA` は `required_form_count`, `matched_form_count`, `distance_is_match`, `token_is_match` など token 系が中心。  
  `text_is_match` だけ足しても、どの `texts` が何件マッチしたか、どの scope で評価したか、どの group 結合で真になったかが残らない。  
  `form_group_explanations_text` と同等の **text_group explanation / matched_text_group_ids / mixed_scope_warning** を用意しないと、デバッグ不能に近い。

### 4. 意図しない動作の可能性

- [中] **段落本文の結合規則が source によって変わると、sentence 境界またぎ検索の結果がブレる。**  
  現状の paragraph 表示は `is_table_paragraph` に応じて `""` もしくは `"\n"` で sentence を連結している。  
  DB 側 paragraph text が別規則で保存されている場合、`"A\nB"` にだけマッチする query や `"AB"` にだけマッチする query の結果が source 依存になる。  
  これは table paragraph で特に起きやすいので、設計書の「どの列を正とするか」だけでなく **sentence join ルールの固定**まで必要。

- [中] **mixed-scope + not の意味がユーザー想像より段落寄りになる。**  
  現行 `form_groups` は `_combine_group_matches_df` で mixed-scope を paragraph に promote している。  
  `text_groups` でも同型実装にすると、`sentence not` と `paragraph and/or` を混ぜた瞬間に、真理値は paragraph へ潰れる。  
  仕様としては一貫するが、直感的ではないので、少なくとも設計書に具体例を入れないと「文に無い」のつもりが「段落全体に無い」へ読み替わる危険がある。

- [中] **二重の `_normalized_conditions_to_dicts` が将来の取りこぼし点になる。**  
  `analysis_backend/condition_evaluator.py` と `analysis_backend/analysis_core.py` に同名の dict 化ロジックがあり、paragraph 描画経路では raw `filter_config.cooccurrence_conditions` も別に通る。  
  `text_groups` を一部の経路にだけ追加すると、selection は動くのに debug/export/hit rebuilding が古い schema のまま、というズレが起きやすい。

### 5. テスト不足として追加した方がよいもの

- `text_groups` のみを持つ condition を condition editor でロードし、そのまま保存できるか。
- `text_groups` を持つ condition が condition editor 一覧で不可視化・誤認されないか。
- paragraph モードで text-only condition が token candidate 0 件でも paragraph を選択できるか。
- sentence モードで sentence-scope text-only condition が正しく sentence truth に反映されるか。
- `required_categories_*` が text-only condition の categories を参照できるか。paragraph / sentence の両方で確認すること。
- `limit_rows` 指定時に text match の対象 universe がどう制限されるか。仕様どおりの warning が出るか。
- token row を持たない paragraph / sentence が text 条件で選ばれたとき、出力件数検証が壊れないか。
- DB 文本文と token 再構成本文が異なるケースで、評価本文と表示本文のどちらを正とするかが一貫しているか。
- table paragraph で改行有無が検索結果に与える影響を固定できているか。

### 6. 設計書に追加した方がよい前提

- **canonical text source を evaluator と renderer で共通化する**。これを決めないと、検索成功と画面表示の整合が取れない。
- **condition editor の扱いを先に決める**。初版で UI 未対応にするなら、「読み込みはできるが保存はブロックする」「read-only バッジを出す」などの方針が必要。
- **sentence モードは別タスク化しない方がよい**。現行実装では token-hit ベースのため、paragraph モード実装後の追加対応にすると差分バグが出やすい。
- **説明列は optional ではなく必須に近い**。text-only match を UI/CSV で説明できないと、運用上はバグと見分けがつかない。

結論:

- この設計は実装可能だが、現状コードベースに対しては **「検索条件の追加」ではなく、「評価テキスト contract と出力 contract の拡張」** と捉えた方が安全である。
- 少なくとも初版の着手前に、`sentences_df` の text 契約、renderer/export の canonical text、condition editor の保存方針、sentence モードの真理値合成を先に固定しないと、後戻りコストが高い。

## 14. 設計への反映（セカンドオピニオン統合）

本節は §13 の指摘を設計レベルで取り込んだ**合意・追加前提**をまとめる。実装タスクの優先順位付けに用いる。

### 14.1 必須の先決事項（着手前）

| 項目 | 反映内容 |
|------|----------|
| 評価テキストの契約 | **canonical text source** を 1 つに決め、`condition_evaluator` と `rendering` / export が**同じ規則**で本文を得る。DB 保存文と token `surface` 再構成がズレる場合は、設計上どちらを「検索・表示の正」とするかを明示し、もう一方は変換または注記で吸収する。 |
| `sentences_df` | 現行 `read_analysis_sentences_result` はメタ列のみで **`sentence_text` を返していない**（§13 参照）。文スコープの substring 評価の前に、**SQL / スキーマ / `frame_schema` の拡張**と呼び出し側の受け渡しを揃える。 |
| condition editor（Rust） | `sanitize_document_for_save` の clause 判定に **`text_groups` を含める**。未対応のままだと text-only 条件が保存不能。UI は一覧・詳細の**可視化方針**（折りたたみ・タブ・read-only バッジ等）を先に決める。 |
| sentence モード | token hit に依存した要約だけでは text-only が常に偽になりうる。**sentence 用の text truth パス**を paragraph 実装と**同フェーズ**で設計・テストに含める（§7.4 を「後回し可」にしない）。 |

### 14.2 出力・検証経路（CLI / 件数）

- text-only で選ばれた paragraph / sentence に **token 行が無い**場合でも、`cli` の `selected*Count` 検証や `build_tokens_with_position_df` ベースの描画が破綻しないよう、**空トークン行の期待値**と**件数の定義**を仕様に書き分ける。
- `limit_rows` 併用時は、text match の universe が **DB 全文ではなく token 読込に inner join された sentence 集合**に制限されうることを、利用者向けに **仕様または warning** で明示する。

### 14.3 観測可能性（列・UI）

- `CONDITION_EVAL_SCHEMA` への `text_is_match` 追加に加え、可能なら **`form_group_explanations_text` と対になる text 側の説明列**（どの group / どの `texts` で真になったか）を検討する。text-only は本文強調が付かないため、**誤認防止**のバッジや explanation が実質必須に近い。
- `required_categories_*`（参照 clause）が **text-only 条件の categories を paragraph / sentence の両モードで一貫して参照できるか**をテストで固定する。

### 14.4 実装時の二重管理に注意

- `_normalized_conditions_to_dicts` 等、**条件 dict を複製している経路**には `text_groups` を**同じ形で**通す。selection と hit rebuild / export で schema が食い違わないようにする。

## 15. 細分タスク（実装用チェックリスト）

前提: **§14.1 の先決事項**を満たす順で進める（並列可能なものは ID を参照）。

| ID | タスク | 主な変更箇所 | 依存 | 完了条件（受け入れ基準の要約） |
|----|--------|----------------|------|--------------------------------|
| **FT-00** | **canonical 本文**の決定メモを本書 §6.2 / §8.1 に 1 段落で確定（DB 文 vs 再構成のどちらを正とするか、または変換方針） | 本書のみ | なし | 実装者が迷わず同一ルールで evaluator・renderer・export を揃えられる文言になっている |
| **FT-01** | `analysis_sentences` から **文本文列**を読む SQL と **`frame_schema`（または専用 schema）**の列定義を追加 | `data_access.py`, `frame_schema.py` | FT-00 | `read_analysis_sentences_result` が設計で決めた列名の本文を返す。既存呼び出しが壊れない（後方互換または段階移行方針つき） |
| **FT-02** | `NormalizedTextGroup` / `NormalizedCondition.text_groups` の dataclass 追加 | `condition_model.py` | なし | フィールドが §3.2 と一致し、frozen/immutable 方針が既存に揃う |
| **FT-03** | `_normalize_text_groups`（バリデーション・issue コード・空 `texts` 排除・ユニーク化） | `condition_evaluator.py` | FT-02 | 不正 JSON は error issue、正常系は `NormalizedCondition` に載る |
| **FT-04** | **clause 必須**ロジック更新（`text_groups` のみでも条件として有効） | `condition_evaluator.py` | FT-03 | text-only が正規化で落ちない。既存条件の挙動不変 |
| **FT-05** | **リテラル** `str.contains` 用のエスケープヘルパー（正規表現メタ無効化）と単体テスト | 新規小モジュール or `condition_evaluator` 内 | なし | `.` `(` 等を含むクエリで意図しない regex 化が起きない |
| **FT-06** | `_build_text_group_matched_units_df` 相当: 1 グループの paragraph/sentence 単位の真偽 | `condition_evaluator.py` | FT-01, FT-03, FT-05 | `and` / `or` / `not` が §3.3 どおり |
| **FT-07** | 複数 `text_groups` の **`combine_logic` 畳み込み**（`_combine_group_matches_df` と同型） | `condition_evaluator.py` | FT-06 | form_groups と同じ結合意味で段落キーに揃う |
| **FT-08** | `text_paragraph_eval_df` 生成と **`global_candidate_paragraphs_df` 拡張**（text 条件あり時に universe 非空） | `condition_evaluator.py` | FT-01, FT-07 | text-only でも候補段落 0 件にならない |
| **FT-09** | `_build_base_condition_eval_df` 拡張: `text_is_match`, `has_text_clause`, **`base_is_match` = token ∧ text ∧ annot** | `condition_evaluator.py` | FT-08 | §7.2 の式どおり。token / text / annot の片方欠けでも破綻しない |
| **FT-10** | **`CONDITION_EVAL_SCHEMA`**（および必要なら説明列）更新 | `condition_evaluator.py` | FT-09 | 出力 DataFrame が schema 一致。既存列の意味が変わらない |
| **FT-11** | **`analysis_unit == "sentence"`** 分岐: text truth を `sentence_truth_df` / 要約へ合流（token hit 非依存） | `condition_evaluator.py` | FT-07, FT-09 | text-only が sentence モードで偽固定にならない（§7.4） |
| **FT-12** | **参照 clause**（`required_categories_*`）が text-only の categories を paragraph / sentence 両方で一貫参照 | `condition_evaluator.py` | FT-09, FT-11 | §13 の懸念が再現しない回帰テスト付き |
| **FT-13** | `_normalized_conditions_to_dicts`（`condition_evaluator` / **`analysis_core` 両方**）に `text_groups` を反映 | `condition_evaluator.py`, `analysis_core.py` | FT-03 | 複製経路でキー欠落なし（§14.4） |
| **FT-14** | **`cli`**: text-only 選択後の **件数検証**・空トークン描画・`limit_rows` 時の universe 説明 or warning | `cli.py` | FT-08, FT-11 | `selected*Count mismatch` が出ない。利用者が検索範囲を誤解しない |
| **FT-15** | **`rendering` / `export_formatter`**: canonical 本文に合わせた表示または注記 | `rendering.py`, `export_formatter.py` | FT-00, FT-01 | ヒット語句と画面/CSV 本文の説明がつじつま合う（§8.1） |
| **FT-16** | **Rust** `sanitize_document_for_save` に `text_groups` を clause 判定へ追加 | `condition_editor.rs` | FT-03（仕様同期） | text-only 条件が保存可能 |
| **FT-17** | **Rust** 一覧ラベル・詳細に `text_groups` セクション（折りたたみ推奨） | `condition_editor_view.rs` | FT-16 | 「groups:0」と誤認されない（§13） |
| **FT-18** | **アプリ本体**（必要なら）: text-only 時の説明バッジ・`app_main_layout` 周辺の注意文整合 | `app_main_layout.rs` 等 | FT-15 | 強調ゼロでも理由が分かる |
| **FT-19** | **Python テスト**: `tests/test_analysis_core.py` 等に §13 チェックリスト相当を追加 | `tests/` | FT-04–FT-14 | CI でカバー。既存スナップショット破壊なしまたは意図的更新のみ |
| **FT-20** | （任意）**Rust テスト**またはスモーク: 条件 JSON に `text_groups` を含めた round-trip | `tests/` or 手動手順書 | FT-16, FT-17 | editor 保存→再読込で `text_groups` 保持 |

### 15.1 推奨フェーズ分け

1. **フェーズ A（データ契約）**: FT-00, FT-01, FT-05  
2. **フェーズ B（正規化・段落評価）**: FT-02–FT-10  
3. **フェーズ C（sentence・参照・CLI）**: FT-11, FT-12, FT-14  
4. **フェーズ D（表示・editor・テスト）**: FT-13, FT-15–FT-20  

`distance_matcher.py` は初版 **変更不要**（§8）だが、FT-19 で「token hit 空でも selection 真」系の結合テストを入れる。

---

## 改訂履歴

| 日付 | 内容 |
|------|------|
| 2026-03-25 | 初版 |
| 2026-03-25 | セカンドオピニオン（§13）追記、設計反映（§14）、本文 §6・§7・§8・§12 の補強 |
| 2026-03-25 | §15 細分タスク（FT-00〜FT-20）追記 |
