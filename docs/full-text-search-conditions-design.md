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

- **文スコープ**: `sentences_df` の **文テキスト列**（既存の `read_analysis_sentences` / スキーマに合わせる）。トークンからの再構成ではなく、**DB に保存されている文本文**を正とする（再構成との不一致があれば設計上リスクとして文書化）。
- **段落スコープ**: 同一段落に属する文を **既存ルールで連結した段落テキスト**、または段落テキスト列があればそれを使用。

実装タスクで **実際の列名**を `frame_schema` または `read_analysis_sentences` の契約として固定する。

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

### 7.4 `analysis_unit == "sentence"` 分岐

`select_target_ids_by_conditions_result` の文単位モードでは、文スコープ条件と段落スコープ条件が分離されている。`text_groups` の `search_scope` も **sentence / paragraph** を取りうるため、

- **文スコープのテキスト条件**は文単位真理値に組み込み、
- **段落スコープ**は既存の段落要約へのマージ方針（form と同様）に従う。

詳細は実装時に `sentence_truth_df` 構築箇所と整合させる（本設計では **form_groups の mixed-scope と同じ思想**とする）。

## 8. ヒットトークン列・可視化（`distance_matcher` / `build_condition_hit_result`）

- トークンヒットは **従来どおり `forms` / `form_groups` からのみ**生成する（変更しない）。
- **`text_groups` だけの条件**では、`condition_hit_tokens_df` は空になりうる。UI・CSV 上は「条件に合致だがトークンヒットなし」として扱う想定を文書化する。
- **トークン条件 AND テキスト条件**では、テキストはフィルタのみ、ハイライトはトークン側に限定（初版）。

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

## 11. 実装タスク分割（参考）

1. `condition_model`: `NormalizedTextGroup` / `NormalizedCondition.text_groups`。
2. `condition_evaluator`: `_normalize_text_groups`、clause 必須条件の更新、`text_paragraph_eval_df` 構築、`_build_base_condition_eval_df` の AND 結合、`global_candidate_paragraphs_df` 拡張。
3. `analysis_unit=sentence` パスでの真理値合成（必要なら段落プロモート）。
4. テスト追加（`tests/test_analysis_core.py` 等）。
5. （任意）Rust `condition_editor` の JSON 編集・検証、ドキュメント。

## 12. 未決事項・リスク

| 項目 | 内容 |
|------|------|
| 文/段落テキストの正 | DB 列とトークン再構成のどちらを正とするかで結果が変わりうる。 |
| 性能 | 全文書段落を universe に入れると Polars の `str.contains` が重くなりうる。必要なら段落 ID を事前絞り（トークン条件との積）を維持。 |
| 正規表現 | 初版はリテラルのみ。ユーザー拡張で regex を許す場合は **別フィールド**（例: `text_regex`）で明示し、ReDoS 対策を別設計とする。 |

---

## 改訂履歴

| 日付 | 内容 |
|------|------|
| 2026-03-25 | 初版 |
