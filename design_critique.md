# `analysis_unit=sentence` での paragraph 条件評価に関する設計レビュー

提示された設計書について、関連するコードベース (`analysis_backend/condition_evaluator.py`, `analysis_backend/cli.py`, `analysis_backend/rendering.py`, `src/app_main_layout.rs`) を確認した上でのセカンドオピニオンを提示します。

総評として、方針自体は実現可能ですが、**「表示 sentence 全体に対する summary（非直接ヒット sentence は matched_* を空扱い）」とする仕様**が、UI表示およびデータ出力において複数の重大な不整合やバグを引き起こすリスクがあります。

以下に「バグ・計算ミス」「UI崩れ」「意図しない動作」の観点から詳細を指摘します。

## 1. バグの可能性・計算ミスの可能性

### 1-1. メタデータ (Summary) の欠落と CSV 出力の不整合
設計書の実装方針に「非直接ヒット sentence は `matched_*` を空扱い」とありますが、これは重大な計算ミス（データ欠落）を引き起こします。
`condition_match_logic = "any"` で paragraph 条件にヒットして展開された sentence や、`all` で paragraph 条件にも依存して表示された sentence において、`matched_condition_ids` や `matched_categories` が空になります。
これにより、**CSV 出力 (`output-csv-path`) において、その sentence がどの条件で抽出されたのかの情報が全く記録されない**という不具合になります。

**対策案**:
`sentence_match_summary_df` を構築する際、単に直接ヒットの有無を見るだけでなく、親 paragraph の `paragraph_match_summary_df` から `matched_condition_ids` や `matched_categories` などを継承（マージ）して付与する必要があります。

### 1-2. `is_selected` (all 合成) の判定ロジックの破綻リスク
現状の `select_target_ids_by_conditions_result` では、マッチした条件数 (`matched_condition_count`) と全条件数 (`condition_count`) が一致するかどうかで `all` の判定を行っています。
sentence 条件と paragraph 条件を別々に評価して後から合成する場合、内部の DataFrame 上で `condition_count` の分母が変わってしまいます。単純に `target_sentence_ids` を和集合・積集合で計算するだけでは、後続の `sentence_match_summary_df` の生成時や、`cli.py` での整合性チェック (`selectedSentenceCount == records件数`) において件数やロジックが合わなくなり、実行時エラーや結果が正しく抽出されないバグが懸念されます。

### 1-3. Paragraph サマリの逆算ロジック (`cli.py`) の破綻
`cli.py` の現状の実装では、`_build_paragraph_match_summary_from_sentence_summary_df` を使って、sentence 側のヒット情報から paragraph 側のサマリを逆算して構築しています。
本設計のように「paragraph 条件が主導で sentence を選択する（しかし sentence 側は空扱い）」という状態になると、この逆算ロジックでは正確な paragraph サマリが作れなくなります。評価順序を「paragraph 評価結果 → sentence サマリへの順方向の伝搬」へと根本的に見直す必要があります。

## 2. UI 崩れ

### 2-1. Ghost レコード（理由なき表示）による混乱
`src/app_main_layout.rs` の `draw_record_summary` では、選択されたレコードの上部に `conditions: {matched_condition_ids_text}` や `categories: ...` を表示します。
設計通りに非直接ヒット sentence の `matched_*` を空扱いとすると、ユーザがリストからその sentence 行を選択した際、**条件もカテゴリも空欄、かつハイライトも一切無いレコードが表示**されます。
ユーザから見れば「なぜこの行が検索結果にヒットしたのか全く分からない」状態となり、深刻な UX の低下（UI 上の不整合）を招きます。

### 2-2. 高度条件の説明パネル (`form_group_explanations_text`) の欠落
現在の paragraph 表示では、`app_main_layout.rs` の `draw_form_group_explanations_panel` によって高度条件の適用理由が折りたたみパネルで表示されます。
sentence 出力であっても paragraph 条件が適用されるのであれば、sentence 行の表示時にもこの説明文 (`form_group_explanations_text`) が伝搬されて表示されるべきですが、提案されている「空扱い」のままでは完全に欠落してしまいます。

## 3. 意図しない動作の可能性（要件定義への疑問）

### 3-1. 非ハイライトに対するユーザの期待値とのズレ
要件3に「paragraph 展開で含まれた sentence は非ハイライトでよい」とあります。
しかし、例えば paragraph 条件が「"条例"という単語を含む」であった場合、ユーザはリストに表示された sentence を見たとき、直接ヒットでなくても**「その文の中にある"条例"という単語」がハイライトされること**を期待するはずです。
ハイライトが完全に省略されると、段落全体のどこに条件単語があるか探す手間が生じ、「検索機能が壊れている」と誤認されるリスクが高いです。
**再考の提案**: `sentence_hit_tokens_df` には、直接の sentence 条件だけでなく、paragraph 条件の評価でヒットした token 情報もそのまま残して、ハイライト対象とする方が自然な挙動になります。

### 3-2. `selectedSentenceCount` の意味の変質による影響
「`selectedSentenceCount` は『直接ヒット件数』ではなく『表示件数』を意味する」とデータ契約を変更する方針ですが、これによって従来の「ヒットした文の件数」という意味合いが変わってしまいます（段落展開によって水増しされた件数になります）。
バッチ処理や外部スクリプトがこの数値を「該当件数」としての統計に使っていた場合、互換性が失われます。「上限制御は追加しない」とのことですが、表示件数が爆発的に増えるリスクもあるため、仕様として許容できるか慎重な判断が必要です。純粋なヒット件数を知るための `matchedSentenceCount` の新設を併せて行うことを強く推奨します。