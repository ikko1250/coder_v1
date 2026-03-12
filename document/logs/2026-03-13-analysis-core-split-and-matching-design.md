# 2026-03-13 analysis_core.py 分割 + distance matching 方式明確化 設計案

- 目的: `analysis_backend/analysis_core.py` の責務分割と、distance matching の使用方式を明示的な仕様へ整理する
- 日時: 2026-03-13 00:49:51 +0900

## 既存挙動の確認

- 現在の `analysis_core.py` は以下を一括で持っている。
  - filter config 読込
  - SQLite / Polars 読込
  - 条件正規化
  - token 位置付け
  - 条件評価と target paragraph 選定
  - distance matching を含む hit token 抽出
  - token annotation 生成
  - tagged text / HTML 生成
  - paragraph 再構成
  - metadata join
  - export 用 DataFrame 整形
- distance matching は `max_token_distance` 有効時に `_find_distance_match_groups_by_unit` で行われる。
- 各 form 候補数の積が `MAX_DISTANCE_MATCH_COMBINATIONS = 10000` を超えると、全組合せ探索から greedy fallback へ切り替わる。
- 現状の呼び出し側は、この方式差を受け取れない。

## 設計目標

1. 分析結果の意味が、データ量に応じて暗黙に変わらないようにする
2. matching 方式を、実装詳細ではなく明示的な仕様として扱う
3. `analysis_core.py` を façade として残しつつ、内部責務を分割する
4. 既存 CLI / test への破壊的影響を段階的に抑える

## 推奨方針

- 方式仕様を先に固定し、その仕様に合わせて責務分割する
- 内部モジュールを分けるが、最初の段階では `analysis_core.py` を orchestrator / compatibility façade として残す
- distance matching は `strict / auto-approx / approx` の3モードを仕様化する

## distance matching 方式仕様案

### 推奨する mode

- `strict`
  - 常に厳密探索
  - 結果意味: 全 valid match group を列挙
  - 長所: 意味が最も明確
  - 短所: 候補爆発時に重い
  - 補足: 無制限実行にはせず、別途 safety limit を持つ
- `auto-approx`
  - 組合せ数が cap 以下なら厳密探索
  - cap 超過時のみ approx へ切替
  - 結果意味: 通常は strict、過負荷時のみ近似
  - 長所: 現状に近い運用性
  - 短所: mode 切替の説明責任が必要
- `approx`
  - 常に greedy 近似
  - 結果意味: 非重複 match group の近似抽出
  - 長所: 性能予測がしやすい
  - 短所: strict とは意味が異なる

### 推奨 default

- ライブラリ内部 default: `auto-approx`
- 将来的な研究再現用途の推奨実行: `strict`

理由:

- 現行運用との連続性を維持しつつ、fallback 発動を仕様として表面化できるため。
- CLI や GUI の通常利用では止まりにくさが重要だが、分析再現時には strict を選べる余地が必要。

### mode ごとの返却情報

- matching 処理は、hit token DataFrame だけでなく以下を返す。
  - `requestedMode`
  - `usedMode`
  - `combinationCap`
  - `combinationCount`
  - `warningMessages`
- `auto-approx` で approx に落ちた場合は warning を必須で返す。

### 安全装置の追加

- `strict` でも無制限に全探索させない。
- `distance_match_combination_cap`
  - `auto-approx` で strict から approx へ切り替える閾値
- `distance_match_strict_safety_limit`
  - `strict` 実行時に、これを超える場合は近似へ落とさずエラーにする上限
- 期待挙動:
  - `strict`: safety limit 超過で失敗
  - `auto-approx`: cap 超過で approx に切替 + warning
  - `approx`: 近似で継続

理由:

- `strict` を再現性用途に使えるまま、OOM や極端なタイムアウトの危険を抑えるため。

### warning の最小仕様

- code: `distance_match_fallback`
- message: `distance matching switched from strict to approx because combination cap was exceeded`
- fields:
  - `conditionId`
  - `unitId`
  - `requestedMode`
  - `usedMode`
  - `combinationCount`
  - `combinationCap`

## 分割構成案

### 1. `analysis_backend/filter_config.py`

- 役割:
  - JSON 読込
  - root-level config validate
  - `condition_match_logic`
  - `max_reconstructed_paragraphs`
  - distance matching mode / cap 設定の読込
- 公開候補:
  - `loadFilterConfig(...)`

### 2. `analysis_backend/condition_model.py`

- 役割:
  - 条件定義と result 型の集約
- 公開候補:
  - `FilterConfig`
  - `NormalizedCondition`
  - `MatchingWarning`
  - `DistanceMatchingMode`
  - `TargetSelectionResult`
  - `ConditionHitResult`

### 3. `analysis_backend/data_access.py`

- 役割:
  - SQLite / Polars 読込
  - paragraph metadata 読込
- 公開候補:
  - `readAnalysisTokens(...)`
  - `readAnalysisSentences(...)`
  - `readParagraphDocumentMetadata(...)`

### 4. `analysis_backend/token_position.py`

- 役割:
  - token への sentence / paragraph position 付与
  - candidate token 抽出
- 公開候補:
  - `buildTokensWithPositionDf(...)`
  - `buildCandidateTokensWithPositionDf(...)`

### 5. `analysis_backend/distance_matcher.py`

- 役割:
  - strict / approx の distance group 抽出
  - combination cap 判定
  - warning 生成
- 公開候補:
  - `buildDistanceConditionHits(...)`
  - `evaluateDistanceMatchesByUnit(...)`

### 6. `analysis_backend/condition_evaluator.py`

- 役割:
  - 条件正規化
  - paragraph / sentence 単位の条件評価
  - target paragraph / sentence 選定
  - token hit 生成の orchestration
- 公開候補:
  - `normalizeConditions(...)`
  - `selectTargetIdsByConditions(...)`
  - `buildConditionHitTokens(...)`

### 7. `analysis_backend/rendering.py`

- 役割:
  - annotation lookup
  - token annotation 生成
  - tagged text / html render
  - paragraph 再構成
- 公開候補:
  - `buildTokenAnnotationsDf(...)`
  - `renderTaggedToken(...)`
  - `buildRenderedParagraphsDf(...)`

### 8. `analysis_backend/export_formatter.py`

- 役割:
  - metadata join
  - ordinance/rule 分類
  - CSV export 用列整形
- 公開候補:
  - `enrichReconstructedParagraphsDf(...)`
  - `buildReconstructedParagraphsExportDf(...)`

### 9. `analysis_backend/analysis_core.py`

- 役割:
  - compatibility façade
  - 既存 import 先の維持
  - 段階移行中の orchestrator
  - 旧 tuple return を一時的に維持する互換レイヤー

## 戻り値設計案

### 現状の問題

- 5 要素や 6 要素の tuple return があり、意味を順番で覚える必要がある。
- `dict[str, object]` が多く、フィールド契約が弱い。

### 推奨する result 型

```python
@dataclass(frozen=True)
class MatchingWarning:
    code: str
    message: str
    conditionId: str
    unitId: int | None
    requestedMode: str
    usedMode: str
    combinationCount: int | None
    combinationCap: int | None


@dataclass(frozen=True)
class ConditionHitResult:
    conditionHitTokensDf: pl.DataFrame
    requestedMode: str
    usedMode: str
    warningMessages: list[MatchingWarning]


@dataclass(frozen=True)
class TargetSelectionResult:
    candidateTokensDf: pl.DataFrame
    conditionEvalDf: pl.DataFrame
    paragraphMatchSummaryDf: pl.DataFrame
    targetParagraphIds: list[int]
    targetSentenceIds: list[int]
    warningMessages: list[MatchingWarning]
```

## 推奨する実行フロー

1. `filter_config.py` で config を読込
2. `condition_evaluator.py` で条件を正規化
3. `token_position.py` で candidate token に位置を付与
4. `condition_evaluator.py` で target paragraph を選定
5. `distance_matcher.py` を含む matcher 群で hit token を抽出
6. `rendering.py` で annotation と paragraph 表現を作成
7. `export_formatter.py` で metadata join と export 列整形
8. `analysis_core.py` は上記を束ねる façade として残す

## 段階的移行案

### Phase 1: 方式仕様の固定

- `distance_matching_mode`
- `distance_match_combination_cap`
- warning 契約
- result 型

この段階では `analysis_core.py` 内実装でもよい。まず public contract を固定する。

### Phase 2: matcher 分離

- distance matching 関連関数を `distance_matcher.py` へ移す
- strict / auto-approx / approx の切替を集約する

### Phase 3: evaluator 分離

- 条件正規化と target selection を `condition_evaluator.py` へ移す
- tuple return を result 型へ置換する

### Phase 4: rendering / export 分離

- annotation / render / export 整形を別モジュール化する

### Phase 5: façade 整理

- `analysis_core.py` を薄い再 export / orchestration 層へ寄せる

## テスト方針

- 既存契約テストは維持する
- 追加するべきテスト:
  - `strict` は全 valid group を返す
  - `approx` は strict と異なる結果を返しうる
  - `auto-approx` で fallback warning が出る
  - `strict` が safety limit 超過で失敗する
  - `usedMode` が meta / result に反映される
  - façade が旧戻り値契約を維持する
  - warning が CLI meta へ伝播する
  - `normalizeConditions` の失敗・警告契約

## この設計案での判断

- 分割と方式設計は設計フェーズでは並行に詰めてよい
- ただし実装順は、方式仕様の固定を先にする
- 理由は、mode / warning / result 契約が決まらないと、matcher と evaluator の責務境界が確定しないため

## 次アクション候補

1. この設計案をベースに second opinion を取る
2. 方式仕様の最終決定をする
3. Phase ごとの細切れタスクを作る
4. 承認後に実装へ入る
