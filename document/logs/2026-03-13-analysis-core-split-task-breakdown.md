# 2026-03-13 analysis_core.py 分割 + matching 方式明確化 細切れタスク

- 目的: `analysis_backend/analysis_core.py` の分割と `distance matching` 方式明確化を、安全に段階実装できる単位へ落とす
- 前提: セカンドオピニオンで指摘された `strict の安全限界`, `façade の互換性`, `warning の UI/メタ伝播` を反映する
- 日時: 2026-03-13 00:55:41 +0900

## 既存挙動

- `analysis_core.py` は config 読込から export 整形までを一括で担っている。
- distance matching は現在 `max_token_distance` 条件でのみ有効で、候補数が `10000` を超えると silently に greedy fallback する。
- 呼び出し側は fallback の有無や使用 mode を受け取れない。
- CLI の meta JSON も warning を持たない。

## 実装方針

- 先に contract を固定する。
- その contract を壊さないように façade を残しながら内部を分割する。
- 各タスクは単独でレビューしやすく、原則1コミットで閉じる粒度にする。

## タスク一覧

### Task 1. Matching 方式 contract 固定

- 目的:
  - `strict / auto-approx / approx` を正式仕様にする
  - `distance_match_combination_cap`
  - `distance_match_strict_safety_limit`
  - warning / error code を定義する
- 作業:
  - 設定キー名と default を確定
  - `strict` 超過時のエラー契約を決める
  - `auto-approx` fallback warning 契約を決める
  - CLI meta JSON に載せる warning 情報の最小項目を決める
- 完了条件:
  - 設定仕様が文書化されている
  - warning / error / usedMode の意味が曖昧でない
- 依存:
  - なし
- コミット案:
  - `docs: define distance matching modes and safety limits`

### Task 2. 既存契約の characterization test 追加

- 目的:
  - 分割前に、現行の壊してはいけない挙動を固定する
- 作業:
  - `render_tagged_token` の escape 契約を維持するテスト追加
  - export DataFrame の列順・型を固定するテスト追加
  - façade 公開関数の戻り値形状を固定するテスト追加
  - 既存 fallback 分岐の存在を明示するテストを補強
- 完了条件:
  - 分割前後で比較可能な保護テストが揃う
- 依存:
  - Task 1 の名称方針があると望ましいが、先行着手は可能
- コミット案:
  - `test: add characterization coverage for analysis core contracts`

### Task 3. 型と result モデル導入

- 目的:
  - `dict[str, object]` と tuple return の一部を置換する基盤を作る
- 作業:
  - `condition_model.py` を追加
  - `DistanceMatchingMode`
  - `MatchingWarning`
  - `ConditionHitResult`
  - `TargetSelectionResult`
  - `NormalizedCondition`
  - `FilterConfig` を移設または再 export
- 完了条件:
  - 型定義が1か所に集約されている
  - 既存コードから import できる
- 依存:
  - Task 1
- コミット案:
  - `refactor: introduce analysis condition and result models`

### Task 4. Filter config 読込の分離

- 目的:
  - config 読込責務を独立させ、matching mode / safety limit を正式に受け取れるようにする
- 作業:
  - `filter_config.py` を追加
  - `loadFilterConfig(...)` を移設
  - 新設定項目:
    - `distance_matching_mode`
    - `distance_match_combination_cap`
    - `distance_match_strict_safety_limit`
  - 既定値と validate を実装
  - `analysis_core.py` から再 export
- 完了条件:
  - config から mode と guardrail を読める
  - 旧 import 先が壊れない
- 依存:
  - Task 1
- コミット案:
  - `refactor: extract filter config loading and matching settings`

### Task 5. Data access / token position 分離

- 目的:
  - I/O と位置計算を matcher/evaluator から切り離す
- 作業:
  - `data_access.py` へ DB 読込関数を移す
  - `token_position.py` へ位置付け関数を移す
  - `analysis_core.py` では再 export または薄い委譲にする
- 完了条件:
  - SQLite 読込と token position が独立モジュールになっている
  - 既存テストが通る
- 依存:
  - Task 2
- コミット案:
  - `refactor: extract data access and token positioning modules`

### Task 6. Distance matcher 分離 + safety limit 実装

- 目的:
  - 最重要ロジックを仕様化されたモードで切り出す
- 作業:
  - `distance_matcher.py` を追加
  - strict / auto-approx / approx を実装
  - `strict` の safety limit 超過で明示的エラー
  - `auto-approx` fallback 時に warning 生成
  - combination count / cap / usedMode を result に含める
- 完了条件:
  - distance matcher 単体で mode と warning が完結する
  - strict の OOM 回避策が入っている
- 依存:
  - Task 1
  - Task 3
  - Task 4
- コミット案:
  - `refactor: extract distance matcher with explicit modes and guards`

### Task 7. Condition evaluator 分離

- 目的:
  - 条件正規化、条件評価、target selection を matcher から切り離す
- 作業:
  - `condition_evaluator.py` を追加
  - `_clean_cooccurrence_conditions` を `normalizeConditions(...)` へ再編
  - `select_target_ids_by_cooccurrence_conditions(...)` の内部を result 型ベースへ置換
  - warning の集約ポイントを evaluator 側に定義
- 完了条件:
  - target selection と hit extraction orchestration の責務が分離される
- 依存:
  - Task 3
  - Task 5
  - Task 6
- コミット案:
  - `refactor: extract condition evaluator and target selection flow`

### Task 8. Façade 互換レイヤー導入

- 目的:
  - 新内部構造へ切り替えつつ、既存呼び出し元を壊さない
- 作業:
  - `analysis_core.py` を compatibility façade 化
  - 旧公開関数名を維持
  - result dataclass を旧 tuple / DataFrame return へ一時的に変換
  - 互換変換の責務を明示するコメントを追加
- 完了条件:
  - CLI / 既存テストが façade 経由で動く
  - 内部変更が外部 API に漏れない
- 依存:
  - Task 3
  - Task 5
  - Task 6
  - Task 7
- コミット案:
  - `refactor: turn analysis_core into compatibility facade`

### Task 9. Rendering / export 分離

- 目的:
  - UI と CSV 出力に直結する責務を独立させ、崩れを防ぎやすくする
- 作業:
  - `rendering.py` を追加
  - `export_formatter.py` を追加
  - HTML escape と tag attribute escape の順序を固定
  - export 列順・型を維持
- 完了条件:
  - render / export 処理が独立モジュールになる
  - UI に関わる既存出力契約が維持される
- 依存:
  - Task 2
  - Task 8
- コミット案:
  - `refactor: extract rendering and export formatting modules`

### Task 10. CLI meta / warning 伝播

- 目的:
  - fallback や safety-limit 超過の情報を利用者へ見える形で出す
- 作業:
  - `analysis_backend/cli.py` に warningMessages 出力を追加
  - mode / usedMode / warning count の meta JSON 反映方針を実装
  - strict safety limit 失敗時の errorSummary を調整
- 完了条件:
  - warning が meta JSON まで届く
  - `auto-approx` の fallback が運用上見える
- 依存:
  - Task 6
  - Task 8
- コミット案:
  - `feat: surface matching warnings in cli metadata`

### Task 11. 契約テストと回帰テストの拡張

- 目的:
  - 仕様差と互換性を継続的に守る
- 作業:
  - strict と approx の意味差テスト
  - auto-approx fallback warning テスト
  - strict safety limit エラーテスト
  - façade 後方互換テスト
  - CLI meta warning 伝播テスト
- 完了条件:
  - 方式差、互換性、warning がテストで保護される
- 依存:
  - Task 6
  - Task 8
  - Task 10
- コミット案:
  - `test: cover matching modes compatibility and warning propagation`

### Task 12. フェーズ完了後の整理

- 目的:
  - 今回の主目的達成後に残る次段改善へ戻りやすくする
- 作業:
  - schema 集約
  - 条件正規化の hard fail / warning 再整理
  - Polars と Python ループ境界の整理
  - façade の縮退または新 API への移行計画
- 完了条件:
  - 次の改善入口が明文化される
- 依存:
  - Task 11
- コミット案:
  - `docs: summarize post-split cleanup tasks`

## 推奨実行順

1. Task 1
2. Task 2
3. Task 3
4. Task 4
5. Task 5
6. Task 6
7. Task 7
8. Task 8
9. Task 9
10. Task 10
11. Task 11
12. Task 12

## 先に承認を取るべき論点

1. `strict` に safety limit を入れるか
2. default mode を `auto-approx` にするか
3. warning を meta JSON のみに出すか、将来的に UI 表示対象にするか
4. façade を何フェーズ維持するか

## 実装開始時の最小スコープ案

- 最初の着手セットは `Task 1 + Task 2` が安全。
- ここで contract とテストを固めてから、`Task 3-8` の本体リファクタリングへ進む。
