# analysis_core 分割後の後続整理計画

- 日付: 2026-03-13
- 対象: `analysis_backend`, `analysis_backend/cli.py`, `src/analysis_runner.rs`
- 目的: `analysis_core.py` の分割と `distance matching` 方式明確化完了後に、次段改善へ戻りやすくする

## 既存挙動

- `analysis_core.py` の主要責務は分割済みで、互換 façade として残している
- `distance matching` は `strict / auto-approx / approx` の mode を持ち、CLI meta JSON へ warning を出せる
- 契約テストでは mode 差分、façade 互換、warning 伝播、strict safety limit failure を保護している
- ただし、次段改善の入口はまだ複数残っている

## 優先順位

### P0. Rust `analysis_runner` との meta 契約整合

- 現状:
  - Python CLI の `warningMessages` は dict 配列
  - Rust 側 `AnalysisMeta.warning_messages` は `Vec<String>`
- 問題:
  - warning が 1 件以上ある成功系で `meta.json` deserialize が壊れる可能性が高い
  - Rust 側は parse failure を `meta.json が生成されませんでした` と誤認する
- 対応案:
  - Rust 側を `Vec<WarningMessage>` へ更新する
  - 少なくとも `code`, `message`, `conditionId`, `usedMode` を読めるようにする
  - parse 失敗時は `meta.json の解析に失敗しました` と明示する
- 完了条件:
  - warning あり成功系でも Rust UI が正常に job 完了扱いできる

### P1. schema と列定義の集約

- 現状:
  - render/export 周辺で schema, cast, select 対象列が分散している
- 問題:
  - 列追加時に修正漏れが起きやすい
  - CSV 契約を壊す変更が混入しやすい
- 対応案:
  - render/export 用の列定義を専用 module 定数へ集約する
  - empty DataFrame schema と export 列順を同じ定義源に寄せる
- 完了条件:
  - 列順・型の変更点が 1 か所で追える

### P1. 条件正規化の hard fail / warning / auto-fix 再整理

- 現状:
  - 条件正規化は無効条件の破棄、未知値の丸め込み、重複 ID の補正を行う
- 問題:
  - 誤設定が静かに隠れる
  - どこまでが仕様でどこからが救済処理か曖昧
- 対応案:
  - hard fail 条件を定義する
  - warning に落とす条件を定義する
  - 自動補正の対象を最小化する
- 完了条件:
  - config 誤りが利用者から見える

### P2. Polars と逐次処理の境界整理

- 現状:
  - `partition_by`, `iter_rows(named=True)`, `dict` 化が複数 module に散っている
- 問題:
  - 認知負荷が高い
  - 性能と可読性の責任境界が曖昧
- 対応案:
  - DataFrame 主体で処理する段と逐次アルゴリズム段を明示する
  - helper 名で「表操作」か「group 単位処理」か分かるようにする
- 完了条件:
  - matcher / renderer / export の処理境界が説明しやすい

### P2. façade の縮退計画

- 現状:
  - `analysis_core.py` は compatibility façade として旧 API を維持している
- 問題:
  - 互換レイヤーが長期化すると、内部 API と外部 API の二重保守になる
- 対応案:
  - 新規利用は module 直 import を推奨する
  - 旧 API の利用箇所を列挙し、移行順を決める
  - 移行完了後に façade の責務を絞る
- 完了条件:
  - façade の存在理由が限定される

## 推奨実行順

1. Rust `analysis_runner` の meta 契約を直す
2. schema / 列定義を集約する
3. 条件正規化の fail/warning 方針を再定義する
4. Polars と逐次処理の境界を整理する
5. façade の縮退計画を実行する

## 再開時の入口

- 互換性起点で再開する場合:
  - `document/logs/2026-03-13-analysis-runner-compatibility-check.md`
- analysis_backend 内部整理から再開する場合:
  - `document/logs/2026-03-13-analysis-core-followup-tasks.md`
- 分割フェーズの経緯確認が必要な場合:
  - `document/logs/2026-03-13-analysis-core-split-task-breakdown.md`
