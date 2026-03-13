# 2026-03-13 条件正規化 / Result 型移行 設計ログ

- 対象: `document/analysis-backend-normalization-and-result-plan.md`, `document/analysis-core-post-split-cleanup-plan.md`
- 目的: セカンドオピニオンを踏まえて、条件正規化ポリシーと Result 型移行方針を具体化する

## 既存挙動

- 条件正規化は silent skip と default への丸め込みが混在している
- `filter_config.py`, `data_access.py`, `cli.py` は例外中心
- Task 12 の cleanup plan には優先順位があるが、依存関係と完了条件の一部が曖昧だった

## 反映した点

1. 条件正規化の現行 auto-fix / warning 候補 / hard fail 候補を一覧化
2. `ConfigIssue`, `NormalizeConditionsResult`, `LoadFilterConfigResult` のたたき台を追加
3. Result 型移行を Phase 1-5 へ段階化
4. cleanup plan を更新
   - P1 を `P1-A`, `P1-B` に分割
   - schema 変更は Rust CSV 契約と依存があることを明記
   - façade 完了条件を指標ベースに変更
   - P0 対応済みなどの進捗メモを追加

## 備考

- 今回は設計整理のみで、コード本体は変更していない
- 次の実装は `NormalizeConditionsResult` の導入から始めるのが自然
