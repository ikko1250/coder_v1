# P3-03: update 内の受信イベント→apply_event パイプライン一本化（§9.1 P3）

設計書 §9.1 P3-03 に基づき、分析ジョブ受信処理を **`update` 側パイプライン**へ寄せた。

## 変更内容

| 項目 | 変更 |
|------|------|
| 受信結果の表現 | `app_analysis_job` に `AnalysisJobPollOutput { core_event, needs_repaint, repaint_after }` を追加 |
| 受信側 | `poll_analysis_job` は `egui::Context` に直接触らず、出力だけ返す |
| 適用側 | `app_lifecycle::run_update_prelude` で `poll_analysis_job` の `core_event` を `app.apply_event(...)` に渡して適用 |
| 再描画 | 即時再描画と遅延再描画（100ms）を `run_update_prelude` で一元処理 |

これにより、**チャネル受信は update 内に留まり、受信イベントのコア適用経路が一本化**された。

## 手動確認（完了条件）

1. 分析実行後、結果が従来どおり一覧へ反映されること。
2. 失敗時にエラー表示・警告表示が従来どおりであること。
3. 分析実行中のステータス表示が更新され続けること（100ms ポーリング）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P3-03 初版 |
