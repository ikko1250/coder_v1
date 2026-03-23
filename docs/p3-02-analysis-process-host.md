# P3-02: `AnalysisProcessHost` と分析ジョブ起動の集約（§9.1 P3）

設計書 §9.1 P3-02 に基づき、**`analysis_runner::spawn_analysis_job` / `spawn_export_job`** の呼び出しを **`AnalysisProcessHost` トレイト**に集約した。

## 構成

| 項目 | 内容 |
|------|------|
| トレイト | `src/analysis_process_host.rs` の `AnalysisProcessHost`（起動依頼 → `job_id` + `Receiver<AnalysisJobEvent>`） |
| 既定実装 | `ThreadAnalysisProcessHost`（既存の `spawn_*` に委譲） |
| `App` | `analysis_process_host: Box<dyn AnalysisProcessHost>`（`App::new` で `ThreadAnalysisProcessHost` を格納） |
| 利用箇所 | `app_analysis_job::start_analysis_job` / `start_export_job` のみ |

イベント受信（`poll_analysis_job` の `try_recv`）とコアの `expected_job_id` 照合は **従来どおり UI 層**（`app_analysis_job`）に残す。`viewer_core` は子プロセスを起動しない。

## 手動確認（完了条件）

1. 「分析実行」でジョブが従来どおり完了し、結果が一覧に反映されること。
2. 「CSV保存(全件)」でエクスポートが従来どおり完了すること。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P3-02 初版 |
