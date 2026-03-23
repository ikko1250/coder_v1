# P2-05: ジョブ ID と `expected_job_id`（§5.3）

設計書 §5.3・§9.1 P2-05 に基づき、`src/viewer_core.rs` の **`ViewerCoreState`** に **`expected_job_id: Option<String>`** を追加し、分析／エクスポートの **完了イベント**で `job_id` を突き合わせる。

## 挙動

| タイミング | 処理 |
|------------|------|
| 分析／エクスポート開始 | `set_expected_job_id(spawn が返した ID)` |
| CSV 読込成功（`load_csv`） | `clear_expected_job_id`（データソース切替で遅延完了を無効化） |
| 完了（成功／失敗） | `meta.job_id` がある場合は `job_id_matches_expected`。一致しない場合は **UI を更新せず** `status` を `Idle` に戻す。 |
| `meta` なしの失敗 | `expected_job_id` が **無い**ときは破棄（遅延とみなす）。**ある**ときは同一チャネル由来のエラーとみなして従来どおり `handle_analysis_failure`。 |
| チャネル切断 | `clear_expected_job_id` |

## ユニットテスト（`viewer_core`）

- 期待 ID と異なる `job_id` は拒否（`stale_job_id_is_rejected`）
- 一致は受理（`matching_job_id_is_accepted`）
- `clear_expected_job_id` で無効化（`clear_expected_invalidates_job`）

## 非目的（P2-05 ではやらないこと）

- `AnalysisJobFailure` への `job_id` 必須フィールド追加（`meta` が無い経路は上記ヒューリスティック）
- データソース世代（P2-09）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-05 初版 |
