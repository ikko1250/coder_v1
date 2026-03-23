# P3-05: ログ出力インタフェース導入（§9.1 P3）

設計書 §9.1 P3-05 に基づき、ログ出力を `AppLogger` で抽象化した。

## 変更内容

| 項目 | 内容 |
|------|------|
| 新規 | `src/app_logger.rs`（`AppLogger` / `StderrAppLogger`） |
| `App` | `logger: Box<dyn AppLogger>` を保持（既定は `StderrAppLogger`） |
| 適用 | `app_analysis_job` のジョブ開始・成功・失敗・古いイベント破棄・チャネル切断で logger を利用 |

## 方針

- 出力先は `AppLogger` 実装に隠蔽し、呼び出し側はログ基盤（stderr / tracing 等）を意識しない。
- 今回は依存追加なしで `StderrAppLogger` を既定実装にした。

## 手動確認（完了条件）

1. 分析実行開始時に `[INFO] analysis job started: ...` が出る。
2. 失敗時に `[ERROR] ...` が出る。
3. 古いジョブ結果破棄時に `[WARN] ...` が出る。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P3-05 初版 |
