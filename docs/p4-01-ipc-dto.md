# P4-01: IPC DTO（Command/Event）を serde で定義（§6）

設計書 §6 / §9.1 P4-01 に基づき、Tauri 移行のための最小 DTO を Rust 型として固定した。

## 追加型

| 区分 | 型 |
|------|----|
| 共通 | `ApiEnvelope<T> { api_version, payload }` |
| Command | `IpcCommand`（`loadCsv`, `setFilter`, `selectRow`, `runAnalysis`, `openDbViewer`） |
| Event | `IpcEvent`（`analysisProgress`, `analysisFinished`, `error`） |
| 補助 | `AnalysisOverridesDto`, `AnalysisOutcomeDto` |

`#[serde(tag = "type", rename_all = "camelCase")]` を採用し、JSON 契約を明示した。

## テスト

- `ipc_command_round_trip_json`
- `ipc_event_round_trip_json`

`cargo test` で round-trip が通ることを確認。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P4-01 初版 |
