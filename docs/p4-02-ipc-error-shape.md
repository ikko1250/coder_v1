# P4-02: IPC エラー型を `code + message + job_id?` に統一

設計書 §9.1 P4-02 に基づき、IPC のエラー契約を `IpcErrorDto` として固定し、
ドキュメントに例 JSON を追加した。

## 実装

- `src/ipc_dto.rs`
  - `IpcErrorDto { code, message, job_id? }` を追加。
  - `IpcEvent::Error` は `#[serde(flatten)] error: IpcErrorDto` を保持。
    - JSON 形は従来と同じく `type/error + code + message + jobId?`。
  - テスト追加:
    - `job_id: None` で `jobId` が出ないこと。
    - `job_id: Some` で `jobId` が出ること。

## 例 JSON

### ジョブ非依存エラー（`jobId` なし）

```json
{
  "apiVersion": "2026-03-23",
  "payload": {
    "type": "error",
    "code": "csv_not_found",
    "message": "CSV が見つかりません"
  }
}
```

### ジョブ起因エラー（`jobId` あり）

```json
{
  "apiVersion": "2026-03-23",
  "payload": {
    "type": "error",
    "code": "analysis_failed",
    "message": "analysis subprocess exited with code 1",
    "jobId": "job-999"
  }
}
```

## 検証

- `cargo test`（43 tests OK）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P4-02 初版 |
