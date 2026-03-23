# P5-02: 最小フロント + invoke で P4 DTO 読み取り

設計書 §9.1 P5-02 に基づき、`src-tauri` クレートに **ボタン1つ＋表示欄**の最小フロントを実装した。

## 実装概要

- `src-tauri/src/main.rs`
  - eframe の最小ウィンドウを起動。
  - ボタン `invoke: getIpcDtoSnapshot` を配置。
  - クリック時に invoke 相当として `ipc_dto::run_ipc_dto_self_check()` を呼び、結果文字列を表示。
- `src-tauri/Cargo.toml`
  - `eframe`, `egui`, `serde`, `serde_json` を追加。
- `#[path = "../../src/ipc_dto.rs"] mod ipc_dto;` により P4 DTO 実装を直接参照し、
  同一 DTO から読み取りを行う。

## 手動確認（完了条件）

```powershell
cargo run -p csv_viewer_tauri_host
```

1. ウィンドウが起動する。
2. ボタンを押すと表示欄に `IPC DTO self-check passed.` と JSON が表示される。

## 検証

- `cargo build --workspace`
- `cargo test`

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P5-02 初版 |
