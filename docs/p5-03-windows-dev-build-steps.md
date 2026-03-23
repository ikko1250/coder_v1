# P5-03: Windows 開発時のビルド・起動手順

設計書 §9.1 P5-03 に基づき、Windows での再現可能な手順を整理する。

## 前提

- OS: Windows 10/11
- Rust ツールチェイン導入済み（`cargo --version` が通ること）
- 作業ディレクトリ: リポジトリルート

## 手順

### 1) workspace 全体をビルド

```powershell
cargo build --workspace
```

- ルート GUI（`csv_highlight_viewer`）
- `src-tauri` パイロット（`csv_viewer_tauri_host`）

の両方がビルドされる。

### 2) 既存回帰テスト

```powershell
cargo test
```

### 3) IPC DTO 自己検証（P4 連携確認）

```powershell
cargo run -- --ipc-dto-self-check
```

成功時に `IPC DTO self-check passed.` と JSON が表示される。

### 4) P5-02 最小フロントを起動

```powershell
cargo run -p csv_viewer_tauri_host
```

- ボタン `invoke: getIpcDtoSnapshot` を押す
- 下部表示欄に DTO JSON が表示されることを確認

## トラブル時の確認

- `cargo clean` 後に `cargo build --workspace` を再実行
- `cargo run -p csv_viewer_tauri_host` 実行時は GUI アプリのため、同一端末で別インスタンスが残っていないか確認

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P5-03 初版 |
