# P4-04: Tauri 未導入でも DTO を生成・検証できる CLI

設計書 §9.1 P4-04 に基づき、同じ DTO を **CLI から生成・検証**する経路を追加した。

## 既存挙動

- DTO はユニットテストでのみ round-trip 検証していた。
- 実行可能な CLI での自己検証経路は未提供だった。

## 実装

- `src/ipc_dto.rs`
  - `run_ipc_dto_self_check() -> Result<String, String>` を追加。
    - `ApiEnvelope<IpcCommand>` と `ApiEnvelope<IpcEvent>` を生成。
    - serialize / deserialize / 等価比較 / `apiVersion` 互換判定まで実施。
    - 成功時はサンプル JSON を返す。
  - テスト `ipc_dto_self_check_succeeds` を追加。
- `src/main.rs`
  - `--ipc-dto-self-check` オプションを追加。
    - 成功時: JSON を標準出力へ表示して終了コード 0。
    - 失敗時: エラーを標準エラーへ表示して終了コード 1。

## 実行例

```powershell
cargo run -- --ipc-dto-self-check
```

## 検証

- `cargo test`（46 tests OK）
- `cargo run -- --ipc-dto-self-check`（成功）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P4-04 初版 |
