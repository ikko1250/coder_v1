# P4-03: `api_version` 運用方針（将来互換ルール）

設計書 §9.1 P4-03 に基づき、`api_version` の運用方針をコードと文書で固定した。

## 方針

- envelope には **常に `apiVersion` を含める**。
- 現行値は `IPC_API_VERSION = "2026-03-23"`。
- 受信側の互換判定は **現行版との完全一致**を初期ルールとする。
  - 一致: 受理
  - 不一致: 非互換として扱う（エラー応答方針は P4-04/P4-05 で具体化）

## 実装

- `src/ipc_dto.rs`
  - `pub const IPC_API_VERSION: &str`
  - `ApiEnvelope::new(payload)`（現行 `apiVersion` を自動付与）
  - `ApiEnvelope::is_supported_api_version()`（一致判定）

## テスト

- `api_envelope_new_sets_current_api_version`
- `api_version_support_check_works`

`cargo test` で通過。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P4-03 初版 |
