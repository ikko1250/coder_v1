# P2-01: `viewer_core` モジュールの新設

設計書 §9.1 P2-01 の完了条件に基づき、**egui / eframe を含まない**コア用モジュールとして `src/viewer_core.rs` を追加した。

## 内容

| 項目 | 説明 |
|------|------|
| モジュール | `viewer_core`（`main.rs` で `mod viewer_core;`） |
| 状態型 | `ViewerCoreState` — P2-01 時点ではフィールドなしのプレースホルダ（`Default`） |
| `dead_code` | P2-02 で `App` が保持するまで、`ViewerCoreState` に `#[allow(dead_code)]`（非 test ビルドでは未構築のため） |
| テスト | `viewer_core_state_defaults`（`Default` が組み立て可能であることのみ） |

## 非目的（P2-01 ではやらないこと）

- `App` への組み込み
- レコード・フィルタの実データ移行（P2-02 以降）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-01 初版 |
