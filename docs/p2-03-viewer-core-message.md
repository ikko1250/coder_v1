# P2-03: `ViewerCoreMessage` 列挙型と主要パスの集約

設計書 §9.1 P2-03 の完了条件に基づき、一覧・フィルタ・選択まわりの更新を **`ViewerCoreMessage`**（`src/viewer_core.rs`）で表現し、**`App::apply_core_message`**（`src/app.rs`）が **列挙子経由**で処理するようにした。

## 列挙子と対応する入口（例）

| `ViewerCoreMessage` | 主な呼び出し元 |
|---------------------|----------------|
| `ReplaceRecords { .. }` | `load_csv`（成功時）、分析ジョブ完了 `handle_analysis_success` |
| `SelectionMoveUp` / `SelectionMoveDown` | `app_lifecycle`（キーボード） |
| `SelectionSetFilteredRow { .. }` | `eframe::App::update`（一覧行クリック） |
| `FilterToggle { .. }` | `app_main_layout`（フィルタパネル） |
| `FilterClearColumn` / `FilterClearAll` | 同上 |

## `apply_core_message` の戻り値

`bool` は **再描画を促す状態変化があったか**の目安（`request_repaint` 連携用）。P2-04 で `CoreOutput` に寄せる前提の足場。

## 非目的（P2-03 ではやらないこと）

- `apply_event(&mut CoreState, …) -> CoreOutput` への完全移行（P2-04）
- ジョブ ID 検証・`can_close`（P2-05〜）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-03 初版 |
