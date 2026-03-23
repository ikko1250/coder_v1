# P2-03: `ViewerCoreMessage` 列挙型と主要パスの集約

設計書 §9.1 P2-03 の完了条件に基づき、一覧・フィルタ・選択まわりの更新を **`ViewerCoreMessage`**（`src/viewer_core.rs`）で表現し、**`App::apply_event`**（`src/app.rs`）が **列挙子経由**で処理するようにした（P2-04 で戻り値を [`CoreOutput`](p2-04-core-output.md) に変更）。

## 列挙子と対応する入口（例）

| `ViewerCoreMessage` | 主な呼び出し元 |
|---------------------|----------------|
| `ReplaceRecords { .. }` | `load_csv`（成功時）、分析ジョブ完了 `handle_analysis_success` |
| `SelectionMoveUp` / `SelectionMoveDown` | `app_lifecycle`（キーボード） |
| `SelectionSetFilteredRow { .. }` | `eframe::App::update`（一覧行クリック） |
| `FilterToggle { .. }` | `app_main_layout`（フィルタパネル） |
| `FilterClearColumn` / `FilterClearAll` | 同上 |

## 戻り値（P2-04 以降）

[`CoreOutput`](p2-04-core-output.md) の `needs_repaint` を参照。

## 非目的（P2-03 ではやらないこと）

- `ViewerCoreState` だけを取る `apply_event`（P2-04 は `App::apply_event` + `CoreOutput` まで）
- ジョブ ID 検証・`can_close`（P2-05〜）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-03 初版 |
| 2026-03-23 | P2-04 対応: メソッド名・戻り値は `docs/p2-04-core-output.md` を参照 |
