# P2-01: `viewer_core` モジュールの新設

設計書 §9.1 P2-01 の完了条件に基づき、**egui / eframe を含まない**コア用モジュールとして `src/viewer_core.rs` を追加した。

## 内容

| 項目 | 説明 |
|------|------|
| モジュール | `viewer_core`（`main.rs` で `mod viewer_core;`） |
| 状態型 | P2-01 時点では `ViewerCoreState` はフィールドなしのプレースホルダ（`Default`）。**P2-02 で**レコード・フィルタ・選択のフィールドを [`docs/p2-02-viewer-core-domain-state.md`](p2-02-viewer-core-domain-state.md) に従い集約した。 |
| テスト | `viewer_core_state_defaults`（`Default` と `clamp_selected_row` の簡易チェック） |

## 非目的（P2-01 当時のスコープ外）

- `App` への組み込み（→ P2-02）
- レコード・フィルタの実データ移行（→ P2-02）

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-01 初版 |
