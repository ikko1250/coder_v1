# P2-09: データソース世代（`data_source_generation`、§11.4）

設計書 §9.1 P2-09・§11.4 に基づき、最小実装として **`ViewerCoreState::data_source_generation: u64`** を追加した。

## 挙動

| 項目 | 内容 |
|------|------|
| 増加タイミング | `replace_records`（CSV 読込・分析ジョブ完了）のたびに `bump_data_source_generation()`（1 ずつ増加） |
| 初期値 | `0` |
| 条件エディタ | 条件 JSON を読み込んだときの世代を `ConditionEditorState::data_source_generation_at_load` に保存。現在の `core.data_source_generation` と異なればヘッダーに **再読込推奨**の警告を表示 |
| DB Viewer | `prepare_db_viewer_state` 時点の世代を `DbViewerState::data_source_generation_when_prepared` に保存。`replace_records` で `reset_loaded_state` により参照状態はクリアされるため、現行フローでは主に **将来の分散 UI** 用のフック |

## 手動確認（完了条件）

1. 条件エディタを開く → 別 CSV を開く（または分析完了でレコード差し替え）→ 条件エディタに **オレンジ系の警告**が出ること。
2. 条件 JSON を再読込すると警告が消える（または世代が一致する）こと。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-09 初版 |
