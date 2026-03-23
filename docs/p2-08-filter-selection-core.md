# P2-08: `filtered_indices` 再計算と選択クランプの集約

設計書 §9.1 P2-08 に基づき、`ViewerCoreState` に次を集約した。

| API | 役割 |
|-----|------|
| `recompute_filtered_indices` | `all_records` と `selected_filter_values` から `filtered_indices` を再計算 |
| `record_matches_filters` | 1 レコードが現在のフィルタに合致するか（従来 `App::record_matches_filters`） |
| `clamp_selected_row_to_filtered_len` | `selected_row` を `filtered_indices.len()` にクランプ |

`App::replace_records`・`App::apply_filters` は上記を呼ぶ。一覧の **選択変更**（`apply_selection_change`）は従来どおり **`clamp_selected_row`**（純関数）で希望インデックスをクランプする。

## テスト

- フィルタなしで全行が `filtered_indices` に入ること
- `clamp_selected_row_to_filtered_len` で `selected_row` が `len` 未満または `None` になること
- 再計算後にクランプすると範囲内に収まること

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-08 初版 |
