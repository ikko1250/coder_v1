# P2-07: 詳細ペインのセグメントキャッシュ無効化（コア明示）

設計書 §9.1 P2-07・P1-09 に基づき、本文ハイライト用の **`detail_segment_cache`**（旧 `App::cached_segments`）を **`ViewerCoreState`** に移し、無効化は **`invalidate_detail_segment_cache(SegmentCacheInvalidateReason)`** で **経路ごと**に明示する。

## 無効化理由（列挙子）

| `SegmentCacheInvalidateReason` | トリガー（アプリ側） |
|--------------------------------|----------------------|
| `ReplaceRecords` | `replace_records`（CSV・分析ジョブ完了） |
| `SelectionChanged` | 一覧の選択が変わったとき（`apply_selection_change`） |
| `FilterApplied` | `apply_filters` |
| `AnnotationSaved` | `apply_saved_annotation_to_selected_record` |

列挙子は実行時には **`detail_segment_cache = None`** のみに使い、デバッグ・ドキュメント上の区別用である。

## 更新

`App::get_segments` は `parse_tagged_text` 後に **`ViewerCoreState::set_detail_segment_cache`** で格納する（従来どおり）。

## テスト

`viewer_core` で、無効化後にキャッシュが空になり、別 `row_no` 用のキャッシュを再設定できることを検証する（**別行のセグメントが残らない**）。

## 非目的（P2-07 ではやらないこと）

- `parse_tagged_text` 自体のコア移管（ホストが呼ぶまま）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-07 初版 |
