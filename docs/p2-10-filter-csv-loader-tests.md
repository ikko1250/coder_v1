# P2-10: `filter` / `csv_loader` のユニットテスト（§8 優先）

設計書 §9.1 P2-10・§8（テスト戦略）に基づき、**純粋関数・データ変換**として `filter` と `csv_loader` にユニットテストを追加した。

## 対象と内容

| モジュール | テスト内容 |
|------------|------------|
| `src/filter.rs` | `display_filter_value`、`normalize_filter_candidate_search_text`、`FilterColumn::matches`（空選択・自治体・カテゴリ）、`build_filter_options`（件数・数値ソート） |
| `src/csv_loader.rs` | ファイル不存在、必須列不足（`detect_analysis_unit` 経由のエラー）、UTF-8 BOM 付きヘッダの受理 |

## 検証

```text
cargo test
```

該当テストを含む全テストが成功すること。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-10 初版 |
