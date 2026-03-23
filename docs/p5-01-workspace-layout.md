# P5-01: workspace 構成の決定（ルート + `src-tauri`）

設計書 §9.1 P5-01 に基づき、同一リポジトリ内で次の構成を採用した。

- 既存 GUI クレート: ルート `csv_highlight_viewer`
- Tauri パイロット用クレート: `src-tauri/csv_viewer_tauri_host`
- Cargo workspace: ルート `Cargo.toml` に `members = [".", "src-tauri"]`

## 追加/変更

- `Cargo.toml`
  - `[workspace]`
  - `members = [".", "src-tauri"]`
  - `resolver = "2"`
- `src-tauri/Cargo.toml`（新規）
- `src-tauri/src/main.rs`（新規、プレースホルダ）

## 検証

```powershell
cargo build --workspace
```

上記が成功し、ルートクレートと `src-tauri` クレートの両方がビルドできること。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P5-01 初版 |
