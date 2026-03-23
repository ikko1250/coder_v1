# P3-01: `FileDialogHost` と `rfd` の閉じ込め（§9.1 P3）

設計書 §9.1 P3-01 に基づき、ネイティブファイルダイアログを **`FileDialogHost` トレイト**で抽象化し、**`rfd` の呼び出しは `RfdFileDialogHost` のみ**に集約した。

## 構成

| 項目 | 内容 |
|------|------|
| トレイト | `src/file_dialog_host.rs` の `FileDialogHost`（`PathBuf` の受け渡しのみ） |
| 既定実装 | `RfdFileDialogHost`（`rfd::FileDialog`） |
| `App` | `file_dialog_host: Box<dyn FileDialogHost>`（`App::new` で `RfdFileDialogHost` を格納） |
| 呼び出し元 | `app_toolbar`（CSV 開く・分析結果保存）、`app_analysis_settings`（Python / JSON / annotation CSV） |

`viewer_core` はファイルダイアログを参照しない。

## 手動確認（完了条件）

1. 「CSVを開く」で CSV を選択して読み込めること。
2. 「CSV保存(全件)」で保存ダイアログが従来どおり開くこと。
3. 分析設定の「選択」で Python / 条件 JSON / annotation CSV の各ダイアログが従来どおり動くこと。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P3-01 初版 |
