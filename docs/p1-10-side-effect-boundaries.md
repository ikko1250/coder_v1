# P1-10: 副作用の境界（コア候補 / ホスト必須）

設計書 §9.1 P1-10 の完了条件に基づき、主要な **I/O・プロセス・ダイアログ・フレームワーク API** を列挙し、将来の **ドメインコア分離（P2）** を見据えたラベルを付ける。

**用語**

- **コア候補**: 入力を「パスやバイト列などの値」に限定すれば、**egui / OS ダイアログなし**で単体テストしやすいロジック、またはそのように切り出せる部分。
- **ホスト必須**: **ネイティブ UI**（ファイルダイアログ、ウィンドウ命令）、**子プロセス**、**実ファイル／SQLite 接続**など、現行アーキテクチャではアダプタ層に残す前提の副作用。

※ 境界は重なりうる（例: JSON デシリアライズはコア候補、**同一ファイルへの atomic 書き込み**はホスト必須）。

---

## 1. 総覧表

| 区分 | 代表エントリ | 主モジュール | ラベル |
|------|----------------|--------------|--------|
| CSV 読込 | `load_records` → `load_csv` | `csv_loader`, `app` | ホスト必須（実ファイル）／パース部は **コア候補** |
| 条件 JSON 読み書き | `load_condition_document` / `save_condition_document_atomic` | `condition_editor`, `app_condition_editor` | ホスト必須（`fs`）／**スキーマ・検証**は **コア候補** |
| 手動アノテーション追記 | `append_manual_annotation_row` | `manual_annotation_store`, `app` | ホスト必須 |
| 分析・エクスポート | `spawn_analysis_job` / `spawn_export_job` | `analysis_runner`, `app_analysis_job` | ホスト必須 |
| ジョブ作業ディレクトリ | `cleanup_job_directories` | `analysis_runner`, `app_analysis_job` | ホスト必須 |
| SQLite 参照 | `fetch_paragraph_context` 等 | `db`, `app_db_viewer` | ホスト必須 |
| ファイルダイアログ | `rfd::FileDialog` | `app_toolbar`, `app_analysis_settings` | ホスト必須 |
| ウィンドウ／ビューポート | `ViewportCommand` 等 | `app_condition_editor`, `app_analysis_job`, … | ホスト必須 |
| ランタイム設定解決 | `build_runtime_config`, `resolve_filter_config_path` 等 | `analysis_runner` | **設定値の組み立て**はコア候補／**実在パス検証**はホスト寄り |
| フォント | `configure_japanese_font` | `font`, `main` | ホスト必須 |
| フィルタ・レコード整合 | `record_matches_filters`, `apply_filters` の論理 | `app`, `filter` | **コア候補**（純粋関数化しやすい） |
| タグ付きテキスト | `parse_tagged_text` | `tagged_text` | **コア候補** |

---

## 2. カテゴリ別

### 2.1 ファイル読み書き（`std::fs` 等）

| 処理 | 場所 | ラベル | メモ |
|------|------|--------|------|
| CSV 読込 | `csv_loader::load_records` | ホスト必須 | ディスク読み。行→`AnalysisRecord` の変換ロジックはコア候補。 |
| 条件 JSON 読込 | `condition_editor::load_condition_document` | ホスト必須 | `read_to_string`。 |
| 条件 JSON 保存（atomic） | `condition_editor::save_condition_document_atomic` | ホスト必須 | 一時ファイル + rename。 |
| annotation CSV 追記 | `manual_annotation_store::append_manual_annotation_row` | ホスト必須 | `OpenOptions` 追記・ヘッダ作成。 |
| ジョブディレクトリ掃除 | `analysis_runner::cleanup_job_directories` | ホスト必須 | 期限付きディレクトリ削除。 |
| メタ JSON 等（デバッグ／補助） | `analysis_runner` 内 `read_to_string` 等 | ホスト必須 | 成果物パス前提。 |

### 2.2 ネイティブ UI（`rfd`）

| 処理 | 場所 | ラベル |
|------|------|--------|
| CSV オープン | `app_toolbar` | ホスト必須 |
| エクスポート先 CSV | `app_toolbar` | ホスト必須 |
| Python / 条件 JSON / annotation パス上書き | `app_analysis_settings` | ホスト必須 |

### 2.3 子プロセス・IPC（Python ワーカー）

| 処理 | 場所 | ラベル |
|------|------|--------|
| ワーカ起動・stdin/stdout フレーム | `analysis_runner::spawn_worker`, `write_framed_json` | ホスト必須 |
| 分析ジョブ要求 | `spawn_analysis_job` | ホスト必須 |
| エクスポートジョブ | `spawn_export_job` | ホスト必須 |
| グローバルワーカスロット | `WORKER_SLOT`, `invalidate_worker_slot` | ホスト必須 |

### 2.4 SQLite

| 処理 | 場所 | ラベル |
|------|------|--------|
| 読み取り専用接続・クエリ | `db::fetch_paragraph_context` 等 | ホスト必須 |

### 2.5 egui / ウィンドウ

| 処理 | 場所 | ラベル |
|------|------|--------|
| ビューポートフォーカス／閉じる／閉じる取消 | `app_condition_editor`, `app_analysis_job` | ホスト必須 |
| 再描画要求 | `poll_analysis_job` 等 | ホスト必須 |

### 2.6 起動・表示環境

| 処理 | 場所 | ラベル |
|------|------|--------|
| CLI 引数から初期 CSV | `main` | ホスト必須 |
| 日本語フォントファイル読み込み | `font` | ホスト必須 |

---

## 3. コア候補としてまとめやすいロジック（副作用なし）

- **`filter`**: 列定義と `record_matches` の判定。
- **`tagged_text::parse_tagged_text`**: 文字列→セグメント（正規表現は `OnceLock` で静的）。
- **`condition_editor`**: `FilterConfigDocument` の構造体と、バイト列／文字列が与えられたあとの変換・整合性チェック（ファイル I/O を除く）。
- **`model::AnalysisRecord` 上の派生フィールド更新**: 注釈追記後の文字列組み立て（`append_manual_annotation_pairs_text` 等）は純粋関数として切り出し可能。

---

## 4. 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P1-10 初版 |
