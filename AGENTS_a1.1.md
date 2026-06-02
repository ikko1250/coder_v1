# AGENTS.md — 条例分析ビューア

AI コーディングエージェント向けプロジェクトガイド。人間向けの概要は `README.md` を参照。

---

## プロジェクト概要

条例・規則テキストの分析結果を確認・編集するデスクトップアプリケーションです。

- **Rust GUI 本体**: `eframe` / `egui` で構築した CSV 閲覧・フィルタ・分析実行 UI
- **Python 分析バックエンド**: `analysis_backend` パッケージが Polars + SQLite でトークン分析を実行
- **PDF 変換・OCR モジュール**: `pdf_converter` パッケージが Gemini API 経由で PDF → Markdown 変換を行う
- **Tauri パイロット**: `src-tauri` に最小フロントを置き、将来の WebView 移行を見据えた IPC DTO の検証用

### 技術スタック

| 層 | 言語 / フレームワーク | 主要クレート・パッケージ |
|---|---|---|
| GUI | Rust 2021 | `eframe 0.31`, `egui 0.31`, `csv`, `regex`, `serde`, `rusqlite`, `rfd` |
| 分析バックエンド | Python 3.12+ | `polars`, `google-genai`, `httpx`, `requests`, `sudachipy` (オプション) |
| ビルド | Cargo workspace + uv | `cargo`, `uv` |

---

## ディレクトリ構成

```
├── src/                    # Rust GUI 本体（ルートクレート csv_highlight_viewer）
│   ├── main.rs             # エントリポイント（eframe 起動 / IPC DTO 自己検証 CLI）
│   ├── app.rs              # メイン App（egui 非依存の viewer_core へ委譲）
│   ├── viewer_core.rs      # ドメインコア（フィルタ・選択・レコード管理、egui 非依存）
│   ├── model.rs            # AnalysisRecord, AnalysisUnit など共通データ型
│   ├── ipc_dto.rs          # Tauri/IPC 向け serde DTO（P4 設計）
│   ├── analysis_runner.rs  # Python 子プロセス起動・ジョブ管理
│   ├── analysis_process_host.rs
│   ├── csv_loader.rs       # CSV 読込（段落/文両対応）
│   ├── filter.rs           # フィルタロジック
│   ├── db.rs               # SQLite 読込（DB Viewer 用）
│   ├── condition_editor.rs # 条件 JSON の読み書き・編集モデル
│   ├── tagged_text.rs      # タグ付きテキストのパース・表示
│   └── app_*.rs            # UI サブモジュール（ツールバー、設定、ジョブ、エラーダイアログなど）
├── src-tauri/              # Tauri パイロットクレート（csv_viewer_tauri_host）
│   └── src/main.rs         # IPC DTO 自己検証を表示する最小フロント
├── analysis_backend/       # Python 分析パイプライン（パッケージ名 = analysis_backend）
│   ├── __init__.py         # lazy import による公開 API 集約
│   ├── analysis_core.py    # フレーム構築・結果組み立てのファサード
│   ├── cli.py              # run-analysis.py から呼ばれる CLI エントリ
│   ├── worker.py           # Rust から起動されるワーカープロセス（stdin/stdout プロトコル）
│   ├── condition_model.py  # dataclass 定義（FilterConfig, DataAccessResult など）
│   ├── condition_evaluator.py
│   ├── data_access.py      # SQLite → Polars 読込
│   ├── distance_matcher.py
│   ├── export_formatter.py
│   ├── filter_config.py    # 条件 JSON の読み込み・検証
│   ├── frame_schema.py     # Polars schema 定義
│   ├── rendering.py        # タグ付きテキストのレンダリング
│   ├── text_unit_frames.py
│   └── token_position.py
├── pdf_converter/          # PDF → Markdown / OCR モジュール（import 名 = pdf_converter）
│   ├── call_gemma4_gemini.py   # Gemini API CLI（uv run call-gemma4-gemini で起動）
│   ├── call-gemma4-gemini.py   # 旧来互換 shim
│   ├── project_paths.py
│   └── tool_call_logger.py
├── tests/                  # Python テスト（pytest、178 テスト程度）
├── docs/                   # 設計メモ（P1〜P5 番号付き）
├── document/               # 要件・タスク分解・レビュー記録
├── asset/                  # 既定の DB、条件 JSON、annotation CSV（開発用サンプル資産）
├── Cargo.toml              # ルートクレート + workspace 定義
├── pyproject.toml          # Python プロジェクト（uv 管理）
└── run-analysis.py         # Python 分析エントリ（cli / worker 分岐）
```

### Cargo workspace

`Cargo.toml` に `members = [".", "src-tauri"]` とあり、ルートクレートと Tauri パイロットの両方を一括ビルドします。

---

## セットアップ

### 前提

- OS: Windows 10/11（開発手順は Windows を強く意識）
- Rust ツールチェイン（`cargo --version` が通ること）
- Python 3.12 以上
- `uv`（Python 依存管理・仮想環境）

### Rust

```powershell
cargo build --workspace
```

### Python

```powershell
uv sync
```

Sudachi によるトークナイズも必要な場合:

```powershell
uv sync --extra analysis-db
```

---

## ビルドと実行

### GUI 起動

```powershell
# CSV を開かずに起動
cargo run

# CSV パスを指定して起動
cargo run -- path/to/input.csv
```

### ワークスペース全体ビルド

```powershell
cargo build --workspace
```

### Tauri パイロット起動

```powershell
cargo run -p csv_viewer_tauri_host
```

### IPC DTO 自己検証

```powershell
cargo run -- --ipc-dto-self-check
```

成功時に `IPC DTO self-check passed.` と JSON が表示されます。

### analysis DB 生成（Python）

```powershell
python3 docs/build_ordinance_analysis_db.py `
  --input-dir path/to/texts `
  --analysis-db data/ordinance_analysis.db `
  --skip-tokenize
```

入力ファイル名は `<category1>_<category2>.txt` または `.md` の形式が必須です。規約違反があると処理を中止し、`<analysis-db>.report.json` にレポートを出力します。

---

## テスト

### Rust テスト

```powershell
cargo test
```

- ルートクレート: 65 テスト程度
- `src-tauri`: 7 テスト程度

### Python テスト

```powershell
python -m pytest tests/
```

収集されているテスト数は約 178 です。一部のテストでは一時ファイルの削除で Windows 上で `PermissionError` が出ることがあります（SQLite 接続が開いたままの場合）。

---

## Python 実行環境の解決順

Rust 側が Python 子プロセスを起動する際、次の順で実行環境を解決します:

1. 分析設定で明示指定した Python
2. 環境変数 `CSV_VIEWER_PYTHON`
3. プロジェクト直下の `.venv`
4. `uv run python`
5. `python3` / `python`

### プロジェクトルートの解決

Python 側の helper は以下の順でルートを決定します:

1. 環境変数 `CSV_VIEWER_PROJECT_ROOT`
2. `pyproject.toml` の上位探索
3. `__file__` fallback

editable install 前提で安定します。non-editable install では source tree を辿れないため、`CSV_VIEWER_PROJECT_ROOT` を設定するか source tree から実行してください。

---

## コードスタイルと規約

### Rust

- **エディション**: 2021
- **コメント**: モジュール doc comment（`//!`）で設計番号（P1〜P5）を参照する習慣があります。例: `//! P4-01: Tauri/IPC 向け DTO`
- **可視性**: `pub(crate)` を基本とし、公開 API は慎重に限定しています
- **UI とドメイン分離**: `viewer_core.rs` には `egui` 非依存の状態・ロジックのみを置き、`app.rs` が UI アダプタとして橋渡しします
- **IPC DTO**: `ipc_dto.rs` に `ApiEnvelope`, `IpcCommand`, `IpcEvent` などを定義。`IPC_API_VERSION` を更新して互換性を管理します

### Python

- `from __future__ import annotations` を各モジュール先頭に置く習慣があります
- `analysis_backend/__init__.py` では `__getattr__` による lazy import で公開 API を集約しています
- dataclass（`frozen=True`）を多用して結果型を定義します
- Polars DataFrame を中心としたパイプライン処理を行います

---

## 設計文書の体系

`docs/` には `p{フェーズ}-{連番}-{テーマ}.md` という命名規則の設計メモが置かれています。

- **P1**: アプリ実装・レコード選択・キャッシュ・副作用境界
- **P2**: viewer_core（ドメインコア）
- **P3**: ホスト抽象（ファイルダイアログ、分析プロセス、イベントパイプライン）
- **P4**: IPC DTO、API バージョン、ブレークチェンジ手順
- **P5**: ワークスペース、最小フロント、Windows ビルド手順

`document/` には要件定義・タスク分解・レビュー記録が置かれています。

---

## 主要ファイルと既定パス

| 用途 | 既定相対パス |
|---|---|
| 条件 JSON | `asset/cooccurrence-conditions.json` |
| annotation CSV | `asset/manual-annotations.csv` |
| analysis DB | `asset/ordinance_analysis5.db` |
| 分析スクリプト | `run-analysis.py` |
| DB 生成スクリプト | `docs/build_ordinance_analysis_db.py` |
| ジョブ出力先 | `runtime/jobs` |

---

## セキュリティと運用上の注意

- **`.env`**: `pdf_converter/.env` に API キーなどを置きます。`.gitignore` で無視されています
- **Python パッケージ名の不一致**: `pyproject.toml` の project/distribution 名は `csv-viewer`、import するパッケージ名は `pdf_converter` です。`uv run call-gemma4-gemini` は script entry point 名として機能します
- **ブレークチェンジ手順**: IPC DTO を変更する際は `IPC_API_VERSION` を更新し、`cargo test` と `--ipc-dto-self-check` を通す必要があります（P4-05）
- **Windows 前提**: ファイルパス解決やフォント設定は Windows 環境を想定しています
