# pdf_converter コード利用メモ

`pdf_converter` は、PDF から Markdown を作る処理と、Gemini API で PDF / OCR Markdown を確認・修正する処理を置く Python パッケージです。主な実行入口は `uv run call-gemma4-gemini` と `python -m pdf_converter.pdf_converter` です。

## 前提

- Python 依存関係はリポジトリルートで `uv sync` して用意します。
- Gemini API を使う処理は `pdf_converter/.env` または環境変数に `GEMINI_API_KEY` を設定します。
- レイアウト解析 API を使う `pdf_converter.py` は `pdf_converter/.env` または環境変数に `PDF_CONVERTER_TOKEN` を設定します。
- OCR 手作業用の標準ディレクトリは `asset/ocr_manual/` です。

`.env` の例:

```env
GEMINI_API_KEY=your-gemini-api-key
PDF_CONVERTER_TOKEN=your-layout-api-token
```

## ディレクトリ規約

`pdf_converter` 系の手作業 OCR ワークスペースは、基本的に次の構造を使います。

```text
asset/ocr_manual/
├── pdf/    # 入力 PDF
├── md/     # OCR 済み Markdown
└── work/   # Gemini OCR 修正モードが作業コピーを書き出す場所
```

`pdf_converter/pdf_converter.py` の既定出力先はリポジトリルート直下の `output/` です。これは `call-gemma4-gemini --task ocr-correct` の入力元とは自動接続されません。OCR 修正モードに渡す Markdown は、必要に応じて `output/` から `asset/ocr_manual/md/` へコピーして使います。

## ファイル別の使い道

| ファイル | 使い道 | 通常の使い方 |
|---|---|---|
| `__init__.py` | `pdf_converter` を Python パッケージとして扱うための最小ファイル | 直接実行しません |
| `pdf_converter.py` | 外部のレイアウト解析 API に PDF を送り、Markdown と画像を `output/` に保存する単発変換スクリプト | `python -m pdf_converter.pdf_converter path/to/file.pdf` |
| `call_gemma4_gemini.py` | Gemini API 呼び出しの本体。テキスト/PDF 単発質問と OCR Markdown 修正モードを持つ | `uv run call-gemma4-gemini ...` |
| `call-gemma4-gemini.py` | 旧来のハイフン付きファイル名で直接実行するための互換 wrapper | `python pdf_converter/call-gemma4-gemini.py ...` |
| `project_paths.py` | リポジトリルート、`.env`、`asset/ocr_manual`、`output` のパス解決 helper | 他モジュールから import して使います |
| `tool_call_logger.py` | OCR 修正モードの read/write tool 呼び出しを JSONL で追跡する logger | `--tool-call-log-path` 経由で使います |
| `verify-task-0-1-pdf-inline.py` | Gemini/Gemma に PDF inline 入力だけを渡せるか確認する検証スクリプト | 開発・切り分け用 |
| `verify-task-0-2-pdf-inline-thinking.py` | PDF inline と `thinking_config` の併用可否を確認する検証スクリプト | 開発・切り分け用 |
| `verify-task-0-3-response-shape.py` | `GenerateContentResponse` の成功時 shape を観察する検証スクリプト | 開発・切り分け用 |

## `pdf_converter.py`: PDF から Markdown を作る

外部レイアウト解析 API に PDF を送り、返ってきた Markdown と画像を保存します。

```powershell
python -m pdf_converter.pdf_converter asset/ocr_manual/pdf/根室市_条例.pdf
```

ページ画像も保存する場合:

```powershell
python -m pdf_converter.pdf_converter asset/ocr_manual/pdf/根室市_条例.pdf --save-page-jpg
```

入力:

- 位置引数 `file_path`: PDF パス。省略時は `asset/ocr_manual/pdf/根室市_条例.pdf` を既定値として使います。
- `--save-page-jpg`: API が返すページ画像も JPG として保存します。
- `PDF_CONVERTER_TOKEN`: レイアウト解析 API の token。

出力:

- `output/<PDFファイル名>.md`
- API レスポンスに含まれる Markdown 画像
- `--save-page-jpg` 指定時はページ JPG

注意:

- 出力先は `output/` 固定です。OCR 修正モードの `asset/ocr_manual/md/` へは自動コピーされません。
- API レスポンスの schema を前提にしているため、API 側の変更や部分失敗では例外で停止する可能性があります。

## `call_gemma4_gemini.py`: Gemini API を呼ぶ

`pyproject.toml` の script entry point により、通常は次の形式で実行します。

```powershell
uv run call-gemma4-gemini "水の化学式は？"
```

PDF を inline 添付して単発質問する場合:

```powershell
uv run call-gemma4-gemini "この PDF を要約してください" --pdf-path asset/ocr_manual/pdf/根室市_条例.pdf
```

位置引数の prompt を省略すると、PDF ありの場合は PDF 要約、PDF なしの場合は短いデモ質問が使われます。

主な引数:

- `prompt`: 任意のユーザープロンプト。
- `--pdf-path PATH`: PDF を `application/pdf` の inline Part として送ります。
- `--provider gemini|qwen`: 使用する API プロバイダー。既定は `gemini`。
- `--api-key-env NAME`: API key を読む環境変数名。既定は provider によって変わります（`gemini` 時は `GEMINI_API_KEY`、`qwen` 時は `DASHSCOPE_API_KEY`）。明示的に指定した場合はその値が使われます。
- `--model MODEL`: 使うモデル ID。既定は provider によって変わります（`gemini` 時は `gemini-3.1-flash-lite-preview`、`qwen` 時は `qwen3.6-plus`）。
- `--http-timeout-ms MS`: `generate_content` の HTTP timeout。既定は `300000`。
- `--task single-shot`: 既定の単発実行モード。
- `--task ocr-correct`: OCR Markdown 修正モード。

### Provider: Gemini（既定）

Gemini API を使った単発実行です。テキストのみ、または PDF inline 添付のいずれも利用できます。

### Provider: Qwen

Qwen API（DashScope 互換エンドポイント）を使ったテキスト単発実行です。

```powershell
uv run call-gemma4-gemini --provider qwen "水の化学式は？"
```

Qwen 関連の環境変数:

- `DASHSCOPE_API_KEY`: Qwen API キー。
- `CSV_VIEWER_QWEN_BASE_URL`: Qwen API のベース URL。省略時は `https://dashscope-intl.aliyuncs.com/compatible-mode/v1`。

制約:

- `--provider qwen --pdf-path` は未対応です。PDF 対応は request shape 検証後の後続タスクです。
- `--provider qwen --task ocr-correct` は未対応です。

## OCR Markdown 修正モード

PDF を正本として参照し、対応する OCR Markdown を `work/` にコピーして、Gemini の tool call で必要箇所だけ修正します。元の `md/` ファイルは直接上書きせず、作業コピーと unified diff を出力します。

```powershell
uv run call-gemma4-gemini --task ocr-correct --pdf-path asset/ocr_manual/pdf/根室市_条例.pdf
```

Markdown を明示する場合:

```powershell
uv run call-gemma4-gemini --task ocr-correct `
  --pdf-path asset/ocr_manual/pdf/根室市_条例.pdf `
  --markdown-path asset/ocr_manual/md/根室市_条例-2026-04-24_10-30-00.md
```

tool call の実行ログを残す場合:

```powershell
uv run call-gemma4-gemini --task ocr-correct `
  --pdf-path asset/ocr_manual/pdf/根室市_条例.pdf `
  --tool-call-log-path runtime/ocr-tool-calls.jsonl
```

入力解決:

- `--pdf-path` は必須です。
- `--markdown-path` を指定した場合、その Markdown を使います。
- `--markdown-path` を省略した場合、PDF stem と一致する `md/<PDF stem>-YYYY-MM-DD_HH-MM-SS.md` 形式の Markdown を探し、最新 timestamp のものを使います。
- Markdown は `asset/ocr_manual/md/` 配下が標準です。移行互換のため旧 `asset/texts_2nd/manual/md/` も候補として扱う箇所があります。

出力:

- `asset/ocr_manual/work/<元stem>-working-<timestamp>.md`
- 標準出力に最終メッセージ
- 差分がある場合は unified diff
- `--tool-call-log-path` 指定時は JSONL ログ

制約:

- read tool は `md/` と `work/` を読みます。
- write tool は `work/` 配下だけを書き換えます。
- OCR 修正モードの write lock は OS 別実装です。Windows ではローカル NTFS 上の通常ファイルのみを保証対象とし、OneDrive、ネットワークドライブ、FAT/exFAT、WSL 経由の Windows filesystem は保証対象外です。
- `--max-tool-calls` で read/write tool の合計回数上限を変更できます。既定は `48`、上限は `256` です。
- `output/` 配下の Markdown は OCR 修正モードの入力として拒否されます。

## `call-gemma4-gemini.py`: 互換 wrapper

旧来の実行形式を残すための薄い wrapper です。実処理は `pdf_converter.call_gemma4_gemini:main` に委譲します。

```powershell
python pdf_converter/call-gemma4-gemini.py "この PDF を要約してください" --pdf-path asset/ocr_manual/pdf/根室市_条例.pdf
```

新規の利用案内では `uv run call-gemma4-gemini ...` を優先してください。

## `project_paths.py`: パス解決 helper

主に他モジュールから import して使います。

主な関数:

- `resolve_project_root()`: `CSV_VIEWER_PROJECT_ROOT`、`pyproject.toml` 上位探索、`__file__` fallback の順でリポジトリルートを解決します。
- `resolve_manual_root()`: 標準の OCR 手作業 root として `asset/ocr_manual` を返します。
- `resolve_manual_root_candidates()`: `asset/ocr_manual` と旧 `asset/texts_2nd/manual` を候補として返します。
- `resolve_default_ocr_output_dir()`: `output/` を返します。
- `resolve_dotenv_path()`: `pdf_converter/.env` を返します。

source tree の解決に失敗した場合は `ProjectRootResolutionError` を送出します。non-editable install などで source tree を辿れない場合は、`CSV_VIEWER_PROJECT_ROOT` を明示してください。

## `tool_call_logger.py`: OCR tool call ログ

OCR 修正モードの tool call を JSONL で追跡するための補助モジュールです。通常は CLI の `--tool-call-log-path` から使います。

同じ JSONL ログファイルへ複数プロセスから同時に追記する運用は保証対象外です。並列実行する場合は run ごとに別の `--tool-call-log-path` を指定してください。

ログ 1 行の主な項目:

- `timestamp`: UTC ISO 形式の記録時刻
- `phase`: request / executed などの段階
- `turn_index`: multi-turn の turn 番号
- `tool_name`: `read_markdown_file` または `write_markdown_file`
- `args`: tool call 引数
- `status`: `ok` または `error`
- `details`: 追加情報

## 検証用スクリプト

以下は通常運用ではなく、Gemini API / SDK / モデル挙動の切り分け用です。いずれも `GEMINI_API_KEY` を使います。

PDF inline 入力だけを検証:

```powershell
python pdf_converter/verify-task-0-1-pdf-inline.py --pdf-path path/to/small.pdf
```

PDF inline と `thinking_level=high` の併用を検証:

```powershell
python pdf_converter/verify-task-0-2-pdf-inline-thinking.py --pdf-path path/to/small.pdf
```

成功時のレスポンス shape を観察:

```powershell
python pdf_converter/verify-task-0-3-response-shape.py --pdf-path path/to/small.pdf
```

共通引数:

- `--pdf-path`: 検証に使う小さめの PDF。`verify-task-0-3` では省略するとテキストのみ scenario だけ実行します。
- `--api-key-env`: API key を読む環境変数名。既定は `GEMINI_API_KEY`。
- `--model`: 検証対象モデル。既定は `gemma-4-31b-it`。

## よく使う手順

PDF から OCR Markdown を作り、Gemini で修正する場合:

1. `python -m pdf_converter.pdf_converter asset/ocr_manual/pdf/対象.pdf`
2. `output/対象.md` を `asset/ocr_manual/md/対象-YYYY-MM-DD_HH-MM-SS.md` の形式でコピーします。
3. `uv run call-gemma4-gemini --task ocr-correct --pdf-path asset/ocr_manual/pdf/対象.pdf`
4. 標準出力の unified diff と `asset/ocr_manual/work/` の作業 Markdown を確認します。
5. 問題なければ作業 Markdown の内容を採用先へ反映します。

単に PDF の内容を Gemini に質問する場合:

```powershell
uv run call-gemma4-gemini "この PDF の重要点を箇条書きでまとめてください" --pdf-path asset/ocr_manual/pdf/対象.pdf
```
