# Qwen Provider 選択の実装ログ

## 実装概要

`pdf_converter/call_gemma4_gemini.py` に `--provider gemini|qwen` 引数を追加し、Qwen API（DashScope 互換エンドポイント）経由でのテキスト単発実行に対応しました。

## 変更ファイル一覧

| ファイル | 変更内容 |
|---|---|
| `pdf_converter/call_gemma4_gemini.py` | `--provider`、`--qwen-base-url` 引数追加。`resolve_effective_api_key_env`、`resolve_effective_model` で provider 別既定値解決。`run_single_shot_mode` に Qwen 分岐追加。`run_ocr_correction_mode` で Qwen 使用時にエラー返却。 |
| `pdf_converter/qwen_client.py` | 新規作成。Qwen Chat Completion API 呼び出し、レスポンス抽出、エラー整形を担当。 |
| `pdf_converter/README.md` | Qwen provider の使い方、環境変数、制約事項を追加。 |
| `document/logs/2026-05-06-qwen-provider-selection-implementation.md` | 本ログファイル。 |

## 既存挙動の維持内容

- `--provider` を省略した場合は `gemini` が選択され、これまでと同じ挙動になります。
- `--provider gemini` 時の `--api-key-env` 既定値は `GEMINI_API_KEY`、`--model` 既定値は `gemini-3.1-flash-lite-preview` で変更ありません。
- OCR Markdown 修正モード（`--task ocr-correct`）は Gemini 専用のまま維持されます。
- PDF inline 添付（`--pdf-path`）は Gemini 専用のまま維持されます。

## 検証結果（テスト通過状況）

- `py_compile` による構文チェック: `pdf_converter/call_gemma4_gemini.py`、`pdf_converter/qwen_client.py` ともに通過。
- import smoke check: `pdf_converter.call_gemma4_gemini`、`pdf_converter.qwen_client` ともに通過。

## 未対応範囲

| 機能 | 未対応理由 | 今後の対応方針 |
|---|---|---|
| `--provider qwen --pdf-path` | Qwen API への PDF inline 入力の request shape が未検証 | request shape 検証後の後続タスク |
| `--provider qwen --task ocr-correct` | OCR 修正モードの tool call ループが Gemini SDK に依存 | provider-neutral な tool 抽象化が必要 |

## 実行例

### Qwen テキスト単発実行

```powershell
uv run call-gemma4-gemini --provider qwen "水の化学式は？"
```

### モデル・API キー環境変数を明示

```powershell
$env:DASHSCOPE_API_KEY="your-key"
uv run call-gemma4-gemini --provider qwen --model qwen3.6-plus --api-key-env DASHSCOPE_API_KEY "Hello"
```

### ベース URL 上書き

```powershell
uv run call-gemma4-gemini --provider qwen --qwen-base-url "https://custom.example.com/v1" "Hello"
```
