# call-gemma4-gemini.py uv CLI package 化細分タスク

## 結論

uv CLI package 化は、Phase 0 から Phase 6 までの 7 段階に分ける。最初に `pdf_converter.py` のモジュールレベル副作用と依存宣言を整理し、その後に package 化、実装本体移設、互換 shim、テスト、文書更新を進める。

この順序にする理由は、現状の import 衝突と副作用が package 化の途中確認を壊しやすいためである。先に「import しても落ちない」状態を作ってから package 化へ進む方が、切り分けが明確で手戻りが少ない。

## 既存挙動

現状の `pdf_converter/call-gemma4-gemini.py` には以下がある。

- OCR 修正モード
- `read` / `write` tool
- 多ターン loop
- tool call JSONL logging
- `pdf_converter.tool_call_logger` への import

一方で、以下が package 化の障害になっている。

- `pdf_converter/pdf_converter.py` がモジュールレベルで `.env` 読込、`parse_args()`、token 検証、I/O 準備を行う
- `call-gemma4-gemini.py` は `__file__` ベースで asset / output / `.env` の位置を決めている
- `pyproject.toml` に build system と project script がない
- `httpx` が direct import されるが、依存として明示されていない

したがって、今回の細分タスクは「既存 CLI 挙動を維持しつつ、uv run で package command として起動可能にする」ことを目的に構成する。

## Phase 0: package 化前の安全化

### Task 0-1: `pdf_converter.py` のモジュールレベル副作用を退避

- `.env` 読込
- `parse_args()`
- token 検証
- ファイル読込
- API 実行準備

上記をモジュール import 時には走らせず、`main()` 呼び出し時のみ動く形へ整理する。

受け入れ条件:

- `import pdf_converter.pdf_converter` だけでは例外にならない
- `PDF_CONVERTER_TOKEN` 未設定でも import 時点では落ちない
- 既存の `python pdf_converter/pdf_converter.py ...` 相当の CLI 実行は維持される

### Task 0-2: `httpx` を明示依存へ追加

- `call-gemma4-gemini.py` が参照する `httpx.TimeoutException` / `httpx.RequestError` を transitive dependency 任せにしない
- `pyproject.toml` の `dependencies` に `httpx` を追加する

受け入れ条件:

- `pyproject.toml` から direct dependency として `httpx` が確認できる
- `google-genai` 側の内部実装変更に依存しない

### Task 0-3: project root 解決仕様を固定

- `CSV_VIEWER_PROJECT_ROOT`
- `pyproject.toml` の上位探索
- `__file__` fallback

の優先順位を helper 仕様として固定する。

受け入れ条件:

- helper の優先順位が文書とコードで一致する
- 非 editable install 想定時に、静かに壊れず明示エラーへ落ちる方針が定義される

## Phase 1: package 化の土台

### Task 1-1: `pdf_converter` package 宣言を追加

- `pdf_converter/__init__.py` を追加する
- import package として `pdf_converter` を成立させる

### Task 1-2: `pyproject.toml` に build system と script を追加

- `[build-system]`
- `[project.scripts]`
- `[tool.uv.build-backend]`

を追加し、`uv run call-gemma4-gemini` を公開する。

受け入れ条件:

- build backend は `uv_build`
- script entry point は `pdf_converter.call_gemma4_gemini:main`
- module discovery は `pdf_converter` を指す

### Task 1-3: project 名と package 名のズレを明記

- project 名 `csv-viewer`
- import package 名 `pdf_converter`

のズレを README または関連文書に明示する。

## Phase 2: 本体の import 可能 module 化

### Task 2-1: `call_gemma4_gemini.py` を新設して本体を移設

- `pdf_converter/call_gemma4_gemini.py` を追加する
- 既存 `call-gemma4-gemini.py` の本体を新 module へ移す
- `main()`、`parse_args()`、主要 helper 群の責務を維持する

受け入れ条件:

- entry point 側が hyphen file ではなく import 可能 module を参照する
- 既存の OCR 修正ロジックには意図しない変更が入らない

### Task 2-2: `__file__` 依存を helper 経由へ置換

- `DEFAULT_MANUAL_ROOT`
- `DEFAULT_OCR_OUTPUT_DIR`
- `.env` 位置解決

を `resolve_project_root()` 経由へ寄せる。

受け入れ条件:

- asset / output / `.env` 参照が helper 経由に統一される
- helper 経由の失敗時はメッセージが明示的である

### Task 2-3: CLI 引数仕様の互換維持を確認

- `--task`
- `--pdf-path`
- `--markdown-path`
- `--working-dir`
- `--tool-call-log-path`
- `--api-key-env`
- `--model`

が旧 CLI と同じ意味で残ることを確認する。

## Phase 3: 互換 shim

### Task 3-1: 旧 `call-gemma4-gemini.py` を wrapper 化

- `pdf_converter/call-gemma4-gemini.py` を最小 wrapper に差し替える
- repo root を `sys.path` へ追加し、必要なら script directory を外す
- 明示 import には `importlib` 利用も検討する

受け入れ条件:

- `python pdf_converter/call-gemma4-gemini.py --help` が動く
- 旧経路は互換 shim として扱い、案内の第一推奨は `uv run call-gemma4-gemini --help` とする
- 旧経路でも `pdf_converter.py` へ衝突しない

### Task 3-2: shim の import 解決順を固定

- `sys.path` 操作の順序を固定する
- Windows / PowerShell での直接実行を優先確認する

## Phase 4: テスト更新

### Task 4-1: `test_call_gemma4_gemini_tool_call_logging.py` の import を更新

- `spec_from_file_location` 前提の読込を見直す
- package import 前提へ寄せる

受け入れ条件:

- `parse_args()` テストが通る
- turn loop logging テストが通る

### Task 4-2: `pdf_converter.py` import-safe テストを追加

- import 時に `.env` 依存や `parse_args()` が走らないことを確認する
- 副作用回帰を防止する

### Task 4-3: CLI smoke テストを追加または手動確認項目化

- `uv run call-gemma4-gemini --help`
- `uv run call-gemma4-gemini --task ocr-correct --help`
- `python pdf_converter/call-gemma4-gemini.py --help` は互換 shim 確認として残す

の最低限を確認対象に含める。

## Phase 5: 文書更新

### Task 5-1: 実行例を `uv run call-gemma4-gemini ...` 優先へ更新

- help 文
- package plan
- 関連 task breakdown

の順に反映する。

### Task 5-2: editable install 前提と fallback を明記

- `CSV_VIEWER_PROJECT_ROOT`
- 非 editable install 時の制約
- 旧スクリプト経路が互換 shim であること

を README と task breakdown の見つけやすい箇所へ追記する。

追記する内容は、`CSV_VIEWER_PROJECT_ROOT` が project root の明示上書きであること、helper が `CSV_VIEWER_PROJECT_ROOT` -> `pyproject.toml` 上位探索 -> `__file__` fallback の順で解決すること、non-editable install では source tree 解決が崩れる場合があること、そして `python pdf_converter/call-gemma4-gemini.py ...` が当面は互換 shim であることに限定する。

### Task 5-3: package 化と既存バグ修正の境界を明記

- diff 出力バグ
- write lock
- turn 上限制御
- CRLF/LF 正規化

は別タスク扱いであることを文書上で明確化する。ここで行うのは「package 化の直接要件から外す」整理までであり、個別のバグ修正は行わない。

受け入れ条件:

- `diff 出力バグ`、`write lock`、`turn 上限制御`、`CRLF/LF 正規化` が package 化の直接要件ではないと明記されている
- package 化の完了条件に上記 4 件が含まれていない
- 上記 4 件が `Task 6-1` の follow-up として参照されている

## Phase 6: 残課題の切り出し

### Task 6-1: package 化外の既存バグを follow-up として列挙

package 化の直接要件ではない既存バグを、[follow-up 一覧](./call-gemma4-gemini-uv-cli-package-follow-up-bug-list.md) に集約して見返しやすくする。

対象:

- `build_unified_diff_text` 改行バグ
- write lock の孤立ロック検出不足
- turn 上限制御
- LF 正規化影響

`Task 5-3` で package 化から外した内容をここで受ける。

### Task 6-2: 将来の rename 候補を明記

package 化本体とは別の整理候補として、現時点の判断を文書に残す。

- `pdf_converter/pdf_converter.py` の rename
- dotenv 共通化

を将来の整理候補として残す。ここでは実装しない。

## 実装順の推奨

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 3
5. Phase 4
6. Phase 5
7. Phase 6

## 完了条件

- `uv run call-gemma4-gemini --help` が成功する
- `uv run call-gemma4-gemini --task ocr-correct ...` が少なくとも引数解釈まで正常に進む
- `python pdf_converter/call-gemma4-gemini.py --help` も wrapper 経由で成功するが、文書の第一推奨は `uv run call-gemma4-gemini --help` とする
- `import pdf_converter.pdf_converter` が副作用なく完了する
- `tests/test_call_gemma4_gemini_tool_call_logging.py` が package 化後も通る
- project root 解決失敗時に、理由が明示されたエラーになる
- `diff 出力バグ`、`write lock`、`turn 上限制御`、`CRLF/LF 正規化` はこの完了条件に含めない
- 詳細は [follow-up 一覧](./call-gemma4-gemini-uv-cli-package-follow-up-bug-list.md) に集約されている

## セカンドオピニオン反映メモ

- build backend は `uv_build` 前提に変更
- `__file__` 依存は helper 化して fallback を持たせる
- `pdf_converter.py` の副作用除去を package 化前の必須タスクへ昇格
- `httpx` 明示依存を追加
- package 化と既存バグ修正は原則分離する
