# call-gemma4-gemini.py uv CLI package 化実装案

## 結論

`uv run call-gemma4-gemini ...` を安定して動かす方針自体は妥当だが、実装案は以下を前提に更新する必要がある。

1. build backend は `setuptools` ではなく `uv_build` を第一候補にする
2. `__file__` ベースのパス解決は editable install 前提であることを明示し、非 editable install 時の逃げ道を用意する
3. `pdf_converter/pdf_converter.py` のモジュールレベル副作用を先に除去する
4. `httpx` を transitive dependency 任せにせず `pyproject.toml` へ明示する
5. package 化と無関係な既存バグは、同時修正するか別タスクへ切り出すかを明確にする

なお、`build_unified_diff_text` の diff 出力バグ、write lock、turn 上限制御、CRLF/LF 正規化は package 化の直接要件ではない。今回の package 化では修正対象に含めず、[follow-up 一覧](./call-gemma4-gemini-uv-cli-package-follow-up-bug-list.md) にまとめて別タスクとして扱う。

この更新後の案では、実装本体を import 可能な module へ移し、`uv` からは entry point 経由で起動する。あわせて、既存の `python pdf_converter/call-gemma4-gemini.py ...` を完全には捨てず、旧ファイルは互換 shim として残す。

## 既存挙動

- `pdf_converter/call-gemma4-gemini.py` は `from pdf_converter.tool_call_logger import ...` を使う
- `pdf_converter/` 配下には `pdf_converter.py` という sibling file も存在する
- `python pdf_converter/call-gemma4-gemini.py ...` のような直接スクリプト実行では `sys.path[0]` が `pdf_converter/` になり、top-level の `pdf_converter` 解決が sibling file `pdf_converter.py` に衝突する
- その結果、`pdf_converter.py` 側の `parse_args()` が import 時点で走り、`--task` などを未認識引数として落とす
- 現在の `pyproject.toml` には `[build-system]` がなく、`[project.scripts]` もない
- `pdf_converter/` には `__init__.py` もないため、package として install される前提になっていない
- `call-gemma4-gemini.py` は `DEFAULT_MANUAL_ROOT = Path(__file__).resolve().parent.parent / "asset" / ...` のように `__file__` ベースで asset と output の位置を決めている
- `pdf_converter/pdf_converter.py` は import 時点で `.env` 読込、`parse_args()`、token 検証、I/O 準備まで進む副作用を持つ
- `tests/test_call_gemma4_gemini_tool_call_logging.py` は現行 repo に存在する
- `pyproject.toml` の dependencies には `httpx` が明示されていない

## 目的

- `uv run call-gemma4-gemini --task ocr-correct ...` で起動できるようにする
- `uv run` が repo 内 package を import して起動する構成にする
- 既存 CLI の引数仕様と OCR 修正フローの挙動は変えない
- 可能なら旧 `python pdf_converter/call-gemma4-gemini.py ...` も互換維持する
- editable install 前提が崩れた場合でも、少なくとも失敗理由が明確になるようにする

## 対象外

- OCR 修正ロジック自体の変更
- `tool_call_logger` の JSONL schema 変更
- `pdf_converter.py` のレイアウト解析 API の振る舞い変更
- `src/` や `src-tauri/` 側の変更
- `build_unified_diff_text`、write lock、turn 上限制御など既存バグの包括修正

## 実装方針

### 1. `pdf_converter` を package 化する

- `pdf_converter/__init__.py` を追加する
- `pyproject.toml` に `[build-system]` を追加する
- `pyproject.toml` に `[project.scripts]` を追加し、`call-gemma4-gemini` を entry point として公開する
- build backend は `uv_build` を優先する

### 2. 実装本体を import 可能な module 名へ移す

- 新規に `pdf_converter/call_gemma4_gemini.py` を作成し、現在の `call-gemma4-gemini.py` の本体をそこへ移す
- entry point は `pdf_converter.call_gemma4_gemini:main` を指す

補足:

- file 名は project 規約上 kebab-case が基本だが、Python import path では hyphen を module 名に使えない
- そのため、import 対象 module に限って underscore 名を許容する

### 3. `__file__` 依存を局所化する

- `DEFAULT_MANUAL_ROOT` や `DEFAULT_OCR_OUTPUT_DIR` は、`__file__` 直参照ではなく `resolve_project_root()` helper を介して解決する
- `resolve_project_root()` は次の優先順位を持つ
  1. 明示的な環境変数 `CSV_VIEWER_PROJECT_ROOT`
  2. source tree から上位探索して `pyproject.toml` を見つけた場所
  3. 最後の手段として `Path(__file__).resolve().parent.parent`
- 非 editable install で source tree が見つからない場合は、静かに壊れるのではなく、必要な asset 位置を解決できない旨を明示的にエラーにする
- `.env` 読込も同じ helper 経由で project root を解決してから行う

### 4. `pdf_converter.py` の副作用を先に除去する

- `pdf_converter/pdf_converter.py` のモジュールレベル実行を `main()` 配下へ移す
- import 時点で `.env` 読込、`parse_args()`、token 検証、ファイル I/O が走らないようにする
- 今回は rename を必須にしないが、少なくとも import-safe にする

### 5. 旧ファイルは互換 shim にする

- `pdf_converter/call-gemma4-gemini.py` は削除せず、最小の wrapper に置き換える
- wrapper は repo root を `sys.path` へ追加するだけでなく、必要なら script directory を `sys.path` から除外して import 衝突を避ける
- shim 内の import は `importlib` を使った明示的な import も選択肢に含める

### 6. `httpx` を明示依存へ追加する

- `call_gemma4_gemini.py` は `httpx.TimeoutException` と `httpx.RequestError` を直接参照する
- そのため `pyproject.toml` の dependencies に `httpx` を追加する

### 7. package 化と既存バグ修正は切り分ける

- `build_unified_diff_text` の改行バグ
- write lock の孤立ロック検出不足
- turn 上限制御の不在
- CRLF から LF への暗黙正規化

上記は package 化の直接要件ではない。今回の実装案では、package 化の完了条件には含めず、`Task 6-1` の follow-up として扱う。ただし package 化作業で対象コードを触る結果、最小修正で同時解消できるものがあれば、そのときは別コミットで扱う。

## 変更ファイル案

### 新規

- `pdf_converter/__init__.py`
- `pdf_converter/call_gemma4_gemini.py`

### 更新

- `pdf_converter/call-gemma4-gemini.py`
- `pdf_converter/pdf_converter.py`
- `pyproject.toml`
- `tests/test_call_gemma4_gemini_tool_call_logging.py`
- 必要なら `tests/test_call_gemma4_gemini_turn_loop.py`
- 既存のドキュメント類

### 任意更新

- `README.md`
- `document/call-gemma4-gemini-tool-call-logging-requirements.md`
- `document/call-gemma4-gemini-tool-call-logging-task-breakdown.md`
- `document/call-gemma4-gemini-ocr-markdown-task-breakdown.md`

## `pyproject.toml` の変更方針

### build system

- `uv` 公式 docs では、Python project の build backend として `uv_build` が用意されている
- 今回は `uv` project としての一貫性を優先し、`uv_build` を採用する

想定:

```toml
[build-system]
requires = ["uv_build>=0.11.6,<0.12"]
build-backend = "uv_build"
```

### module discovery

- project 名は `csv-viewer` だが、公開したい import package は `pdf_converter`
- `uv_build` の default module discovery は project 名から `csv_viewer` を期待するため、そのままでは一致しない
- したがって module 名と root を明示する

想定:

```toml
[tool.uv.build-backend]
module-name = "pdf_converter"
module-root = ""
```

### entry point

```toml
[project.scripts]
call-gemma4-gemini = "pdf_converter.call_gemma4_gemini:main"
```

### dependencies

`httpx` を追加する。

## テスト方針

### 1. module import テストを更新

- 既存 `tests/test_call_gemma4_gemini_tool_call_logging.py` は `importlib.util.spec_from_file_location` で hyphen file を直接ロードしている
- package 化後は `import pdf_converter.call_gemma4_gemini as module` 相当に寄せる
- 少なくとも `parse_args()` と turn loop の既存単体テストは維持する

### 2. `pdf_converter.py` の import-safe テストを追加

- `import pdf_converter.pdf_converter` で `parse_args()` や token 検証が走らないことを確認する
- モジュールレベル副作用除去の回帰防止を入れる

### 3. CLI smoke test を追加

- `uv run call-gemma4-gemini --help` 相当の smoke test までは入れたい
- unit test から `uv` 自体を呼ぶのが重い場合は、少なくとも entry point の import と `main()` 連携を確認する

### 4. 互換 shim テスト

- 旧 `pdf_converter/call-gemma4-gemini.py` が wrapper として `main()` を呼べることを最低限確認する
- ここは subprocess までやるか、wrapper のコード量を極小にして明示的テストを省くかを選ぶ

## 実装ステップ案

### Step 0: package 化前の安全化

- `pdf_converter/pdf_converter.py` のモジュールレベル副作用を `main()` 配下へ移す
- `call-gemma4-gemini.py` 依存の import 衝突を起こしにくい状態を先に作る
- `httpx` を `pyproject.toml` に明示追加する

### Step 1: package 化の土台

- `pdf_converter/__init__.py` を追加
- `pyproject.toml` へ `[build-system]`、`[project.scripts]`、`[tool.uv.build-backend]` を追加
- `uv run call-gemma4-gemini --help` が解決される前提を作る

### Step 2: import 可能 module へ本体移設

- `call-gemma4-gemini.py` の本体を `call_gemma4_gemini.py` へ移す
- `main()`、`parse_args()`、既存関数群の import path を崩さない
- project root 解決 helper を導入し、asset / output / `.env` の参照を helper 経由へ寄せる

### Step 3: 互換 shim 化

- 旧 `call-gemma4-gemini.py` を最小 wrapper に差し替える
- 直接実行時の `sys.path` 問題を局所的に吸収する

### Step 4: テスト更新

- 既存テストの import 先を更新
- `pdf_converter.py` の import-safe テストを追加
- package 化後も tool call logging テストが通ることを確認する

### Step 5: ドキュメント更新

- help 文の実行例を `uv run call-gemma4-gemini ...` 優先に更新
- 旧 `python pdf_converter/call-gemma4-gemini.py ...` は互換経路として補足扱いにする
- editable install 前提と `CSV_VIEWER_PROJECT_ROOT` fallback を文書へ明記する

## 想定コマンド

最終的な利用形は以下を第一候補にする。

```powershell
uv run call-gemma4-gemini --task ocr-correct --pdf-path "asset/texts_2nd/manual/pdf/根室市_条例.pdf" --markdown-path "asset/texts_2nd/manual/md/根室市_条例-2026-03-30_06-14-12.md" --tool-call-log-path "asset/texts_2nd/manual/work/logs/根室市_条例-tool-call-log.jsonl"
```

## リスク

### 1. editable install 前提が崩れた場合の asset 解決

- `uv run` では通常 editable install で source tree を見に行くが、`uv pip install .` や `pip install .` のような非 editable install では `__file__` から source tree を辿れない
- そのため project root 解決 helper と環境変数 fallback を入れないと asset / output / `.env` の位置が壊れる

### 2. build backend 追加による `uv run` の意味変化

- build system を追加すると、project 自身も install 対象になる
- 従来の「依存だけが入る project」から意味が変わるため、既存の Python 周辺導線への影響確認が必要

### 3. `pdf_converter.py` を rename しない場合の長期保守性

- import-safe 化しても名前の紛らわしさは残る
- 今回は rename を必須にしないが、将来の follow-up 候補であることを明示する
- dotenv ロードの共通化も同様に、package 化本体とは別の follow-up 候補として残す

### 4. 既存ドキュメントとのズレ

- help 文、task breakdown、requirements 文書に旧 `python pdf_converter/call-gemma4-gemini.py` の例が残っている
- 実装だけ先に変えると利用者が古い起動方法を踏みやすい

### 5. 既存バグを package 化へ混ぜ込む範囲

- diff 出力バグや write lock 問題を同時に直し始めると、package 化の検証範囲が膨らむ
- どこまでを同一 PR に含めるか事前に線引きが必要

この節で挙げる項目は package 化の直接要件ではない。diff 出力バグ、write lock、turn 上限制御、CRLF/LF 正規化は package 化の検証対象から外し、別タスクとして扱う。

## 推奨する小コミット分割

1. `Guard pdf_converter module-level side effects`
2. `Package pdf_converter for uv entry points`
3. `Move call-gemma CLI into importable module`
4. `Add compatibility shim for legacy script path`
5. `Update tests and docs for uv CLI entry point`

## 完了条件

- `uv run call-gemma4-gemini --help` が成功する
- `uv run call-gemma4-gemini --task ocr-correct ...` が少なくとも引数解釈まで正常に進む
- `tests/test_call_gemma4_gemini_tool_call_logging.py` が package 化後も通る
- `import pdf_converter.pdf_converter` が副作用なく完了する
- 旧 `python pdf_converter/call-gemma4-gemini.py --help` も wrapper 経由で成功するが、案内上の第一推奨は `uv run call-gemma4-gemini --help` とする

## 参考

- uv docs: Configuring projects  
  https://docs.astral.sh/uv/concepts/projects/config/
- uv docs: Build backend  
  https://docs.astral.sh/uv/concepts/build-backend/
- uv docs: Project layout  
  https://docs.astral.sh/uv/concepts/projects/layout/
- PyPA: `pyproject.toml` specification  
  https://packaging.python.org/en/latest/specifications/pyproject-toml/

---

## セカンドオピニオン（批判的レビュー）

レビュー日: 2026-04-10  
対象: 本実装案（uv CLI package 化）全体 + 既存コード `pdf_converter/call-gemma4-gemini.py`  
視点: バグの可能性、UI 崩れ（CLI 出力崩れ）、計算ミス、意図しない動作

### Critical: `__file__` ベースのパス解決が package install 後に破綻する可能性

**深刻度: 致命的**

既存コードは `__file__` からの相対パスでアセットディレクトリを解決している。

```python
DEFAULT_MANUAL_ROOT = Path(__file__).resolve().parent.parent / "asset" / "texts_2nd" / "manual"
DEFAULT_OCR_OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
```

package 化して `uv run` で起動する場合、uv は project を editable install する。editable install であれば `__file__` は元のソースを指すため動作する。しかし以下のケースで破綻する:

1. **非 editable install**（`uv pip install .` や `pip install .`）では `__file__` は `site-packages/pdf_converter/call_gemma4_gemini.py` を指し、`parent.parent` は `site-packages/` になる。`asset/texts_2nd/manual` は存在しないため、すべてのパス解決が静かに失敗する。
2. **uv の build isolation** の挙動変更により、将来的に editable install が想定と異なる場所を指す可能性がある。
3. `.env` のロードも `os.path.dirname(__file__)` に依存しており（L1455, L1511）、同様に破綻する。

実装案はこのリスクに言及していない。最低限、`__file__` ベースの解決が editable install 前提であることを明示し、非 editable install 時のフォールバック（環境変数 `PROJECT_ROOT` 等）を検討すべき。

### Critical: build backend の選択が uv 推奨と乖離している

**深刻度: 高**

実装案は `setuptools.build_meta` を提案しているが、2026 年現在の uv 公式ドキュメントでは `uv_build` が推奨される build backend になっている:

```toml
[build-system]
requires = ["uv_build>=0.11.6,<0.12"]
build-backend = "uv_build"
```

`setuptools` を使うデメリット:
- 追加の build dependency のダウンロードが必要
- flat-layout auto-discovery が src/, tests/, asset/, document/ 等の無関係ディレクトリを検出するリスクがある（次項参照）
- uv 組み込みの editable install 最適化が効かない可能性がある

`setuptools` 採用の理由が「最小構成」であれば、`uv_build` の方が uv project としてはさらに最小構成であり、かつ推奨パスに沿っている。

### Critical: setuptools flat-layout auto-discovery によるパッケージ衝突

**深刻度: 高（setuptools 採用時）**

setuptools の flat-layout auto-discovery は、project root 配下のすべての Python package 候補を自動検出する。このリポジトリには `pdf_converter/` 以外にも `src/`、`src-tauri/` 等のディレクトリが存在する。

`pdf_converter/` に `__init__.py` を追加した時点で setuptools が package と認識するが、他のディレクトリに `__init__.py` がなくても setuptools のバージョンによっては namespace package として検出を試みる場合がある。

実装案に `[tool.setuptools.packages.find]` の明示指定が含まれていないため、想定外のディレクトリが install 対象に含まれるリスクがある:

```toml
[tool.setuptools.packages.find]
include = ["pdf_converter*"]
```

この指定がなければ、`uv run` 実行時にビルドエラーまたは意図しないファイルの install が発生し得る。`uv_build` を採用すればこの問題は軽減される。

### Critical: 存在しないテストファイルへの言及

**深刻度: 中（計画の正確性）**

実装案は `tests/test_call_gemma4_gemini_tool_call_logging.py` の更新を複数箇所で参照しているが、このファイルはリポジトリに存在しない。`tests/` ディレクトリ自体も空であり、テストファイルは 0 件。

- 「テスト方針」セクション (L110-122) でこのファイルの import 方式変更に言及
- 「変更ファイル案 > 更新」(L72) にリストされている
- 「完了条件」(L190) にテスト通過が含まれている

存在しないファイルを前提とした計画は実行不能であり、事前にテストファイルの作成タスクを追加するか、参照を修正する必要がある。

### High: `pdf_converter/pdf_converter.py` のモジュールレベル副作用が package 化で顕在化するリスク

**深刻度: 高**

`pdf_converter/pdf_converter.py` はモジュールレベルで以下を実行する:

```python
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)
TOKEN = os.getenv("PDF_CONVERTER_TOKEN", "")
if not TOKEN:
    raise RuntimeError("PDF_CONVERTER_TOKEN is not set. ...")

args = parse_args()         # sys.argv をパース
file_path = args.file_path  # ファイル I/O 開始
```

実装案は「pdf_converter.py は今回は rename しない」としているが、package 化により `pdf_converter.pdf_converter` が正式な import path として有効になる。以下の状況で副作用が意図せず発動する:

1. IDE の自動 import 補完が `from pdf_converter import pdf_converter` を提案する
2. テストフレームワーク（pytest）がモジュール収集時に `pdf_converter.pdf_converter` を発見・import する
3. linter や type checker が静的解析中にモジュールを評価する
4. 将来の開発者が `pdf_converter.pdf_converter` を import 可能と誤認して使用する

これらの場合、`PDF_CONVERTER_TOKEN` 未設定なら `RuntimeError` で即座にクラッシュし、設定済みでも `parse_args()` が `sys.argv` をパースして `--task` 等を未認識引数として拒否する。

実装案のリスク評価で「直ちに rename 必須ではない」とされているが、package 化はまさにこのリスクを顕在化させる変更であり、rename を「今回は実施しない」のであれば、少なくとも `pdf_converter.py` のモジュールレベルコードを `if __name__ == "__main__":` ガード配下に移すべき。

### High: `httpx` が暗黙の transitive dependency として使用されている

**深刻度: 中**

`call-gemma4-gemini.py` L23 で `import httpx` しており、L532 で `httpx.TimeoutException` / `httpx.RequestError` をエラーハンドリングに使用している。しかし `pyproject.toml` の `dependencies` に `httpx` が含まれていない。

`google-genai` の transitive dependency として `httpx` が入っているため現在は動作するが:
- `google-genai` が内部 HTTP クライアントを変更した場合、`httpx` が消える
- 依存関係の明示性が失われ、`uv pip compile` 等でのロック時に問題が起きうる

package 化に際して `httpx` を `dependencies` に明示追加すべき。

### Medium: `build_unified_diff_text` の diff 出力フォーマットが壊れている（既存バグ）

**深刻度: 中（CLI 出力崩れ）**

```python
original_lines = original_text.splitlines(keepends=True)
working_lines = working_text.splitlines(keepends=True)
diff_lines = difflib.unified_diff(
    original_lines,
    working_lines,
    fromfile=..., tofile=...,
    lineterm="",
)
return "\n".join(diff_lines)
```

`splitlines(keepends=True)` は各行末に `\n` を保持する（例: `"hello\n"`）。`lineterm=""` は diff 出力行に追加文字を付けない設定。しかし `"\n".join(diff_lines)` で結合すると、内容行は既に `\n` を含むため二重改行（`"hello\n\n"`）になる。一方、diff ヘッダ行（`--- ...`, `+++ ...`）には元の `\n` がないため単一改行のまま。

結果として、ヘッダ行と内容行の改行数が不揃いになり、diff 出力が視覚的に壊れる。これは既存コードのバグであり、package 化とは無関係だが、package 化後の完了条件にある「diff 表示」の正常動作に直結する。

修正パターン:
```python
original_lines = original_text.splitlines()  # keepends=False
working_lines = working_text.splitlines()
diff_lines = difflib.unified_diff(
    original_lines, working_lines,
    fromfile=..., tofile=...,
    lineterm="",
)
return "\n".join(diff_lines)
```

### Medium: write lock のスターロック（孤立ロック）が検出されない

**深刻度: 中**

`acquire_write_lock` は `O_CREAT | O_EXCL` で sidecar `.lock` ファイルを作成するが、前回プロセスがクラッシュした場合にロックファイルが残留する。ロックファイル内に PID を書き込んでいる（L857）が、その PID が現在も生存しているかの確認は行っていない。

結果として、クラッシュ後の再実行時に `WRITE_LOCK_TIMEOUT_SECONDS`（10 秒）待ったのち `ToolWriteError` で失敗する。ユーザーには「タイムアウト」としか表示されず、手動でロックファイルを削除する必要がある。

OCR 修正モードは長時間の API 呼び出しを含むため、タイムアウトやプロセス強制終了の可能性が高く、このスターロック問題は実運用で遭遇しやすい。

### Medium: turn loop に明示的なターン数上限がない

**深刻度: 中（意図しない動作）**

`run_ocr_correction_turn_loop` は `while True` で回り、`ToolCallBudget`（上限 12 回）で間接的に制限されている。しかし以下のエッジケースで想定以上のターン数が発生する:

1. モデルが 1 ターンに 1 tool call しか返さない場合、最大 13 ターン（12 ターン + 最終応答）の API 呼び出しが発生する。API コスト・時間の両面で想定と乖離しうる。
2. budget 消費は `read_tool_text_limited` / `write_tool_text_limited` 内で行われるが、ターン内で複数の function_call が返された場合、途中で budget が尽きると `OcrToolExecutionError` として raise される。この時点で前の tool call の書き込みは既にファイルに反映済みであり、部分的に適用された状態で停止する。

ターン数自体に明示的な上限（例: `MAX_TURNS = 6`）を設け、budget とは独立に制御すべき。

### Medium: `write_tool_text` が改行コードを暗黙的に LF へ正規化する

**深刻度: 低〜中**

`normalize_tool_text` は `\r\n` → `\n` 変換を行い、`write_tool_text` は `newline="\n"` で書き込む。Windows 環境で CRLF の Markdown ファイルを編集した場合、一度の write で全行の改行コードが LF に変換される。

Git の `core.autocrlf` 設定次第では、この変換自体が大きな差分として現れ、diff 出力が本来の OCR 修正箇所以外で大量の変更行を含むことになる。

実装案はこの改行正規化の影響を検討していない。

### Medium: 互換 shim の `sys.path` 操作が import 解決順序に依存する

**深刻度: 中**

実装案では shim が repo root を `sys.path` に追加して `pdf_converter.call_gemma4_gemini:main` を呼ぶとしている。しかし `python pdf_converter/call-gemma4-gemini.py` で起動すると、Python は自動的に `sys.path[0]` にスクリプトの親ディレクトリ（= `pdf_converter/`）を追加する。

shim が `sys.path.insert(0, repo_root)` しても、`pdf_converter/` が `sys.path` に残るため:
- `import pdf_converter` は repo_root が先にあるため package として解決される（意図通り）
- しかし `sys.path` に `pdf_converter/` が残っているため、`import pdf_converter` の代わりに `pdf_converter.py`（sibling file）が解決されるリスクが Python バージョンや import 実装の微妙な差異によって発生しうる

shim 内で `sys.path` から `pdf_converter/` を明示的に除去するか、`importlib` を使った明示的な import を行うのが安全。

### Low: project 名 `csv-viewer` と package 名 `pdf_converter` の不一致

**深刻度: 低（混乱の原因）**

`pyproject.toml` の `name = "csv-viewer"` に対して、install される package は `pdf_converter`。これは PyPI 命名規則上は問題ないが、`uv pip list` や `pip show` で表示される名前が `csv-viewer` であるのに、実際に import するのは `pdf_converter` であり、開発者の混乱を招く。

### Low: `.env` ロードが `load_dotenv` の自前実装に依存している

**深刻度: 低**

`call-gemma4-gemini.py` と `pdf_converter.py` の両方が独自の `load_dotenv` を持ち、値のクォート除去（`strip("'\"")`）などで微妙に挙動が異なる場合の検証がない。package 化を機に `python-dotenv` パッケージの採用、または共通ユーティリティへの統合を検討すべき。

### Low: `ToolCallBudget` の消費タイミングが実行前である

**深刻度: 低（意図しない動作）**

`read_tool_text_limited` / `write_tool_text_limited` は `budget.consume()` を **実行前** に呼ぶ。つまり tool call の実行が失敗しても budget は消費済みになる。これは「失敗した tool call も回数にカウントする」という設計判断として意図的かもしれないが、実装案ではこの挙動について言及がない。

もし失敗時にも消費する意図であれば問題ないが、意図せずそうなっている場合はバグ。

### 総合評価

実装案の方向性（package 化 + entry point）は正しいが、以下の対応が実装前に必要:

1. build backend を `uv_build` に変更するか、`setuptools` 採用時は `[tool.setuptools.packages.find]` を明示する
2. `__file__` ベースのパス解決の限界を認識し、非 editable install 時の対策を決める
3. `pdf_converter/pdf_converter.py` のモジュールレベル副作用を `if __name__ == "__main__":` でガードする
4. 存在しないテストファイルの参照を修正し、テスト作成タスクを追加する
5. `httpx` を `pyproject.toml` の `dependencies` に追加する
6. `build_unified_diff_text` の diff 出力バグを修正する
