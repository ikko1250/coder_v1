# タスク分解: `pdf_converter` 手作業ディレクトリ分離

- 作成日: 2026-04-22
- 前提設計: [docs/pdf-converter-manual-dir-split-design-revised.md](/mnt/f/program_2026/csv_viewer_v2/coder_v1/docs/pdf-converter-manual-dir-split-design-revised.md)

---

## 1. 現状の既存挙動

- `pdf_converter` は `asset/texts_2nd/manual/` を単一 root 前提で参照している。
- analysis DB builder は `--input-dir` 配下を `os.walk()` で再帰走査する。
- builder は OCR 用ディレクトリの禁止判定を持っていない。
- Rust UI も入力フォルダーの存在確認しかしていない。
- `work/` は seed asset ではなく runtime workspace として使われている。

---

## 2. タスク分解の方針

- Phase 1 では「誤入力防止」と「新旧 path 互換」を先に固める。
- その後に `pdf_converter` 側の canonical path 変更と UI 反映を行う。
- 最後に `pdf/` と `md/` の実ディレクトリ移設を行う。
- `work/` は移設対象ではなく、再生成前提の運用整理に留める。

---

## 3. 実装タスク

### T-01. builder 禁止判定の仕様をコードへ導入 (完了)

- 目的: OCR 用ディレクトリの誤入力を preflight で止める。
- 対象:
  - `docs/build_ordinance_analysis_db.py`
- 内容:
  - 禁止ディレクトリ一覧を定義する
  - `input-dir` と禁止ディレクトリの関係を `same` / `parent` / `child` で判定する
  - 該当時は build 開始前に failure を返す
- 受け入れ条件:
  - `asset/ocr_manual`
  - 旧 `asset/texts_2nd/manual`
  - その親
  - その子
  のすべてで preflight failure になる

### T-02. `os.walk()` 側で禁止ディレクトリを prune (完了)

- 目的: validation 抜け漏れ時でも OCR 用ファイル混入を防ぐ。
- 対象:
  - `docs/build_ordinance_analysis_db.py`
- 内容:
  - `load_source_rows_from_dir()` で禁止ディレクトリ配下を再帰走査対象から除外する
  - 判定は絶対パスベースで行う
- 受け入れ条件:
  - 禁止ディレクトリが配下に存在しても source row に入らない

### T-03. `--limit` を有効候補ベースへ変更 (完了)

- 目的: OCR 用ファイル混入や不正ファイルで件数契約が歪むのを防ぐ。
- 対象:
  - `docs/build_ordinance_analysis_db.py`
- 内容:
  - 走査直後ではなく、有効候補抽出後に `limit` を適用する
  - 「有効候補」の定義をコードコメントとテストで固定する
- 受け入れ条件:
  - filename 不正や禁止ディレクトリ混入で `limit` 消費が起きない

### T-04. builder テスト追加 (完了)

- 目的: builder 側の再発防止仕様を固定する。
- 対象:
  - `tests/test_build_ordinance_analysis_db.py`
- 内容:
  - 禁止ディレクトリ完全一致
  - 親ディレクトリ誤選択
  - 子ディレクトリ誤選択
  - prune
  - `limit` 契約
  をテスト追加する
- 受け入れ条件:
  - 上記ケースを単体テストで再現できる

### T-05. `project_paths.py` を candidate root 方式へ変更 (完了)

- 目的: root 解決と file-aware fallback を分離する。
- 対象:
  - `pdf_converter/project_paths.py`
- 内容:
  - project root 判定を `pdf_converter/` と `asset/` ベースへ整理する
  - OCR manual root 候補列挙関数を追加する
  - root 解決と対象ファイル存在確認の責務を分ける
- 受け入れ条件:
  - 新 root のみ
  - 旧 root のみ
  - 新旧併存
  で root 候補を扱える

### T-06. `call_gemma4_gemini.py` の file-aware fallback 実装 (完了)

- 目的: 中途移行状態でも Markdown / work / pdf の解決を壊さない。
- 対象:
  - `pdf_converter/call_gemma4_gemini.py`
- 内容:
  - 読み取り系は候補 root を見て対象ファイルが存在する側を使う
  - 書き込み既定先は canonical root に固定する
  - help / tool schema / validation message を `asset/ocr_manual/` 基準に更新する
- 受け入れ条件:
  - `pdf` は新 root、`md` は旧 root でも読み取りが壊れない
  - 新規 write は canonical root 側へ行く

### T-07. `pdf_converter.py` の既定 PDF path を resolver ベースへ変更 (完了)

- 目的: path 名だけでなく CWD 依存も減らす。
- 対象:
  - `pdf_converter/pdf_converter.py`
- 内容:
  - 既定 PDF path を文字列定数から resolver ベースに変更する
  - 必要なら import 依存を整理する
- 受け入れ条件:
  - 既定 path が `asset/ocr_manual/pdf/...` を正しく指す
  - CWD に依存しない

### T-08. `pdf_converter` 系テスト更新 (完了)

- 目的: canonical path 変更と fallback 仕様を固定する。
- 対象:
  - `tests/test_call_gemma4_gemini_project_paths.py`
  - `tests/test_call_gemma4_gemini_cli_arg_compatibility.py`
  - `tests/test_call_gemma4_gemini_tool_paths.py`
  - `tests/test_call_gemma4_gemini_write_matching.py`
  - `tests/test_call_gemma4_gemini_tool_call_logging.py`
- 内容:
  - 新 root 前提へ更新する
  - file-aware fallback のケースを追加する
- 受け入れ条件:
  - 新旧 path 並行期間の期待挙動がテストで表現される

### T-09. Rust UI に禁止判定を追加 (完了)

- 目的: 実行前に誤選択を即時に知らせる。
- 対象:
  - `src/app_builder_settings.rs`
  - `src/app_analysis_job.rs`
- 内容:
  - Python builder と同じ親子関係判定を Rust 側にも持たせる
  - 設定画面と job 開始前の両方で再検証する
  - 文言も `asset/ocr_manual/` 基準へ更新する
- 受け入れ条件:
  - 禁止ディレクトリ選択時に UI 上で即時エラーが出る
  - build 開始時にも同じ条件で弾かれる

### T-10. `pdf/` と `md/` の実ディレクトリ移設 (完了)

- 目的: canonical 配置を実体として反映する。
- 対象:
  - `asset/texts_2nd/manual/pdf`
  - `asset/texts_2nd/manual/md`
  - `asset/ocr_manual/pdf`
  - `asset/ocr_manual/md`
- 内容:
  - tracked な初期資産だけを新 root へ移す
  - 旧 path fallback が残っている状態で移行する
- 受け入れ条件:
  - `pdf` / `md` の canonical 実体が新 root 側になる

### T-11. `work/` の運用整理 (完了)

- 目的: runtime workspace を seed asset と混同しない。
- 対象:
  - docs
  - 必要なら `.gitignore` 周辺の運用
- 内容:
  - `work/` を再生成前提と明記する
  - 持ち上げる対象は working markdown の個別ファイルだけに限定する
  - `logs` や lock file は移設対象にしない
- 受け入れ条件:
  - `work/` を丸ごと rename する運用が設計から外れる

### T-12. docs 整合更新 (完了)

- 目的: 設計と実装の契約を揃える。
- 対象:
  - 関連 design docs
  - CLI help 前提を書いている docs
- 内容:
  - `asset/texts_2nd/manual/` 前提記述を更新する
  - fallback 期間の扱いを明記する
- 受け入れ条件:
  - docs 間で canonical path が一致する

---

## 4. 推奨実施順

1. T-01 builder 禁止判定
2. T-02 prune
3. T-03 `limit` 契約修正
4. T-04 builder テスト
5. T-05 candidate root 化
6. T-06 `call_gemma4_gemini.py` fallback 実装
7. T-07 `pdf_converter.py` resolver 化
8. T-08 `pdf_converter` 系テスト
9. T-09 Rust UI 禁止判定
10. T-10 `pdf/` `md/` 移設
11. T-11 `work/` 運用整理
12. T-12 docs 整合更新

---

## 5. ユーザー承認単位の推奨分割

### タスク束 A: builder 安全化

- T-01
- T-02
- T-03
- T-04

### タスク束 B: `pdf_converter` path 移行

- T-05
- T-06
- T-07
- T-08

### タスク束 C: UI と資産整理

- T-09
- T-10
- T-11
- T-12

---

## 6. コミットメッセージ案

- `Reject OCR workspace in analysis DB builder`
- `Add file-aware OCR manual root resolution`
- `Move OCR seed assets to asset/ocr_manual`
- `Validate OCR workspace paths in builder UI`

