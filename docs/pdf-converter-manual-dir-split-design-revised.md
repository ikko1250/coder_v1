# 設計書: `pdf_converter` 手作業ディレクトリ分離 改訂版

- 作成日: 2026-04-22
- 対象: `pdf_converter/`, `asset/`, `docs/build_ordinance_analysis_db.py`, Rust GUI builder 設定
- 関連: `pdf_converter/project_paths.py`, `pdf_converter/pdf_converter.py`, `pdf_converter/call_gemma4_gemini.py`, `src/app_builder_settings.rs`, `src/app_analysis_job.rs`, `src/analysis_runner.rs`
- 位置付け: [docs/pdf-converter-manual-dir-split-design.md](/mnt/f/program_2026/csv_viewer_v2/coder_v1/docs/pdf-converter-manual-dir-split-design.md) の改訂版

---

## 1. 結論

`asset/texts_2nd/manual/` は analysis DB 入力と同居させず、`pdf_converter` 専用の OCR 手作業ワークスペースとして `asset/ocr_manual/` へ分離する。

ただし、単なる配置換えでは不十分である。再発防止のため、以下を同時に成立させる。

1. `pdf_converter` 系の canonical path を `asset/ocr_manual/` に統一する
2. analysis DB builder は OCR 用ディレクトリとの完全一致・親・子のすべてを拒否する
3. `os.walk()` 側でも OCR 用ディレクトリを明示的に prune する
4. `--limit` は「有効候補」に対してのみ適用する
5. 旧 path fallback は root 存在判定ではなく、必要サブディレクトリまたは対象ファイル単位で判定する
6. `work/` は seed asset ではなく runtime workspace として扱い、移行時は原則再生成前提にする

---

## 2. 背景

### 2.1 現状の既存挙動

- `pdf_converter/pdf_converter.py` は既定 PDF パスとして `asset/texts_2nd/manual/pdf/...` を文字列定数で保持している。
- `pdf_converter/call_gemma4_gemini.py` の OCR correction mode は、`asset/texts_2nd/manual/md` と `asset/texts_2nd/manual/work` を前提に read / write / validation / help text を構成している。
- `pdf_converter/project_paths.py` は source tree 判定に `asset/texts_2nd/manual/` の存在を要求している。
- `docs/build_ordinance_analysis_db.py` は `--input-dir` 配下を `os.walk()` で再帰走査し、`.txt` / `.md` を入力候補として列挙する。
- 現在の builder は OCR 作業用ディレクトリを特別扱いしておらず、GUI 側でも入力ディレクトリ禁止判定を持っていない。
- `load_source_rows_from_dir()` では候補一覧に対して `limit` が早い段階で適用されるため、非意図ファイル混入時に件数契約が崩れる余地がある。

### 2.2 問題

- `asset/texts_2nd/` 配下に OCR 用 `manual/` があるため、analysis 用テキスト領域と OCR 作業領域の責務が衝突している。
- builder に親ディレクトリを指定した場合、OCR 用 Markdown が再帰走査に混入しうる。
- `limit` が混入ファイルで先食いされると、正しい analysis 入力の一部が silently drop される。
- 新旧 path 並行期間に「新 root だけ空で先に作られる」などの中途移行状態が起こると、単純な root 優先 fallback は壊れやすい。
- `work/` は作業コピー、log、lock file を含む runtime workspace であり、`pdf/` や `md/` と同列の移行資産として扱うと stale file を持ち込みやすい。

---

## 3. 目的

1. OCR 用ワークスペースと analysis 入力領域をディレクトリ責務で分離する。
2. builder から OCR 用ファイルが混入する経路を設計段階で閉じる。
3. 新旧 path 並行期間でも中途移行に耐える path 解決仕様を定義する。
4. `--limit` を含む analysis DB builder の件数契約を明確にする。
5. GUI / CLI の双方で誤選択を早期に検出できるようにする。

## 4. 非目標

- OCR 補正ルール自体の変更
- builder のファイル名規約変更
- `output/` の役割変更
- OCR working file の履歴保存戦略の設計

---

## 5. 改訂後レイアウト

```text
asset/
  ocr_manual/
    pdf/
    md/
    work/
      logs/
  texts_2nd/
    official_processed_renamed/
    out_db_processed/
    out_pdfplumber_processed/
    ...
```

### 5.1 ルール

- `asset/ocr_manual/`
  - `pdf_converter` 専用
  - OCR 原本 PDF、OCR Markdown、working copy、tool-call log を扱う
- `asset/texts_2nd/`
  - analysis DB builder の入力候補だけを置く
  - OCR 作業用ディレクトリや runtime workspace を置かない

---

## 6. 設計方針

### 6.1 canonical path

`pdf_converter` 系の canonical path は `asset/ocr_manual/` とする。

変更対象:

- `resolve_manual_root()` の返り先
- `pdf_converter.py` の既定 PDF path
- `call_gemma4_gemini.py` の help / error / tool schema / working-dir default
- 関連テスト
- 関連 docs

### 6.2 builder の禁止判定

初版案の「完全一致のみ拒否」は採用しない。拒否条件は次の 3 種類をすべて含める。

1. 入力ディレクトリが禁止ディレクトリそのもの
2. 入力ディレクトリが禁止ディレクトリの親
3. 入力ディレクトリが禁止ディレクトリの子

禁止対象:

- `asset/ocr_manual`
- 互換期間中のみ `asset/texts_2nd/manual`

判定は `Path.resolve()` 後の絶対パスで行う。

判定関数案:

```python
def classify_forbidden_input_relation(input_dir: Path, forbidden_dir: Path) -> str | None:
    if input_dir == forbidden_dir:
        return "same"
    if forbidden_dir in input_dir.parents:
        return "child"
    if input_dir in forbidden_dir.parents:
        return "parent"
    return None
```

### 6.3 `os.walk()` 側の prune

preflight だけでは将来の validation 抜け漏れを防げないため、走査側でも禁止ディレクトリを prune する。

仕様:

- `os.walk(input_dir, followlinks=False)` 中に、`dirs[:]` を書き換えて禁止ディレクトリ名を除外する
- prune 判定は名前一致ではなく絶対パス一致で行う
- preflight で本来は reject 済みのはずだが、保険として残す

これにより、将来別経路から `load_source_rows_from_dir()` が直接使われても被害を抑えられる。

### 6.4 `--limit` の件数契約

`--limit` は「走査候補数」ではなく「有効候補数」に対して適用する。

ここでいう有効候補とは、少なくとも以下を満たしたファイルである。

- 禁止ディレクトリ配下ではない
- 拡張子が `.txt` または `.md`
- ファイル名規約に一致する
- preflight error の対象になっていない

処理順:

1. 走査
2. 禁止ディレクトリ prune
3. 拡張子フィルタ
4. filename validation
5. valid source row 化
6. ソート
7. `limit` 適用

禁止ディレクトリ混入は warning 継続ではなく preflight failure とする。  
`limit` は failure 回避のための救済措置に使わない。

### 6.5 fallback 方針

旧 path fallback は root 存在だけで切り替えない。必要単位で判定する。

原則:

- `pdf/`, `md/`, `work/` の各サブディレクトリは独立に解決可能とする
- 対象ファイルを読む場面では、「そのファイルが存在する側の root」を使う
- working-dir の既定値は canonical root 側に寄せるが、明示指定 path はそのまま優先する

例えば `md` は旧 root にだけ存在し、`pdf` は新 root にだけ存在する中途状態を許容する。

#### 6.5.1 resolver の考え方

`project_paths.py` は root 妥当性判定と、実ファイルの存在確認を分ける。

- project root 判定:
  - `pdf_converter/` と `asset/` があること
  - OCR manual root 自体はこの段階では必須にしない
- OCR manual path 解決:
  - `resolve_manual_root()` は canonical root を返す
  - 旧 path fallback が必要な処理では、別関数で候補群を列挙する

候補列挙関数案:

```python
def resolve_manual_root_candidates(project_root: Path) -> list[Path]:
    return [
        project_root / "asset" / "ocr_manual",
        project_root / "asset" / "texts_2nd" / "manual",
    ]
```

読み取り系は候補 root を順に見て「必要対象が存在する root」を選ぶ。  
書き込み系の既定先は canonical root のみとする。

### 6.6 `pdf_converter.py` の既定 path

単なる文字列定数置換は採用しない。`pdf_converter.py` の既定 PDF path は resolver ベースで組み立てる。

理由:

- 現状は CWD 依存の相対文字列で脆い
- 今回 path 設計を見直すなら、ここだけ旧方式を残す理由が弱い

設計:

- 既定 path は `resolve_manual_root()` から `pdf/根室市_条例.pdf` を組み立てる
- 既定値を argparse に渡す時点で文字列化する

### 6.7 UI 側の禁止判定

UI は文言追加だけで終わらせない。Rust 側にも Python builder と同じ禁止判定を持たせる。

対象:

- `src/app_builder_settings.rs`
- `src/app_analysis_job.rs`

役割:

- フォルダー選択時または build 開始前に即時エラー表示
- Python 側は最終防衛線として同じ判定を保持

これにより、ユーザー体験を「実行してから怒られる」から「選択時点で止める」へ変える。

### 6.8 `work/` の扱い

`work/` は移行資産ではなく runtime workspace として扱う。

原則:

- `pdf/` と `md/` は seed asset として移設対象
- `work/` は原則として再生成前提
- `logs/`, `lock file`, stale working markdown は canonical 資産として移行しない

例外:

- ユーザーが保持したい特定 working Markdown が明示的に必要な場合のみ、個別に持ち上げる

設計上は「`work/` を丸ごと rename する」案は採用しない。

---

## 7. 実装フェーズ

### Phase 1

- canonical path を `asset/ocr_manual/` に変更
- 新旧 root 候補の file-aware fallback を導入
- builder に親子関係込みの禁止判定を追加
- `os.walk()` prune を追加
- `--limit` を有効候補ベースへ変更
- GUI に同等の禁止判定を追加
- `pdf_converter.py` の既定 path を resolver ベースへ変更

### Phase 2

- 旧 `asset/texts_2nd/manual/` fallback を削除
- docs と tests の旧 path 前提を除去
- 必要なら `resolve_manual_root()` の命名見直しを行う

---

## 8. 変更対象

### 8.1 Python

- `pdf_converter/project_paths.py`
  - project root 判定と OCR manual root 候補解決の分離
- `pdf_converter/pdf_converter.py`
  - 既定 PDF path を resolver ベースへ変更
- `pdf_converter/call_gemma4_gemini.py`
  - read / write / validation / help / fallback の改修
- `docs/build_ordinance_analysis_db.py`
  - 親子関係込みの禁止判定
  - `os.walk()` prune
  - `limit` 適用順の変更

### 8.2 Rust

- `src/app_builder_settings.rs`
  - 文言更新
  - 禁止フォルダー即時エラー表示
- `src/app_analysis_job.rs`
  - build 開始前 validation
- 必要なら `src/analysis_runner.rs`
  - エラー表示契約の確認

### 8.3 テスト

- `tests/test_call_gemma4_gemini_project_paths.py`
- `tests/test_call_gemma4_gemini_cli_arg_compatibility.py`
- `tests/test_call_gemma4_gemini_tool_paths.py`
- `tests/test_call_gemma4_gemini_write_matching.py`
- `tests/test_call_gemma4_gemini_tool_call_logging.py`
- `tests/test_build_ordinance_analysis_db.py`

---

## 9. テスト観点

### 9.1 builder

1. `input-dir == asset/ocr_manual` で preflight failure
2. `input-dir` が `asset/ocr_manual` の親で preflight failure
3. `input-dir` が `asset/ocr_manual` の子で preflight failure
4. prune により禁止ディレクトリ配下が走査対象に入らない
5. `limit` が有効候補ベースで数えられる
6. parent 誤選択ケースで件数が歪まない

### 9.2 pdf_converter path 解決

1. 新 root のみ存在
2. 旧 root のみ存在
3. 新旧 root 並存
4. `pdf` だけ新 root、`md` は旧 root
5. canonical 書き込み先は新 root のみ

### 9.3 UI

1. 禁止ディレクトリ選択時に即時エラーになる
2. build 開始ボタン押下時にも再検証される
3. 正常ディレクトリでは既存 UI 挙動を壊さない

### 9.4 `work/`

1. `work/` 未存在でも実行に必要なとき再生成できる
2. 旧 `work/` が残っていても canonical seed asset として扱わない

---

## 10. リスクと対策

### R-1. fallback が複雑化する

対策:

- root 判定と file-aware fallback を明確に分離する
- 読み取り系だけ fallback を許し、書き込み既定先は canonical root に固定する

### R-2. UI と Python で禁止判定がずれる

対策:

- 判定仕様を設計書で固定する
- Rust 側は同じ親子関係ロジックを持つ
- 受け入れテストで両者の期待を揃える

### R-3. `work/` を再生成前提にすると手元の作業中ファイルを失う恐れがある

対策:

- 設計上は canonical 移行対象から外す
- 必要ファイルはユーザー判断で個別退避する運用を明記する

### R-4. `project_paths.py` の root 判定緩和で失敗が後段にずれる

対策:

- root 判定は最小要件だけに留める
- 実処理前に対象サブディレクトリ単位で明示検証する
- エラーメッセージは「root 不正」と「対象ファイル不在」を分ける

---

## 11. 受け入れ条件

- `pdf_converter` の canonical path が `asset/ocr_manual/` になる
- builder が OCR 用ディレクトリとの完全一致・親・子を拒否する
- 走査中も禁止ディレクトリが prune される
- `--limit` が有効候補ベースで適用される
- fallback が file-aware に動作し、中途移行状態でも壊れにくい
- Rust UI でも禁止ディレクトリを即時検出できる
- `work/` が seed asset として自動移行されない

---

## 12. 実装タスク案

1. `project_paths.py` に candidate root / file-aware fallback を追加する
2. `pdf_converter.py` の既定 PDF path を resolver ベースへ寄せる
3. `call_gemma4_gemini.py` の manual root 依存箇所を読み取り fallback / 書き込み canonical に分解する
4. builder に親子関係込みの禁止判定と `os.walk()` prune を追加する
5. `limit` の適用順を有効候補ベースへ変更する
6. Rust UI に同等の禁止判定を追加する
7. `pdf/` と `md/` だけを新 root へ移設し、`work/` は再生成前提にする
8. テストと docs を更新する

---

## 13. 実装確認結果

以下 5 点はすべて実装済みです。

1. builder 禁止判定を親子関係込みで固定してよいか → 実装済み（T-01, T-02, T-04）
2. `--limit` を有効候補ベースで数える仕様でよいか → 実装済み（T-03, T-04）
3. fallback を file-aware にする実装コストを許容するか → 実装済み（T-05, T-06, T-07）
4. Rust UI にも同じ禁止判定を載せるか → 実装済み（T-09）
5. `work/` は canonical 資産ではなく再生成前提でよいか → 実装済み（T-10, T-11）
