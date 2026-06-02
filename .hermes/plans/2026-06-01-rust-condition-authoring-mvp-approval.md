# Rust Condition Authoring MVP 要件定義・実装承認案

> **For Hermes:** 実装に進む場合は `subagent-driven-development` skill に従い、task-by-task で delegate_task 実装 + 仕様レビュー + 品質レビューを行う。

**Goal:** Python 側に追加済みの `condition-authoring/v1` 条件ファイルを、Rust GUI から安全に選択・検証・分析実行できるようにする。

**Architecture:** Rust は authoring compiler を再実装せず、Python の `analysis_backend.condition_authoring_cli` を唯一の変換・検証実装として呼び出す。既存 runtime JSON / 条件エディタ / worker 分析プロトコルは維持し、authoring JSON は分析実行直前に一時 runtime JSON へ compile して worker へ渡す。

**Tech Stack:** Rust 2021, eframe/egui, serde_json, std::process::Command, Python `analysis_backend.condition_authoring_cli`, 既存 `analysis_runner` worker protocol。

**Review Status:** 設計セカンドオピニオン 1回目で `REQUEST_CHANGES`。Critical/Important 指摘を本版へ反映済み。

---

## 1. 背景

Python 側では `condition-authoring/v1` が実装済みで、AI/人間が読み書きしやすい JSON を既存 runtime schema に compile できる。
Rust GUI 側は現時点では `asset/cooccurrence-conditions.json` 相当の runtime JSON を前提にしている。

今回の MVP は、GUI から authoring JSON を「条件ファイル」として使えるようにすることが目的であり、authoring 専用 GUI editor を作ることではない。

---

## 2. スコープ

### 2.1 MVP でやること

1. 条件ファイル種別の自動判定
2. authoring JSON の Python CLI validation / compile bridge
3. 分析実行時に authoring JSON を一時 runtime JSON へ compile
4. compile error/warning を GUI に表示
5. 既存 runtime condition editor で authoring JSON を誤保存しない制御
6. runtime JSON は従来どおり動作させる
7. worker/export/session cache が generated runtime JSON と source authoring JSON の関係を破綻なく扱う

### 2.2 MVP でやらないこと

1. Rust 側 authoring compiler の再実装
2. authoring JSON の GUI 編集
3. YAML 対応
4. `text_not_any` 対応
5. 既存 runtime schema の変更
6. Python worker protocol の大幅変更
7. 既存 condition editor を authoring schema 対応に拡張すること
8. compile cache の高度な再利用・世代管理

---

## 3. 主要要件

### R1. 条件ファイル種別判定

Rust は条件ファイルを read-only で軽量に読み、top-level JSON から種別を判定する。

- `format == "condition-authoring/v1"` → `AuthoringV1`
- top-level に `cooccurrence_conditions` がある、または既存 `FilterConfigDocument` として deserialize 可能 → `Runtime`
- `.yaml` / `.yml` → `UnsupportedYaml`
- JSON parse 失敗 / 不明形式 → `Invalid`

受け入れ条件:

- runtime JSON は従来どおり runtime と判定される
- authoring JSON は runtime editor に直接渡されない
- YAML は v1 非対応として明示エラー
- 判定に失敗してもファイル内容は変更されない
- 判定は compiler ではなく safety guard であり、authoring schema の詳細検証は Python compiler に委ねる

### R2. Python authoring bridge

Rust は `analysis_backend.condition_authoring_cli` を `python -m` で呼び出す bridge を持つ。

入力:

- authoring JSON path
- output runtime JSON path
- issues JSON path
- `AnalysisRuntimeConfig` の `python_command`, `python_args`, `project_root`

出力:

- compiled runtime JSON path
- issues list
- exit status
- stderr/stdout summary

CLI 呼び出し例:

```bash
python -m analysis_backend.condition_authoring_cli \
  --input path/to/authoring.json \
  --output runtime/compiled-conditions/<stable-key>.runtime.json \
  --issues-json runtime/compiled-conditions/<stable-key>.issues.json
```

受け入れ条件:

- Python 実行環境は既存 `resolve_python_command()` の結果を使う
- `CSV_VIEWER_PROJECT_ROOT` は `runtime.project_root` に合わせる
- CLI exit code != 0 または issue severity error がある場合、分析を開始しない
- warning のみなら分析実行可
- Python compiler が未対応フィールド（例: `text_not_any`）を検知して warning/error を出すことを前提にする。Rust は schema 詳細を再実装しない
- stdout/stderr はエラーメッセージに含めるが、長すぎる場合は 4096 文字程度に切り詰める
- Windows 上でも `std::process::Command` に `PathBuf` / `OsString` を args として渡し、shell 文字列連結は避ける

### R3. 一時 runtime JSON 生成

分析実行時、選択中 filter config が authoring の場合だけ compile する。
生成ファイルは元 authoring JSON を上書きしない。

出力先は `cleanup_job_directories()` の対象外にするため、`runtime/jobs/` ではなく以下に固定する。

```text
runtime/compiled-conditions/<stable-key>.runtime.json
runtime/compiled-conditions/<stable-key>.issues.json
```

`stable-key` は少なくとも以下を含む hash 由来とする。

- canonicalized authoring file absolute path
- authoring file content
- Rust bridge version string（例: `rust-authoring-bridge/v1`）

MVP では cache 再利用は任意。単純に毎回 compile して同じ path を上書きしてもよい。ただしファイル名衝突を避ける。

受け入れ条件:

- authoring JSON は変更されない
- compile 成功後、worker へ渡す `AnalysisRuntimeConfig.filter_config_path` は generated runtime JSON
- `WorkerRuntimeFingerprint` は変更しない。worker はリクエストごとに `filter_config_path` を受け取るため、プロセス再利用は許容する
- `ensure_worker()` の前に runtime clone の `filter_config_path` が generated runtime JSON へ差し替わっていること
- GUI/履歴表示では元 authoring path と generated runtime path の対応が分かる
- session cache key は generated runtime JSON の内容変更時に無効化される。authoring JSON の変更は compile 結果に反映されれば自然に検知される
- generated runtime JSON の親ディレクトリがない場合は自動作成し、失敗時は blocking error

### R4. source path / effective path のモデル

MVP では `AnalysisRuntimeConfig` に source path を追加し、worker が読む effective path と UI 表示上の source path を区別する。

```rust
pub(crate) struct AnalysisRuntimeConfig {
    // 既存 fields ...
    pub(crate) filter_config_path: PathBuf,              // worker が読む effective path
    pub(crate) filter_config_source_path: Option<PathBuf>, // authoring JSON 元ファイル。runtime JSON の場合 None
}
```

受け入れ条件:

- runtime JSON の場合: `filter_config_path` は元 runtime JSON、`filter_config_source_path == None`
- authoring JSON の場合: compile 前の base runtime では `filter_config_path` が source authoring path を指してよいが、分析ジョブへ渡す runtime clone では `filter_config_path` を generated runtime JSON に差し替え、`filter_config_source_path == Some(source authoring path)` にする
- `AnalysisMeta.filter_config_path` は worker 互換性のため generated runtime JSON のままでもよい
- Rust UI の status/export context 表示では `filter_config_source_path.unwrap_or(filter_config_path)` を優先表示する
- worker fingerprint は source path / effective path を含めない方針を維持する

### R5. warning/error 表示

compiler issue は既存 `AnalysisWarningMessage` 相当へ変換して表示する。

受け入れ条件:

- error は分析開始前に blocking error として表示
- warning は `AnalysisWarningMessage` 形式へ変換し、分析完了後の warning window に統合表示する
- warning window 統合が難しい場合でも、MVP では少なくとも `analysis_runtime_state.last_warnings` に入れてユーザーが確認できること
- issue code / message / severity / condition_id / field_name を失わない
- Python worker 由来の warning と区別できるよう、authoring compiler issue は code prefix または scope で識別する
- 変換不能な issue は `code: "authoring_compiler"`, `message: <原文>` で fallback 表示する

### R6. 既存 condition editor との関係

既存 condition editor は runtime JSON 専用として維持する。

受け入れ条件:

- runtime JSON の場合は従来どおり開ける・保存できる
- `open_condition_editor()` / ファイル選択 commit 前に `detect_condition_config_format()` を実行する
- authoring JSON の場合、既存 condition editor は開かず説明メッセージを出す
- メッセージ例: `authoring JSON は条件エディタで開けません。分析実行時に自動的に runtime JSON に compile されます。`
- authoring JSON を runtime editor で deserialize/save して壊さない
- ファイル選択 UI では authoring JSON を条件ファイルとして選択可能

### R7. export / cache / status の整合性

分析結果の export や cache は、実際に worker が使った generated runtime JSON と、ユーザーが選択した source authoring JSON の関係を保持する。

受け入れ条件:

- export job が必要とする runtime config path は existing worker が読める generated runtime JSON
- export/status でユーザーに見せる path は source authoring path を優先する
- session cache は generated runtime JSON の SHA256 で判定し、compile 結果が変わらない authoring 変更ではキャッシュヒットしてよい
- authoring source path と generated runtime path の両方をログに残す

---

## 4. 推奨ファイル構成

```text
src/condition_config_format.rs
  - ConditionConfigFormat enum
  - detect_condition_config_format(path)
  - JSON top-level 判定

src/condition_authoring_bridge.rs
  - AuthoringCompileRequest
  - AuthoringCompileResult
  - AuthoringIssue DTO
  - compile_authoring_to_runtime(runtime, source_path, output_dir)
  - issue severity 判定
  - stable-key / output path builder

src/analysis_runner.rs
  - AnalysisRuntimeConfig に filter_config_source_path を追加
  - build_runtime_config() では source path None を初期化
  - tests で worker request が effective path を使うことを維持

src/app_analysis_job.rs
  - start_analysis_job_with_mode() で compile gate を通す
  - runtime clone の filter_config_path を generated runtime JSON に差し替える
  - warning/status へ issue を反映
  - export/status 表示は source path 優先

src/app_condition_editor.rs
  - authoring JSON の場合は editor を開かず説明

src/app.rs
  - 新 module 宣言
```

---

## 5. エラー方針

| ケース | 扱い |
|---|---|
| `.yaml` / `.yml` | v1 非対応 error |
| authoring JSON parse error | blocking error |
| authoring compiler error issue | blocking error |
| authoring compiler warning issue | 分析可、warning 表示 |
| Python CLI 起動失敗 | blocking error |
| generated runtime JSON 書込失敗 | blocking error |
| generated runtime JSON 親ディレクトリ作成失敗 | blocking error |
| authoring JSON を condition editor で開こうとした | 説明メッセージ、editor は開かない |
| runtime JSON | 従来どおり |

---

## 6. テスト方針

Rust unit tests:

- format detection
- YAML unsupported
- authoring bridge command building / issue parsing
- generated output path hashing（canonicalized path + content）
- runtime config source/effective path
- runtime editor guard
- cache key が generated runtime JSON content を見ること

Integration-ish tests:

- fake Python script を一時 `.py` ファイルとして書き出し、既存 python command 経由で bridge success / warning / error を検証
- Windows 対応のため shebang 依存や shell 文字列連結を避ける
- `cargo test`
- 必要なら `cargo run -- --ipc-dto-self-check` は影響なし確認

Python tests:

- 既存 authoring tests は Rust 側変更後も通す

---

## 7. 設計セカンドオピニオン反映メモ

反映済み:

- generated runtime JSON 出力先を `runtime/jobs/condition-authoring/` から `runtime/compiled-conditions/` へ変更
- `cleanup_job_directories()` 対象外を明記
- `WorkerRuntimeFingerprint` は変更不要。ただし `ensure_worker()` 前に effective path 差し替えを必須化
- session cache は generated runtime JSON content ベースと明記
- `AnalysisRuntimeConfig.filter_config_source_path` 追加方針を決定
- condition editor guard を deserialize error ではなく事前 format detection に変更
- Windows path / fake Python script test / stdout-stderr truncation を明記
- authoring warning を `AnalysisWarningMessage` に変換する方針を明記

---

## 8. 細分タスク案

### Task 1: 条件ファイル種別判定モジュールを追加

**Objective:** authoring/runtime/YAML/invalid を read-only に判定する。

**Files:**
- Create: `src/condition_config_format.rs`
- Modify: `src/app.rs` module declaration

**Acceptance Criteria:**
- `ConditionConfigFormat::{Runtime, AuthoringV1, UnsupportedYaml, Invalid}` 相当を定義
- `detect_condition_config_format(path: &Path) -> Result<ConditionConfigFormat, String>` を追加
- `format == "condition-authoring/v1"` を authoring と判定
- `cooccurrence_conditions` を含む JSON を runtime と判定
- 既存 `FilterConfigDocument` として deserialize 可能な JSON も runtime と判定
- `.yaml` / `.yml` は unsupported
- JSON parse error は invalid として、呼び出し側が表示可能な message を保持
- ファイルは一切変更しない

**Tests:**
- authoring JSON 判定
- runtime JSON 判定（`cooccurrence_conditions`）
- runtime JSON 判定（`FilterConfigDocument` deserialize）
- YAML unsupported
- invalid JSON
- missing file error

### Task 2: Authoring issue DTO と warning 変換を追加

**Objective:** Python CLI の issues JSON を Rust 側で parse し、既存 `AnalysisWarningMessage` に変換できるようにする。

**Files:**
- Create: `src/condition_authoring_bridge.rs`
- Modify: `src/app.rs` module declaration
- Modify if needed: `src/analysis_runner.rs` visibility / DTO reuse

**Acceptance Criteria:**
- Python `condition_authoring_cli` の issues JSON shape を確認し、既存 `AnalysisWarningMessage` と一致する範囲は再利用する
- 必要なら `AuthoringIssue` を thin wrapper として定義する。Rust 側で独自 schema を厚く再定義しない
- `severity`, `code`, `message`, `condition_id`, `field_name` 相当を保持
- unknown fields を無視して forward-compatible にする
- severity error 判定 helper を追加
- warning issue を `AnalysisWarningMessage` へ変換
- 変換不能時は `code = "authoring_compiler"` fallback

**Tests:**
- issues JSON parse
- error severity detection
- warning conversion
- missing optional fields
- unknown fields ignored
- `AnalysisWarningMessage` 直接再利用可否を確認するテストまたはコメント

### Task 3: compiled runtime output path builder を追加

**Objective:** generated runtime JSON / issues JSON の安定した出力先を作る。

**Files:**
- Modify: `src/condition_authoring_bridge.rs`

**Acceptance Criteria:**
- 出力先は `runtime/compiled-conditions/`
- `runtime/jobs/` 配下を使わない
- stable-key は canonicalized source path + file content + bridge version 由来
- bridge version は将来の互換性破壊変更に備えた予約であり、MVP では固定値とする
- parent directory を自動作成する helper を用意
- 同一入力は同一 output path になる
- authoring content 変更で output path が変わる
- Windows path separator に依存しにくい正規化を行う

**Tests:**
- path が `runtime/compiled-conditions` 配下
- stable-key determinism
- content change changes key
- bridge version が key 入力に含まれる
- Windows path separator に依存しにくい正規化

### Task 4: Python CLI bridge の command execution を実装

**Objective:** Rust から `analysis_backend.condition_authoring_cli` を呼び出して compile できるようにする。

**Files:**
- Modify: `src/condition_authoring_bridge.rs`

**Acceptance Criteria:**
- `compile_authoring_to_runtime(runtime, source_path)` 相当を追加
- `runtime.python_command` / `runtime.python_args` / `runtime.project_root` を使う
- `Command` に `OsString`/`PathBuf` args を渡し、shell 連結しない
- fake Python script test は `.py` ファイルを書き出し、`python <path>` 形式で実行する（shebang 不使用）
- `CSV_VIEWER_PROJECT_ROOT` を設定
- `--input`, `--output`, `--issues-json` を渡す
- exit code != 0 は blocking error
- stdout/stderr は長すぎる場合に切り詰める（厳密境界値ではなく、明らかな長文を短縮できればよい）
- issues JSON を読んで result に含める

**Tests:**
- fake `.py` script による success
- fake `.py` script による warning issue
- fake `.py` script による error exit
- missing issues JSON handling
- stderr truncation

### Task 5: AnalysisRuntimeConfig と AnalysisExportContext に source path を追加

**Objective:** worker effective path と UI/export source path を区別する基盤を追加する。

**Files:**
- Modify: `src/analysis_runner.rs`
- Modify: `src/app.rs` (`AnalysisExportContext`)
- Modify compile errors in dependent tests/modules

**Acceptance Criteria:**
- `AnalysisRuntimeConfig` に `filter_config_source_path: Option<PathBuf>` を追加
- `AnalysisExportContext` にも source path を保持できる field を追加する、または source/effective 表示 helper を追加する
- `build_runtime_config()` は source path `None` で初期化
- runtime JSON の従来挙動は変えない
- worker request は引き続き `filter_config_path` を使う
- `WorkerRuntimeFingerprint` は変更しない
- runtime clone 時に `filter_config_source_path` が保持される

**Tests:**
- `build_runtime_config()` source path None
- source path を入れた runtime clone/roundtrip
- worker request serialization が effective path を使う既存テストを維持/追加
- fingerprint が source/effective path に依存しないことを確認できる既存テストがあれば更新

### Task 6: 分析開始前 compile gate を追加

**Objective:** authoring JSON 選択時、分析開始前に compile して runtime clone を差し替える。

**Files:**
- Modify: `src/app_analysis_job.rs`
- Modify: `src/analysis_runner.rs` if request struct helper needed

**Acceptance Criteria:**
- `start_analysis_job_with_mode()` で runtime を取得後、format detection を実行
- compile gate は `cleanup_job_directories(&runtime.jobs_root)` より先に実行し、compile 失敗時は cleanup も job spawn も行わない
- runtime JSON の場合は従来どおり
- authoring JSON の場合は bridge compile を実行
- compiler error / CLI error では job を起動しない
- warning のみなら job を起動する
- job request に渡す runtime clone は `filter_config_path = generated runtime JSON`, `filter_config_source_path = Some(authoring path)`
- spawn 前の runtime clone の `filter_config_path` が generated runtime JSON であることを確認可能にする
- session cache key は generated runtime JSON の content hash を使うことを確認する

**Tests:**
- runtime JSON path は compile しない
- authoring success で generated path に差し替え
- authoring error で cleanup/spawn しない
- warning only で spawn する
- session cache key が generated runtime JSON を読む

### Task 7: authoring compiler warning を GUI warning state に統合

**Objective:** compile warning を既存 warning window/status で確認できるようにする。

**Files:**
- Modify: `src/app_analysis_job.rs`
- Possibly modify: warning rendering helper if needed

**Acceptance Criteria:**
- compile warning を `analysis_runtime_state.last_warnings` に追加
- Python worker 完了時の warning と併合して消えない
- 同一分析 run 内で compile warning が二重追加されない。run 間の同一 warning 再表示は MVP では許容する
- status summary に warning 件数が反映される、または warning window で確認できる
- error は status error として表示し、job は起動しない

**Tests:**
- compile warning + worker success warning の併合
- compile warning only の表示 state
- 同一 run 内で compile warning が二重化しない
- compile error の status error

### Task 8: authoring JSON 選択と condition editor guard を追加

**Objective:** authoring JSON は条件ファイルとして選べるが、既存 runtime editor では開いて壊さない。

**Files:**
- Modify: `src/app_condition_editor.rs`
- Modify: `src/app_analysis_settings.rs` or relevant settings picker module if needed
- Possibly modify: `src/condition_editor.rs` tests if necessary

**Acceptance Criteria:**
- analysis settings の file picker で authoring JSON を選んだ場合、それは有効な filter config として受け入れられ、分析時に compile される
- `open_condition_editor()` は load 前に format detection を行う
- authoring JSON の場合は editor を開かず説明 message
- condition editor 内の file picker で authoring JSON を選んだ場合も runtime editor に commit しない
- runtime JSON は従来どおり開ける
- deserialize error を authoring guard として使わない

**Tests:**
- analysis settings で authoring JSON 選択可能
- authoring JSON で editor open blocked
- runtime JSON で editor open allowed
- dirty state がある場合の confirm flow を壊さない

### Task 9: export/status 表示の source path 優先化

**Objective:** ユーザー向け表示では authoring source path を優先し、worker/export は generated runtime path を使う。

**Files:**
- Modify: `src/app_analysis_job.rs`
- Possibly modify: `src/analysis_session_cache.rs` if display/cache helpers overlap

**Acceptance Criteria:**
- `AnalysisExportContext` 等の表示用途では `filter_config_source_path.unwrap_or(filter_config_path)` を使う
- worker/export request が runtime JSON を必要とする箇所では generated runtime path を使う
- authoring JSON 選択時、export job の `filter_config_path` は generated runtime JSON である
- logs に source/effective path の両方を出す
- session cache は generated runtime JSON content hash を使う

**Tests:**
- authoring source path 優先表示
- export effective path は generated runtime JSON
- session cache key は generated runtime JSON を読む

### Task 10: 回帰テストとドキュメント更新

**Objective:** MVP の使い方・制限・検証結果を記録する。

**Files:**
- Modify: `docs/condition-authoring-format.md` または新規 Rust integration note
- Modify: `README.md` if user-facing launch/usage needs mention

**Acceptance Criteria:**
- Rust GUI では authoring JSON を選択・分析実行できるが GUI 編集は不可と明記
- generated runtime JSON の保存先を記載
- YAML / text_not_any 非対応を再掲
- `cargo test` 実行
- Python authoring tests 実行
- 必要なら `cargo run -- --ipc-dto-self-check` 実行

**Verification Commands:**

```bash
cargo test
uv run python -m pytest tests/test_condition_authoring.py tests/test_condition_authoring_sets.py tests/test_condition_authoring_filter_config.py tests/test_condition_authoring_loader.py tests/test_condition_authoring_cli.py -q
cargo run -- --ipc-dto-self-check
```

---

## 9. 実装時の順序とゲート

1. Task 1-4: bridge 基盤。ここまでは app integration なしで unit/integration-ish test を固める。
2. Task 5-7: analysis job integration。source/effective path、worker path 差し替え、warning 統合を確認する。
3. Task 8: settings 選択 + editor guard。選択は許可、runtime editor 破壊は防止。
4. Task 9: export/status/cache 表示整合性。
5. Task 10: 回帰・docs。

各 Task は実装 delegate → 仕様レビュー delegate → 品質レビュー delegate → 必要なら修正 delegate → 再レビューで進める。

---

## 10. タスク分解セカンドオピニオン反映メモ

反映済み:

- compile gate を `cleanup_job_directories()` より先に実行し、compile 失敗時は cleanup/spawn しないと明記
- session cache が generated runtime JSON content hash を使う検証を Task 6/9 に追加
- fake Python script は shebang 不使用の `.py` として実行すると明記
- Task 5 に `AnalysisExportContext` source path 基盤を追加
- warning 重複方針を「同一 run 内二重化なし、run 間再表示は許容」と明記
- stable-key の bridge version は固定予約値と明記
- analysis settings file picker では authoring JSON 選択を許可し、condition editor では開かない方針を Task 8 に追加
- export job が generated runtime JSON を使う検証を Task 9 に追加
- Task 1 に `FilterConfigDocument` deserialize による runtime 判定を追加

---

## 11. 実装承認前チェックリスト

- [x] 設計セカンドオピニオンを実施する
- [x] 指摘を反映する
- [x] 細分タスクを追加する
- [x] タスク分解セカンドオピニオンを実施する
- [x] 指摘を反映する
- [ ] ユーザー承認後に実装へ進む
