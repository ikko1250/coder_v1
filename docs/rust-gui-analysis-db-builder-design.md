# Rust GUI から Analysis DB Builder を起動する実装案

## 目的

Rust GUI 側から `docs/build_ordinance_analysis_db.py` を起動できるようにし、フォルダー入力からの Analysis DB 生成を GUI 操作で実行できるようにする。

対象は次の 2 点である。

1. GUI 上に「DB生成 / トークン化実行」の起動導線を追加すること。
2. `--input-dir`、`--analysis-db`、`--skip-tokenize` など builder 用設定を GUI から編集できるようにすること。

## 現状

既存 GUI には Python 実行環境と条件 JSON を扱う設定 UI、および Python subprocess をバックグラウンド実行する仕組みがある。

関係箇所:

- [src/app_toolbar.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_toolbar.rs)
- [src/app_analysis_settings.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_settings.rs)
- [src/app_analysis_job.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_job.rs)
- [src/analysis_process_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_process_host.rs)
- [src/analysis_runner.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_runner.rs)
- [src/file_dialog_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/file_dialog_host.rs)

ただし現在の job 実行経路は `run-analysis.py` 専用であり、builder 用 CLI は GUI からは呼べない。

builder 側の既存引数:

- [docs/build_ordinance_analysis_db.py](/mnt/f/program_2026/csv_viewer_v2/coder_v1/docs/build_ordinance_analysis_db.py)

現在利用可能な主な引数:

- `--input-dir`
- `--analysis-db`
- `--report-path`
- `--skip-tokenize`
- `--sudachi-dict`
- `--split-mode`
- `--split-inside-parentheses`
- `--merge-table-lines`
- `--purge`
- `--recreate-db`
- `--fresh-db`
- `--limit`
- `--note`

## 基本方針

builder 起動は、既存の analysis/export job とは別の job 種別として追加する。

理由:

- builder は worker protocol を使わず、単発 CLI として実行する方が自然。
- analysis job の責務は「既存 DB に対する条件評価」であり、builder は「DB 作成」で責務が異なる。
- UI 上も「分析」と「DB生成」は別ボタン、別設定群に分けた方が誤操作を減らせる。

## 追加する UI

### 1. ツールバー

対象:

- [src/app_toolbar.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_toolbar.rs)

追加案:

- `DB生成`
- `DB生成設定`

挙動:

- `DB生成`: 現在の builder 設定で job を開始する。
- `DB生成設定`: builder 専用設定ウィンドウを開く。

ジョブ実行中は analysis/export と同様にボタンを無効化する。

### 2. 設定ウィンドウ

対象:

- [src/app_analysis_settings.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_settings.rs)
- または builder 専用の新規モジュール

推奨:

- builder 専用ウィンドウを新規作成する。

理由:

- 既存の「分析設定」は `run-analysis.py` の runtime override に特化している。
- builder 用設定は項目数が多く、同じウィンドウに混ぜると責務が曖昧になる。

新規候補:

- [src/app_builder_settings.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_builder_settings.rs)

## GUI で持つ設定項目

最低限:

- Python 実行ファイル override
- 入力フォルダー (`input_dir`)
- 出力 DB パス (`analysis_db`)
- レポート出力先 (`report_path`)
- `skip_tokenize`
- `sudachi_dict`
- `split_mode`

初版で入れてよい追加項目:

- `split_inside_parentheses`
- `merge_table_lines`
- `purge`
- `fresh_db`
- `limit`
- `note`

初版では見送ってよい項目:

- `recreate_db`

理由:

- `fresh_db` があれば通常用途はほぼ足りる。
- `recreate_db` は意味が近く、GUI 上で両方を出すと利用者が迷いやすい。

## 状態モデル

対象:

- [src/app.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app.rs)

既存:

- `AnalysisRequestState`
- `AnalysisRuntimeState`

追加案:

```rust
struct BuilderRequestState {
    python_path_override: Option<PathBuf>,
    input_dir_path: Option<PathBuf>,
    analysis_db_path: Option<PathBuf>,
    report_path: Option<PathBuf>,
    skip_tokenize: bool,
    sudachi_dict: BuilderSudachiDict,
    split_mode: BuilderSplitMode,
    split_inside_parentheses: bool,
    merge_table_lines: bool,
    purge: bool,
    fresh_db: bool,
    limit: String,
    note: String,
    settings_window_open: bool,
}
```

補助 enum:

- `BuilderSudachiDict::{Core, Full, Small}`
- `BuilderSplitMode::{A, B, C}`

`limit` はテキスト入力を受けるため、内部保持は `String` が扱いやすい。

## ファイルダイアログ拡張

対象:

- [src/file_dialog_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/file_dialog_host.rs)

追加候補:

- `pick_open_folder()`
- `pick_save_analysis_db()`
- `pick_save_report_json()`

期待挙動:

- 入力はフォルダー選択。
- DB は `.db` または `.sqlite` フィルタ付き保存先選択。
- report は `.json` 保存先選択。

## job 実行経路

### 1. 新しい request / event / success 型

対象:

- [src/analysis_runner.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_runner.rs)

追加案:

```rust
pub(crate) struct AnalysisDbBuildRequest {
    pub(crate) runtime: AnalysisRuntimeConfig,
    pub(crate) input_dir: PathBuf,
    pub(crate) analysis_db_path: PathBuf,
    pub(crate) report_path: Option<PathBuf>,
    pub(crate) skip_tokenize: bool,
    pub(crate) sudachi_dict: String,
    pub(crate) split_mode: String,
    pub(crate) split_inside_parentheses: bool,
    pub(crate) merge_table_lines: bool,
    pub(crate) purge: bool,
    pub(crate) fresh_db: bool,
    pub(crate) limit: Option<usize>,
    pub(crate) note: String,
}
```

```rust
pub(crate) struct AnalysisDbBuildSuccess {
    pub(crate) analysis_db_path: PathBuf,
    pub(crate) report_path: PathBuf,
    pub(crate) stdout: String,
}
```

```rust
pub(crate) enum AnalysisJobEvent {
    AnalysisCompleted(Result<AnalysisJobSuccess, AnalysisJobFailure>),
    ExportCompleted(Result<AnalysisExportSuccess, AnalysisJobFailure>),
    BuildCompleted(Result<AnalysisDbBuildSuccess, AnalysisJobFailure>),
}
```

`AnalysisJobFailure` はそのまま再利用できる。

### 2. builder 実行関数

対象:

- [src/analysis_runner.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_runner.rs)

追加関数:

- `spawn_build_job()`
- `run_build_job()`

方針:

- `run-analysis.py --worker` ではなく、`python docs/build_ordinance_analysis_db.py ...` を直接実行する。
- stdout/stderr を回収する。
- 終了コード 0 なら `BuildCompleted(Ok(...))`
- 非 0 なら `BuildCompleted(Err(...))`

builder スクリプトは完了時に stdout へ summary JSON を出し、失敗時は stderr と `report.json` を出すため、初版の UI には十分である。

### 3. process host 拡張

対象:

- [src/analysis_process_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_process_host.rs)

追加:

- `spawn_build_job()`

既定実装:

- `ThreadAnalysisProcessHost` から `analysis_runner::spawn_build_job()` に委譲する。

## App 側フロー

対象:

- [src/app_analysis_job.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_job.rs)
- [src/app.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app.rs)

追加する処理:

- `start_build_job()`
- `handle_build_success()`
- `handle_build_failure()`
- `poll_analysis_job()` 内で `BuildCompleted` を処理

成功時の推奨挙動:

1. status を `Succeeded`
2. `db_viewer_state.db_path` を生成済み DB に切り替える
3. 必要なら「生成した DB を開く」相当の導線を出す

初版では自動で analysis 実行まではしない方が安全である。

理由:

- DB 生成と条件評価は別操作であり、失敗切り分けがしやすい。
- builder 完了直後に analysis を自動実行すると、設定差異や warning の責務が混ざる。

## 初版の仕様

### 入れるもの

- `DB生成` ボタン
- builder 設定ウィンドウ
- builder job 実行
- 成功/失敗の状態表示
- stderr または report path の表示
- 成功時の DB パス反映

### 入れないもの

- リアルタイム進捗率
- 段落数 / 文数 / token 数の途中表示
- build 完了後の自動分析実行
- 複数 build preset の保存

## 進捗表示について

現状の [docs/build_ordinance_analysis_db.py](/mnt/f/program_2026/csv_viewer_v2/coder_v1/docs/build_ordinance_analysis_db.py) は、完了時 summary と失敗時 report 出力はあるが、機械可読な進捗イベントは逐次出していない。

そのため初版は次の表示で十分である。

- `DB生成中`
- `成功: run completed`
- `失敗: stderr / report を参照`

将来、進捗を細かく出したい場合は builder 側に JSON Lines 形式の progress event を追加する。

例:

```json
{"event":"progress","stage":"read_files","done":10,"total":120}
{"event":"progress","stage":"tokenize","document":"foo.txt","paragraphs":32}
{"event":"completed","run_id":12}
```

これは初版の必須条件ではない。

## 実装順

### Step 1. 最小の起動経路

- `AnalysisDbBuildRequest` / `BuildCompleted` 追加
- `spawn_build_job()` 追加
- toolbar に `DB生成` ボタン追加
- 固定値で builder を起動できるところまで作る

完了条件:

- GUI から builder を起動できる
- 成功/失敗が status に出る

### Step 2. 設定 UI

- `BuilderRequestState` 追加
- builder 設定ウィンドウ追加
- file dialog 拡張
- 設定値を request に反映

完了条件:

- GUI から入力フォルダーと DB 出力先を選べる
- tokenize 関連フラグを設定できる

### Step 3. 成功後の導線

- 生成 DB を `db_viewer_state.db_path` へ反映
- 必要なら「この DB を分析対象にする」導線を明示

完了条件:

- build 後に GUI が新しい DB を参照できる

### Step 4. 品質強化

- report path の表示改善
- 設定バリデーション
- テスト追加

## バリデーション方針

GUI 側で最低限チェックするもの:

- `input_dir` が空でない
- `analysis_db_path` が空でない
- `limit` が空文字または正整数
- `skip_tokenize == false` のとき `sudachi_dict` / `split_mode` は常に有効値

builder 側でも同様の検証は行うため、GUI 側の責務は「早期フィードバック」であり、最終防衛線ではない。

## テスト観点

Rust 側:

- builder request から CLI 引数列が正しく組み立つ
- `BuildCompleted` 成功時に status が更新される
- `BuildCompleted` 失敗時に error summary が出る
- `limit` の parse 失敗が UI で弾かれる

手動確認:

1. GUI で入力フォルダーを選ぶ
2. `skip_tokenize=true` で DB 生成が成功する
3. `skip_tokenize=false` で DB 生成が成功する
4. ファイル名規約違反フォルダーで report が出る
5. 生成後の DB を GUI が参照できる

## リスク

### 1. 既存 analysis job と責務が混ざる

対策:

- builder 専用 request / event / settings を分離する

### 2. 設定画面が肥大化する

対策:

- builder 設定は別ウィンドウへ分離する

### 3. 進捗が見えにくい

対策:

- 初版は status + stderr/report 表示で割り切る
- 必要なら builder 側に progress event を追加する

### 4. DB 生成直後の UI 状態が曖昧になる

対策:

- build 成功時は `db_viewer_state.db_path` を生成先へ切り替える
- ただし分析実行は自動化しない

## 推奨結論

実装は可能であり、既存アーキテクチャとの整合も取れる。

ただし最初から「分析 job と builder job を完全統合する」のではなく、以下の最小構成で始めるのが妥当である。

1. builder 専用 job を追加する
2. builder 専用設定ウィンドウを追加する
3. 成功時に生成 DB を GUI の現在 DB として反映する
4. 進捗は初版では詳細化しない

この構成なら差分が局所化し、既存の analysis/export 経路を壊しにくい。

## 変更対象ファイルの目安

- [src/app.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app.rs)
- [src/app_toolbar.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_toolbar.rs)
- [src/app_analysis_job.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_job.rs)
- [src/app_analysis_settings.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/app_analysis_settings.rs)
- [src/analysis_runner.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_runner.rs)
- [src/analysis_process_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/analysis_process_host.rs)
- [src/file_dialog_host.rs](/mnt/f/program_2026/csv_viewer_v2/coder_v1/src/file_dialog_host.rs)
- [docs/build_ordinance_analysis_db.py](/mnt/f/program_2026/csv_viewer_v2/coder_v1/docs/build_ordinance_analysis_db.py)

必要なら次は、この実装案をそのまま作業タスクへ分解する。

---

## セカンドオピニオン（2026-03-27）

以下は、実コードとの照合に基づく批判的レビューである。カテゴリ別に指摘を整理する。

### A. バグの可能性

#### A-1. `build_runtime_config()` が `run-analysis.py` の存在を前提とする

`AnalysisDbBuildRequest` に `runtime: AnalysisRuntimeConfig` を持たせる設計になっているが、既存の `build_runtime_config()`（`analysis_runner.rs:364`）は最初に `run-analysis.py` を解決し、見つからなければ `Err` を返す。

builder のみを使いたいケース（分析はまだ不要で DB 生成だけしたい場合）でも、`run-analysis.py` が存在しないと runtime config の構築自体が失敗する。

builder 用には `build_runtime_config()` とは別の、`run-analysis.py` を前提としない config 構築パスが必要である。

#### A-2. Python stdout の解析が失敗する

設計案では「builder スクリプトは完了時に stdout へ summary JSON を出し」と記載しているが、実際の `main()` 関数（`build_ordinance_analysis_db.py:1167-1168`）は以下を出力する:

```python
print(f"analysis run completed: run_id={run_id}")
print(json.dumps(summary, ensure_ascii=False, indent=2))
```

テキスト行と JSON が混在しており、stdout 全体を JSON として解析すると失敗する。Rust 側で stdout をパースする場合、テキスト行を除外して JSON 部分だけを抽出するロジックが必要だが、設計に言及がない。

#### A-3. `report_path` の解決手段が未定義

`AnalysisDbBuildSuccess` の `report_path: PathBuf` は non-optional だが、Python 側の `--report-path` は省略可能で、省略時は `resolve_report_path()` により DB パスから自動導出される（`build_ordinance_analysis_db.py:856-860`）。

Rust 側はこの自動導出後のパスを知る必要があるが:

1. Python の stdout にはレポートパスが含まれない
2. Rust 側で Python と同じ導出ロジックを再実装する必要がある
3. いずれの方法も設計に記載がない

レポートパスの不一致により、GUI 上の「report を参照」リンクが無効なファイルを指す危険がある。

#### A-4. analysis job と builder job の同時起動が防止されない

既存の `can_start()` は `self.runtime.is_some() && self.current_job.is_none()` で判定する（`app.rs:238`）。builder は別の state（`BuilderRequestState`）で管理されるため、analysis job 実行中に builder を起動できてしまう可能性がある。

`AnalysisProcessHost` trait は analysis/export の 2 種のみ定義しており、builder は subprocess を別経路で起動する。この場合、同時に Python プロセスが複数起動され、DB ファイルのロック競合や Python 仮想環境の排他問題が発生しうる。

設計には「ジョブ実行中は analysis/export と同様にボタンを無効化する」とあるが、cross-state な排他制御（`analysis_runtime_state.current_job` と builder の current job の両方を参照）の具体策がない。

#### A-5. `AnalysisRuntimeConfig` の流用が不適切なフィールドを含む

`AnalysisDbBuildRequest` に `runtime: AnalysisRuntimeConfig` を持たせるが、`AnalysisRuntimeConfig` は以下の analysis 専用フィールドを含む:

- `script_path`: `run-analysis.py` を指す（builder には無関係）
- `filter_config_path`: builder では不要
- `annotation_csv_path`: builder では不要
- `jobs_root`: builder では使わない

これらの無関係なフィールドが存在することで:

1. builder request 構築時に不必要な依存が生じる
2. 将来の保守者が「builder に filter_config_path が必要なのか」と混乱する
3. validation ロジックが analysis 用の制約を builder にも適用してしまう

builder 用に `BuilderRuntimeConfig` を別定義するか、共通部分（Python パスのみ）を抽出すべきである。

#### A-6. `limit` の parse タイミングと失敗ハンドリングが曖昧

`BuilderRequestState` では `limit: String` だが、`AnalysisDbBuildRequest` では `limit: Option<usize>`。parse は GUI 側バリデーション（正整数判定）の他に、CLI 引数構築時にも行う必要がある。

parse 失敗時のハンドリングが 2 箇所に分散する設計であり:

1. GUI バリデーション時（実行ボタン押下前）
2. CLI 引数構築時（request 生成時）

いずれで弾くのかの明確な優先順位がなく、「バリデーションは通ったが request 生成で型変換に失敗する」エッジケースがありうる（例: `String` → `usize` のオーバーフロー）。

#### A-7. `note` の空文字列処理が未定義

`AnalysisDbBuildRequest` の `note: String` が空文字列の場合、CLI 引数として `--note ""` を渡すのか `--note` を省略するのかが設計に未記載。Python の argparse では `--note` のデフォルトが `""` なので省略しても空文字列になるが、`--note ""` を渡すとプラットフォームごとの引用符処理の差異が生じうる（特に Windows の `cmd.exe` と PowerShell で挙動が異なる）。

#### A-8. `AnalysisJobEvent::BuildCompleted` 追加時のパターンマッチ網羅性

既存の `poll_analysis_job()`（`app_analysis_job.rs:234-358`）は `AnalysisJobEvent` に対して `AnalysisCompleted` と `ExportCompleted` のみを処理している。`BuildCompleted` を追加すると、このマッチが非網羅になる。

Rust コンパイラが警告を出すので見逃しにくいが、設計案が「`poll_analysis_job()` 内で `BuildCompleted` を処理」としている一方、builder は分析ジョブとは異なるフロー（worker protocol 不使用）で動くため、同じ `poll_analysis_job()` 関数内に builder 処理を混在させるべきかどうかは再検討が必要。

### B. UI 崩れ・UX 問題

#### B-1. 長時間実行時にアプリが無応答に見える

builder の tokenize 処理は大量のファイル＋ Sudachi 形態素解析を含むため、数分〜数十分かかりうる。設計の「初版の進捗表示」は:

- `DB生成中`
- `成功: run completed`
- `失敗: stderr / report を参照`

のみであり、経過時間表示すらない。egui の `Spinner` は回り続けるが、ユーザーが「固まった」と判断してアプリを強制終了するリスクが高い。

最低限、ステータス行に「DB生成中（経過 XX 秒）」を表示するか、builder プロセスの生存確認（`try_wait()`）結果を定期表示すべきである。

#### B-2. ツールバーのボタン増加でレイアウトが崩れる

既存のツールバーは `horizontal_wrapped` 内に「分析実行」「再分析」「再読込分析」「CSV保存(全件)」「分析設定」「条件編集」の 6 ボタン＋ラベル群を配置している（`app_toolbar.rs:46-148`）。

ここに「DB生成」「DB生成設定」の 2 ボタンを追加すると、ウィンドウ幅によっては折り返しが多段になり、ツールバー領域が肥大化して中央ペインが圧迫される。

ツールバーをカテゴリごとにグループ化するか、セパレータで区切るかの UI 設計が必要。

#### B-3. `fresh_db` と `purge` の併用が GUI 上で防止されていない

Python 側では `--purge` と `--fresh-db` の同時指定は `ValueError` で弾かれる（`build_ordinance_analysis_db.py:1046-1047`）。しかし GUI のバリデーション方針にこの排他制御が含まれていない。

ユーザーが GUI で両方にチェックを入れて実行すると、Python プロセスが即座にエラーで終了し、stderr に Python の traceback が出力される。GUI 側で事前に弾く方が UX が良い。

#### B-4. builder 設定ウィンドウと分析設定ウィンドウの混同リスク

「builder 専用ウィンドウを新規作成する」方針自体は良いが、ウィンドウタイトルの命名規則が未定義。既存の「分析設定」と新規の「DB生成設定」は、日本語としてはどちらも「分析」関連に見えるため、ユーザーが間違えて開くリスクがある。

ウィンドウタイトルに明確な接頭辞（例:「[Builder] DB生成設定」「[Analysis] 分析設定」）を付けるなどの対策が必要。

#### B-5. 成功時の DB パス反映が暗黙的すぎる

成功時に `db_viewer_state.db_path` を生成先へ自動切替するとあるが、ユーザーへの通知が不十分。ユーザーは:

1. 切り替わったことに気づかない可能性がある
2. 意図せず別の DB から切り替わってしまう

明示的な確認ダイアログ（「生成した DB を現在の参照先に設定しますか？」）か、少なくとも status 行で「DB参照先を変更しました: path/to/new.db」を表示すべきである。

### C. 計算ミス・データ不整合

#### C-1. `line_count` の計算ロジック

`build_ordinance_analysis_db.py:683` にて:

```python
line_count = raw_text.count("\n") + (1 if raw_text else 0)
```

空のテキストファイル（`raw_text = ""`）の場合 `line_count = 0` で正しいが、末尾改行なしの 1 行テキスト（例: `"hello"`）は `0 + 1 = 1` で正しい。末尾改行ありの 1 行テキスト（例: `"hello\n"`）は `1 + 1 = 2` となるが、これは行数としては議論の余地がある。

直接的なバグではないが、DB 内の `line_count` を後工程で使う場合に 1 のズレが生じうる。GUI 上で行数を表示する計画がある場合は注意が必要。

#### C-2. `tokens_inserted` の計上方法

`build_ordinance_analysis_db.py:1038` にて:

```python
summary["tokens_inserted"] += token_no
```

`enumerate(tokenizer.tokenize(...), start=1)` で `token_no` は最後のトークンの番号。文ごとのトークン数は「最後の `token_no`」で正しいが、空文字列をトークナイズした場合 `enumerate` が空で `token_no` は前回ループの値のままになる。

`for token_no, morpheme in enumerate(...)` は空イテラブルの場合に変数を束縛しないため、前のループの `token_no = 0` が加算される。これ自体は 0 加算なので計算結果に影響しないが、`token_no` 変数のスコープが紛らわしく、将来の改修でバグの温床になりうる。

#### C-3. `db_viewer_state` の DB パス切替時の loaded state 不整合

成功時に `db_viewer_state.db_path` を生成先に切り替えるとあるが、既存の `replace_records()` は `db_viewer_state.reset_loaded_state()` を呼んでいる（`app.rs:443`）。builder 成功時はレコード入れ替えを伴わない（builder は DB を生成するだけで analysis records は返さない）ため、`reset_loaded_state()` は呼ばれず、前回の loaded state（古い DB の情報）が残ったまま新しい DB パスになる。

DB viewer ウィンドウを開いたときに、古い DB の情報が表示される、または新しい DB のテーブルが見えないといった不整合が生じうる。

### D. 意図しない動作の可能性

#### D-1. `BuilderSplitMode` / `BuilderSudachiDict` のデフォルト不一致

Python 側のデフォルトは `--sudachi-dict core` / `--split-mode C`（`build_ordinance_analysis_db.py:58-59`）。Rust 側 `BuilderRequestState` に明示的なデフォルト値がない。

Rust の `Default` derive で enum の最初の variant がデフォルトになるが、設計案の enum 定義順が `Core, Full, Small` / `A, B, C` であるため、`Core` / `A` がデフォルトになる。`split_mode` のデフォルトが `A`（最も短い分割単位）になると、Python 側のデフォルト `C` と不一致が生じ、ユーザーが設定を変更しないまま実行した場合に意図しない分割結果になる。

#### D-2. `--input-dir` が `required=True` だが GUI 側でのハンドリングが不十分

Python 側で `--input-dir` は必須引数（`build_ordinance_analysis_db.py:55`）。GUI 側バリデーションでは「`input_dir` が空でない」をチェックするが、フォルダーの存在チェックは含まれていない（「早期フィードバック」の責務だがパスの存在確認まではしない方針）。

存在しないフォルダーを指定した場合、Python 側で `input_dir_not_found` エラーとして report が出力されるが、GUI のエラー表示が「stderr / report を参照」のみでは、ユーザーにとって原因特定が困難。

#### D-3. ファイル名規約違反時のエラーメッセージが不親切

`build_ordinance_analysis_db.py:550-558` にて、ファイル名が `<category1>_<category2>.(txt|md)` にマッチしないファイルは warning として report に記録されるが、処理はスキップされるだけで build 自体は継続する。

GUI 上では「成功」と表示されるが、一部ファイルがスキップされていることをユーザーが認識できない。report の表示改善は Step 4 に含まれるが、初版で warning が見えないのは UX として問題。

#### D-4. `os.replace()` の Windows 上での挙動

`build_ordinance_analysis_db.py:1140` で `os.replace(temp_db_path, analysis_db_path)` を使っている。Windows では、ターゲットファイルが他のプロセス（例: GUI の SQLite 接続）で開かれている場合、`os.replace()` が `PermissionError` で失敗する。

GUI が生成先の DB パスに対して既に SQLite 接続を保持している場合（`db_viewer_state` がその DB を開いている場合）、builder の最終ステップでこのエラーが発生する。設計には「成功時に DB パスを切り替える」とあるが、切り替え前に GUI 側の接続を閉じるタイミングの考慮がない。

#### D-5. `sudachi.json` のカレントディレクトリ依存

`build_ordinance_analysis_db.py:650` にて:

```python
if os.path.exists("sudachi.json"):
    config_kwarg["config_path"] = "sudachi.json"
```

カレントディレクトリに `sudachi.json` があれば使用する。GUI から subprocess を起動する場合、カレントディレクトリは GUI のプロセスの作業ディレクトリに依存する。

既存の analysis job は `current_dir(&runtime.project_root)` でカレントディレクトリを設定するが、builder job の `current_dir` をどこに設定するかが設計に明記されていない。設定を誤ると `sudachi.json` が読み込まれない、または意図しないディレクトリの `sudachi.json` が読み込まれる。

#### D-6. `--recreate-db` の初版見送りが将来の互換性問題を招く

設計では「`fresh_db` があれば通常用途はほぼ足りる」として `recreate_db` を初版で見送るとしている。しかし Python 側では `should_recreate_db = args.recreate_db or args.fresh_db` で同一扱いされている（`build_ordinance_analysis_db.py:1045`）。

現時点では同一動作だが、Python 側で将来 `recreate_db` と `fresh_db` の動作を分離した場合（例: `recreate_db` はスキーマ再作成のみ、`fresh_db` はファイル自体を新規作成）、GUI が `recreate_db` を送れないことで機能差が生じる。

#### D-7. builder 実行中のウィンドウ閉じでプロセスが孤立する

既存の analysis job は worker protocol を使い、`shutdown_worker()` で `child.kill()` を呼ぶ。builder は単発 CLI として実行される設計だが、GUI が閉じられた際に builder subprocess を kill する仕組みが設計に含まれていない。

`guard_root_close_with_dirty_editor()`（`app_analysis_job.rs:551`）は条件エディタの未保存変更のみをチェックしており、builder job の実行状態はチェックしない。builder 実行中にウィンドウを閉じると、Python プロセスが孤立（orphan process）する。

#### D-8. `BuilderRequestState` の永続化が未考慮

既存の `AnalysisRequestState` はアプリ起動ごとに初期化され、設定は永続化されない。builder 設定も同様だが、builder は分析より設定項目が多い（入力フォルダー、出力 DB、tokenize 関連フラグなど）。

アプリを再起動するたびに全設定を再入力するのは現実的でない。設定の永続化（JSON / TOML ファイルへの保存・読み込み）が初版に含まれていないのは、運用上の大きな不便になる。

#### D-9. Windows パス区切りの CLI 引数渡し

Windows 環境で `PathBuf::display()` はバックスラッシュ（`\`）を使う。Python の `Path()` はバックスラッシュも受け付けるが、`subprocess` への引数渡しで以下の問題が起こりうる:

1. バックスラッシュがエスケープ文字と解釈される場合がある（特に末尾が `\` の場合）
2. 日本語パスを含む場合のエンコーディング問題

CLI 引数には `OsString` を直接使用し、`display()` による文字列変換を避けるべきである。既存の analysis runner では `db_path.display().to_string()` を worker JSON に渡しているが、builder は JSON ではなく CLI 引数として渡すため、より慎重な処理が必要。

### E. テスト観点の追加提案

設計案のテスト観点に以下が欠けている:

1. **Windows パス（バックスラッシュ、日本語パス）** で CLI 引数が正しく組み立つ
2. **builder job と analysis job の排他制御** が正しく機能する
3. **builder 実行中にウィンドウ閉じを試みた場合** のプロセス cleanup
4. **`fresh_db` と `purge` の同時指定** が GUI 側で事前に弾かれる
5. **`split_mode` / `sudachi_dict` のデフォルト値** が Python 側と一致する
6. **builder stdout のテキスト行 + JSON 混在** を正しくパースできる
7. **生成先 DB が既に GUI で開かれている場合** の `os.replace()` 失敗ハンドリング
8. **空の入力フォルダー** を指定した場合のエラー表示が十分か

### F. 設計改善の推奨

| # | 内容 | 優先度 |
|---|------|--------|
| F-1 | builder 用の `BuilderRuntimeConfig` を `AnalysisRuntimeConfig` から分離し、Python パスのみを共有する | 高 |
| F-2 | Python stdout 出力を JSON のみに統一するか、Rust 側で行単位パース＋JSON 検出ロジックを入れる | 高 |
| F-3 | analysis job と builder job の排他制御を `App` レベルで一元管理する（共通の `is_any_job_running()` メソッド） | 高 |
| F-4 | builder 実行中のウィンドウ閉じ時に subprocess を kill する仕組みを追加する | 高 |
| F-5 | `BuilderSplitMode` / `BuilderSudachiDict` のデフォルト値を Python 側と明示的に一致させる | 中 |
| F-6 | builder 成功時の DB パス切替は確認ダイアログを挟むか、`db_viewer_state` の loaded state をリセットする | 中 |
| F-7 | `fresh_db` と `purge` の排他制御を GUI バリデーションに追加する | 中 |
| F-8 | 経過時間表示を初版に含める（Spinner + 経過秒数で十分） | 中 |
| F-9 | builder 設定の永続化を初版または Step 2 に含める | 低 |
| F-10 | `report_path` の解決ロジックを Rust 側に実装するか、Python 側の stdout に含める | 中 |
