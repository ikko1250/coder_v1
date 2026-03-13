# Rust 側 warning 表示 実装案

- 日付: 2026-03-13
- 対象: `src/analysis_runner.rs`, `src/app.rs`
- 目的: Python CLI が出力する warning 情報、とくに distance matching の mode 切替や config/data access warning を Rust UI で確認できるようにする

## 既存挙動

- Rust 側は `AnalysisWarningMessage` として以下を deserialize できる
  - `code`
  - `message`
  - `condition_id`
  - `unit_id`
  - `requested_mode`
  - `used_mode`
  - `combination_count`
  - `combination_cap`
  - `safety_limit`
- ただし UI 側では、成功時 summary に `警告 N 件` を出すだけで、warning の詳細は表示していない
- Python 側では warning payload が拡張されており、現在は追加で以下も出る
  - `severity`
  - `scope`
  - `fieldName`
  - `queryName`
  - `dbPath`

## 問題

- distance matching が `auto-approx` から `approx` へ落ちた事実を UI で確認できない
- config default 化 warning や data access error の structured 情報も UI に出ない
- 失敗時は `errorSummary` と `stderr` は見えるが、warning detail が埋もれる

## 実装方針

### Phase 1. warning payload の受け皿を揃える

- `src/analysis_runner.rs` の `AnalysisWarningMessage` に以下を追加
  - `severity: Option<String>`
  - `scope: Option<String>`
  - `field_name: Option<String>`
  - `query_name: Option<String>`
  - `db_path: Option<String>`
- 既存の `serde(default)` を維持し、古い payload も読めるようにする

### Phase 2. runtime state に warning 表示用の保持先を追加

- `src/app.rs` の `AnalysisRuntimeState` か `App` に warning detail を保持する
- 推奨:
  - `analysis_runtime_state.last_warnings: Vec<AnalysisWarningMessage>`
  - `analysis_runtime_state.warning_window_open: bool`
- 理由:
  - success / failure をまたいで直近 job の warning を表示できる
  - status summary と詳細表示を分離できる

### Phase 3. 成功時・失敗時に warning を保存する

- `handle_analysis_success(...)`
  - `success.meta.warning_messages.clone()` を state に保存
- `handle_analysis_failure(...)`
  - `failure.meta.as_ref().map(|m| m.warning_messages.clone())` を state に保存
- 既存の summary 文言は維持

### Phase 4. 最小 UI を追加する

- まずは toolbar か status 行の近くに `警告詳細` ボタンを出す
- 押下時に `egui::Window` で一覧表示
- 表示内容の優先順:
  1. `message`
  2. `code`
  3. `condition_id`
  4. `requested_mode -> used_mode`
  5. `combination_count / combination_cap`
  6. `field_name` or `query_name`

### Phase 5. 表示文言を warning code ごとに読みやすくする

- 直接 struct を列挙するだけだと読みにくいので、code 別の summary builder を追加する
- 例:
  - `distance_match_fallback`
    - `distance matching: auto-approx -> approx (10100 / cap 10000)`
  - `condition_match_logic_defaulted`
    - `条件設定を既定値に補正: condition_match_logic -> any`
  - `sqlite_read_failed`
    - `DB 読込失敗: analysis_tokens`

## 最小実装スコープ

- `AnalysisWarningMessage` に新項目を追加
- success / failure 時に warning list を state 保存
- `警告詳細` window を追加
- code ごとの簡易 summary を 3 種だけ実装
  - `distance_match_fallback`
  - `*_defaulted`
  - `sqlite_*_failed`

これで利用者は distance matching 方式変更と config/data access warning を視認できる。

## 追加テスト案

### Rust unit test

- `AnalysisWarningMessage` が `severity`, `scope`, `fieldName`, `queryName`, `dbPath` を読める
- 旧 payload でも deserialize が壊れない

### Rust integration / app-level test

- `handle_analysis_success(...)` 後に warning count と detail state が一致する
- `handle_analysis_failure(...)` 後に meta warning が detail state へ載る

## 完了条件

- warning 件数だけでなく詳細内容を UI で確認できる
- distance matching fallback の `requested_mode / used_mode` が見える
- config default 化 warning と data access error も識別できる
- 旧 meta payload との互換を壊さない
