# Tauri 移行を見据えた app/state/service 分離のリファクタ計画

## 背景

直近のリファクタで、`AppState` と `app_services` の抽出は完了した。
これにより `app.rs` の一部責務は分離できたが、Tauri 移行という観点ではまだ以下が混在している。

- egui 固有の UI 制御
- アプリケーション状態
- ジョブハンドルやファイルダイアログのような実行環境依存の処理
- UI イベントを状態変更へ変換する接着ロジック

この文書は、残りのリファクタを段階的に進めるための実施計画をまとめたものである。

## 現状整理

### すでに分離できているもの

- `src/app_state.rs`
  - レコード一覧、フィルタ、選択、annotation editor、condition editor などの主要 state
  - selection / filter / cached segment などの state helper
- `src/app_services.rs`
  - CSV 読込、annotation 保存、DB viewer context 読込
  - runtime refresh
  - condition editor の load/save/reload/sync
  - analysis/export job の開始とポーリング

### まだ `src/app.rs` に強く残っているもの

- warning details window / close guard
- DB viewer viewport の構築と close ハンドリング
- toolbar の button click と action 呼び出し
- analysis settings window の override 編集と runtime refresh 起動
- condition editor window の viewport, overlay, command draft 適用
- filter/tree/detail の描画と直接 state 更新

## ゴール

### 最終ゴール

Tauri 移行時に、backend 側へ残すロジックと frontend 側へ移すロジックの境界が明確な構成にする。

### 中間ゴール

1. `AppState` から UI 専用 state を切り離す。
2. job receiver のような非シリアライズ資源を state から外す。
3. `app.rs` の UI イベント処理を action / reducer / controller 層へ寄せる。
4. service 層を `&mut AppState` 密結合から request/result ベースへ近づける。
5. egui 固有処理を adapter として閉じ込める。

## 非ゴール

この計画では、次のものは直ちに着手対象としない。

- UI デザインの刷新
- 分析 backend の Python 側仕様変更
- condition JSON フォーマットの大規模変更
- DB schema の変更

## 優先順位

### 優先度 A

1. UI state 分離
2. Job 管理分離
3. Condition editor の action / reducer 化

### 優先度 B

4. Analysis settings の controller 化
5. Filter / tree / detail の ViewModel 化
6. Service 層の DTO 化

### 優先度 C

7. adapter 固有処理の明示分離
8. `app_state` / `app_services` の直接テスト追加

## 実施フェーズ

### Phase 1: UI state を `AppState` から分離する

#### 目的

backend 寄り state と egui 表示状態を切り分ける。

#### 対象候補

- `pending_tree_scroll`
- `record_list_panel_ratio`
- `annotation_panel_expanded`
- `settings_window_open`
- `warning_window_open`
- `condition_editor_state.window_open`
- `condition_editor_state.confirm_action`

#### 実施内容

- `UiState` または `AppUiState` を新設する。
- `egui::Align` を含む型は `AppState` から追い出す。
- `App` は `core_state` と `ui_state` を合成して持つ形に寄せる。

#### 完了条件

- `AppState` が UI レイアウトや viewport 固有の状態を持たない。
- `app_state.rs` が egui 固有型へ依存しない、もしくは依存が最小限に抑えられている。

### Phase 2: Job 管理を state から分離する

#### 目的

`Receiver<AnalysisJobEvent>` を state から外し、Tauri backend で扱いやすい形にする。

#### 実施内容

- `JobManager` または `AnalysisJobRegistry` を新設する。
- `RunningAnalysisJob` を廃止、または state 非保持にする。
- state 側には以下のみ残す。
  - `current_job_id`
  - `status`
  - `last_warnings`
  - `last_export_context`
- `poll_analysis_job` は manager を介して進捗反映する形へ変更する。

#### 完了条件

- `AppState` / `AnalysisRuntimeState` が `Receiver` を直接持たない。
- job 完了通知処理が state 更新と job handle 管理に分離されている。

### Phase 3: Condition editor を action / reducer 化する

#### 目的

condition editor の UI イベント処理を `app.rs` から抜き、将来の Tauri frontend に再利用しやすくする。

#### 実施内容

- `ConditionEditorAction` を定義する。
- `ConditionEditorReducer` または controller 関数を追加する。
- `apply_condition_editor_command_draft` の責務を reducer / service に分配する。
- close/reload/save/confirm を action として明示化する。

#### 完了条件

- `draw_condition_editor_window` は描画と response 収集に集中する。
- 状態遷移ロジックが `app.rs` の外に移る。

### Phase 4: Analysis settings を controller 化する

#### 目的

パス選択 UI と runtime 更新ロジックを切り分ける。

#### 実施内容

- path picker の結果を action 化する。
- override の更新を controller へ集約する。
- runtime refresh の起動条件を controller で判定する。

#### 完了条件

- `draw_analysis_settings_window` が `runtime_changed` の組み立てを持たない。
- override 更新の単体テストが可能になる。

### Phase 5: Filter / tree / detail を ViewModel 化する

#### 目的

一覧・詳細表示の描画データを state から直接引くのではなく、描画専用モデルを介して扱う。

#### 実施内容

- `ToolbarViewModel`
- `FilterPanelViewModel`
- `RecordTreeViewModel`
- `DetailViewModel`

を段階的に追加する。

#### 完了条件

- `draw_filters`, `draw_tree`, `draw_detail` が `self.state` を直接強く操作しない。
- Tauri 側でも同じ view model を使える構造になる。

### Phase 6: Service 層を request/result ベースへ寄せる

#### 目的

`app_services` を状態直更新層から、より再利用しやすい application service 層へ進める。

#### 実施内容

- `load_csv(path) -> Vec<AnalysisRecord>` のような純度の高い関数へ分割する。
- `load_condition_editor_from_path(path) -> LoadedConditionEditor` のような DTO を返す。
- controller / reducer 側で state へ反映する。

#### 完了条件

- `app_services` の主要 API が `&mut AppState` 依存を減らしている。
- service 単体テストが書きやすくなる。

### Phase 7: adapter 固有処理を明示的に隔離する

#### 目的

egui 専用処理を backend/application 層から切り離す。

#### 実施内容

- file dialog の呼び出しを adapter 化する。
- viewport close/focus/open を adapter に寄せる。
- 将来の `tauri_adapter` を想定した trait / interface を整理する。

#### 完了条件

- `rfd::FileDialog` や `ViewportCommand` を使う箇所が adapter 層へ閉じる。

### Phase 8: 回帰防止のためのテスト追加

#### 目的

分離後のロジックを UI から独立して検証できるようにする。

#### 優先テスト候補

- `AppState::apply_selection_change`
- `AppState::toggle_filter_value`
- `AppState::apply_filters`
- runtime refresh の反映ルール
- condition editor の save/reload/sync
- job 完了時の状態遷移

#### 完了条件

- `app_state.rs` と `app_services.rs` に直接対応するテストが追加される。
- UI を起動せずに主要挙動の回帰確認ができる。

## 作業順序の推奨

最小の手戻りで進めるなら、次の順番を推奨する。

1. Phase 1: UI state 分離
2. Phase 2: Job 管理分離
3. Phase 3: Condition editor reducer 化
4. Phase 4: Analysis settings controller 化
5. Phase 8: テスト追加
6. Phase 5: ViewModel 化
7. Phase 6: Service DTO 化
8. Phase 7: adapter 分離

理由は、前半 3 フェーズが backend/frontend 境界の整理に最も効き、後続フェーズの設計自由度も上げるためである。

## 想定されるリスク

### 1. 中途半端な層分けになるリスク

service だけ分けても controller/reducer がないと、`app.rs` が別の形で肥大化し直す可能性がある。

### 2. UI state と domain state の責務が再混在するリスク

命名だけ分けて実態が混ざったままだと、Tauri 移行で再度分解が必要になる。

### 3. Job 管理の不整合

receiver の分離時に、job 完了通知と state 反映の責務が曖昧だと不具合が入りやすい。

### 4. テスト不足による回帰

condition editor と filter 周りは状態遷移が多いため、テスト追加前に大きく分離すると regressions が起きやすい。

## 直近 1〜2 PR の推奨スコープ

### PR 1

- `UiState` 新設
- `AppState` から panel ratio / window open / tree scroll を移動
- 既存描画コードを `ui_state` 経由へ差し替え
- 最低限の state テスト追加

### PR 2

- `JobManager` 新設
- `Receiver` を state から削除
- polling と job 終了時反映の責務分離
- runtime / job status 周りのテスト追加

### PR 3

- condition editor action / reducer 導入
- `app.rs` の command draft 適用責務縮小
- save/reload/confirm のテスト追加

## 成功指標

以下を満たしたら、この計画は十分に前進したとみなせる。

- `app.rs` が UI 描画と adapter 配線に集中している。
- `AppState` が UI フレームワーク依存をほぼ持たない。
- background job handle が state から除去されている。
- condition editor / runtime settings の状態遷移を UI 非依存でテストできる。
- Tauri frontend/backend の責務境界を文書だけで説明できる。
