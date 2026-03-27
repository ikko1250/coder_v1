# 設計書: 条件エディターから条件 JSON をファイルダイアログで選択する

- **作成日**: 2026-03-26  
- **対象**: デスクトップアプリ（`eframe` / `egui`）  
- **関連**: P3-01 ファイルダイアログ抽象（`FileDialogHost`）、分析設定の条件 JSON 上書き（`filter_config_path_override`）

---

## 1. 背景・目的

### 1.1 現状

- 条件エディターは「分析ランタイムが解決した条件 JSON パス」（`resolved_filter_config_path`）から文書を読み込む。
- 別の JSON を使うには、**分析設定**ウィンドウで「条件 JSON」の「選択」からパスを指定する必要がある。
- 条件エディターヘッダーにはパス表示のみで、そこから直接ファイルを選べない。

### 1.2 目的

- 条件エディター**ヘッダー**に「**選択**」ボタンを追加し、OS のファイルダイアログ（エクスプローラー）で条件 JSON を指定できるようにする。
- 指定したパスは**分析実行・エクスポート時に Python に渡る条件 JSON**（`filter_config_path`）と一致させる。  
  → セッション内の `filter_config_path_override` を更新し、既存の `build_runtime_config` / worker リクエストの流れをそのまま利用する。

### 1.3 非目的（本設計の範囲外）

- 条件 JSON パスの永続化（起動をまたいだ保存）は行わない。既存どおりセッション内オーバーライドのみ。
- Python スクリプトの引数形式・IPC スキーマの変更（不要のため行わない）。

---

## 2. 要件

### 2.1 機能要件

| ID | 内容 |
|----|------|
| F-1 | ヘッダーに「選択」ボタンを表示する。 |
| F-2 | クリックで `FileDialogHost::pick_open_json()` を呼び、ユーザーが選んだ `PathBuf` を取得する（既定フィルタは JSON だが **All files も選べる**ため、読込時にパースで検証する。§4.7）。 |
| F-3 | 取得したパスを `analysis_request_state.filter_config_path_override` に設定し、分析ランタイムを更新する（分析設定の「条件 JSON」と同じ意味）。 |
| F-4 | エディターは**新しい解決パス**の内容を読み込み、`loaded_path`・表示・選択インデックスを既存の `load_condition_editor_from_path` 系と整合させる。 |
| F-5 | **未保存**（`is_dirty`）のときは、別ファイルへ切り替える前に**破棄確認**を出す（閉じる／再読込と同系統の UX）。 |
| F-6 | **分析ジョブ実行中**（`can_modify == false`）は「選択」を無効化する。分析設定と同様、実行中はパス変更不可。 |

### 2.2 非機能・制約

- `rfd` のファイルダイアログは既存どおり UI スレッド／フレームコールバック内から呼ぶ（分析設定と同パターン）。
- `refresh_analysis_runtime` はジョブ実行中は何もしないため、F-6 と組み合わせて一貫させる。

---

## 3. 現行アーキテクチャ（関連のみ）

```
[分析設定 UI]
    → filter_config_path_override 更新
    → refresh_analysis_runtime()
          → build_runtime_config()
          → sync_condition_editor_with_runtime_path()

[条件エディター]
    loaded_path: Option<PathBuf>     … 実際に編集中のファイル
    runtime.filter_config_path       … 分析に使う解決済みパス

[分析ジョブ]
    WorkerAnalyzeRequest.filter_config_path  … display().to_string() で Python へ
```

- `sync_condition_editor_with_runtime_path`: 解決パスと `loaded_path` がずれているとき、未保存なら警告（`pending_path_sync`）、保存済みなら自動再読込。

---

## 4. 設計方針

### 4.1 単一の真実（分析用パス）

- ユーザーがエディターで選んだパスは、**読込に成功したあと** `filter_config_path_override` に反映する（§11 参照: override を先に変えると runtime と editor が不整合になりやすい）。
- 反映後はツールバー「条件: …」、セッションキャッシュキー、次回分析／エクスポートの Python へのパスが一括で整合する。

### 4.2 パス切替を原子的に扱う（必須）

`refresh_analysis_runtime()` は `AnalysisRuntimeState` を再構築しセッションキャッシュ等を捨てる一方、`load_condition_editor_from_path` は**成功時のみ** editor 状態を更新する。順序を誤ると「runtime だけ新パス・editor は旧ファイル」のまま固定される（§11.1）。

**採用する順序（トランザクション）**:

1. ファイルダイアログで `PathBuf` を得る（キャンセルなら終了）。
2. 未保存なら破棄確認。**続行するまで** `filter_config_path_override` も `refresh_analysis_runtime` も呼ばない（§11.1: 先に override するとキャンセル時の巻き戻しが複雑になる）。
3. **まず** `load_condition_document(&path)`（または同等の検証＋パース）で成功可否を判定。失敗時はエラーメッセージのみ表示し、**override / runtime / editor は一切変えない**（§11.5）。
4. 成功したら、editor 状態を新文書で更新（既存の `load_condition_editor_from_path` と同等のフィールド更新。二重 I/O を避けるなら、手順 3 の結果を再利用する実装でもよい）。
5. 最後に `filter_config_path_override = Some(path)` → `refresh_analysis_runtime()`。UI の更新が必要なら **別途** `egui::Context::request_repaint()` 等を呼ぶ（`refresh_analysis_runtime` 自体は `Context` を受け取らない。§13.3）。

この順序により、失敗時にロールバック不要・「runtime だけ進む」事故を防ぐ。

**二重読込の回避**: `refresh_analysis_runtime` は末尾で `sync_condition_editor_with_runtime_path` を呼ぶ。手順 4 の時点ではまだ override が古いため、**自動再読込は走らない**。手順 5 の後は `loaded_path` と解決パスが一致するため、sync は追加の再読込を不要とする。よって **「refresh の直後に必ず `load_condition_editor_from_path` をもう一度呼ぶ」設計は採用しない**（§11.1）。

### 4.3 UI 層の責務分割（P1-06 との整合）

- `condition_editor_view.rs`: ヘッダー描画＋クリック結果の返却（フッターと同様の **Response 構造体**を推奨）。
- `app_condition_editor.rs`: 上記トランザクション、`refresh_analysis_runtime`、確認モーダル、`CommandDraft` への統合。可能なら **専用の path-switch ヘルパー**に集約し、分析設定からの `sync` 経路と文言・副作用を混在させない（§11.5）。

### 4.4 未保存時の遷移

確認ダイアログ用に `ConditionEditorConfirmAction` を拡張する。

- 既存: `CloseWindow`, `ReloadPath(PathBuf)`
- 追加案: `OpenPickedPath(PathBuf)`（名称は実装時に統一）

メッセージ例（既存の `ReloadPath` に準拠）:

> 未保存の変更があります。次の条件 JSON を開くと変更は破棄されます。  
> `{path}`

- **続行**: §4.2 の手順 3 以降を実行（override はこの成功後のみ）。
- **キャンセル**: `confirm_action` のみクリア。override / runtime / `pending_path_sync` は触らない。

**未保存でない場合**: 同じく §4.2 の手順 3 から（確認スキップ）。

### 4.5 同一パス再選択（**T0-1 確定**・2026-03-26）

- **採用: (A) 無操作**（ディスクからの再読込は行わない。最新化はフッター「再読込」に任せる）。
- ユーザーがファイルダイアログで選んだパスが **現在の `loaded_path` と同一**である場合（実装では `loaded_path.as_ref() == Some(&picked)` 等の `PathBuf` 等価でよい。OS により表記が揺れる場合は後続で `canonicalize` を検討）:
  - **§4.2 のトランザクション（手順 3〜5）は実行しない**（`filter_config_path_override`・runtime・`document` は変更しない）。
  - 無反応に見えないよう、条件エディターの **ステータス行**に次を表示する（エラー扱いにしない。`status_is_error = false`）:  
    **`既に開いているファイルです。`**
  - `is_dirty` はそのまま（未保存の有無は変えない）。

### 4.6 ダイアログキャンセル

- ファイルダイアログで「キャンセル」した場合は状態を変えない。

### 4.7 エラー・ロールバック（設計確定事項）

- `pick_open_json()` は「All files」も選べるため、**拡張子に依存せず**パース失敗は通常起こりうる（§11.3）。
- 読込・パースが失敗した場合: ステータスにエラーを表示し、**override は更新しない**（§11.5）。手順 4→5 を実行していないためロールバック不要。
- 手順 4 まで成功し**手順 5 で失敗**した場合、`filter_config_path_override` だけ戻しても **`AnalysisRuntimeState` 全体**（`status`、`last_warnings`、`last_export_context`、`session_analysis_cache` 等）は自動では元に戻らない（§13.1）。**対称ロールバック**を本気で行うなら、手順 5 の直前に **`analysis_runtime_state`（必要なら `filter_config_path_override` も）のスナップショット**を取り、失敗時に復元する。
- 同様に editor 側の巻き戻しは `loaded_path` / `document` / 選択インデックスだけでなく、`data_source_generation_at_load`、`pending_path_sync`、`projected_legacy_condition_count`、`status_message`、`status_is_error`、`is_dirty`、`confirm_action` まで含める（§13.1）。中途半端な部分更新は回帰の原因になりやすい。
- **推奨実装形態**: `(document, load_info, path, status_message)` から `ConditionEditorState` の関連フィールドを**一括更新する helper を 1 箇所**に寄せ、`load_condition_editor_from_path` と path-switch ヘルパーから共有する（§13.1）。

**保存ボタンと `resolved_path_ok`**: 不正な override が乗ると、画面上は旧ドキュメントでも保存が無効化されうる（現行仕様）。本機能では **override 確定を読込成功後に限定**することで、この経路を原則防ぐ（§11.1）。

**D-4 の位置づけ**: path-switch は「helper 1 個追加」ではなく、**editor state と runtime state の二相 commit／失敗時 rollback の方針をコードで固定する中核タスク**である（§13.5）。

---

## 5. UI 仕様（ヘッダー）

- 既存の 1 行目（読込中／現在の解決先）の近傍に「**選択**」を配置する。長い絶対パスでは `horizontal_wrapped` だけだとボタン位置が不安定になりやすい（§11.2）ため、**分析設定**と同様、固定幅の非編集テキスト（`ime_safe_singleline` 等）＋ボタン行に寄せることを推奨する。
- **高さ**: 条件エディターは `loaded_path` と `resolved_path` の**2系統**があるため、分析設定の 1 行レイアウトをそのまま当てるとヘッダーが縦に伸びやすい。**2 行固定**にするか、片方を要約表示にするかを実装前に決める（§13.3）。
- `can_modify == false` のときは `add_enabled(false, …)` で無効化。
- ジョブ実行中の説明文を「保存・再読込できません」から **「保存・再読込・ファイルの選択はできません」** 等へ更新し、無効化対象と一致させる（§11.2）。
- ホバーテキスト例: 「別の条件 JSON を開き、分析でもそのパスを使います。」

---

## 6. 変更対象ファイル（予定）

| ファイル | 変更内容 |
|----------|----------|
| `src/condition_editor_view.rs` | `ConditionEditorHeaderResponse`（仮）の追加、`draw_condition_editor_header_panel` の戻り値または out パラメータ化、「選択」ボタン、§5 のレイアウト |
| `src/app_condition_editor.rs` | **path-switch 専用ヘルパー**（§4.2 のトランザクション）、確認アクション拡張、`CommandDraft` 連携、`refresh_analysis_runtime`（成功後のみ） |
| `src/app.rs` | 必要なら `refresh_analysis_runtime` を条件エディターから呼ぶための公開パス（既に `App` メソッドありならその利用のみ） |

**変更しない（予定）**

- `src/file_dialog_host.rs`（`pick_open_json` 再利用）
- `analysis_backend` / Python worker のリクエスト形式
- `src/analysis_runner.rs` のパス解決ロジック（既存の `resolve_filter_config_path` のみ利用）

---

## 7. テスト観点（手動／自動）

| 観点 | 内容 |
|------|------|
| T-1 | 未保存なしで「選択」→ 別ファイルが開き、ツールバー「条件:」表示が新パスに変わる |
| T-2 | 未保存ありで「選択」→ 確認後に切替、破棄されること |
| T-3 | 分析実行中は「選択」無効 |
| T-4 | キャンセルで状態不変 |
| T-5 | 不正 JSON／非 JSON ファイル選択時: override が変わらず、editor／runtime が旧状態のままであること（§4.7） |
| T-5b | 上記失敗のあとも **保存ボタンが従来どおり有効**（`resolved_path_ok` と表示中ドキュメントの整合。§13.4） |
| T-6 | 分析設定で条件 JSON を変えた後の既存 `pending_path_sync` 表示と競合しないこと |
| T-7 | 破棄確認で「キャンセル」: override・`pending_path_sync`・ステータスが汚れていないこと（§11.1） |
| T-8 | 同一パス再選択時: トランザクション未実行・ステータス「既に開いているファイルです。」・`is_dirty` 不変（§4.5） |
| T-9 | path 切替成功後に同じファイルを二重に読み込んでいないこと（ログやブレークポイントで確認でも可。§4.2） |

自動テスト: `egui` 全体の結合は重いため、path-switch の純粋部分を切り出す。**最低 1 ケース**、`parse fail → override 不変 / loaded_path 不変 / is_dirty 不変` を `#[cfg(test)]` で持つことを推奨（§13.4）。追加ケースは任意。

---

## 8. 受け入れ条件（チェックリスト）

- [ ] 条件エディターヘッダーに「選択」がある。
- [ ] 選択した JSON がエディターに表示され、保存はそのパスに対して行われる。
- [ ] 次回の分析／エクスポートで Python に渡る `filter_config_path` が、その選択と一致する。
- [ ] 未保存時は確認あり。分析中は操作不可。
- [ ] 不正／非 JSON 選択で失敗したあとも、表示中ドキュメントは従来どおり保存できる（T-5b）。

---

## 9. リスク・未決事項

| 項目 | 内容 |
|------|------|
| 同一パス再選択 | §4.5 で「再読込のみ／何もしない」のどちらかを実装時に固定。 |
| 手順 5 失敗時 | §4.7 の対称ロールバック（稀）。 |
| 埋め込み／独立ビューポート | 両経路で `HeaderResponse` を適用。 |
| `refresh` の副作用 | 成功時のみキャッシュ等が消える。editor からの操作でも許容するかはプロダクト判断（§11.4）。必要なら将来「軽量パス更新 API」を検討。 |
| 文言の発火元 | `pending_path_sync` 系は分析設定前提の文面のまま。editor 内「選択」は専用ヘルパーで `sync` に依存させず、誤ったメッセージを出さない（§11.3）。 |
| 二相 commit | 手順 5 失敗時は runtime／editor のスナップショット復元を設計どおり実装しないと状態が不完全に残る（§4.7, §13.1）。 |
| 確認オーバーレイのタイミング | `confirm_action` は `show_viewport_immediate` 前にスナップショットされるため、**オーバーレイ表示は次フレーム**になりうる。許容だが実装者向けに共有する（§13.2）。 |

---

## 10. 参考（既存コード）

- 条件 JSON ダイアログ: `RfdFileDialogHost::pick_open_json`（`src/file_dialog_host.rs`）
- 分析設定での上書き: `src/app_analysis_settings.rs`（`filter_config_path_override` + `refresh_analysis_runtime`）
- 確認オーバーレイ: `ConditionEditorConfirmAction` / `apply_condition_editor_modal_response`（`src/app_condition_editor.rs`）

---

## 11. セカンドオピニオン（実装確認ベースの批判的レビュー）

> **本文への反映**: 上記の指摘を踏まえ、**§4・§5・§6・§7・§9 を改訂**した。**§13**（§12 へのセカンドオピニオン）についても **§4.5・§4.7・§5・§7・§9・§12** に取り込み済み（2026-03-26）。以下 §11.1〜11.6 はレビュー当時の論点の記録として残す。

実装を確認したうえでの結論として、この設計は方向性自体は自然だが、現行コードにそのまま載せると **「override だけ先に変わる」「editor は旧ファイルのまま」「分析ランタイムだけ壊れる」** という不整合がかなり起きやすい。初版では「override → refresh → load」の順など、順序を誤るとバグ化しやすい記述があった（改訂後は §4.2 のトランザクションに置き換え）。

### 11.1 バグの可能性が高い点

- **`filter_config_path_override` を先に更新して `refresh_analysis_runtime()` を呼ぶ案は危険。**
  `refresh_analysis_runtime()` は `AnalysisRuntimeState` を丸ごと作り直し、`last_export_context` と `session_analysis_cache` も捨てる（`src/app_analysis_job.rs`、`src/app.rs`）。一方で editor 側の `load_condition_editor_from_path()` は読込成功時にしか `loaded_path` / `document` / `is_dirty` を更新しない（`src/app_condition_editor.rs`）。そのため「override 反映 → refresh → editor 読込失敗」になると、分析ランタイムだけ新パスへ進み、editor は旧ファイルのまま残る。

- **`resolve_filter_config_path()` は JSON 妥当性を見ていない。**
  `src/analysis_runner.rs` では `is_file()` しか見ていないため、存在するが壊れた JSON、JSON ではない別ファイルでも runtime は通る。`load_condition_document()` はその後に `serde_json` で失敗するので、runtime と editor の不一致が現実に起こる。

- **未保存時に override を先に反映すると、キャンセル時の巻き戻しがかなり面倒。**
  現行の `sync_condition_editor_with_runtime_path()` は、dirty 状態で runtime 側のパスが変わると `pending_path_sync` とエラーステータスを立てる（`src/app_condition_editor.rs`）。つまり確認ダイアログでまだ「続行」していないのに、内部状態だけ先に「分析設定で変更済み」になる。キャンセル時に override・runtime・`pending_path_sync`・ステータスメッセージを全部元に戻さないと、状態が汚れる。

- **設計書の「常に refresh の後に `load_condition_editor_from_path()` を呼ぶ」案は二重読込を起こしやすい。**
  `refresh_analysis_runtime()` 自体が最後に `sync_condition_editor_with_runtime_path()` を呼び、clean 状態ならそこで自動再読込する。その直後にさらに `load_condition_editor_from_path()` を呼ぶと、同じファイルを 2 回読むことになる。I/O の無駄だけでなく、ステータスメッセージや選択状態が二度初期化される。

- **失敗時ロールバックを「実装時に決める」では弱い。必須事項に近い。**
  いまの save 可否は `loaded_path` ではなく `resolved_path_ok` に依存している（`src/app_condition_editor.rs`）。つまり不正な override が一度でも乗ると、旧ファイルを編集中でも保存ボタンが無効化されうる。これは UX 劣化ではなく、実害のあるバグに近い。

### 11.2 UI 崩れ・見え方の懸念

- **ヘッダーの 1 行目は、現状でも長いパス 2 本を `horizontal_wrapped` で並べているだけ。**
  ここに「選択」ボタンを足すと、Windows の長い絶対パスで折り返し位置が不安定になりやすい。埋め込みウィンドウと独立ビューポートの両方で、ボタンだけ次行に落ちたり、情報の並び順が崩れたりする可能性がある（`src/condition_editor_view.rs`）。

- **分析設定ウィンドウと見た目の一貫性が崩れる可能性が高い。**
  分析設定側は、固定幅の非編集テキストボックス + ボタンという構成になっている（`src/app_analysis_settings.rs`）。条件エディター側だけ単純ラベル + ボタンにすると、長いパスの可読性が落ちる。UI 崩れ回避の観点では、分析設定側のレイアウトを寄せて再利用したほうが安全。

- **ジョブ実行中メッセージが古いままになる。**
  現在のヘッダー文言は「保存・再読込できません」だけで、「選択」禁止は含んでいない（`src/condition_editor_view.rs`）。ボタン追加後にこの文言を更新しないと、画面上の説明と実際の無効化対象がずれる。

### 11.3 計算・状態遷移の見落とし

- **`pending_path_sync` は「外部から runtime が変わった」ときの補助状態であり、editor 自身の明示操作にはやや不向き。**
  現在の警告文は「分析設定で条件 JSON の解決先が変更されています」「分析設定の変更に合わせて再読込しました」と、発火元が分析設定である前提の文言になっている（`src/app_condition_editor.rs`、`src/condition_editor_view.rs`）。条件エディター内の「選択」で同じ経路を再利用すると、メッセージが事実とずれる。

- **同一パス再選択の扱いは、単なる仕様メモではなく実装分岐として固定したほうがよい。**
  現在すでにフッターに「再読込」があるため、ヘッダーの「選択」で同一パスを選んだ場合を曖昧にすると、同じ再読込でも確認ダイアログやステータス文言が微妙に変わる可能性がある。ユーザー視点では「再読込」と「同じファイルを選び直す」の差が説明しづらい。

- **`pick_open_json()` は `All files` フィルタも持っている。**
  したがって「`.json` フィルタ付きだから安全」という書き方は少し強すぎる。実際には非 JSON ファイルも選べるため、選択完了時の妥当性確認は必須。

### 11.4 意図しない動作の可能性

- **失敗した選択操作が、分析結果の利用可能状態まで落とす可能性がある。**
  runtime 再構築は `last_export_context` とセッションキャッシュを消すので、ファイル切替に失敗しただけで「直前の分析結果をそのまま export する」導線まで失う。これは分析設定画面ではまだ許容しやすいが、editor からの軽いファイル選択操作としては副作用が大きい。

- **不正な path override を残したまま editor を閉じると、次回 open 失敗の原因になる。**
  `open_condition_editor()` は `resolved_filter_config_path()` に成功しないと開けない。つまり editor 内の「選択」で不整合な override を残すと、次回以降に editor 自体を開けなくなる可能性がある。

- **dirty な旧ドキュメントを編集中に選択失敗すると、保存不能状態に陥る設計になりやすい。**
  現行コードでは save 可否が runtime 解決成功に依存するため、失敗した新パスのせいで、まだ画面上に残っている旧ドキュメントを保存できなくなる危険がある。ここは「表示中ドキュメントの救済」を優先しないと事故になりやすい。

### 11.5 設計修正の提案

- **path 選択は transaction 的に扱うべき。**
  具体的には「ファイル選択」「必要なら破棄確認」「選んだファイルの editor 読込検証」「成功したら override commit + runtime refresh」「失敗したら現状態維持」の順にしたほうが安全。

- **`refresh_analysis_runtime()` と `sync_condition_editor_with_runtime_path()` の副作用に、editor 内の選択 UX を乗せすぎないほうがよい。**
  分析設定からの変更同期と、editor 自身の明示的なファイル切替は、似ていても意図が違う。後者は専用の path-switch ヘルパーに切り出したほうが、文言・ロールバック・二重読込を整理しやすい。

- **失敗時ロールバックは設計確定事項にすべき。**
  → **反映済み**（§4.7）。

### 11.6 総評

この機能追加自体は妥当だが、現行コードでは `filter_config_path_override` と `condition_editor_state.loaded_path` が別管理で、しかも `refresh_analysis_runtime()` が状態を広く作り直す。したがって本件は単純な「ボタン追加」ではなく、**path 切替を 1 つの原子的な操作として扱う設計にしないと壊れやすい**。設計書の本文にも、その前提を明記したほうが安全。

---

## 12. 細分タスク（実装順）

依存の浅い順に並べる。**前提**: 各タスク完了時に `cargo check`（および該当あれば `cargo test`）で破綻がないこと。

### 12.0 コマンド処理の原則（§13.2 反映）

- 既存 `apply_condition_editor_command_draft` は概ね **`close` → `add/delete` → `save` → `reload` → `modal_response`** の順。
- **原則**: **モーダル応答（Continue/Cancel）は最優先で単独処理**し、そのフレームでは他の永続状態更新と競合させない。
- **原則**: **pick 系（ヘッダー「選択」）と `save` / `reload` は同一フレームで両立させない**（排他）。実装では `select_clicked` や `picked_path` を **収集のみ**し、**状態確定は 1 箇所**（例: `apply_condition_editor_command_draft` 前後の単一関数）で行う。埋め込み／独立ビューポートで二重実装しない（§13.2）。
- `confirm_action` を立てたフレームでは、オーバーレイ描画は **次フレーム**になりうる（`window_inputs` のスナップショット都合）。許容だが実装時に留意（§13.2）。

### 12.1 準備・方針固定

| ID | タスク | 内容 | 対応 |
|----|--------|------|------|
| T0-1 | §4.5 の確定 | **完了**（2026-03-26）: **(A) 無操作**＋同一パス時ステータス **`既に開いているファイルです。`**（§4.5 参照。実装は D-6/D-4 で分岐）。 | §4.5, T-8 |

### 12.2 ビュー層（`condition_editor_view.rs`）

| ID | タスク | 内容 | 対応 |
|----|--------|------|------|
| V-1 | `ConditionEditorHeaderResponse` | **完了**: `select_clicked` を追加（`ConditionEditorFooterResponse` と同パターン）。 | F-1 |
| V-2 | ヘッダー API 変更 | **完了**: `draw_condition_editor_header_panel` → `ConditionEditorHeaderResponse`。埋め込み／トップパネル両経路で `apply_condition_editor_header_response`。 | F-1, §9 |
| V-3 | レイアウト §5 | **完了**: 2 行（読込中＋選択／解決先）、幅 460、`ime_safe_singleline`。 | §5, §11.2 |
| V-4 | ボタン状態 | **完了**: `can_modify` で無効化、ホバー文言。 | F-6 |
| V-5 | ジョブ中文言 | **完了**: 「保存・再読込・ファイルの選択はできません」。 | §5 |

### 12.3 ドメイン・トランザクション（`app_condition_editor.rs` 中心）

| ID | タスク | 内容 | 対応 |
|----|--------|------|------|
| D-1 | `ConditionEditorConfirmAction` 拡張 | `OpenPickedPath(PathBuf)`（名称は実装で統一）を追加。 | F-5, §4.4 |
| D-2 | 確認メッセージ | `condition_editor_confirm_message` に `OpenPickedPath` 用の文面を追加（`ReloadPath` と同系統）。 | F-5 |
| D-3 | モーダル `Continue` 分岐 | `apply_condition_editor_modal_response` で `OpenPickedPath` のとき §4.2 手順 3〜5 を実行するよう分岐。 | F-5 |
| D-4 | path-switch ヘルパー（中核） | 入力 `PathBuf`、戻り `Result<(), String>`。**二相 commit**: 手順 5 実行前に **`analysis_runtime_state`（＋必要なら `filter_config_path_override`）のスナップショット**を取得。**(1)** `load_condition_document` で検証 **(2)** 成功時のみ editor を §4.7 の一括 helper で更新（二重 I/O なし）**(3)** override 設定 **(4)** `refresh_analysis_runtime()`。**(4) 失敗時**はスナップショットで runtime（および関連 override）を復元し、editor も §4.7 列挙フィールドまで巻き戻す（§13.1）。 | §4.2, §4.7, F-3, F-4, T-5, T-9 |
| D-5 | editor 一括更新 helper | `(document, load_info, path, status_message)` → `ConditionEditorState` の関連フィールドを**一括**更新する関数を 1 つに寄せ、`load_condition_editor_from_path` と D-4 から共有。部分コピーは避ける（§13.1）。 | §4.7, T-9 |
| D-6 | 「選択」クリック〜ダイアログ | `select_clicked` 時: `pick_open_json()`。`None` なら終了。`Some(path)` は §12.0 に従い **収集**し、確定は単一箇所へ。`is_dirty` なら `OpenPickedPath(path)` をセット（**override は未設定**。オーバーレイは次フレーム表示あり得る。§13.2）。 | F-2, F-5, T-7 |
| D-7 | `CommandDraft` 連携 | §12.0 の順序原則に従い、`apply_condition_editor_command_draft` 内または直前の **1 箇所**で pick 確定処理を呼ぶ。`save`/`reload` と同フレーム競合を定義上排除。 | §12.0 |
| D-8 | repaint / viewport | `refresh_analysis_runtime()` 後に UI を更新したい場合、`egui::Context::request_repaint()` や viewport 操作に **別途** `Context` を渡す（refresh API 自体は `Context` 非受け取り。§13.3）。 | 動作確認 |

### 12.4 `App` 境界（`app.rs`）

| ID | タスク | 内容 | 対応 |
|----|--------|------|------|
| A-1 | 境界確認 | `App::refresh_analysis_runtime` は既存。`app_condition_editor` から親経由で呼べるか確認するのみ。**原則、新規公開メソッドは追加しない**（不要な境界変更を避ける。§13.3）。 | §6 |

### 12.5 検証

| ID | タスク | 内容 | 対応 |
|----|--------|------|------|
| Q-1 | 手動テスト | §7 の T-1〜T-9 をチェックリスト化し、実装後に一通り実施。特に T-5, T-7, T-9。 | §7 |
| Q-2 | 回帰 | 分析設定からの条件 JSON 変更 → `pending_path_sync`、フッター保存／再読込、閉じる確認。**不正選択失敗後も保存ボタンが従来どおり有効**（T-5b）。 | T-6, §8 |
| Q-3 | 自動テスト | **完了**: `app_condition_editor::commit_pick_tests::invalid_json_leaves_override_loaded_path_and_dirty_unchanged`（§7, §13.4）。追加は任意。 | §7 |

### 12.6 推奨実装順（まとめ）

1. ~~**T0-1**~~ **完了**（§4.5 確定）  
2. **V-1 → V-5**（ヘッダー UI。`select_clicked` は収集のみでも可）  
3. **D-1 → D-3**（確認アクションとモーダル）  
4. **D-5 → D-4**（一括 editor helper のうえで path-switch ＋ スナップショット／rollback）  
5. **D-6, D-7, D-8**（§12.0 の順序で統合）  
6. **A-1**（確認のみ）  
7. **Q-1 → Q-3**（Q-3 は最低 1 ケース必須）

---

## 13. §12 細分タスクへのセカンドオピニオン

`§12` は前回よりかなり改善されており、特に **override を commit 前に読込検証する**方針と、`path-switch` を専用ヘルパーへ寄せる方針は妥当。ただし、既存コードに当てるとまだ数点、実装タスクとして曖昧または過小見積もりな箇所がある。

### 13.1 もっとも重要な懸念

- **D-4 のロールバック範囲がまだ甘い。**
  `refresh_analysis_runtime()` は `AnalysisRuntimeState` を丸ごと再構築するため、失敗時に `filter_config_path_override` だけ戻しても、`status`、`last_warnings`、`last_export_context`、`session_analysis_cache` は元に戻らない。`§4.7` の「対称ロールバック」を本当にやるなら、**旧 `analysis_runtime_state` 全体の退避**が必要。

- **同様に editor 側も snapshot 前提で書いたほうが安全。**
  D-4 は「手順 3 成功後に editor 状態更新 → 手順 4 失敗なら rollback」としているが、その rollback には `loaded_path` / `document` / `selected_index` だけでなく、`data_source_generation_at_load`、`pending_path_sync`、`projected_legacy_condition_count`、`status_message`、`status_is_error`、`is_dirty`、`confirm_action` まで含む必要がある。ここは D-4 か D-5 に明記したほうがよい。

- **D-5 は「共有関数に抽出」の粒度がまだ粗い。**
  現行の `load_condition_editor_from_path()` は単に `document` を入れるだけではなく、選択 index 初期化、legacy 投影件数、dirty 解消、confirm 解消、status 更新までまとめて行っている。中途半端に一部だけ共有すると回帰しやすい。  
  推奨は「`(document, load_info, path, status_message)` から `ConditionEditorState` の関連フィールドを一括更新する helper」を 1 個に寄せること。

### 13.2 順序・統合まわりの懸念

- **D-7 は、既存の `apply_condition_editor_command_draft()` の順序を踏まえて明文化したほうがよい。**
  現在は `close` → `add/delete` → `save` → `reload` → `modal_response` の順で処理している。`OpenPickedPath` をこの流れへ雑に足すと、モーダル `Continue` や header の `select_clicked` が save/reload と同フレームで競合したときの優先順位が曖昧になる。  
  少なくとも「pick 系は save/reload と排他的」「モーダル応答は最優先で単独処理」など、処理順の原則を §12 に 1 行入れたほうが安全。

- **D-6 / D-7 は、ダイアログをどこで開いて state をどこで確定するかをもう少し固定したほうがよい。**
  現行コードでは header 描画経路が 2 つあり、最後に `apply_condition_editor_command_draft()` を 1 回呼ぶ構造になっている。したがって実装は「UI では `select_clicked` または `picked_path` を収集し、実際の状態更新は 1 箇所で行う」と決めたほうが重複しにくい。  
  ここが曖昧だと、埋め込み経路と独立ビューポート経路で別々に path 処理を書き始める危険がある。

- **dirty 時の確認オーバーレイは“次フレームで出る”前提になる。**
  現在の confirm overlay は `window_inputs.current_confirm_action` を使って描画しており、その値は `show_viewport_immediate()` の前に snapshot されている。つまり D-6 で同フレーム中に `confirm_action = OpenPickedPath(...)` を立てても、オーバーレイ表示は次フレームになる。これは許容できるが、設計上そう書いておいたほうが実装者が混乱しにくい。

### 13.3 タスク定義の精度に関する指摘

- **D-8 の書き方は少し不正確。**
  現在の `refresh_analysis_runtime` 自体は `egui::Context` を受け取らない。`ctx` が必要なのは `request_repaint()` や viewport close/focus 系であって、refresh API そのものではない。  
  タスク名は「refresh 後の repaint / viewport command に必要な `ctx` の受け渡し」に寄せたほうが正確。

- **A-1 は現状だと不要寄り。**
  `App::refresh_analysis_runtime` はすでに存在し、`app_condition_editor.rs` から親モジュールのメソッドを呼ぶ構造も既存で使っている。追加の公開メソッド作業をタスク化すると、必要のない境界変更を誘発する可能性がある。  
  ここは「既存メソッドで足りるか確認。原則追加しない」と書いたほうがよい。

- **T0-1 で (A) 無操作を推奨するなら、無反応に見せない工夫を決めたほうがよい。**
  同一パス再選択で本当に何も起こらないと、ユーザーには「ボタンが効いていない」ように見える可能性がある。`status_message` を変えないのか、「同じファイルです」と出すのかは、実装前に決めたほうがよい。

- **V-3 は、分析設定の見た目を寄せるだけだと高さが増えやすい。**
  条件エディターのヘッダーは `loaded_path` と `resolved_path` の 2 系統を出しているため、分析設定の 1 行レイアウトを機械的に当てると縦に伸びやすい。埋め込み／独立ビューポートの両方で許容する高さなのか、2 行固定なのか、片方を要約表示にするのかを決めておいたほうが UI 崩れを避けやすい。

### 13.4 テスト計画への意見

- **Q-3 は “任意” より一段強くしたほうがよい。**
  今回もっとも事故りやすいのは D-4 の「失敗時に state が汚れないこと」なので、最低でも 1 ケース、`parse fail -> override 不変 / loaded_path 不変 / is_dirty 不変` は自動テストを持ったほうがよい。

- **Q-2 には「save enabled の回帰」も入れたほうがよい。**
  本件は `resolved_path_ok` と save 可否の関係が絡むので、回帰観点として「不正 path を選んで失敗したあとも、元の document は保存可能なままか」を明示したほうがよい。

### 13.5 総評

`§12` は実装順としてかなり現実的になった。ただし、現行コードでは `refresh_analysis_runtime()` の副作用が大きいため、**D-4 を単なる helper 追加と見なすのは危ない**。実際には「editor state と runtime state の二相 commit / rollback をどう扱うか」を決める中核タスクなので、その点だけはもう一段具体化してから着手したほうが安全。

> **本文への反映（§13）**: §13.1〜13.4 の指摘は **§4.5・§4.7・§5・§7・§9・§12** に取り込み済み（2026-03-26）。§13 はレビュー記録として残す。
