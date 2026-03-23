# P1-09: `cached_segments` および類似キャッシュの無効化・更新箇所

設計書 §9.1 P1-09 の完了条件に基づき、**アプリ状態に紐づくキャッシュ**と、**静的な正規表現キャッシュ**を分けて列挙する。

---

## 1. `App::cached_segments`（本文ハイライト用セグメント）

### 1.1 役割

- 型: `Option<(usize, Vec<TextSegment>)>`
- **第 1 要素**は `AnalysisRecord::row_no`（一覧上の行番号キー）。選択行のタグ付きテキストを `parse_tagged_text` した結果を保持し、`get_segments` の再パースを避ける。

### 1.2 無効化（`None` へのクリア）

| 箇所（メソッド） | ファイル | トリガー |
|------------------|----------|----------|
| `App::new` の初期化 | `app.rs` | 起動時 `cached_segments: None` |
| `replace_records` | `app.rs` | CSV／分析結果でレコード配列を差し替え |
| `apply_selection_change`（選択が変わったときのみ） | `app.rs` | `selection_changed` が true のとき |
| `apply_filters` | `app.rs` | フィルタ再計算（一覧の行集合が変わるため） |
| `apply_saved_annotation_to_selected_record` | `app.rs` | 手動アノテーションを選択行に反映した直後（レコード内容更新に合わせ詳細ペインの整合を取る） |

**間接経路**: `load_csv` → `replace_records`；分析成功 `handle_analysis_success` → `replace_records`；フィルタ UI → `apply_filters` / `clear_*` / `toggle_filter_value` → `apply_filters`；キーボード・クリック → `apply_selection_change`；注釈保存成功 → `apply_saved_annotation_to_selected_record`（`save_annotation_for_selected_record` 経由）。

### 1.3 更新（ヒット時は再パースしない／ミス時は再計算して格納）

| 箇所 | ファイル | 内容 |
|------|----------|------|
| `get_segments` | `app.rs` | 選択行の `row_no` がキャッシュと一致すれば `Vec` を clone で返す。一致しなければ `parse_tagged_text` 後に `Some((row_no, segs))` を代入。選択なしは `Vec::new()` のみでキャッシュは更新しない。 |

### 1.4 利用箇所（読み取り）

| 箇所 | ファイル |
|------|----------|
| 詳細ペインのレイアウト | `app_main_layout.rs`（`app.get_segments()`） |

### 1.5 インプレース更新（手動アノテーション）

同一 `row_no` のまま `AnalysisRecord` を更新する **`apply_saved_annotation_to_selected_record`** では、**`filter_options` 再構築に加え `cached_segments = None`** を行う（P1 レビュー後の追補）。本文タグ付き文字列が注釈と無関係でも、ツリー側の表示整合のためキャッシュを捨てる。

---

## 2. 類似: タグパーサの静的キャッシュ（`App` 外）

| 箇所 | ファイル | 内容 |
|------|----------|------|
| `OnceLock<Regex>` | `tagged_text.rs` | 正規表現を**初回のみ**コンパイル。`App` の状態とは独立。無効化の概念はなく、プロセス生存中は保持。 |

---

## 3. 類似: 分析ワーカーのグローバルスロット（UI キャッシュではない）

| 箇所 | ファイル | 内容 |
|------|----------|------|
| `WORKER_SLOT` + `invalidate_worker_slot` | `analysis_runner.rs` | Python ワーカープロセスの再利用スロット。**セグメント表示キャッシュとは別**だが、「実行環境の指紋が変わったら捨てる」という意味で無効化がある。 |

---

## 4. データソース切替に連動する別状態（参考）

| 箇所 | ファイル | 内容 |
|------|----------|------|
| `DbViewerState::reset_loaded_state` | `model.rs`（`replace_records` から呼び出し） | DB 参照ウィンドウのロード済み段落・文脈をクリア。**`cached_segments` とは別**だが、同じ「データが差し替わった」タイミングで実行される。 |

---

## 5. 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P1-09 初版 |
| 2026-03-23 | 注釈保存経路で `cached_segments` を無効化する旨を追記（`apply_saved_annotation_to_selected_record`） |
