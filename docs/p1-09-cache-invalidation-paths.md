# P1-09: `cached_segments` および類似キャッシュの無効化・更新箇所

設計書 §9.1 P1-09 の完了条件に基づき、**アプリ状態に紐づくキャッシュ**と、**静的な正規表現キャッシュ**を分けて列挙する。

---

## 1. `ViewerCoreState::detail_segment_cache`（本文ハイライト用セグメント）

### 1.1 役割

- 型: `Option<(usize, Vec<TextSegment>)>`（**P2-07** で `App::cached_segments` から `viewer_core` へ移動）
- **第 1 要素**は `AnalysisRecord::row_no`（一覧上の行番号キー）。選択行のタグ付きテキストを `parse_tagged_text` した結果を保持し、`get_segments` の再パースを避ける。

### 1.2 無効化（`None` へのクリア）

`ViewerCoreState::invalidate_detail_segment_cache(SegmentCacheInvalidateReason)`（`src/viewer_core.rs`）。理由列挙子は P2-07 で経路を明示するため。

| 箇所（メソッド） | ファイル | `SegmentCacheInvalidateReason` |
|------------------|----------|-------------------------------|
| `ViewerCoreState::default` | `viewer_core.rs` | 初期化で `None` |
| `replace_records` | `app.rs` | `ReplaceRecords` |
| `apply_selection_change`（選択が変わったときのみ） | `app.rs` | `SelectionChanged` |
| `apply_filters` | `app.rs` | `FilterApplied` |
| `apply_saved_annotation_to_selected_record` | `app.rs` | `AnnotationSaved` |

**間接経路**: `load_csv` → `replace_records`；分析成功 `handle_analysis_success` → `replace_records`；フィルタ UI → `apply_filters` / `clear_*` / `toggle_filter_value` → `apply_filters`；キーボード・クリック → `apply_selection_change`；注釈保存成功 → `apply_saved_annotation_to_selected_record`（`save_annotation_for_selected_record` 経由）。

### 1.3 更新（ヒット時は再パースしない／ミス時は再計算して格納）

| 箇所 | ファイル | 内容 |
|------|----------|------|
| `get_segments` | `app.rs` | 選択行の `row_no` がキャッシュと一致すれば `Vec` を clone で返す。一致しなければ `parse_tagged_text` 後に `set_detail_segment_cache` で代入。選択なしは `Vec::new()` のみでキャッシュは更新しない。 |

### 1.4 利用箇所（読み取り）

| 箇所 | ファイル |
|------|----------|
| 詳細ペインのレイアウト | `app_main_layout.rs`（`app.get_segments()`） |

### 1.5 インプレース更新（手動アノテーション）

同一 `row_no` のまま `AnalysisRecord` を更新する **`apply_saved_annotation_to_selected_record`** では、**`filter_options` 再構築に加え `invalidate_detail_segment_cache(AnnotationSaved)`** を行う（P1 レビュー後の追補、P2-07 で理由明示）。本文タグ付き文字列が注釈と無関係でも、ツリー側の表示整合のためキャッシュを捨てる。

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
| 2026-03-23 | P2-07: `detail_segment_cache` を `ViewerCoreState` へ、`SegmentCacheInvalidateReason` を追記 |
