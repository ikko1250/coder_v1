# P1-08: `all_records` / `filtered_indices` / `selected_row` 変更パス一覧

本書は設計書 §9.1 P1-08 の完了条件（grep 相当の網羅）に基づき、**ベクトル／フィールドの代入**と**間接的な更新経路**を列挙する。  
参照時点のコードベース: `src/`（2026-03-23 時点の構成に準拠）。

---

## 総覧

| 状態 | 直接代入が発生するファイル | 備考 |
|------|---------------------------|------|
| `all_records` | `app.rs` のみ | ベクトル**置換**は `replace_records` のみ。行データの**内容更新**は `selected_record_mut` 経由。 |
| `filtered_indices` | `app.rs` のみ | `replace_records` と `apply_filters`。 |
| `selected_row` | `app.rs` のみ | **`apply_selection_change` 内の単一箇所**（`self.selected_row = next`）のみが代入。 |

他モジュール（`app_main_layout` / `app_toolbar` / `app_lifecycle` 等）はこれらフィールドを**読む**か、**`App` のメソッド経由**で間接的に更新する。

---

## 1. `all_records`

### 1.1 ベクトル全体の置換

| 呼び出し元（エントリ） | 実装 | 内容 |
|-------------------------|------|------|
| `App::new` → 任意で `load_csv` | `app.rs` `load_csv` → `replace_records` | CSV 読込成功時に差し替え。 |
| ツールバー「CSVを開く」 | `app_toolbar.rs` → `app.load_csv` | 上と同じ。 |
| 分析ジョブ成功 | `app_analysis_job.rs` `handle_analysis_success` → `app.replace_records` | Python 側の結果レコードで差し替え。 |

集約メソッド: **`App::replace_records`**（`app.rs`）

- `self.all_records = records;`
- あわせて `filtered_indices` を全件 `(0..len)` に再構築
- `apply_selection_change(… first_filtered_row …)` で `selected_row` を更新

### 1.2 行の内容（`AnalysisRecord`）のインプレース更新

ベクトルのアドレスは変えず、**選択行のレコード**のフィールドを更新する。

| 経路 | 実装 | 内容 |
|------|------|------|
| 手動アノテーション保存 | `app.rs` `apply_saved_annotation_to_selected_record` | `selected_record_mut()` → `all_records.get_mut(record_idx)` で注釈フィールド等を更新。 |

※ この経路では **`filtered_indices` / `selected_row` は変更しない**（同一行のまま）。

---

## 2. `filtered_indices`

### 2.1 代入箇所

| メソッド | ファイル | 内容 |
|----------|----------|------|
| `replace_records` | `app.rs` | 新データに対し **全件** `(0..all_records.len()).collect()`。 |
| `apply_filters` | `app.rs` | `all_records` を走査し、フィルタに合致する **元インデックス** のみを `collect()`。 |

### 2.2 間接呼び出し（`apply_filters` へ）

フィルタ状態を変えたあと `apply_filters` が呼ばれる。

| 操作 | `app.rs` 内の経路 |
|------|-------------------|
| フィルタ適用ロジック | `apply_filters` 本体 |
| 列のフィルタ解除 | `clear_filters_for_column` → `apply_filters` |
| 全解除 | `clear_all_filters` → `apply_filters` |
| 値トグル | `toggle_filter_value` → `apply_filters` |

`apply_filters` の末尾で **`select_first_filtered_row`** が呼ばれ、結果として **`selected_row` も更新**される（§3 参照）。

---

## 3. `selected_row`

### 3.1 代入の単一箇所

**`App::apply_selection_change`**（`app.rs`）内:

- `self.selected_row = next;`（`clamp_selected_row` 済みの `Option<usize>`）

これ以外に `selected_row` への代入はない（初期化は `App::new` の struct literal のみ）。

### 3.2 `apply_selection_change` への到達経路

| 経路 | 呼び出し元（概要） |
|------|---------------------|
| データ差し替え直後 | `replace_records` → `SelectionChange::first_filtered_row` |
| フィルタ再計算後 | `apply_filters` → `select_first_filtered_row` → `apply_selection_change` |
| キーボード ↑↓ | `app_lifecycle` `handle_keyboard_navigation` → `move_selection_up` / `move_selection_down` → `apply_selection_change` または `select_first_filtered_row` |
| ツリークリック | `impl eframe::App::update` で `draw_body` の戻り `clicked_row` → `apply_selection_change` |

---

## 4. 読み取りのみ（変更なし）

次は **変更しない** が、UI やナビに利用する。

| ファイル | 用途例 |
|----------|--------|
| `app_main_layout.rs` | 一覧描画で `filtered_indices` / `selected_row` を参照 |
| `app_toolbar.rs` | 件数表示で `all_records.len()` / `filtered_indices.len()` / `selected_row` |
| `app_lifecycle.rs` | キーボードナビ可否で `filtered_indices.is_empty()` |

---

## 5. 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P1-08 初版（grep ベースの一覧） |
