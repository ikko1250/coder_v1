# P2-04: `apply_event` → `CoreOutput` と `needs_repaint`

設計書 §9.1 P2-04・§5.5 に基づき、コア更新の戻り値を **`CoreOutput { needs_repaint }`**（`src/viewer_core.rs`）に統一し、**`App::apply_event`**（`src/app.rs`）がこれを返すようにした。

## 変更概要

| 項目 | 内容 |
|------|------|
| `CoreOutput` | `needs_repaint: bool`。論理状態が変化し再描画が必要なときに `true`。 |
| `ViewerCoreEvent` | `ViewerCoreMessage` の型エイリアス（`apply_event` の引数名に合わせた読み替え）。 |
| `apply_event` | `apply_core_message` を置換。戻り値は `bool` ではなく `CoreOutput`。 |
| `load_csv` | 成功時 `Some(CoreOutput)`、失敗時 `None`。ツールバーで `needs_repaint` に応じて `request_repaint`。 |

## 呼び出し側の連携

- **キーボード**（`app_lifecycle`）: `needs_repaint` なら `ctx.request_repaint()`。
- **フィルタ**（`app_main_layout`）: 同上を `ui.ctx()` で。
- **行クリック**（`App::update`）: 同上。
- **CSV ツールバー**（`app_toolbar`）: `load_csv` の `Some` かつ `needs_repaint` で `request_repaint`。
- **分析完了**（`app_analysis_job`）: `poll_analysis_job` 側で既に `request_repaint` するため、`apply_event` の戻り値は未使用でもよい。

## 非目的（P2-04 ではやらないこと）

- `ViewerCoreState` だけを `&mut ViewerCoreState` に閉じた `apply_event`（状態はまだ `App` 全体を更新する）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-04 初版 |
