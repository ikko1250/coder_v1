# P2-06: `can_close` と `CloseBlockReason`（§5.4）

設計書 §5.4・§9.1 P2-06 に基づき、`src/viewer_core.rs` に次を追加した。

- **`CloseBlockReason`**: 終了をブロックするドメイン理由（現状は `UnsavedConditionEditor` のみ）。
- **`ViewerCoreCloseInput`**: ホストが渡す終了判定用の入力（`condition_editor_dirty` など）。`egui` 非依存。
- **`ViewerCoreState::can_close(&self, &ViewerCoreCloseInput) -> Result<(), CloseBlockReason>`**

`app_analysis_job::guard_root_close_with_dirty_editor` は、従来の `is_dirty` 直参照の代わりに **`app.core.can_close(&input)`** で可否を問い合わせる。終了キャンセル・メッセージ表示は引き続き **egui ホスト**側。

## テスト

`viewer_core` のユニットテストで、**未保存（`condition_editor_dirty: true`）のとき `can_close` が `Err`** になることを検証する。

## 非目的（P2-06 ではやらないこと）

- 条件エディタ状態そのものの `ViewerCoreState` への移管（入力はホストが渡すのみ）。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P2-06 初版 |
