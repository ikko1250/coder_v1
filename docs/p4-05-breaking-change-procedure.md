# P4-05: ブレークチェンジ時の運用手順

設計書 §9.1 P4-05 に基づき、IPC DTO のブレークチェンジ時に実施する最小手順を `document/p4-05-breaking-change-procedure.md` に定義した。

## 参照先

- [`document/p4-05-breaking-change-procedure.md`](../document/p4-05-breaking-change-procedure.md)

## 要点

- `IPC_API_VERSION` を更新する。
- DTO 差分の意図と移行観点を `docs/p4-0x` に追記する。
- `cargo test` と `cargo run -- --ipc-dto-self-check` を通す。
- 設計書改訂履歴にブレークチェンジを明記する。

## 改訂

| 日付 | 内容 |
|------|------|
| 2026-03-23 | P4-05 初版 |
