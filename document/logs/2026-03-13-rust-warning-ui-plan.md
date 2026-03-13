# 2026-03-13 Rust warning 表示 実装案ログ

- 対象: `document/rust-warning-ui-plan.md`
- 目的: Rust 側で distance matching warning を見える化するための実装案を整理する

## 既存挙動

- `analysis_runner.rs` は warning の主要項目を deserialize できる
- ただし `app.rs` は成功時 summary に警告件数を出すだけで、warning detail は表示しない
- Python 側では warning payload が増えているが、Rust UI は活用していない

## 実施内容

1. 現状の deserialize 範囲と UI 表示箇所を確認
2. `AnalysisWarningMessage` 拡張
3. App state に warning detail を保持
4. success / failure 双方で warning を保存
5. 最小 warning detail window を追加
という段階実装案を作成

## 備考

- 今回は設計整理のみで、Rust コード本体は変更していない
- 次に実装へ入るなら、Phase 1-4 までを一塊にして差し込むのが妥当
