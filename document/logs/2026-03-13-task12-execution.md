# 2026-03-13 Task 12 実施ログ

- 対象: `document/analysis-core-post-split-cleanup-plan.md`
- 目的: 分割フェーズ完了後の次段改善入口を整理し、再開順を固定する

## 既存挙動

- Task 11 までで、`analysis_core.py` の分割、matching mode 明確化、warning 伝播、契約テスト拡張までは完了していた
- ただし、以後の改善項目はログに点在しており、優先順位が 1 枚にまとまっていなかった
- 直前確認で、Rust `analysis_runner` と Python CLI の `warningMessages` 契約に非互換があることも判明した

## 実施内容

1. `document/analysis-core-post-split-cleanup-plan.md` を追加
   - 分割後の現状を要約
   - 後続タスクを優先順位付きで整理
   - `analysis_runner` 非互換を P0 に設定
   - schema 集約、条件正規化ポリシー、Polars 境界、façade 縮退を次段課題として明文化
2. 再開時の入口ドキュメントを明記
   - compatibility 起点
   - analysis_backend 内部整理起点
   - 分割経緯確認起点

## 検証

- ドキュメント整理のみのため、自動テストは未実施

## 備考

- 今回はコード本体を変更していない
- 次に実装へ進むなら、P0 の `analysis_runner` meta 契約整合から着手するのが妥当
