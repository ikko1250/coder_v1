# call-gemma4-gemini package 化外 follow-up bug list

## 目的

package 化の直接要件ではない既存バグを、別タスクに切り出しやすい粒度で整理する。

## 扱い方針

- ここに並ぶ項目は package 化の完了条件に含めない。
- それぞれ独立した follow-up task として扱う。
- 1 項目 1 修正を基本にする。

## Follow-up 候補

### 1. `build_unified_diff_text` の改行バグ

- unified diff の行結合で改行が二重化し、出力が崩れる。
- CLI の diff 表示品質に直結するが、package 化そのものとは独立している。

### 2. write lock の孤立ロック

- 前回異常終了後に `.lock` が残ると、次回実行がタイムアウトしやすい。
- ロック回収や stale lock 判定の専用対応が必要。

### 3. turn 上限制御

- turn 数の明示上限がなく、異常応答時にループが長引く可能性がある。
- budget 制御とは別に、実行回数の上限を設ける follow-up として扱う。

### 4. LF 正規化の影響

- 入出力の改行正規化が diff や書き戻しの見え方に影響する。
- Windows / Git 設定差の影響確認を含めて別途扱う。

### 5. 将来の rename 候補と dotenv 共通化

- `pdf_converter/pdf_converter.py` は import-safe 化後も名前が紛らわしいため、rename 候補として残す。
- `call-gemma4-gemini.py` と `pdf_converter.py` の dotenv ロードは重複しているため、共通ユーティリティ化の候補として残す。
- いずれも package 化本体とは別タスクとして扱い、現時点では実装しない。

## 参照先

- 詳細な境界整理は [call-gemma4-gemini-uv-cli-package-plan.md](./call-gemma4-gemini-uv-cli-package-plan.md)
- task breakdown 側の受け皿は [call-gemma4-gemini-uv-cli-package-task-breakdown.md](./call-gemma4-gemini-uv-cli-package-task-breakdown.md)
