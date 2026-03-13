# 2026-03-13 analysis_runner warningMessages 互換修正ログ

- 対象: `src/analysis_runner.rs`
- 目的: Python CLI が出力する structured `warningMessages` を Rust 側で読めるようにし、旧形式との後方互換も維持する

## 既存挙動

- Rust 側 `AnalysisMeta.warning_messages` は `Vec<String>` だった
- Python CLI は `warningMessages` を dict 配列で出力する
- そのため warning が 1 件以上ある成功系で `meta.json` の deserialize が壊れる可能性があった
- さらに成功終了時の meta parse failure は `meta.json が生成されませんでした` と誤った文言で返されていた

## 実施内容

1. `AnalysisWarningMessage` を追加
   - `code`
   - `message`
   - `condition_id`
   - `unit_id`
   - `requested_mode`
   - `used_mode`
   - `combination_count`
   - `combination_cap`
   - `safety_limit`
2. `warning_messages` を `Vec<AnalysisWarningMessage>` へ変更
3. `deserialize_warning_messages(...)` を追加
   - structured object 配列を受理
   - 旧 `Vec<String>` 形式も受理し、`message` のみ持つ warning へ変換
4. 成功時の meta read/parse failure 文言を修正
   - `meta.json が生成されませんでした` 固定ではなく、実際の read/parse error を返す
5. unit test を追加
   - structured warning 配列を読めること
   - legacy string warning 配列も読めること

## 検証

- この環境では `cargo` と `rustfmt` が未導入のため、Rust テスト実行と整形は未実施
- 追加したテスト:
  - `read_meta_json_accepts_structured_warning_messages`
  - `read_meta_json_accepts_legacy_string_warning_messages`

## 備考

- `src/app.rs` は warning の件数しか使っていないため、この変更で UI 側の利用箇所は維持される
- 次段では `cli.py` の `limit_rows` と schema 重複整理へ進む
