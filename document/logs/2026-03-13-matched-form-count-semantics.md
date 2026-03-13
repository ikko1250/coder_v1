# 2026-03-13 matched_form_count 意味整理ログ

- 対象: `analysis_backend/condition_evaluator.py`, `tests/test_analysis_core.py`
- 目的: `matched_form_count` が paragraph 単位で何を表すかを明確化する

## 既存挙動

- `matched_form_count` は unit 単位では `n_unique(normalized_form)` だった
- paragraph 集約時には `max()` が使われていた
- この列は現在、UI や export CSV では使われておらず、condition evaluation の説明用メトリクスとして残っている

## 判断

- `matched_form_count` は「paragraph 内で最も form が揃った unit の coverage」を表す列として扱う
- これは `is_match` 判定そのものではなく、paragraph の診断用サマリ値
- したがって、今ここで `min()` や `avg()` に変えるより、意味を固定する方が安全

## 実施内容

1. `condition_evaluator.py` に `_paragraph_matched_form_count_expr()` を追加
   - `max()` 集約の意図を helper 名とコメントで明示
2. `tests/test_analysis_core.py` に回帰テストを追加
   - paragraph 内に 2 sentence あり、1 sentence だけが full match のケースを作成
   - `matched_form_count == 2`
   - `evaluated_unit_count == 2`
   - `matched_unit_count == 1`
   を確認

## 検証

- コマンド:
  - `UV_CACHE_DIR=/tmp/uv-cache uv run python -m unittest tests.test_analysis_core tests.test_cli`
- 結果:
  - 24 tests passed

## 備考

- 今回は列名自体は変えていない
- 将来的により明示的な名前へ変える場合は、façade 互換や notebook 利用を見ながら別タスクで行う
