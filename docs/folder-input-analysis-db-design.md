# フォルダー入力による Analysis DB 生成と汎用カテゴリ化 設計

## 目的

`docs/build_ordinance_analysis_db.py` を、source DB から入力ファイル一覧を取る方式から、指定フォルダー内の `txt` / `md` を直接走査する方式へ変更する。

同時に、既存コードベースに残っている条例ドメイン依存の名称を、アプリケーション汎用化のために `category1` / `category2` へ置き換える。

本変更の最終目標は次の 3 点である。

1. `<category1>_<category2>.(txt|md)` 形式のファイル群から analysis DB を生成できること。
2. 段落分割、文分割、Sudachi によるトークナイズ、SQLite 格納の既存処理を流用できること。
3. 入力不正や実行時エラーを握りつぶさず、利用者に明示的に提出できること。

## スコープ

対象:

- `docs/build_ordinance_analysis_db.py`
- analysis DB スキーマの文書メタデータ列
- Python 側の metadata reader / export formatter
- Rust 側の CSV / JSON 受け取りモデル、フィルター、表示ラベル
- 実行時エラーのレポート方法
- Sudachi 依存の導入方法

対象外:

- Markdown の前処理や正規化
- 既存の段落・文分割ルールの変更
- 条件評価ロジックそのもの

## 要件

### 入力ファイル規約

- 入力元は指定フォルダー配下の `*.txt` と `*.md` とする。
- 収集は再帰走査とする。
- 対象ファイル名は必ず `<category1>_<category2>.(txt|md)` に一致しなければならない。
- `category1` と `category2` は空文字不可とする。
- 区切り文字 `_` は予約文字とし、`category1` / `category2` の値自体には含めない。
- 拡張子は小文字の `.txt` / `.md` のみを正とする。

採用する判定規則:

```text
stem regex: ^(?P<category1>[^_]+)_(?P<category2>[^_]+)$
ext: .txt or .md
```

この制約により、ファイル名から一意に `category1` / `category2` を抽出できる。

### 失敗時の基本方針

- 1 件でも入力規約違反ファイルが見つかった場合、処理全体を開始しない。
- 事前検証で不正が見つかった時点では analysis DB を更新しない。
- 実行途中の read / tokenize / DB insert エラーも黙殺しない。
- エラー情報は標準エラー出力に要約を出し、詳細はレポートファイルへ保存する。

### 前処理

- Markdown / text の本文に対する追加前処理は行わない。
- 現行の段落分割、表段落判定、文分割をそのまま使う。

### 依存

- Sudachi を使う通常モードは、必要な Python パッケージが入っていることを前提にする。
- `--skip-tokenize` 時のみ Sudachi 依存なしで実行可能とする。

## 現状整理

現行の `build_ordinance_analysis_db.py` は、実質的に以下の 3 層に分かれている。

1. source DB から `SourceFileRow` を組み立てる層
2. 本文から段落、文、形態素を生成する層
3. analysis DB へ insert する層

このうち source DB 依存は `load_source_rows()` と `main()` 冒頭に集中しており、`process_rows()` 以降は `SourceFileRow` のリストを受けるだけである。したがって、入力列挙部分をフォルダー走査に差し替えても、本文処理と DB 生成本体はほぼ再利用できる。

## 提案設計

## 1. 入力モード

CLI は source DB モードを廃止せず、以下の二択にする。

- `--input-dir <path>`
- `--source-db <path> --source-table <table>`

ただし新しい標準運用は `--input-dir` とする。`--input-dir` と `--source-db` は同時指定不可とする。

追加引数案:

- `--input-dir`: 入力フォルダー
- `--report-path`: エラーレポート出力先。未指定時は `analysis-db` と同じディレクトリに自動生成

`--strict` は追加しない。今回要件では常時 strict のため、無効化可能なフラグを設けない。

## 2. フォルダー走査と事前検証

新規関数:

- `load_source_rows_from_dir(input_dir: Path, limit: int | None) -> list[SourceFileRow]`
- `validate_source_files(source_rows: list[SourceFileRow]) -> list[InputIssue]`

処理順:

1. `os.walk(input_dir, followlinks=False)` で再帰走査
2. `.txt` / `.md` のみ抽出し、それ以外は無視
3. `relative_path.as_posix()` 昇順でソート
4. ファイル名を strict に検証
5. 1 件でも不正があれば警告一覧を出して終了
6. 全件妥当なら `process_rows()` に渡す

`SourceFileRow` は次のように変更する。

```python
@dataclass
class SourceFileRow:
    file_path: str
    file_name: str
    ext: str
    category1: str
    category2: str
```

source DB モードを残す場合も、DB 側の `municipality_name` / `ordinance_or_rule` からこの 2 フィールドへ正規化して受け渡す。

カテゴリ値は filename の表記をそのまま保存し、大小文字の自動正規化は行わない。カテゴリの一意性は `source_file_path` に依存させる。

使用可能文字は原則として「`_`、パス区切り、制御文字を除く任意の Unicode 文字」とする。ドットや空白は許容する。

## 3. メタデータ名称の一般化

### 3-1. 正式な論理名

今後の正規名は次の 2 つに統一する。

- `category1`
- `category2`

現行名称との対応:

- `municipality_name` -> `category1`
- `ordinance_or_rule` -> `category2`
- `doc_type` -> Phase 1 では互換 alias として存置、Phase 2 で廃止

`doc_type` は条例ドメインの派生概念であり、汎用入力ではファイル名から直接得られない。したがって canonical なメタデータは `category1` / `category2` とし、`doc_type` は移行期間だけ互換用途で残す。

source DB モードで `category2` を組み立てる場合は、現行の `ordinance_or_rule` 導出と同じ規則を使う。

- `doc_type` に `施行規則` を含む -> `category2 = "施行規則"`
- それ以外で `条例` を含む -> `category2 = "条例"`
- それ以外 -> `category2 = COALESCE(doc_type, "不明")`

### 3-2. DB スキーマ

`analysis_documents` は以下へ変更する。

- Phase 1 で追加:
  - `category1 TEXT NOT NULL`
  - `category2 TEXT NOT NULL`
- Phase 1 では旧列を物理削除しない:
  - `doc_type`
  - `municipality_id`
  - `municipality_name`
  - `ordinance_id`
- Phase 2 で fresh DB を新規作成する場合のみ、旧列を含まない新スキーマへ移行する

SQLite の既存 DB に対しては destructive migration を前提にしない。既存 DB は `ALTER TABLE ADD COLUMN` ベースで拡張し、旧列は互換用に残す。

`analysis_runs.source_db_path` は意味が変わるため、以下へリネームする。

- `source_db_path` -> `source_locator`

格納値:

- フォルダーモード: 入力ディレクトリの絶対パス
- source DB モード: source DB の絶対パス

### 3-3. Python 側 reader / export

以下の出力列を generic に変更する。

- canonical:
  - `category1`
  - `category2`
- Phase 1 の互換 alias:
  - `municipality_name = category1`
  - `ordinance_or_rule = category2`
  - `doc_type = category2`

Phase 1 では Python 側の JSON / CSV / Polars 出力で新旧両方の列名を同時に出す。Rust 側の移行が終わるまで、旧クライアント契約を壊さない。

対象ファイル:

- `analysis_backend/frame_schema.py`
- `analysis_backend/data_access.py`
- `analysis_backend/export_formatter.py`

### 3-4. Rust 側の UI / CSV ローダー

以下の名称を generic に変更する。

- Phase 1:
  - Rust は旧名入力と新名入力の両方を受けられるようにする
  - `AnalysisJsonRecord` / CSV loader は `category1` / `category2` を追加受理する
  - UI ラベルは互換維持のため当面据え置きでもよい
- Phase 2:
  - `AnalysisRecord.municipality_name` -> `category1`
  - `AnalysisRecord.ordinance_or_rule` -> `category2`
  - `AnalysisRecord.doc_type` は削除
  - `FilterColumn::MunicipalityName` -> `FilterColumn::Category1`
  - `FilterColumn::OrdinanceOrRule` -> `FilterColumn::Category2`
  - `FilterColumn::DocType` は削除
  - フィルター表示ラベルは `category1` / `category2` とする

主な影響箇所:

- `src/model.rs`
- `src/filter.rs`
- `src/csv_loader.rs`
- `src/analysis_runner.rs`
- `src/app_main_layout.rs`
- `analysis_backend/export_formatter.py`

## 4. エラー報告設計

### 4-1. 事前検証エラー

対象:

- 入力フォルダーが存在しない
- 対象ファイル 0 件
- ファイル名規約違反
- Sudachi 依存不足
- `--input-dir` と `--source-db` の併用など CLI 不正

方針:

- run を開始しない
- analysis DB を更新しない
- `stderr` に件数と先頭数件を出す
- 詳細は report JSON に保存する

`--report-path` 未指定時は、`Path(args.analysis_db).resolve()` を基準に sibling ファイルとして
`<analysis_db_name>.report.json`
を生成する。カレントディレクトリ依存にはしない。

レポート形式案:

```json
{
  "status": "preflight_failed",
  "input_dir": "C:/data/source",
  "issue_count": 2,
  "issues": [
    {
      "severity": "warning",
      "code": "invalid_file_name",
      "path": "foo/bar.txt",
      "message": "file name must match <category1>_<category2>.(txt|md)"
    }
  ]
}
```

### 4-2. 実行時エラー

対象:

- ファイル読み込み失敗
- 文字コードデコード失敗
- トークナイズ失敗
- SQLite insert 失敗

方針:

- 出力先 DB の sibling に一時 DB ファイルを作成し、そこへ書き込む
- 一時 DB 内では 1 run を 1 transaction で扱う
- 途中で 1 件でも致命エラーが出たら rollback し、一時 DB を破棄する
- 成功時のみ一時 DB を最終パスへ rename / replace する
- 既存 DB は成功確定まで触らない
- エラー詳細を report JSON に保存する

これにより、`start_run()` / `finish_run()` のコミット矛盾と、`--recreate-db` 失敗時のバックアップ復元問題を同時に回避する。大規模データ時の lock 競合も最終置換時だけに限定できる。

### 4-3. 文字コード

現行の `encoding="utf-8", errors="ignore"` は silent corruption を起こし得るため廃止する。

新方針:

1. 生バイトを読む
2. 先頭が UTF-8 BOM の場合は `utf-8-sig` で decode する
3. それ以外は `utf-8` strict で decode する
4. 失敗したら `decode_error` としてレポートし、run を失敗させる

今回の設計では自動文字コード推定は導入しない。

## 5. 既存本文処理の扱い

以下は既存実装をそのまま流用する。

- 段落分割
- 表段落の検出
- 文分割
- Sudachi による token insert

したがって、テキスト処理ロジックの変更点は入力列挙とエラー制御が中心であり、本文解析仕様の再設計は不要である。

## 6. Sudachi 依存の対処

`pyproject.toml` に optional dependency group を追加する。

案:

```toml
[project.optional-dependencies]
analysis-db = [
  "sudachipy>=0.6",
  "sudachidict-core>=20240109"
]
```

運用:

- トークナイズあり: `uv sync --extra analysis-db`
- トークナイズなし: `uv sync`

加えて README に次を追記する。

- analysis DB builder を使うには `analysis-db` extra が必要
- `--skip-tokenize` は Sudachi 未導入でも使える

## 7. 互換性方針

完全移行を一度に行うと影響が広いため、実装は 2 段階で進める。

### Phase 1

- builder をフォルダーモード対応
- analysis DB に `category1` / `category2` を追加
- Python / Rust の読み取り系は新旧列の両方を受けられるようにする
- 旧列がある場合は fallback として読む
- JSON / CSV の出力は新旧両方のキーを並行出力する
- `data_access.py` は `analysis_documents` の列存在チェックを行い、`category1/category2` 優先、旧列 fallback の SQL を使う

### Phase 2

- export / GUI / filter の canonical 名称を `category1` / `category2` に統一
- `doc_type` と `municipality_name` 系の内部名を削除
- 旧 CSV / 旧 DB 互換コードを段階的に削除
- `analysis_tokens_marimo.py` など直接 DB を読む補助ツールも追従更新する

この 2 段階により、既存利用者の DB や CSV を即時破壊せずに移行できる。

## 8. 想定フロー

### 正常系

1. CLI 引数を解釈
2. 入力ディレクトリを走査
3. 対象 `txt` / `md` を収集
4. ファイル名規約を全件検証
5. tokenizer を初期化
6. 一時 DB を作成し transaction 開始
7. schema 作成 / migration
8. 各ファイルを読み込み
9. 段落分割、文分割、tokenize、insert
10. commit
11. 最終 DB パスへ atomic replace
12. summary を標準出力へ出す

### 異常系

1. 事前検証エラー発生
2. report JSON を出力
3. `stderr` に要約表示
4. exit code 非 0 で終了

または

1. 実行途中で decode / tokenize / insert エラー発生
2. rollback
3. report JSON を出力
4. `stderr` に要約表示
5. exit code 非 0 で終了

## 9. テスト観点

最低限必要な自動テスト:

- 正常な `<category1>_<category2>.txt` を読み込める
- `.md` も同様に読み込める
- `_` が 2 個以上あるファイル名を拒否する
- `_` がないファイル名を拒否する
- `.TXT` / `.MD` を拒否する
- 対象ファイル 0 件で失敗する
- invalid file name が 1 件でもあれば DB を更新しない
- `--skip-tokenize` では Sudachi なしでも実行可能
- Sudachi 未導入で tokenize モード実行時は preflight で失敗する
- UTF-8 非対応ファイルで `decode_error` を返す
- 実行中エラーで transaction が rollback される

手動確認:

- 生成 DB を既存 viewer で読み、`category1` / `category2` 表示が崩れない
- filter/export で新名称が反映される

## 10. 実装順序

1. `build_ordinance_analysis_db.py` に `--input-dir` と事前検証を追加
2. `SourceFileRow` を `category1` / `category2` ベースへ変更
3. `os.walk(followlinks=False)`、`as_posix()` ソート、BOM 対応 decode を導入
4. 一時 DB + atomic replace + report JSON を導入
5. analysis DB schema に `category1` / `category2` を追加
6. Python reader / export を新旧列併用対応
7. Rust の record / filter / loader / UI 表示を新旧入力対応
8. README、補助ツール、テスト DB fixture を更新
9. 旧名称互換コードの整理

## 11. 判断事項

本設計で明示的に採用する判断は次の通り。

- ファイル名区切り `_` は予約文字とし、カテゴリ値には使わない
- 走査は `os.walk(followlinks=False)` を使う
- ソートは `relative_path.as_posix()` を使う
- 不正ファイルが 1 件でもあれば全体中止
- 実行時エラーでも partial DB は残さず、一時 DB を破棄する
- Markdown 前処理は入れない
- Sudachi は optional extra として導入する
- canonical metadata は `category1` / `category2` とする
- Phase 1 は新旧キー併用、Phase 2 で旧名撤去

## 12. 補足

今回の変更は「入力ソースの変更」だけではなく、「条例向けメタデータ名を一般用途向けへ正規化する変更」を含む。そのため、builder 単体の改修では終わらず、viewer / export / filter / CSV schema まで通した移行設計が必要である。

ただし、テキスト解析本体は既存実装の再利用が可能であり、変更の中心は入出力契約とエラー制御である。

---

## セカンドオピニオン: 批判的レビュー

本設計に対して、実際のコードベースを精査した上での批判的レビューを以下に記す。バグの可能性、UI 崩れ、計算ミスの可能性、意図しない動作の可能性を網羅的に列挙する。

レビュー実施日: 2026-03-27
対象コード: `docs/build_ordinance_analysis_db.py`, `analysis_backend/data_access.py`, `analysis_backend/frame_schema.py`, `analysis_backend/export_formatter.py`, `analysis_backend/cli.py`, `src/model.rs`, `src/filter.rs`, `src/csv_loader.rs`, `src/analysis_runner.rs`, `src/app_main_layout.rs`, `src/app.rs`, `src/db.rs`, `tests/test_cli.py`

### R-01. `ordinance_or_rule` の導出ロジック消失（バグ確度: 高）

**現状**: `export_formatter.py` (L174-179) で `doc_type` から `ordinance_or_rule` を導出している。

```python
pl.when(pl.col("doc_type").fill_null("").str.contains("施行規則", literal=True))
  .then(pl.lit("施行規則"))
  .when(pl.col("doc_type").fill_null("").str.contains("条例", literal=True))
  .then(pl.lit("条例"))
  .otherwise(pl.lit("不明"))
  .alias("ordinance_or_rule"),
```

**問題**: 設計書は `doc_type` を廃止し `category2` に置き換えると述べている。しかし `ordinance_or_rule` は `doc_type` から派生する二次的フィールドであり、`category2` がそのまま置き換わるのか、導出ロジック自体を廃止するのか不明確。

- `category2` に自由テキストが入る場合、Rust 側の `FilterColumn::OrdinanceOrRule`（`filter.rs` L54-57、ラベル「条例/規則」）に表示される値が「条例」「施行規則」「不明」の3値から任意値に変わり、フィルタの意味が崩壊する。
- `app.rs` L146-149 のツリー列ヘッダー「条例/規則」が `category2` の汎用値と不整合を起こす。
- `enrich_reconstructed_sentences_result`（`export_formatter.py` L385-390）にも同じ導出ロジックがあり、ここも同時に対処が必要。

**対処案**: `ordinance_or_rule` の導出ロジックを廃止するか、`category2` のまま直接渡すか、判断と影響範囲を明示すべき。

### R-02. `frame_schema.py` のスキーマ定数がハードコードされている（バグ確度: 高）

**現状**: `frame_schema.py` で以下のスキーマが定義されている。

```python
PARAGRAPH_METADATA_SCHEMA = {
    "paragraph_id": pl.Int64,
    "document_id": pl.Int64,
    "municipality_name": pl.String,   # ← ハードコード
    "doc_type": pl.String,            # ← ハードコード
    "is_table_paragraph": pl.Int64,
}
SENTENCE_METADATA_SCHEMA = {
    ...
    "municipality_name": pl.String,   # ← 同上
    "doc_type": pl.String,            # ← 同上
    ...
}
```

**問題**: DB スキーマが `category1` / `category2` に変わると、`data_access.py` の SQL クエリ結果のカラム名と `frame_schema.py` のスキーマ定数が不一致になる。`_sqlite_select_to_polars` は `schema=column_names` で DataFrame を構築するが、後続の `select(list(ANALYSIS_SENTENCES_READ_SCHEMA.keys()))` 等でカラム名不一致により Polars の `ColumnNotFoundError` が発生する。

**影響箇所**: `data_access.py` L137-151, L226-235, L316-329 の全メタデータ読み取り関数。

### R-03. `data_access.py` の SQL が旧カラム名を直接参照している（バグ確度: 高）

**現状**: `read_paragraph_document_metadata_result`（L199）と `read_sentence_document_metadata_result`（L283）が直接 `d.municipality_name` と `d.doc_type` を SELECT している。

```python
query = f"""
    SELECT
        p.paragraph_id,
        p.document_id,
        d.municipality_name,     # ← DB に存在しなくなる
        d.doc_type,              # ← DB に存在しなくなる
        {table_flag_select}
    FROM analysis_paragraphs AS p
    JOIN analysis_documents AS d
      ON d.document_id = p.document_id
    WHERE p.paragraph_id IN ({placeholders})
"""
```

**問題**: DB スキーマ変更後、`municipality_name` / `doc_type` 列が存在しなくなるため `sqlite3.OperationalError` が発生する。設計書の Phase 1 は「Python / Rust の読み取り系は新旧列の両方を受けられるようにする」と述べているが、具体的なフォールバック SQL は定義されていない。

**対処案**: `_read_table_columns` パターンを `analysis_documents` にも適用し、`category1` / `category2` と `municipality_name` / `doc_type` の両方に対応する分岐 SQL を明示すべき。

### R-04. Rust 側 CSV ローダーが旧カラム名をハードコードしている（バグ確度: 高）

**現状**: `csv_loader.rs` L5-18, L20-35 に `PARAGRAPH_REQUIRED_COLUMNS` / `SENTENCE_REQUIRED_COLUMNS` が定義されており、`municipality_name`, `ordinance_or_rule`, `doc_type` が required として列挙されている。

**問題**: Python 側が `category1` / `category2` を出力するように変更された場合、Rust 側の CSV ローダーが「必要な列が不足しています」エラーを返す。Phase 1 の「旧列がある場合は fallback として読む」方針が CSV ローダーに反映されていない。

**影響**: `csv_loader.rs` L71-79 の `get(&row, "municipality_name")` 等が空文字を返すようになり、フィルタやツリー表示で全行が「(空)」になる UI 崩れが発生する。

### R-05. Rust JSON DTO も旧フィールド名をハードコードしている（バグ確度: 高）

**現状**: `analysis_runner.rs` L189-243 の `AnalysisJsonRecord` が `municipality_name`, `ordinance_or_rule`, `doc_type` を `serde(default)` で定義している。

**問題**: Python worker が `category1` / `category2` で応答した場合、`municipality_name` 等は `serde(default)` により空文字になる。`AnalysisRecord` に変換後、ツリー一覧の「自治体」列（`app.rs` L142-145）が全行空白になる。

**結論**: Python → Rust の JSON レスポンス仕様が未定義のまま、Python 側だけ変更するとワーカー連携が壊れる。

### R-06. Transaction 設計と `start_run` の矛盾（意図しない動作）

**現状**: `build_ordinance_analysis_db.py` の `start_run()` (L569-570) は `conn.commit()` を即座に実行する。

**問題**: 設計書は「1 run を 1 transaction で扱う」「途中で 1 件でも致命エラーが出たら rollback する」と述べているが、`start_run()` のコミットは transaction の外で行われる。

- プロセスが `start_run()` 後 `process_rows()` 中にクラッシュすると、`analysis_runs` に status="running" のレコードが残る。
- `finish_run()` も独自に `conn.commit()` する（L585）ため、rollback で巻き戻せない。
- 設計書の「all-or-nothing」は、`start_run` / `finish_run` のコミットを含むと実現できない。

**対処案**: `start_run` のコミットを廃止し、全体を `BEGIN` ... `COMMIT` / `ROLLBACK` で囲むか、`analysis_runs` を transaction 外で管理する旨を明記すべき。

### R-07. 大規模データでの単一トランザクションの実用性（意図しない動作）

**現状**: 設計書は「現行の『20 文書ごと commit』は廃止する」と述べている。

**問題**: 数千ファイルの処理を単一トランザクションで行う場合:

- SQLite の WAL ジャーナルが肥大化する（数 GB 規模のテキストデータの場合）。
- 処理中にプロセスが kill された場合、全作業が失われる。
- 処理中に viewer 側から DB を参照すると、書き込みロック競合が発生する可能性がある。

**対処案**: all-or-nothing の閾値（例: 対象ファイル数が N 以下の場合のみ単一トランザクション）、またはロック競合の注意事項を明記すべき。

### R-08. `utf-8-sig` 試行ロジックの曖昧さ（意図しない動作）

**現状**: 設計書 4-3 で「必要であれば utf-8-sig も試す」と記載。

**問題**: 「必要であれば」の判定基準が不明確。

- 常に utf-8-sig で先に試す → BOM なしファイルも受理するが、BOM ありファイルで先頭 3 バイトを除去できる。
- utf-8 strict で失敗した場合のみ utf-8-sig を試す → utf-8 と utf-8-sig の failure mode は同一（BOM は有効な UTF-8 シーケンスのため utf-8 でも decode 成功する。結果として先頭に `\ufeff` が残る）。

実際には、utf-8 strict で BOM ありファイルを読むと decode は成功するが先頭に U+FEFF が残り、段落分割結果に影響する。utf-8-sig なら自動除去される。このため「失敗したら試す」戦略では不十分であり、常に utf-8-sig で読むか、BOM 検出ロジックを入れる必要がある。

### R-09. `rglob` によるシンボリックリンクのループリスク（意図しない動作）

**現状**: 設計書は `input_dir.rglob("*")` で再帰走査と記載。

**問題**: Python の `Path.rglob()` はデフォルトでシンボリックリンクをフォローする。入力ディレクトリ内にシンボリックリンクのループがあると、無限再帰に陥る可能性がある。

**対処案**: `rglob` の代わりに `os.walk(followlinks=False)` を使うか、走査済みパスの inode を管理すべき。

### R-10. Windows でのパス区切りとソート順序の差異（意図しない動作）

**現状**: 設計書は「relative path 昇順でソート」と記載。

**問題**: Windows では `rglob` が返すパスのセパレータが `\` であり、Linux では `/` である。`Path` オブジェクトの比較は OS 依存のためソート結果が環境によって異なる。入力順序がプラットフォーム間で再現しない可能性がある。

**対処案**: `path.as_posix()` などで正規化してからソートする旨を明記すべき。

### R-11. ファイル名の大文字小文字とカテゴリの一意性（意図しない動作）

**現状**: 設計書は拡張子の大文字小文字を厳格に判定する（`.TXT` 拒否）が、`category1` / `category2` の case sensitivity については未定義。

**問題**: Windows の場合、`Tokyo_条例.txt` と `tokyo_条例.txt` は同一ファイルとして扱われるが、`category1` は「Tokyo」と「tokyo」の 2 つに分裂する可能性がある。逆に Linux では別ファイルとして存在可能。

**対処案**: category 値の正規化ルール（小文字化等）を定めるか、「OS のファイルシステム挙動に依存する」旨を明記すべき。

### R-12. ファイル名正規表現がドット等の特殊文字を許容する（意図しない動作）

**現状**: 設計書の正規表現 `^(?P<category1>[^_]+)_(?P<category2>[^_]+)$` は stem に対して適用される。

**問題**: `[^_]+` はアンダースコア以外の全文字を許容するため、以下のようなファイル名が妥当と判定される。

- `foo.bar_baz.txt` → stem=`foo.bar_baz` → ❌ 正規表現不一致（`baz` でなく `bar_baz` 部分にドットが入る）。実際には stem は `foo.bar_baz` なので `category1=foo.bar`, `category2=baz` にはならない。

再確認: `Path("foo.bar_baz.txt").stem` は `"foo.bar_baz"` を返す。正規表現 `^[^_]+_[^_]+$` でマッチすると `category1=foo.bar`, `category2=baz` になる。待って、`foo.bar_baz` を `^([^_]+)_([^_]+)$` で照合すると `category1=foo.bar`, `category2=baz` にマッチする。

つまり `foo.bar_baz.txt` は `category1="foo.bar"` / `category2="baz"` として受理される。ドットを含むカテゴリ名は意図された挙動か？スペースやその他記号（`#`, `&` 等）も同様に許容される。

**対処案**: カテゴリ値に使用可能な文字セットを明示的に制限すべき（例: `[a-zA-Z0-9\u3000-\u9fff]+` 等）。

### R-13. source DB モードでの `category1` / `category2` マッピングが未定義（計算ミスの可能性）

**現状**: 設計書は「DB 側の `municipality_name` / `ordinance_or_rule` からこの 2 フィールドへ正規化して受け渡す」と記載。

**問題**: source DB には `municipality_name` と `ordinance_id` があるが、`ordinance_or_rule` は存在しない。`ordinance_or_rule` は `doc_type` からの派生値（R-01 参照）。source DB の `doc_type` から `category2` への変換ルールが不明確。

- `doc_type` が「太陽光発電設備の設置に関する条例（令和4年改正）」のような長い文字列の場合、`category2` にそのまま入れるのか？
- ファイル名規約の `_` 禁止ルールと矛盾しないか？（source DB モードではファイル名から抽出しないため、`_` を含む値が `category2` に入る可能性がある）

### R-14. `--strict` フラグの設計上の無用さ（設計上の問題）

**現状**: 設計書は `--strict` を追加引数として提案し、「既定値 true。現在要件上は常時 strict 扱いでもよい」と記載。

**問題**: 常に strict ならフラグを追加する意味がない。使用されないフラグは将来の保守負担になる。

**対処案**: 明確なユースケースが出るまで `--strict` は追加しない。

### R-15. DB マイグレーション手段の欠如（バグ確度: 中）

**現状**: `create_schema` は `CREATE TABLE IF NOT EXISTS` を使用し、`ensure_analysis_paragraph_columns` は `ALTER TABLE ADD COLUMN` で後方互換的に列を追加している。

**問題**:

1. `analysis_documents` に `category1` / `category2` を追加する `ALTER TABLE` は設計されているが、`municipality_name` / `doc_type` / `municipality_id` / `ordinance_id` の削除は `SQLite の ALTER TABLE` では不可能（SQLite は `DROP COLUMN` を 3.35.0+ でのみサポート）。
2. `analysis_runs.source_db_path` → `source_locator` のリネームも SQLite では直接できない。
3. 既存 DB のデータを `category1` / `category2` に移行するマイグレーションスクリプトが未定義。
4. Alembic 等のマイグレーションツールが導入されていない。

**結論**: 「段階的廃止」と記載しているが、実際の SQLite の制約上、旧列は物理的に残り続ける。この点を明記し、旧列に NULL を入れるか、新規 DB のみ新スキーマにするか、方針を決める必要がある。

### R-16. テスト用 DB スキーマとの不整合（テスト破壊）

**現状**: `tests/test_cli.py` L20-60 で作成されるテスト用 DB は `analysis_documents(document_id, municipality_name, doc_type)` という最小スキーマ。

**問題**: `category1` / `category2` 列が追加されると、`data_access.py` の SQL が `category1` を SELECT しようとするが、テスト DB にはその列が存在しない。テスト全体が失敗する。

**影響範囲**: `test_cli.py`, `test_cli_json_output.py` の全テストケース。

### R-17. `GUI_RECORD_COLUMNS` / `SENTENCE_GUI_RECORD_COLUMNS` の同期漏れリスク（UI 崩れ確度: 高）

**現状**: `export_formatter.py` L11-33, L34-55 に定数として列名リストが定義され、Rust 側の `AnalysisRecord` / `AnalysisJsonRecord` / CSV ローダーと完全に一致する必要がある。

**問題**: Python 側で `municipality_name` → `category1`、`ordinance_or_rule` → `category2` に変更した場合:

- Rust 側 `AnalysisRecord.municipality_name` フィールドが JSON の `category1` キーから値を受け取れない（`serde(default)` で空文字になる）。
- ツリー一覧の「自治体」列（`app.rs` L142-145 `tree_municipality_value`）が全行空白になる。
- 詳細パネルの `draw_record_summary`（`app_main_layout.rs` L294-301）で `record.municipality_name` / `record.ordinance_or_rule` が空で表示される。
- CSV export した結果を再度 CSV ローダーで読むと、`PARAGRAPH_REQUIRED_COLUMNS` に `municipality_name` が含まれるため `detect_analysis_unit` が失敗する。

**結論**: Python と Rust の列名変更は原子的に同時デプロイする必要があり、Phase 1 の「新旧列の両方を受けられるようにする」は実装上非常に複雑になる。

### R-18. `process_rows` の `empty_documents` カウントの二重計上（既存バグ）

**現状**: `build_ordinance_analysis_db.py` L864-869:

```python
document_id = insert_document_row(conn, run_id, source_row, raw_text)
summary["documents_inserted"] += 1

paragraph_blocks = build_paragraph_blocks(raw_text, ...)
if not paragraph_blocks:
    summary["empty_documents"] += 1
    continue
```

**問題**: 空テキストのファイルは `documents_inserted` にカウントされた後に `empty_documents` にもカウントされる。summary の `documents_inserted` には「空文書を含む挿入数」、`empty_documents` には「そのうち空のもの」が入るが、「有効文書数 = documents_inserted - empty_documents」という計算が暗黙の前提になっている。レポートで混乱を招く。

**対処案**: `empty_documents` の定義を明示するか、カウント方法を見直すべき。

### R-19. `--recreate-db` / `--fresh-db` 失敗時のバックアップ復元未定義（意図しない動作）

**現状**: `recreate_analysis_db_file()` は既存 DB をリネームしてバックアップし、新規 DB を作成する。

**問題**: 設計書の all-or-nothing 方針では、build 失敗時に rollback する。しかし `--recreate-db` で旧 DB がバックアップされた後に build が失敗すると:

- 新 DB は空（rollback 済み）のまま残る。
- 旧 DB は `.bak_YYYYMMDD_HHMMSS` として退避されたまま。
- 利用者は手動でバックアップを復元する必要がある。

**対処案**: build 失敗時にバックアップを自動復元するか、その旨をエラーレポートに明記すべき。

### R-20. Sudachi バージョンの上限未指定（互換性リスク）

**現状**: 設計書は `sudachipy>=0.6` を指定。

**問題**: Sudachi の API は バージョン間で変更がある。現行コード `safe_is_oov()` が既に API 差異を吸収しているが、将来の破壊的変更に対する防御がない。`sudachidict-core>=20240109` も辞書の更新でトークン化結果が変わりうる。

**対処案**: `sudachipy>=0.6,<1.0` のように上限を設定し、メジャーバージョンアップ時に検証を挟むべき。

### R-21. エラーレポートパスのデフォルト解決が曖昧（意図しない動作）

**現状**: 設計書は `--report-path` 未指定時に「analysis-db と同じディレクトリに自動生成」と記載。

**問題**: `--analysis-db` がデフォルト値 `data/ordinance_analysis.db`（相対パス）の場合、レポートのパスはカレントディレクトリ依存になる。実行場所によってレポートの出力先が変わる。

### R-22. Phase 1 / Phase 2 の境界が曖昧（実装順序リスク）

**現状**: 設計書は Phase 1 で「Python / Rust の読み取り系は新旧列の両方を受けられるようにする」、Phase 2 で「canonical 名称を統一」と記載。

**問題**:

- Phase 1 で `analysis_documents` に `category1` / `category2` を追加しつつ `municipality_name` / `doc_type` を残す場合、`insert_document_row` は新旧両方の列に値を INSERT する必要がある。しかしフォルダーモードでは `municipality_name` に入れる値がない。
- `data_access.py` のフォールバック SQL は `_read_table_columns` で列存在チェックが必要だが、`read_paragraph_document_metadata_result` は現在そのチェックを行っていない（`is_table_paragraph` のみチェック）。
- 結果として Phase 1 の「両方受けられる」実装は、`data_access.py` の全メタデータクエリにカラム存在チェック分岐を追加する大改修になる。

**対処案**: Phase 1 の具体的な SQL・コード変更の範囲を事前に列挙し、実装量を見積もるべき。

### R-23. `analysis_tokens_marimo.py` への影響未評価

**現状**: `analysis_tokens_marimo.py` は Polars で DB を直接読む対話ノート。

**問題**: DB スキーマが変わると、このノートブックのクエリも影響を受ける可能性がある。スコープの「対象」にも「対象外」にも記載がない。

### R-24. Rust の `AnalysisRecord` にフィールドを追加する場合の影響波及

**現状**: `AnalysisRecord` は `model.rs` で 27 フィールドを持ち、`csv_loader.rs`, `analysis_runner.rs`, `filter.rs`, `app_main_layout.rs`, `app.rs` の全箇所でフィールド名が直接参照されている。

**問題**: `municipality_name` → `category1` のリネームは、以下の全箇所を同時に変更する必要がある:

- `model.rs`: struct フィールド
- `csv_loader.rs`: `get(&row, "municipality_name")` → `get(&row, "category1")`
- `analysis_runner.rs`: `AnalysisJsonRecord` の serde フィールド名
- `filter.rs`: `FilterColumn::MunicipalityName`, ラベル「自治体」, `record_municipality_name_value`
- `app.rs`: `TREE_COLUMN_SPECS` のヘッダー「自治体」、`tree_municipality_value`
- `app_main_layout.rs`: `draw_record_summary` の表示文字列
- `filter.rs` のテスト: `empty_paragraph_record` のフィールド初期化

1 箇所でも漏れるとコンパイルエラーまたは表示不整合が発生する。設計書にこの変更箇所一覧がない。

### R-25. `doc_type` 削除時の `FilterColumn::DocType` の扱い未定義

**現状**: `filter.rs` L59-63 に `FilterColumn::DocType`（ラベル `"doc_type"`）が存在する。

**問題**: 設計書は `doc_type` を廃止対象としているが、`FilterColumn::DocType` を削除するのか、`Category2` 等にリネームするのか未定義。削除する場合、`FILTER_COLUMN_ORDER` (L25-37) と `FILTER_COLUMN_SPECS` (L39-106) の両方から除去し、対応するテストも更新する必要がある。

### R-26. `db.rs` の既定 DB パス `ordinance_analysis5.db` との整合性

**現状**: `db.rs` L8 で `DEFAULT_DB_RELATIVE_PATH = "asset/ordinance_analysis5.db"` と定義されている。

**問題**: 設計書の対象は `build_ordinance_analysis_db.py` のデフォルト出力先 `data/ordinance_analysis.db` であり、`db.rs` の参照先と異なる。新しいスキーマで生成された DB を viewer の DB Viewer 機能で参照する場合、パスの不一致またはスキーマの不一致が発生する。

**補足**: `db.rs` は `analysis_paragraphs` の `paragraph_id`, `document_id`, `paragraph_no`, `paragraph_text` のみを参照するため、メタデータ列の変更による直接的な破壊はないが、DB パスの運用上の整合性は要確認。

### R-27. テスト観点の不足

設計書のテスト観点（セクション 9）に以下が欠けている:

1. **Phase 1 互換性テスト**: 旧スキーマ DB を新コードで読めることのテスト。
2. **Rust CSV ローダーの新旧カラムテスト**: `category1` / `category2` カラムの CSV を Rust で読めるテスト。
3. **JSON DTO の新旧互換テスト**: Python worker が `category1` を返した場合に Rust が正しく処理するテスト。
4. **export → reimport のラウンドトリップテスト**: 新スキーマで export した CSV を再度読めるテスト。
5. **source DB モードでの `category1` / `category2` マッピングテスト**。
6. **シンボリックリンク・特殊文字ファイル名のエッジケーステスト**。
7. **大量ファイル（数千件）での単一トランザクション性能テスト**。

### まとめ

| ID | 分類 | 深刻度 | 概要 |
|----|------|--------|------|
| R-01 | バグ | 高 | `ordinance_or_rule` 導出ロジックの行き先が不明 |
| R-02 | バグ | 高 | `frame_schema.py` のスキーマ定数に旧カラム名がハードコード |
| R-03 | バグ | 高 | `data_access.py` の SQL が旧カラム名を直接参照 |
| R-04 | バグ | 高 | Rust CSV ローダーの required columns が旧名 |
| R-05 | バグ | 高 | Rust JSON DTO のフィールド名が旧名 |
| R-06 | 意図しない動作 | 中 | `start_run` の即時コミットと all-or-nothing の矛盾 |
| R-07 | 意図しない動作 | 中 | 大規模データでの単一トランザクションの実用性 |
| R-08 | 意図しない動作 | 中 | utf-8-sig 試行ロジックの曖昧さ |
| R-09 | 意図しない動作 | 低 | シンボリックリンクのループリスク |
| R-10 | 意図しない動作 | 低 | Windows パスソート順序の差異 |
| R-11 | 意図しない動作 | 低 | カテゴリ値の大文字小文字未定義 |
| R-12 | 意図しない動作 | 低 | 正規表現が特殊文字を許容 |
| R-13 | 計算ミス | 中 | source DB モードの category マッピング未定義 |
| R-14 | 設計 | 低 | `--strict` フラグの無用さ |
| R-15 | バグ | 中 | DB マイグレーション手段の欠如 |
| R-16 | テスト破壊 | 高 | テスト用 DB スキーマの不整合 |
| R-17 | UI 崩れ | 高 | Python-Rust 間の列名同期漏れ |
| R-18 | 計算ミス | 低 | `empty_documents` の二重計上（既存バグ） |
| R-19 | 意図しない動作 | 中 | DB 再作成失敗時のバックアップ復元未定義 |
| R-20 | 互換性 | 低 | Sudachi バージョン上限未指定 |
| R-21 | 意図しない動作 | 低 | レポートパスのデフォルト解決が曖昧 |
| R-22 | 設計 | 中 | Phase 1/2 の境界と実装量が曖昧 |
| R-23 | 漏れ | 低 | `analysis_tokens_marimo.py` への影響未評価 |
| R-24 | UI 崩れ | 高 | Rust `AnalysisRecord` の変更箇所一覧が未記載 |
| R-25 | UI 崩れ | 中 | `FilterColumn::DocType` の扱い未定義 |
| R-26 | 設計 | 低 | `db.rs` の既定 DB パスとの整合性 |
| R-27 | テスト | 中 | テスト観点の不足（7 項目） |

## セカンドオピニオンの反映

上記レビューのうち、設計へ反映する判断を以下に固定する。

### 採用

- R-01, R-13:
  `category2` は canonical には `ordinance_or_rule` 相当とし、source DB モードでは現行の `doc_type -> ordinance_or_rule` 導出規則をそのまま使って組み立てる。
- R-02, R-03, R-04, R-05, R-17, R-24, R-25:
  Phase 1 は Python / Rust / CSV / JSON の契約を同時に壊さないため、新旧キー併用とする。`data_access.py` は `analysis_documents` の列存在チェックを追加する。
- R-06, R-07, R-19:
  all-or-nothing は「既存 DB への単一大規模 transaction」ではなく、「一時 DB への構築 + 成功時 atomic replace」で実現する。
- R-08:
  BOM 対応は「utf-8 失敗時に utf-8-sig」ではなく、「BOM 検出時は utf-8-sig、そうでなければ utf-8 strict」とする。
- R-09:
  再帰走査は `rglob` ではなく `os.walk(followlinks=False)` とする。
- R-10:
  ソートは `relative_path.as_posix()` を使う。
- R-14:
  `--strict` は追加しない。
- R-15:
  既存 DB では destructive migration を行わず、Phase 1 は additive migration のみとする。
- R-16, R-27:
  テスト DB fixture と新旧互換テストを明示的に追加対象へ含める。
- R-20:
  Sudachi 依存は上限制約を付ける。例:
  `sudachipy>=0.6,<1.0`
- R-21:
  report path の既定値は `analysis_db` の絶対パス基準で決定する。
- R-23:
  `analysis_tokens_marimo.py` を補助ツールとして追従対象に含める。

### 非採用または明示的に固定

- R-11:
  `category1` / `category2` の大小文字正規化は行わない。入力ファイル名の表記を尊重する。
- R-12:
  カテゴリ文字種は `_`、パス区切り、制御文字を除き許容する。文字集合をさらに狭めるのは今回のスコープ外とする。
- R-26:
  `src/db.rs` の既定 DB パスは今回の builder 設計とは独立とする。ただし運用文書で差異を説明する。

### 補足判断

- `start_run()` / `finish_run()` の責務は temp DB 化に合わせて再設計する。失敗 run を DB へ残すかどうかは実装時に明示し、少なくとも target DB の partial update は起こさない。
- `empty_documents` の既存カウント仕様は今回の主題ではないが、report JSON の定義時に意味を明文化する。

---

## 実装タスク案に対するセカンドオピニオン

レビュー実施日: 2026-03-27
対象: 上記セカンドオピニオン反映後に策定された Phase 1 / Phase 2 実装タスク案

### 結論

全体の方向性と Phase 分割は妥当である。ただし、タスクの粒度が不均一であり、依存関係の記述が不足しているため、そのまま着手すると手戻りが発生する箇所がある。以下に項目ごとの検証結果を述べる。

### Phase 1 タスクの検証

#### T-01. `build_ordinance_analysis_db.py` に `--input-dir` モードを追加（妥当）

妥当。既存の `parse_args()` に `--input-dir` を追加し、`--source-db` との排他制御を入れるだけの単体変更であり、他ファイルへの影響がない。Phase 1 の最初のタスクとして適切。

#### T-02. `os.walk(followlinks=False)` / strict 検証 / report.json / BOM 対応 decode（粒度が粗すぎる）

**問題**: 4 つの独立した機能が 1 タスクに詰め込まれている。

- `os.walk` による走査 + `as_posix()` ソート → 純粋な入力列挙
- `<category1>_<category2>` strict 検証 → 正規表現バリデーション
- report.json 出力 → エラー報告の I/O
- BOM 判定つき UTF-8 → ファイル読み込みのエンコーディング処理

これらはそれぞれ単体テスト可能な単位であり、4 つの個別タスクに分割すべき。特に BOM 対応は `process_rows` 内の `path.read_text(encoding="utf-8", errors="ignore")`（現行 L859）の書き換えであり、走査ロジックとは独立している。

#### T-03. 一時 DB + atomic replace の all-or-nothing フロー（妥当だが難易度の認識が必要）

妥当。ただし実装上の注意点が記載されていない。

- 現行の `main()` は `source_conn` と `analysis_conn` を別々に開いている。一時 DB 方式では、一時ファイルに `analysis_conn` を接続し、成功時に `shutil.move` または `os.replace` で置換する必要がある。
- Windows では、対象ファイルが他プロセス（viewer）で開かれている場合、`os.replace` が `PermissionError` を出す。この点のエラーハンドリングが必要。
- `--recreate-db` / `--fresh-db` フラグとの関係を整理する必要がある。一時 DB 方式を導入すれば、これらのフラグは不要になる可能性がある。

#### T-04. `SourceFileRow` を category1/category2 ベースへ拡張（妥当だが順序に注意）

妥当。ただし、このタスクは T-01 の `--input-dir` モード追加と同時に行うべき。`SourceFileRow` を変更すると `insert_document_row()` の INSERT 文（L639-670）も連動して変更する必要があり、T-05 のスキーマ変更と密結合している。

**依存**: T-04 → T-05（スキーマ追加）→ T-06（data_access.py 更新）の順序制約がある。

#### T-05. additive migration で `category1`/`category2` / `source_locator` 追加（妥当）

妥当。`ensure_analysis_paragraph_columns` と同じパターンで `ensure_analysis_document_columns` と `ensure_analysis_run_columns` を追加すればよい。

ただし、既存 DB で `category1` / `category2` が追加された場合、既存行にはこれらの値が `NULL` になる。`category1 TEXT NOT NULL` という制約は `ALTER TABLE ADD COLUMN` では付けられない（SQLite は `ADD COLUMN` で `NOT NULL` を許可しない（デフォルト値なしの場合））。設計書のセクション 3-2 で `NOT NULL` と記載されているが、additive migration では `NOT NULL DEFAULT ''` にするか、`NULL` を許容するかの判断が必要。

#### T-06. `data_access.py` の列存在チェック追加（妥当だが工数大）

妥当。ただし、タスク記載が「列存在チェックを入れ、category1/category2 優先、旧列 fallback の SQL にする」と 1 行で書かれているが、実際には以下の 3 関数すべてに分岐を入れる必要がある。

- `read_paragraph_document_metadata_result`: `municipality_name` / `doc_type` → `category1` / `category2`
- `read_sentence_document_metadata_result`: 同上
- SELECT の結果カラム名が変わるため、`PARAGRAPH_METADATA_SCHEMA` / `SENTENCE_METADATA_SCHEMA`（`frame_schema.py`）の出力キー名を統一する必要がある

この工数は見た目以上に大きい。1 タスクではなく、3 関数 × （SQL 分岐 + スキーマ対応）= 6 個の小タスクとして管理すべき。

#### T-07. `frame_schema.py` の更新（妥当だが T-06 と同時）

妥当。ただし T-06 と密結合しているため、同時に実施する必要がある。`PARAGRAPH_METADATA_SCHEMA` のキー名を変えると、`data_access.py` の `empty_df()` や `pl.DataFrame` コンストラクタが壊れる。

**重要な判断**: Phase 1 で `frame_schema.py` のキー名を `category1` / `category2` に変えるのか、`municipality_name` / `doc_type` のまま残して値だけ新列から読むのか。後者のほうが Rust 側への影響が最小になるが、タスク案では明示されていない。

#### T-08. `export_formatter.py` で互換 alias 出力（妥当だが最も複雑）

妥当だが、このタスクの具体的な出力仕様が不明確。

現行コード確認: `export_formatter.py` の `enrich_reconstructed_paragraphs_result`（L144-213）は、DB メタデータを JOIN した後に `ordinance_or_rule` を `doc_type` から導出し、最終的に `select(["municipality_name", "ordinance_or_rule", "doc_type", ...])` で列を選択している。

Phase 1 で「category1/category2 を canonical にしつつ、municipality_name/ordinance_or_rule/doc_type も互換 alias として出す」ということは、出力 DataFrame に `category1`, `category2`, `municipality_name`, `ordinance_or_rule`, `doc_type` の 5 列すべてを持たせる必要がある。

これは `GUI_RECORD_COLUMNS`（L11-33）と `SENTENCE_GUI_RECORD_COLUMNS`（L34-55）への列追加を意味し、Rust 側の `AnalysisRecord` も同時にフィールドを追加する必要がある。

**判断が必要**: 出力列を増やすのか、それとも Phase 1 では旧名のまま出力して値だけ新列から取るのか。後者のほうが Rust 側の変更が不要になる。

#### T-09. `csv_loader.rs` で新旧 CSV 列の両方を読む（妥当だが方針次第で不要）

T-08 の判断次第。Phase 1 で Python 側が旧名で出力し続けるなら、`csv_loader.rs` の変更は不要になる。

仮に新旧両方を読む場合、`detect_analysis_unit` の `PARAGRAPH_REQUIRED_COLUMNS` を緩和する必要がある。具体的には `municipality_name` を required から外し、`category1` OR `municipality_name` のどちらか一方があれば OK とする分岐が必要。これは `missing_columns` 関数（L124-130）のロジック変更を伴う。

#### T-10. `analysis_runner.rs` の JSON DTO 新旧キー対応（妥当だが方針次第で不要）

T-08 と同じ。Python worker 側が旧キー名で出力し続けるなら不要。

仮に対応する場合、`AnalysisJsonRecord` に `#[serde(alias = "category1")]` を追加する方法が最もシンプルだが、`serde` の `alias` は `rename` と併用できない制約がある。実際には `AnalysisJsonRecord` は `#[serde(rename_all = "...")]` を使っていない（フィールド名がそのまま JSON キーに対応）ため、`#[serde(alias = "category1")]` を `municipality_name` フィールドに追加するだけで対応可能。

#### T-11. Rust の model / filter / app / layout の互換維持更新（粒度が粗すぎる）

**問題**: 「まず互換維持で動作を壊さないよう更新する」とだけ記載されているが、具体的に何を変更するのかが不明。

Phase 1 で Python 側が旧名で出力し続ける場合、Rust 側の変更は **ゼロ** になりうる。逆に Python 側が新名で出力する場合、R-24 で列挙した全箇所（7 ファイル）の同時変更が必要。

この曖昧さは、T-08 の判断が未確定であることに起因している。T-08 を先に確定させないと、T-09〜T-11 の内容が決まらない。

#### T-12. `analysis_tokens_marimo.py` の追従（妥当）

妥当。ただし確認だけで済む可能性が高い。`analysis_tokens_marimo.py` は `analysis_tokens` テーブルを直接 Polars で読むが、このテーブルにはメタデータ列（`municipality_name` 等）がないため、スキーマ変更の影響を受けない可能性が高い。

確認すべきは、ノートブック内で `analysis_documents` や `analysis_paragraphs` を JOIN している箇所があるかどうかだけ。

#### T-13. `pyproject.toml` に Sudachi optional dependency 追加（妥当）

妥当。現行 `pyproject.toml` には `[project.optional-dependencies]` セクションがなく、`dependencies` は `polars>=1.38.1` のみ。追加は単純。

ただし `sudachipy>=0.6,<1.0` と `sudachidict-core>=20240109` の両方を記載する必要がある。

#### T-14. README 更新（妥当、最後に実施）

妥当。他の全タスクが完了してから書くべき。

### テストタスクの検証

#### テスト-01. builder テスト群（妥当だが粒度を分割すべき）

「正常系、ファイル名違反、0件入力、BOM あり UTF-8、decode error、--skip-tokenize、トランザクション失敗時 cleanup」が 1 行に列挙されているが、これは 7 個の独立したテストケース（群）であり、各々が独立してレビュー可能な単位。

#### テスト-02. Python 旧 DB / 新 DB の reader テスト（妥当かつ重要）

妥当。既存の `test_cli.py` の `build_test_db` を改修し、新スキーマ版の `build_test_db_with_categories` のようなヘルパーを追加する必要がある。

#### テスト-03. Rust 旧 CSV / 新 CSV / 旧 JSON / 新 JSON テスト（方針次第）

T-08 の判断次第。Phase 1 で旧名出力を維持するなら、「新 CSV / 新 JSON」テストは Phase 2 で追加すればよい。

#### テスト-04. export → reimport round-trip（妥当かつ重要）

妥当。現行にはこのテストがない。Python で生成した CSV を Rust の `csv_loader::load_records` で読めることを確認する統合テストが必要。ただし Python テストと Rust テストをまたぐため、CI パイプラインの設計が必要。

### Phase 2 タスクの検証

#### P2-01. canonical 名称の完全統一（妥当）

妥当。Phase 1 が完了し、新旧併用が安定稼働してから着手すべき。

#### P2-02. 互換コードの削除（妥当だが慎重に）

妥当。ただし「互換コード」の範囲を明示する必要がある。少なくとも以下を含む:

- `export_formatter.py` の `ordinance_or_rule` 導出ロジック
- `data_access.py` の `municipality_name` / `doc_type` フォールバック SQL
- `csv_loader.rs` の旧カラム名対応（T-09 で追加した場合）
- `analysis_runner.rs` の `#[serde(alias)]`（T-10 で追加した場合）

#### P2-03. `FilterColumn::DocType` 削除 / UI ラベル変更（妥当）

妥当。ただし `FilterColumn::DocType` を削除すると、旧 CSV を開いたときに `doc_type` 列でフィルタできなくなる。Phase 2 で旧 CSV のサポートを打ち切るかどうかの判断が必要。

#### P2-04. fresh DB で旧列なしスキーマ（妥当）

妥当。これは `create_schema` の DDL から旧列を除去するだけ。

### クリティカルパスの検証

> 実装のクリティカルパスは、builder → Python reader/export → Rust DTO/CSV loader → テスト です。

**概ね妥当だが、不完全。**

正確なクリティカルパスは:

```
T-08 の出力方針決定（旧名維持 or 新名出力）
  ↓
T-01（--input-dir 追加）
  ↓
T-04（SourceFileRow 変更）+ T-05（スキーマ追加）
  ↓
T-02 分割（走査 → 検証 → BOM → report.json）
  ↓
T-03（一時 DB + atomic replace）
  ↓
T-06 + T-07（data_access.py + frame_schema.py）
  ↓
T-08（export_formatter.py）
  ↓
T-09〜T-11（Rust 側）※ T-08 の出力方針に依存
  ↓
テスト群
  ↓
T-13 + T-14（pyproject.toml + README）
```

**最大のボトルネックは T-08 の出力方針決定**である。この判断が遅れると T-09〜T-11 の内容が確定せず、Rust 側の着手ができない。

### 欠落しているタスク

1. **T-08 の出力方針の設計判断**: Phase 1 で Python 出力を旧名のまま維持するか、新名に切り替えるか。これが Phase 1 全体の工数を左右する最大の分岐点。
2. **`worker.py` への影響確認**: `worker.py` は `cli.py` の `run_analysis_job` を内部的に呼び出し、`build_gui_records` / `build_sentence_gui_records` の結果を JSON でフレーミングする。`export_formatter.py` の列名変更は worker 経由の JSON 応答にも波及する。タスク一覧に `worker.py` への言及がない。
3. **`condition_model.py` / `condition_evaluator.py` の影響確認**: metadata 列をフィルタ条件として参照している箇所がないかの確認タスクがない。
4. **CI / ビルド確認タスク**: Rust 側の変更は `cargo build` / `cargo test` の通過確認が必要。Python 側は `pytest` の通過確認が必要。これらを個別タスクに含めるか、各タスク完了の定義に含めるか明示すべき。
5. **`--recreate-db` / `--fresh-db` フラグと一時 DB 方式の整合**: T-03 で一時 DB を導入すると、これらのフラグの意味が変わる。整理タスクが必要。

### 推奨：Phase 1 の出力方針を先に固定する

Phase 1 の実装量を最小化するために、以下の方針を推奨する:

**Phase 1 では Python → Rust の出力インターフェース（CSV 列名、JSON キー名）を一切変更しない。** つまり:

- `export_formatter.py` は引き続き `municipality_name` / `ordinance_or_rule` / `doc_type` で出力する
- 値は `category1` / `category2` から取得するが、キー名は旧名のまま
- Rust 側の変更はゼロ
- T-09, T-10, T-11 は Phase 1 では不要

この方針であれば、Phase 1 は Python 側（builder + reader/export）のみの変更で完結し、Rust 側のコンパイルエラー・UI 崩れリスクをゼロにできる。Phase 2 で Python と Rust を同時にリネームすれば、名称統一が原子的に行える。

## 実装着手用タスクリスト

以下を、セカンドオピニオン反映後の正式な実装タスクとする。

### 前提判断

- Phase 1 では Python -> Rust の出力契約を変更しない
- Phase 1 の CSV 列名と JSON キー名は旧名を維持する
- Phase 1 の canonical metadata は DB 内部でのみ `category1` / `category2` を使う
- Rust 側の名称変更は Phase 2 へ送る

### Phase 1

#### A. builder 入力モード

1. `docs/build_ordinance_analysis_db.py` に `--input-dir` を追加する
2. `--input-dir` と `--source-db` の排他制御を追加する
3. `SourceFileRow` を `category1` / `category2` ベースへ拡張する
4. source DB モードで `municipality_name` / `doc_type` から `category1` / `category2` を組み立てる

#### B. builder 入力検証

1. `os.walk(followlinks=False)` による再帰走査を実装する
2. `.txt` / `.md` のみ収集する
3. `relative_path.as_posix()` で安定ソートする
4. `<category1>_<category2>.(txt|md)` の strict 検証を実装する
5. 不正ファイルが 1 件でもあれば preflight fail にする
6. 入力 0 件を明示的な fail にする

#### C. builder テキスト読込

1. 生バイト読込関数を追加する
2. UTF-8 BOM の有無を判定する
3. BOM ありは `utf-8-sig`、それ以外は `utf-8` strict で decode する
4. `errors=\"ignore\"` を廃止する
5. decode error を構造化エラーとして収集する

#### D. builder エラーレポート

1. `InputIssue` / `RunIssue` 相当の構造を追加する
2. preflight failure 用 report JSON を出力する
3. runtime failure 用 report JSON を出力する
4. `--report-path` 未指定時のデフォルト解決を実装する
5. `stderr` への要約出力を実装する

#### E. builder DB 書込方式

1. temp DB パス解決を実装する
2. temp DB へ schema 作成と書込を行うよう変更する
3. 途中失敗時は rollback して temp DB を破棄する
4. 成功時のみ `os.replace` で最終 DB へ置換する
5. Windows の `PermissionError` を明示的に扱う
6. `--recreate-db` / `--fresh-db` の意味を temp DB 方針に合わせて整理する

#### F. schema / migration

1. `analysis_documents` に `category1` / `category2` を additive に追加する
2. `analysis_runs` に `source_locator` を additive に追加する
3. additive migration では旧列を削除しない
4. additive migration 時の NULL / default 方針をコードへ反映する
5. `create_schema` と列存在チェック関数を整理する

#### G. Python reader / export

1. `analysis_backend/data_access.py` で `analysis_documents` の列存在チェックを追加する
2. `read_paragraph_document_metadata_result` を新旧列対応にする
3. `read_sentence_document_metadata_result` を新旧列対応にする
4. `analysis_backend/frame_schema.py` は Phase 1 では旧キー名維持で調整する
5. `analysis_backend/export_formatter.py` は旧出力列名を維持したまま、新列優先で値を組み立てる
6. `municipality_name = category1`、`ordinance_or_rule = category2`、`doc_type = category2` の互換出力方針を実装する
7. `analysis_backend/worker.py` の JSON 応答に破壊的影響がないことを確認する
8. `analysis_backend/condition_model.py` / `analysis_backend/condition_evaluator.py` に metadata 名依存がないか確認する

#### H. 補助ツール / 依存 / 文書

1. `analysis_tokens_marimo.py` の影響有無を確認する
2. `pyproject.toml` に `sudachipy>=0.6,<1.0` と `sudachidict-core>=20240109` を optional dependency として追加する
3. `README.md` に新しい builder 実行手順と依存導入手順を追加する

#### I. Phase 1 テスト

1. builder 正常系テストを追加する
2. invalid file name テストを追加する
3. 入力 0 件 fail テストを追加する
4. BOM 付き UTF-8 テストを追加する
5. decode error テストを追加する
6. `--skip-tokenize` で Sudachi なし実行テストを追加する
7. temp DB cleanup / atomic replace テストを追加する
8. 旧 DB schema reader テストを追加する
9. 新 DB schema reader テストを追加する
10. export -> existing Rust CSV loader の互換確認を追加する
11. Python テストの実行確認を行う

### Phase 1 では実施しない

- Rust の `AnalysisRecord` 名称変更
- Rust の CSV loader 新列対応
- Rust の JSON DTO alias 対応
- `FilterColumn::DocType` の削除
- UI ラベルの `category1` / `category2` 化

これらは Phase 2 でまとめて実施する。

### Phase 2

#### J. Python / Rust の canonical 名称統一

1. Python の CSV / JSON 出力列名を `category1` / `category2` へ切り替える
2. Rust の DTO、CSV loader、model を `category1` / `category2` にリネームする
3. filter / app / layout の表示ラベルを generic 化する
4. `FilterColumn::DocType` を削除する

#### K. 互換コード削除

1. `data_access.py` の旧列 fallback を段階的に削除する
2. `export_formatter.py` の旧名互換出力を削除する
3. source DB モードの旧名補助ロジックを必要に応じて整理する
4. 旧 CSV / 旧 JSON 互換コードを削除する

#### L. 新スキーマ固定

1. fresh DB 生成時の DDL から旧列を除去する
2. Phase 2 用の migration / 運用手順を文書化する
3. Python / Rust の新名称前提テストを追加する

### クリティカルパス

```text
Phase 1 出力方針固定
  ->
builder 入力モード
  ->
SourceFileRow / schema additive migration
  ->
入力検証 / decode / report
  ->
temp DB + atomic replace
  ->
data_access.py + frame_schema.py
  ->
export_formatter.py + worker.py 確認
  ->
Python テスト
  ->
README / 依存更新
```

### 完了条件

- `--input-dir` から strict ルールで DB を生成できる
- invalid file name が 1 件でもあれば DB は更新されない
- Phase 1 では既存 Rust 側を変更せず、既存の CSV / JSON 契約が維持される
- Python reader/export は旧 DB / 新 DB の両方を扱える
- temp DB 方式により partial update が残らない

## Phase 2 着手条件の確認結果

2026-03-27 時点で、Phase 2 に進むための前提条件を以下のとおり確認した。

### 1. Rust ベースライン

- WSL 上の `cargo` は PATH から直接は利用できなかった。
- ただし Windows 側ツールチェーン `C:\Users\ikkou\.cargo\bin\cargo.exe` は存在し、sandbox 外実行で利用可能であることを確認した。
- 実行結果:
  - `cargo.exe build --workspace`: 成功
  - `cargo.exe test --workspace`: 成功
- テスト結果:
  - `csv_highlight_viewer`: 51 passed
  - `csv_viewer_tauri_host`: 7 passed
- よって、Phase 2 着手前の Rust ベースラインは健全と判断する。

### 2. Phase 2 の互換ポリシー

Phase 2 では、内部 canonical 名称を `category1` / `category2` へ統一する。ただし移行リスクを下げるため、外部入出力の互換は以下の方針で扱う。

- Python 側の export 出力は Phase 2 で `category1` / `category2` を正式列名とする。
- Rust 側の内部モデル、filter、UI は `category1` / `category2` に切り替える。
- `doc_type` は内部モデルから削除する。
- ただし Rust の reader 層は移行期間中、旧列名 / 旧 JSON キーも受理する。
  - CSV:
    - `category1` を優先
    - fallback として `municipality_name` を受理
    - `category2` を優先
    - fallback として `ordinance_or_rule` または `doc_type` を受理
  - JSON:
    - `serde(alias = "...")` を使い旧キーを受理する
- Python 側の export から旧列を同時出力するかは Phase 2 実装中に削る。少なくとも Rust reader が旧入力を受理できる状態になってから削除する。
- 旧 CSV / 旧 JSON の reader 互換は「移行期間の暫定措置」とし、恒久互換にはしない。

この方針により、Phase 2 は「内部名の切替を完了しつつ、既存の手元データ読み込みは一時的に維持する」形で進める。

### 3. Phase 2 の write set と競合ポリシー

2026-03-27 時点で、Phase 2 の主対象ファイルはすべて dirty worktree 上にある。したがって、実装中に別変更が重なると競合しやすい。

Phase 2 の primary write set は次のファイルに固定する。

- `src/model.rs`
- `src/csv_loader.rs`
- `src/analysis_runner.rs`
- `src/filter.rs`
- `src/app.rs`
- `src/app_main_layout.rs`
- `analysis_backend/export_formatter.py`
- `analysis_backend/data_access.py`
- `analysis_backend/frame_schema.py`

運用ルール:

- Phase 2 実装中は上記 write set をこの作業の占有対象とみなす。
- 上記ファイルに別変更が入った場合は、そのまま上書きせず差分を再読込してから統合する。
- Rust 側 rename は原子的に行う。
  - `model`
  - `csv_loader`
  - `analysis_runner`
  - `filter`
  - `app`
  - `app_main_layout`
  を別々に完了扱いしない。
- Python export の旧列削除は、Rust 側 reader の新旧両対応が通ったあとに行う。
- Phase 2 の検証コマンドは最低限以下を毎回通す。
  - Python:
    - `./.venv/bin/python -m unittest tests.test_build_ordinance_analysis_db tests.test_data_access tests.test_cli_json_output tests.test_worker_protocol tests.test_analysis_core tests.test_cli`
  - Rust:
    - `cargo.exe build --workspace`
    - `cargo.exe test --workspace`

以上により、Phase 2 は「Rust ベースライン確認済み」「互換ポリシー確定済み」「競合対象ファイル定義済み」の状態で着手可能とする。

## Phase 2 実施メモ

2026-03-27 時点で、以下は実施済み。

- Rust 内部モデル、filter、UI を `category1` / `category2` ベースへ切替
- Python export の正式列名を `category1` / `category2` へ切替
- Rust CSV / JSON reader に旧キー受理の暫定互換を実装
- `doc_type` を Rust 内部モデルから削除
- Python / Rust の主要テストを通過

同日時点で、以下は後続タスクとして残す。

- `analysis_backend/data_access.py` の旧 alias 列生成を完全に削除するかどうかの最終判断
- Rust reader 層の旧 CSV / 旧 JSON 互換の打ち切り
- `docs/build_ordinance_analysis_db.py` の source DB モード補助フィールド整理
- 補助ツールや周辺文書の完全追従

## 2026-03-27 時点の確定方針

- `docs/build_ordinance_analysis_db.py` の source DB モードは廃止する
- builder の入力は `--input-dir` のみとする
- CSV / JSON / GUI metadata は `category1` / `category2` のみを正式サポートとする
- 旧 `municipality_name` / `ordinance_or_rule` / `doc_type` の reader 互換は廃止する
- 条件 JSON の旧形式 (`forms`, `form_match_logic`, `max_token_distance`) は利便性のため維持する
- したがって `legacy_schema_migrated` warning は条件 JSON 正規化の文脈では引き続き存続する
