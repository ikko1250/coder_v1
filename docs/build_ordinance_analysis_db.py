import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

ANALYSIS_DB_PATH = "data/ordinance_analysis.db"
REPORT_PATH_SUFFIX = ".report.json"
# Optional leading "<digits>_" (e.g. municipality id) before <category1>_<category2>.
FILE_NAME_PATTERN = re.compile(
    r"^(?:\d+_)?(?P<category1>[^_]+)_(?P<category2>[^_]+)$"
)

SENTENCE_END_CHARS = set("。．.!?！？")
SENTENCE_CLOSING_CHARS = set("」』）)]】〉》\"'")
OPENING_BRACKETS = {"(": ")", "（": "）", "「": "」", "『": "』", "【": "】", "［": "］", "〈": "〉", "《": "》"}
CLOSING_BRACKETS = {right: left for left, right in OPENING_BRACKETS.items()}


@dataclass
class SourceFileRow:
    file_path: str
    file_name: str
    ext: str
    category1: str
    category2: str


@dataclass
class IssueRecord:
    severity: str
    code: str
    message: str
    path: Optional[str] = None
    detail: Optional[str] = None


@dataclass
class ParagraphBlock:
    paragraph_text: str
    is_table_paragraph: int = 0
    table_column_count: Optional[int] = None
    table_parse_error: int = 0
    table_trailing_text_detached: int = 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="texts_2nd の txt/md を analysis 用 SQLite に格納し、段落・文・形態素を作成する"
    )
    parser.add_argument("--analysis-db", default=ANALYSIS_DB_PATH)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--report-path", default=None)
    parser.add_argument("--skip-tokenize", action="store_true")
    parser.add_argument("--sudachi-dict", choices=["core", "full", "small"], default="core")
    parser.add_argument("--split-mode", choices=["A", "B", "C"], default="C")
    parser.add_argument(
        "--split-inside-parentheses",
        action="store_true",
        help="括弧内の句読点でも文分割する（旧挙動）",
    )
    parser.add_argument(
        "--merge-table-lines",
        action="store_true",
        help="罫線行を基準に表ブロックを認識し、1段落として扱う",
    )
    parser.add_argument("--purge", action="store_true", help="既存の analysis_runs を全削除してから実行")
    parser.add_argument(
        "--recreate-db",
        action="store_true",
        help="analysis DB ファイルをバックアップ退避して新規作成する（高速リセット向け）",
    )
    parser.add_argument(
        "--fresh-db",
        action="store_true",
        help="既存 analysis DB をバックアップ退避して新規作成する（再実行時の実質上書き用）",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--note", default="")
    return parser.parse_args(argv)


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS analysis_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TEXT,
            status TEXT NOT NULL,
            source_locator TEXT,
            analysis_db_path TEXT NOT NULL,
            sudachi_dict TEXT,
            split_mode TEXT,
            skip_tokenize INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            summary_json TEXT
        );

        CREATE TABLE IF NOT EXISTS analysis_documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            source_file_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            ext TEXT,
            category1 TEXT NOT NULL,
            category2 TEXT NOT NULL,
            raw_text TEXT NOT NULL,
            text_sha256 TEXT NOT NULL,
            char_count INTEGER NOT NULL,
            line_count INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
            UNIQUE (run_id, source_file_path)
        );

        CREATE TABLE IF NOT EXISTS analysis_paragraphs (
            paragraph_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            paragraph_no INTEGER NOT NULL,
            paragraph_text TEXT NOT NULL,
            is_table_paragraph INTEGER NOT NULL DEFAULT 0,
            table_column_count INTEGER,
            table_parse_error INTEGER NOT NULL DEFAULT 0,
            table_trailing_text_detached INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (document_id) REFERENCES analysis_documents(document_id) ON DELETE CASCADE,
            UNIQUE (document_id, paragraph_no)
        );

        CREATE TABLE IF NOT EXISTS analysis_sentences (
            sentence_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            paragraph_id INTEGER NOT NULL,
            sentence_no_in_document INTEGER NOT NULL,
            sentence_no_in_paragraph INTEGER NOT NULL,
            sentence_text TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (document_id) REFERENCES analysis_documents(document_id) ON DELETE CASCADE,
            FOREIGN KEY (paragraph_id) REFERENCES analysis_paragraphs(paragraph_id) ON DELETE CASCADE,
            UNIQUE (paragraph_id, sentence_no_in_paragraph)
        );

        CREATE TABLE IF NOT EXISTS analysis_tokens (
            token_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            document_id INTEGER NOT NULL,
            paragraph_id INTEGER NOT NULL,
            sentence_id INTEGER NOT NULL,
            token_no INTEGER NOT NULL,
            surface TEXT NOT NULL,
            dictionary_form TEXT,
            normalized_form TEXT,
            reading_form TEXT,
            pos1 TEXT,
            pos2 TEXT,
            pos3 TEXT,
            pos4 TEXT,
            c_type TEXT,
            c_form TEXT,
            is_oov INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_id) REFERENCES analysis_runs(run_id) ON DELETE CASCADE,
            FOREIGN KEY (document_id) REFERENCES analysis_documents(document_id) ON DELETE CASCADE,
            FOREIGN KEY (paragraph_id) REFERENCES analysis_paragraphs(paragraph_id) ON DELETE CASCADE,
            FOREIGN KEY (sentence_id) REFERENCES analysis_sentences(sentence_id) ON DELETE CASCADE,
            UNIQUE (sentence_id, token_no)
        );

        CREATE INDEX IF NOT EXISTS idx_analysis_documents_run_id
            ON analysis_documents (run_id);
        CREATE INDEX IF NOT EXISTS idx_analysis_paragraphs_document_id
            ON analysis_paragraphs (document_id);
        CREATE INDEX IF NOT EXISTS idx_analysis_sentences_document_id
            ON analysis_sentences (document_id);
        CREATE INDEX IF NOT EXISTS idx_analysis_tokens_sentence_id
            ON analysis_tokens (sentence_id);
        CREATE INDEX IF NOT EXISTS idx_analysis_tokens_run_sentence_token_no
            ON analysis_tokens (run_id, sentence_id, token_no);
        CREATE INDEX IF NOT EXISTS idx_analysis_tokens_surface
            ON analysis_tokens (surface);
        """
    )
    ensure_analysis_paragraph_columns(conn)
    ensure_analysis_document_columns(conn)
    ensure_analysis_run_columns(conn)
    conn.commit()


def purge_all_runs(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM analysis_runs")
    conn.commit()


def ensure_analysis_paragraph_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(analysis_paragraphs)")
    existing_columns = {str(row[1]) for row in cursor.fetchall()}
    required_columns = {
        "is_table_paragraph": "INTEGER NOT NULL DEFAULT 0",
        "table_column_count": "INTEGER",
        "table_parse_error": "INTEGER NOT NULL DEFAULT 0",
        "table_trailing_text_detached": "INTEGER NOT NULL DEFAULT 0",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE analysis_paragraphs ADD COLUMN {column_name} {column_type}")


def ensure_analysis_document_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(analysis_documents)")
    existing_columns = {str(row[1]) for row in cursor.fetchall()}
    required_columns = {
        "category1": "TEXT NOT NULL DEFAULT ''",
        "category2": "TEXT NOT NULL DEFAULT ''",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE analysis_documents ADD COLUMN {column_name} {column_type}")


def ensure_analysis_run_columns(conn: sqlite3.Connection) -> None:
    cursor = conn.execute("PRAGMA table_info(analysis_runs)")
    existing_columns = {str(row[1]) for row in cursor.fetchall()}
    required_columns = {
        "source_locator": "TEXT",
    }
    for column_name, column_type in required_columns.items():
        if column_name in existing_columns:
            continue
        conn.execute(f"ALTER TABLE analysis_runs ADD COLUMN {column_name} {column_type}")


def get_unescaped_pipe_positions(text: str) -> List[int]:
    positions: List[int] = []
    escaped = False
    for idx, ch in enumerate(text):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "|":
            positions.append(idx)
    return positions


def contains_unescaped_pipe(text: str) -> bool:
    return bool(get_unescaped_pipe_positions(text))


def split_table_cells(line: str) -> List[str]:
    stripped = line.strip()
    if not stripped:
        return []

    pipe_positions = get_unescaped_pipe_positions(stripped)
    if not pipe_positions:
        return [stripped]

    cells: List[str] = []
    start_idx = 0
    for pipe_idx in pipe_positions:
        cells.append(stripped[start_idx:pipe_idx].strip())
        start_idx = pipe_idx + 1
    cells.append(stripped[start_idx:].strip())

    if stripped.startswith("|") and cells:
        cells = cells[1:]
    if stripped.endswith("|") and cells:
        cells = cells[:-1]
    return cells


def count_table_columns(line: str) -> int:
    return len(split_table_cells(line))


def is_table_rule_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped or not contains_unescaped_pipe(stripped):
        return False

    cells = split_table_cells(stripped)
    if not cells:
        return False
    return all(re.fullmatch(r":?-{2,}:?", cell) for cell in cells)


def split_table_row_and_trailing_text(line: str) -> Tuple[str, Optional[str]]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        return stripped, None

    pipe_positions = get_unescaped_pipe_positions(stripped)
    if not pipe_positions:
        return stripped, None

    last_pipe_idx = pipe_positions[-1]
    if last_pipe_idx >= len(stripped) - 1:
        return stripped, None

    row_text = stripped[: last_pipe_idx + 1].rstrip()
    trailing_text = stripped[last_pipe_idx + 1 :].strip()
    if not trailing_text:
        return row_text, None
    return row_text, trailing_text


def build_default_paragraph_blocks(text: str) -> List[ParagraphBlock]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    lines = normalized.split("\n")
    has_blank_line = any(not line.strip() for line in lines)
    blocks: List[ParagraphBlock] = []

    if has_blank_line:
        buf: List[str] = []
        for line in lines:
            stripped = line.strip()
            if stripped:
                buf.append(stripped)
                continue
            if buf:
                blocks.append(ParagraphBlock(paragraph_text="\n".join(buf)))
                buf = []
        if buf:
            blocks.append(ParagraphBlock(paragraph_text="\n".join(buf)))
        return blocks

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        blocks.append(ParagraphBlock(paragraph_text=stripped))
    return blocks


def compute_table_parse_error(table_lines: List[str], expected_columns: Optional[int]) -> int:
    if not table_lines or expected_columns is None:
        return 0

    for line in table_lines:
        if not contains_unescaped_pipe(line):
            return 1
        if count_table_columns(line) != expected_columns:
            return 1
    return 0


def build_paragraph_blocks(text: str, merge_table_lines: bool = False) -> List[ParagraphBlock]:
    if not merge_table_lines:
        return build_default_paragraph_blocks(text)

    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    lines = normalized.split("\n")
    join_normal_lines = any(not line.strip() for line in lines)

    blocks: List[ParagraphBlock] = []
    normal_buf: List[str] = []
    table_lines: List[str] = []
    table_column_count: Optional[int] = None
    table_trailing_text_detached = 0

    def flush_normal_buf() -> None:
        nonlocal normal_buf
        if normal_buf:
            blocks.append(ParagraphBlock(paragraph_text="\n".join(normal_buf)))
            normal_buf = []

    def flush_table_buf() -> None:
        nonlocal table_lines, table_column_count, table_trailing_text_detached
        if table_lines:
            blocks.append(
                ParagraphBlock(
                    paragraph_text="\n".join(table_lines),
                    is_table_paragraph=1,
                    table_column_count=table_column_count,
                    table_parse_error=compute_table_parse_error(table_lines, table_column_count),
                    table_trailing_text_detached=table_trailing_text_detached,
                )
            )
            table_lines = []
            table_column_count = None
            table_trailing_text_detached = 0

    def append_normal_line(line: str) -> None:
        if join_normal_lines:
            normal_buf.append(line)
            return
        flush_normal_buf()
        normal_buf.append(line)

    def start_new_table(rule_line: str, header_candidate: Optional[str]) -> None:
        nonlocal table_lines, table_column_count, table_trailing_text_detached
        table_lines = []
        table_column_count = count_table_columns(rule_line)
        table_trailing_text_detached = 0
        if header_candidate is not None:
            table_lines.append(header_candidate)
        table_lines.append(rule_line)

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            flush_table_buf()
            flush_normal_buf()
            continue

        if is_table_rule_line(stripped):
            header_candidate = None
            if table_lines:
                if (not is_table_rule_line(table_lines[-1])) and contains_unescaped_pipe(table_lines[-1]):
                    header_candidate = table_lines.pop()
                flush_table_buf()
            else:
                if normal_buf and contains_unescaped_pipe(normal_buf[-1]):
                    header_candidate = normal_buf.pop()
                flush_normal_buf()
            start_new_table(stripped, header_candidate)
            continue

        if table_lines:
            if contains_unescaped_pipe(stripped):
                row_text, trailing_text = split_table_row_and_trailing_text(stripped)
                table_lines.append(row_text)
                if trailing_text is not None:
                    table_trailing_text_detached = 1
                    flush_table_buf()
                    append_normal_line(trailing_text)
                continue
            flush_table_buf()

        append_normal_line(stripped)

    flush_table_buf()
    flush_normal_buf()
    return blocks


def split_into_paragraphs(text: str, merge_table_lines: bool = False) -> List[str]:
    return [block.paragraph_text for block in build_paragraph_blocks(text, merge_table_lines=merge_table_lines)]


def split_into_sentences(paragraph_text: str, split_inside_parentheses: bool = False) -> List[str]:
    text = paragraph_text.strip()
    if not text:
        return []

    sentences: List[str] = []
    buf: List[str] = []
    bracket_stack: List[str] = []
    idx = 0
    size = len(text)

    while idx < size:
        ch = text[idx]
        buf.append(ch)
        idx += 1

        if ch in OPENING_BRACKETS:
            bracket_stack.append(ch)
        elif ch in CLOSING_BRACKETS:
            expected_open = CLOSING_BRACKETS[ch]
            if bracket_stack and bracket_stack[-1] == expected_open:
                bracket_stack.pop()
            elif expected_open in bracket_stack:
                last_index = len(bracket_stack) - 1 - bracket_stack[::-1].index(expected_open)
                bracket_stack = bracket_stack[:last_index]

        if ch in SENTENCE_END_CHARS:
            if (not split_inside_parentheses) and bracket_stack:
                continue

            while idx < size and text[idx] in SENTENCE_CLOSING_CHARS:
                buf.append(text[idx])
                if text[idx] in CLOSING_BRACKETS:
                    expected_open = CLOSING_BRACKETS[text[idx]]
                    if bracket_stack and bracket_stack[-1] == expected_open:
                        bracket_stack.pop()
                idx += 1
            sentence = "".join(buf).strip()
            if sentence:
                sentences.append(sentence)
            buf = []

    if buf:
        tail = "".join(buf).strip()
        if tail:
            sentences.append(tail)

    return sentences


def split_table_paragraph_into_sentences(paragraph_text: str) -> Tuple[List[str], int]:
    sentences: List[str] = []
    skipped_rule_lines = 0

    for raw_line in paragraph_text.split("\n"):
        stripped = raw_line.strip()
        if not stripped:
            continue
        if is_table_rule_line(stripped):
            skipped_rule_lines += 1
            continue
        sentences.append(stripped)

    return sentences, skipped_rule_lines

def parse_category_values_from_file_name(file_name: str) -> Tuple[Optional[str], Optional[str]]:
    stem = Path(file_name).stem
    match = FILE_NAME_PATTERN.fullmatch(stem)
    if match is None:
        return None, None
    return match.group("category1"), match.group("category2")


def classify_forbidden_input_relation(input_dir: Path, forbidden_dir: Path) -> str | None:
    """input_dir と forbidden_dir の関係を same / child / parent で返す。"""
    if input_dir == forbidden_dir:
        return "same"
    if forbidden_dir in input_dir.parents:
        return "child"
    if input_dir in forbidden_dir.parents:
        return "parent"
    return None


def resolve_forbidden_dirs(project_root: Path | None = None) -> list[Path]:
    """プロジェクトルートから禁止ディレクトリの絶対パス一覧を返す。"""
    if project_root is None:
        project_root = Path.cwd().resolve()
    return [
        project_root / "asset" / "ocr_manual",
        project_root / "asset" / "texts_2nd" / "manual",
    ]


def check_forbidden_input_dir(input_dir: Path, forbidden_dirs: list[Path]) -> IssueRecord | None:
    """入力ディレクトリが禁止ディレクトリと同じ・親・子のいずれかなら error IssueRecord を返す。"""
    resolved_input = input_dir.resolve()
    for forbidden_dir in forbidden_dirs:
        resolved_forbidden = forbidden_dir.resolve()
        relation = classify_forbidden_input_relation(resolved_input, resolved_forbidden)
        if relation is not None:
            return IssueRecord(
                severity="error",
                code="forbidden_input_dir",
                path=str(resolved_input),
                message=(
                    f"input directory is {relation} of forbidden OCR workspace: "
                    f"{resolved_forbidden}"
                ),
            )
    return None


def load_source_rows_from_dir(
    input_dir: Path,
    limit: Optional[int],
    forbidden_dirs: Optional[list[Path]] = None,
) -> Tuple[List[SourceFileRow], List[IssueRecord]]:
    rows: List[SourceFileRow] = []
    issues: List[IssueRecord] = []
    candidates: List[Path] = []

    resolved_forbidden = [d.resolve() for d in (forbidden_dirs or [])]

    # input_dir 自体が forbidden と同じ、親、または子ならエラー
    resolved_input = input_dir.resolve()
    for forbidden_dir in resolved_forbidden:
        relation = classify_forbidden_input_relation(resolved_input, forbidden_dir)
        if relation is not None:
            return [], [
                IssueRecord(
                    severity="error",
                    code="forbidden_input_dir",
                    path=str(resolved_input),
                    message=(
                        f"input directory is {relation} of forbidden OCR workspace: "
                        f"{forbidden_dir}"
                    ),
                )
            ]

    for root, dirs, files in os.walk(input_dir, followlinks=False):
        root_path = Path(root).resolve()
        # prune: 禁止ディレクトリ配下をスキップ
        if any(root_path == f or f in root_path.parents for f in resolved_forbidden):
            dirs[:] = []
            continue
        for file_name in files:
            file_path = root_path / file_name
            if file_path.suffix not in {".txt", ".md"}:
                continue
            candidates.append(file_path)

    candidates.sort(key=lambda path: path.relative_to(input_dir).as_posix())
    # limit は有効候補抽出後に適用するため、ここでは適用しない

    for file_path in candidates:
        category1, category2 = parse_category_values_from_file_name(file_path.name)
        if category1 is None or category2 is None:
            issues.append(
                IssueRecord(
                    severity="warning",
                    code="invalid_file_name",
                    path=str(file_path.relative_to(input_dir).as_posix()),
                    message=(
                        "file name must match <category1>_<category2>.(txt|md) "
                        "or <digits>_<category1>_<category2>.(txt|md)"
                    ),
                )
            )
            continue

        rows.append(
            SourceFileRow(
                file_path=str(file_path.resolve()),
                file_name=file_path.name,
                ext=file_path.suffix,
                category1=category1,
                category2=category2,
            )
        )

    # limit を有効候補に対して適用
    if limit is not None:
        rows = rows[:limit]

    if not candidates and not any(i.code == "forbidden_input_dir" for i in issues):
        issues.append(
            IssueRecord(
                severity="error",
                code="no_input_files",
                path=str(input_dir),
                message="no .txt or .md files found under input directory",
            )
        )

    return rows, issues


def start_run(
    conn: sqlite3.Connection,
    source_locator: str,
    analysis_db_path: str,
    sudachi_dict: Optional[str],
    split_mode: Optional[str],
    skip_tokenize: bool,
    note: str,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_runs (
            status,
            source_locator,
            analysis_db_path,
            sudachi_dict,
            split_mode,
            skip_tokenize,
            note
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "running",
            source_locator,
            analysis_db_path,
            sudachi_dict,
            split_mode,
            1 if skip_tokenize else 0,
            note,
        ),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(conn: sqlite3.Connection, run_id: int, status: str, summary: Dict[str, object]) -> None:
    conn.execute(
        """
        UPDATE analysis_runs
        SET status = ?,
            finished_at = CURRENT_TIMESTAMP,
            summary_json = ?
        WHERE run_id = ?
        """,
        (status, json.dumps(summary, ensure_ascii=False), run_id),
    )
    conn.commit()


def build_tokenizer(sudachi_dict: str, split_mode: str):
    try:
        from sudachipy import Dictionary, SplitMode
    except ImportError as exc:
        raise RuntimeError(
            "sudachipy が見つかりません。.venv で sudachipy と sudachidict_* をインストールしてください。"
        ) from exc

    try:
        split_mode_obj = getattr(SplitMode, split_mode)
    except AttributeError as exc:
        raise RuntimeError(f"無効な SplitMode です: {split_mode}") from exc
        
    import os
    config_kwarg = {}
    if os.path.exists("sudachi.json"):
        config_kwarg["config_path"] = "sudachi.json"

    tokenizer = Dictionary(dict=sudachi_dict, **config_kwarg).create()
    return tokenizer, split_mode_obj


def safe_is_oov(morpheme) -> Optional[int]:
    checker = getattr(morpheme, "is_oov", None)
    if checker is None:
        return None
    try:
        return 1 if checker() else 0
    except Exception:
        return None


def extract_pos_values(morpheme) -> Tuple[str, str, str, str]:
    pos = morpheme.part_of_speech()
    values = [str(x) for x in pos[:4]]
    while len(values) < 4:
        values.append("")
    return values[0], values[1], values[2], values[3]


def insert_document_row(
    conn: sqlite3.Connection,
    run_id: int,
    source_row: SourceFileRow,
    raw_text: str,
) -> int:
    text_sha256 = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    char_count = len(raw_text)
    line_count = raw_text.count("\n") + (1 if raw_text else 0)

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_documents (
            run_id,
            source_file_path,
            file_name,
            ext,
            category1,
            category2,
            raw_text,
            text_sha256,
            char_count,
            line_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            source_row.file_path,
            source_row.file_name,
            source_row.ext,
            source_row.category1,
            source_row.category2,
            raw_text,
            text_sha256,
            char_count,
            line_count,
        ),
    )
    return int(cursor.lastrowid)


def insert_paragraph_row(
    conn: sqlite3.Connection,
    run_id: int,
    document_id: int,
    paragraph_no: int,
    paragraph_text: str,
    is_table_paragraph: int = 0,
    table_column_count: Optional[int] = None,
    table_parse_error: int = 0,
    table_trailing_text_detached: int = 0,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_paragraphs (
            run_id,
            document_id,
            paragraph_no,
            paragraph_text,
            is_table_paragraph,
            table_column_count,
            table_parse_error,
            table_trailing_text_detached
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            document_id,
            paragraph_no,
            paragraph_text,
            is_table_paragraph,
            table_column_count,
            table_parse_error,
            table_trailing_text_detached,
        ),
    )
    return int(cursor.lastrowid)


def insert_sentence_row(
    conn: sqlite3.Connection,
    run_id: int,
    document_id: int,
    paragraph_id: int,
    sentence_no_in_document: int,
    sentence_no_in_paragraph: int,
    sentence_text: str,
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO analysis_sentences (
            run_id,
            document_id,
            paragraph_id,
            sentence_no_in_document,
            sentence_no_in_paragraph,
            sentence_text
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            document_id,
            paragraph_id,
            sentence_no_in_document,
            sentence_no_in_paragraph,
            sentence_text,
        ),
    )
    return int(cursor.lastrowid)


def insert_token_row(
    conn: sqlite3.Connection,
    run_id: int,
    document_id: int,
    paragraph_id: int,
    sentence_id: int,
    token_no: int,
    morpheme,
) -> None:
    pos1, pos2, pos3, pos4 = extract_pos_values(morpheme)
    conn.execute(
        """
        INSERT INTO analysis_tokens (
            run_id,
            document_id,
            paragraph_id,
            sentence_id,
            token_no,
            surface,
            dictionary_form,
            normalized_form,
            reading_form,
            pos1,
            pos2,
            pos3,
            pos4,
            c_type,
            c_form,
            is_oov
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            document_id,
            paragraph_id,
            sentence_id,
            token_no,
            morpheme.surface(),
            morpheme.dictionary_form(),
            morpheme.normalized_form(),
            morpheme.reading_form(),
            pos1,
            pos2,
            pos3,
            pos4,
            morpheme.part_of_speech()[4] if len(morpheme.part_of_speech()) > 4 else "",
            morpheme.part_of_speech()[5] if len(morpheme.part_of_speech()) > 5 else "",
            safe_is_oov(morpheme),
        ),
    )


def resolve_read_path(file_path: str) -> Path:
    path = Path(file_path)
    if path.exists():
        return path
    project_relative = Path.cwd() / file_path
    return project_relative


def read_source_text(path: Path) -> str:
    raw_bytes = path.read_bytes()
    if raw_bytes.startswith(b"\xef\xbb\xbf"):
        return raw_bytes.decode("utf-8-sig")
    return raw_bytes.decode("utf-8")


def resolve_report_path(analysis_db_path: Path, report_path_arg: Optional[str]) -> Path:
    if report_path_arg:
        return Path(report_path_arg).expanduser().resolve()
    resolved_analysis_db = analysis_db_path.expanduser().resolve()
    return resolved_analysis_db.with_name(f"{resolved_analysis_db.name}{REPORT_PATH_SUFFIX}")


def write_report(
    report_path: Path,
    *,
    status: str,
    analysis_db_path: Path,
    source_locator: Optional[str],
    issues: List[IssueRecord],
    summary: Optional[Dict[str, object]] = None,
) -> None:
    payload: Dict[str, object] = {
        "status": status,
        "analysis_db_path": str(analysis_db_path),
        "source_locator": source_locator,
        "issue_count": len(issues),
        "issues": [
            {
                "severity": issue.severity,
                "code": issue.code,
                "message": issue.message,
                "path": issue.path,
                "detail": issue.detail,
            }
            for issue in issues
        ],
    }
    if summary is not None:
        payload["summary"] = summary

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def print_issue_summary_to_stderr(issues: List[IssueRecord]) -> None:
    print(f"build failed with {len(issues)} issue(s)", file=os.sys.stderr)
    for issue in issues[:10]:
        location = f" path={issue.path}" if issue.path else ""
        print(f"[{issue.severity}] {issue.code}:{location} {issue.message}", file=os.sys.stderr)


def prepare_temp_analysis_db_path(
    analysis_db_path: Path,
    *,
    should_start_fresh: bool,
) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    temp_db_path = analysis_db_path.with_name(f"{analysis_db_path.name}.tmp_{timestamp}")
    if analysis_db_path.exists() and not should_start_fresh:
        shutil.copy2(analysis_db_path, temp_db_path)
    return temp_db_path


def cleanup_temp_path(temp_db_path: Path) -> None:
    try:
        if temp_db_path.exists():
            temp_db_path.unlink()
    except OSError:
        pass


def process_rows(
    conn: sqlite3.Connection,
    run_id: int,
    source_rows: List[SourceFileRow],
    skip_tokenize: bool,
    split_inside_parentheses: bool,
    merge_table_lines: bool,
    tokenizer=None,
    split_mode=None,
) -> Dict[str, object]:
    summary = {
        "source_rows": len(source_rows),
        "documents_inserted": 0,
        "paragraphs_inserted": 0,
        "table_paragraphs_inserted": 0,
        "table_parse_error_paragraphs": 0,
        "table_trailing_text_detached_paragraphs": 0,
        "table_sentences_inserted": 0,
        "table_rule_lines_skipped": 0,
        "sentences_inserted": 0,
        "tokens_inserted": 0,
        "empty_documents": 0,
        "sentence_rule_version": (
            "v1_split_inside_parentheses"
            if split_inside_parentheses
            else "v2_ignore_inner_parentheses_punct"
        ),
        "paragraph_rule_version": (
            "v3_rule_line_table_detection"
            if merge_table_lines
            else "v1_default"
        ),
        "merge_table_lines": 1 if merge_table_lines else 0,
    }

    for idx, source_row in enumerate(source_rows, start=1):
        path = resolve_read_path(source_row.file_path)
        if not path.exists():
            raise FileNotFoundError(f"source file not found: {path}")

        try:
            raw_text = read_source_text(path)
        except (OSError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"failed to read source file: {path} ({exc})") from exc

        document_id = insert_document_row(conn, run_id, source_row, raw_text)
        summary["documents_inserted"] += 1

        paragraph_blocks = build_paragraph_blocks(raw_text, merge_table_lines=merge_table_lines)
        if not paragraph_blocks:
            summary["empty_documents"] += 1
            continue

        sentence_no_in_document = 0
        for paragraph_no, paragraph_block in enumerate(paragraph_blocks, start=1):
            paragraph_id = insert_paragraph_row(
                conn=conn,
                run_id=run_id,
                document_id=document_id,
                paragraph_no=paragraph_no,
                paragraph_text=paragraph_block.paragraph_text,
                is_table_paragraph=paragraph_block.is_table_paragraph,
                table_column_count=paragraph_block.table_column_count,
                table_parse_error=paragraph_block.table_parse_error,
                table_trailing_text_detached=paragraph_block.table_trailing_text_detached,
            )
            summary["paragraphs_inserted"] += 1
            if paragraph_block.is_table_paragraph:
                summary["table_paragraphs_inserted"] += 1
            if paragraph_block.table_parse_error:
                summary["table_parse_error_paragraphs"] += 1
            if paragraph_block.table_trailing_text_detached:
                summary["table_trailing_text_detached_paragraphs"] += 1

            if paragraph_block.is_table_paragraph:
                sentences, skipped_rule_lines = split_table_paragraph_into_sentences(paragraph_block.paragraph_text)
                summary["table_sentences_inserted"] += len(sentences)
                summary["table_rule_lines_skipped"] += skipped_rule_lines
            else:
                sentences = split_into_sentences(
                    paragraph_text=paragraph_block.paragraph_text,
                    split_inside_parentheses=split_inside_parentheses,
                )
                if not sentences:
                    sentences = [paragraph_block.paragraph_text]

            for sentence_no_in_paragraph, sentence_text in enumerate(sentences, start=1):
                sentence_no_in_document += 1
                sentence_id = insert_sentence_row(
                    conn=conn,
                    run_id=run_id,
                    document_id=document_id,
                    paragraph_id=paragraph_id,
                    sentence_no_in_document=sentence_no_in_document,
                    sentence_no_in_paragraph=sentence_no_in_paragraph,
                    sentence_text=sentence_text,
                )
                summary["sentences_inserted"] += 1

                if skip_tokenize:
                    continue

                if tokenizer is None or split_mode is None:
                    raise RuntimeError("tokenize mode requires tokenizer and split_mode")

                token_no = 0
                for token_no, morpheme in enumerate(tokenizer.tokenize(sentence_text, split_mode), start=1):
                    insert_token_row(
                        conn=conn,
                        run_id=run_id,
                        document_id=document_id,
                        paragraph_id=paragraph_id,
                        sentence_id=sentence_id,
                        token_no=token_no,
                        morpheme=morpheme,
                    )
                summary["tokens_inserted"] += token_no

    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    should_recreate_db = args.recreate_db or args.fresh_db
    if args.purge and should_recreate_db:
        raise ValueError("--purge と --recreate-db / --fresh-db は同時に指定できません。")
    analysis_db_path = Path(args.analysis_db).expanduser().resolve()
    report_path = resolve_report_path(analysis_db_path, args.report_path)
    preflight_issues: List[IssueRecord] = []
    source_rows: List[SourceFileRow] = []
    source_locator: Optional[str] = None
    input_dir = Path(args.input_dir).expanduser().resolve()
    source_locator = str(input_dir)
    if not input_dir.exists():
        preflight_issues.append(
            IssueRecord(
                severity="error",
                code="input_dir_not_found",
                path=str(input_dir),
                message="input directory was not found",
            )
        )
    elif not input_dir.is_dir():
        preflight_issues.append(
            IssueRecord(
                severity="error",
                code="input_dir_not_directory",
                path=str(input_dir),
                message="input path is not a directory",
            )
        )
    else:
        # 禁止ディレクトリチェック
        forbidden_dirs = resolve_forbidden_dirs()
        forbidden_issue = check_forbidden_input_dir(input_dir, forbidden_dirs)
        if forbidden_issue is not None:
            preflight_issues.append(forbidden_issue)
        else:
            source_rows, dir_issues = load_source_rows_from_dir(input_dir, args.limit, forbidden_dirs=forbidden_dirs)
            preflight_issues.extend(dir_issues)

    tokenizer = None
    split_mode = None
    if not args.skip_tokenize and not preflight_issues:
        try:
            tokenizer, split_mode = build_tokenizer(args.sudachi_dict, args.split_mode)
        except RuntimeError as exc:
            preflight_issues.append(
                IssueRecord(
                    severity="error",
                    code="tokenizer_init_failed",
                    message=str(exc),
                )
            )

    if preflight_issues:
        write_report(
            report_path,
            status="preflight_failed",
            analysis_db_path=analysis_db_path,
            source_locator=source_locator,
            issues=preflight_issues,
        )
        print_issue_summary_to_stderr(preflight_issues)
        return 1

    analysis_db_path.parent.mkdir(parents=True, exist_ok=True)
    temp_db_path = prepare_temp_analysis_db_path(
        analysis_db_path,
        should_start_fresh=should_recreate_db,
    )
    analysis_conn: Optional[sqlite3.Connection] = None
    run_id: Optional[int] = None

    try:
        analysis_conn = sqlite3.connect(str(temp_db_path))
        analysis_conn.execute("PRAGMA foreign_keys = ON")
        create_schema(analysis_conn)
        if args.purge:
            purge_all_runs(analysis_conn)

        run_id = start_run(
            conn=analysis_conn,
            source_locator=str(source_locator),
            analysis_db_path=str(analysis_db_path),
            sudachi_dict=None if args.skip_tokenize else args.sudachi_dict,
            split_mode=None if args.skip_tokenize else args.split_mode,
            skip_tokenize=args.skip_tokenize,
            note=args.note,
        )

        summary = process_rows(
            conn=analysis_conn,
            run_id=run_id,
            source_rows=source_rows,
            skip_tokenize=args.skip_tokenize,
            split_inside_parentheses=args.split_inside_parentheses,
            merge_table_lines=args.merge_table_lines,
            tokenizer=tokenizer,
            split_mode=split_mode,
        )
        finish_run(analysis_conn, run_id, "completed", summary)
        analysis_conn.close()
        analysis_conn = None
        os.replace(temp_db_path, analysis_db_path)
    except Exception as exc:
        if analysis_conn is not None:
            if run_id is not None:
                try:
                    finish_run(analysis_conn, run_id, "failed", {"error": "run_failed"})
                except Exception:
                    pass
            analysis_conn.close()
        cleanup_temp_path(temp_db_path)
        runtime_issues = [
            IssueRecord(
                severity="error",
                code="build_failed",
                message=str(exc),
            )
        ]
        write_report(
            report_path,
            status="runtime_failed",
            analysis_db_path=analysis_db_path,
            source_locator=source_locator,
            issues=runtime_issues,
        )
        print_issue_summary_to_stderr(runtime_issues)
        return 1

    print(f"analysis run completed: run_id={run_id}")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
