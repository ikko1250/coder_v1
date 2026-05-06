from __future__ import annotations

import difflib
from pathlib import Path


class OcrDiffError(Exception):
    """OCR 修正モードの diff 生成に失敗したとき。"""


def read_utf8_text_preserving_newlines(path: Path) -> str:
    """UTF-8 テキストを改行コードを保持したまま読む。"""
    try:
        with path.open("r", encoding="utf-8", newline="") as input_file:
            return input_file.read()
    except OSError as exc:
        raise OcrDiffError(f"エラー: diff 用のファイル読取に失敗しました: {path}: {exc}") from exc


def build_unified_diff_text(
    original_path: Path,
    working_path: Path,
) -> str:
    """元 Markdown と編集対象 Markdown の unified diff を返す。

    NOTE: path helpers are imported from ocr_paths.py.
    """
    from pdf_converter.ocr_paths import (
        ensure_path_within_directory,
        get_default_manual_markdown_dir,
        get_default_manual_work_dir,
        make_manual_relative_path,
    )

    try:
        resolved_original_path = ensure_path_within_directory(
            original_path,
            get_default_manual_markdown_dir(),
            "元 OCR Markdown",
            error_cls=OcrDiffError,
        )
        resolved_working_path = ensure_path_within_directory(
            working_path,
            get_default_manual_work_dir(),
            "編集対象 Markdown",
            error_cls=OcrDiffError,
        )
    except OcrDiffError:
        raise

    original_text = read_utf8_text_preserving_newlines(resolved_original_path)
    working_text = read_utf8_text_preserving_newlines(resolved_working_path)

    original_lines = original_text.splitlines()
    working_lines = working_text.splitlines()
    diff_lines = difflib.unified_diff(
        original_lines,
        working_lines,
        fromfile=make_manual_relative_path(resolved_original_path),
        tofile=make_manual_relative_path(resolved_working_path),
        lineterm="",
    )
    return "\n".join(diff_lines)


def format_ocr_correction_stdout(final_message: str, diff_text: str) -> str:
    """OCR 修正モードの標準出力本文を組み立てる。"""
    if not diff_text.strip():
        return final_message
    return "\n\n".join(
        [
            final_message,
            "Unified diff:",
            diff_text,
        ]
    )


def emit_ocr_correction_stdout(final_message: str, diff_text: str) -> None:
    """OCR 修正モードの結果を標準出力へ出す。"""
    print(format_ocr_correction_stdout(final_message, diff_text))
