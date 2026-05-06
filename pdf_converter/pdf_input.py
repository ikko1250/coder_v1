from __future__ import annotations

import sys
from pathlib import Path

from google.genai import types

PDF_MAGIC_PREFIX = b"%PDF-"
MAX_INLINE_PDF_BYTES = 50 * 1024 * 1024
WARN_INLINE_PDF_BYTES = 20 * 1024 * 1024


class PdfValidationError(Exception):
    """PDF 事前検証に失敗したとき。メッセージはそのまま標準エラーに出す。"""


def validate_pdf_path(pdf_path: str) -> Path:
    """API 呼び出し前の PDF 検証。成功時は解決済み Path を返し、失敗時は PdfValidationError。"""
    path = Path(pdf_path).expanduser().resolve()

    if not path.exists():
        raise PdfValidationError(f"エラー: PDF ファイルが見つかりません: {path}")
    if not path.is_file():
        raise PdfValidationError(f"エラー: PDF パスがファイルではありません: {path}")
    if path.suffix.lower() != ".pdf":
        raise PdfValidationError(f"エラー: .pdf ファイルのみ対応しています: {path}")

    size = path.stat().st_size
    if size <= 0:
        raise PdfValidationError(f"エラー: PDF ファイルが空です: {path}")
    if size > MAX_INLINE_PDF_BYTES:
        raise PdfValidationError(
            f"エラー: PDF ファイルが 50MB の inline 上限を超えています: {path}"
        )
    if size > WARN_INLINE_PDF_BYTES:
        print(
            "警告: PDF ファイルが 20MB を超えています。"
            "inline 入力では遅延や失敗の可能性があります: "
            f"{path}",
            file=sys.stderr,
        )

    with path.open("rb") as f:
        header = f.read(5)
    if header != PDF_MAGIC_PREFIX:
        raise PdfValidationError(f"エラー: 有効な PDF ヘッダーではありません: {path}")

    return path


def load_pdf_part(path: Path) -> types.Part:
    """検証済み PDF を全文読み込み、inline 用 Part を返す。"""
    try:
        pdf_bytes = path.read_bytes()
    except OSError as exc:
        raise PdfValidationError(
            f"エラー: PDF ファイルの読み込みに失敗しました: {path}: {exc}"
        ) from exc
    return types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")
