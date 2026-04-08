"""
Gemma 4 31B IT を Gemini API（generativelanguage.googleapis.com）経由で呼び出す最小例。

公式: https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api
"""

import argparse
import os
import sys
from pathlib import Path

from google import genai
from google.genai import types


def load_dotenv(dotenv_path: str) -> None:
    if not os.path.exists(dotenv_path):
        return

    with open(dotenv_path, encoding="utf-8") as dotenv_file:
        for raw_line in dotenv_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            os.environ.setdefault(key, value)


# CLI 既定モデル（設計上の標準）。--model で上書きし、PDF inline 不調時の切り分けに使う。
DEFAULT_MODEL = "gemma-4-31b-it"
DEFAULT_API_KEY_ENV = "GEMINI_API_KEY"
DEFAULT_PROMPT_TEXT_ONLY = "水の化学式は何ですか？簡潔に答えてください。"
DEFAULT_PROMPT_WITH_PDF = "この PDF の内容を要約してください。"

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


def resolve_prompt(prompt: str | None, pdf_path: str | None) -> str:
    """位置引数が省略されたときだけ、PDF パス有無に応じた既定プロンプトを返す（CLI 以外の単体テスト用にも使う）。"""
    if prompt is not None:
        return prompt
    if pdf_path:
        return DEFAULT_PROMPT_WITH_PDF
    return DEFAULT_PROMPT_TEXT_ONLY


def resolve_prompt_from_args(args: argparse.Namespace) -> tuple[str, str | None]:
    """`main` 用: `--pdf-path` の正規化と `resolve_prompt` をまとめて行い、(プロンプト文字列, 正規化済み PDF パス) を返す。"""
    pdf_path = (args.pdf_path or "").strip() or None
    return resolve_prompt(args.prompt, pdf_path), pdf_path


def build_contents(prompt: str, pdf_part: types.Part | None) -> list:
    """generate_content 向け contents。戻り値は常に list。

    pdf_part が None のときは [prompt] のみ（--pdf-path 未指定時のテキスト専用経路。PDF の検証・読込は行わない）。
    pdf_part があるときは [pdf_part, prompt]。
    """
    if pdf_part is None:
        return [prompt]
    return [pdf_part, prompt]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Call Gemma 4 31B IT on Gemini API with thinking (thinking_level=high). "
            "Optional --pdf-path attaches a local PDF as inline input (application/pdf)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python pdf_converter/call-gemma4-gemini.py "Summarize this PDF" '
            "--pdf-path path/to/file.pdf\n"
            "  python pdf_converter/call-gemma4-gemini.py --pdf-path path/to/file.pdf\n"
            "\n"
            "If --pdf-path is omitted, only the text prompt is sent (no PDF)."
        ),
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default=None,
        help=(
            "User prompt. If omitted: without --pdf-path, use a short demo question; "
            "with --pdf-path, default to summarizing the PDF."
        ),
    )
    parser.add_argument(
        "--pdf-path",
        default=None,
        metavar="PATH",
        dest="pdf_path",
        help=(
            "Optional path to a local PDF. When set, the PDF input path is used: the file is "
            "read and sent as an inline Part (mime type application/pdf) together with the prompt. "
            "When omitted, the CLI runs in text-only mode. If you omit the positional prompt but "
            "set this option, the default prompt switches to summarizing the PDF."
        ),
    )
    parser.add_argument(
        "--api-key-env",
        default=DEFAULT_API_KEY_ENV,
        metavar="NAME",
        help=f"Environment variable name for the API key (default: {DEFAULT_API_KEY_ENV}).",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        metavar="MODEL",
        help=(
            f"Model id passed to generate_content (default: {DEFAULT_MODEL}). "
            "Use another id for troubleshooting or comparison when behaviour differs by model."
        ),
    )
    return parser.parse_args()


def main() -> int:
    try:
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        pass

    args = parse_args()
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path)

    user_prompt, pdf_path = resolve_prompt_from_args(args)
    model_id = (args.model or "").strip() or DEFAULT_MODEL

    validated_pdf_path: Path | None = None
    if pdf_path is not None:
        try:
            validated_pdf_path = validate_pdf_path(pdf_path)
        except PdfValidationError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    api_key = os.getenv(args.api_key_env, "").strip()
    if not api_key:
        print(
            f"Error: {args.api_key_env} is not set. Add it to pdf_converter/.env or the environment.",
            file=sys.stderr,
        )
        return 1

    client = genai.Client(api_key=api_key)

    pdf_part: types.Part | None = None
    if validated_pdf_path is not None:
        try:
            pdf_part = load_pdf_part(validated_pdf_path)
        except PdfValidationError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    contents = build_contents(user_prompt, pdf_part)

    response = client.models.generate_content(
        model=model_id,
        contents=contents,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="high"),
        ),
    )

    print(response.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
