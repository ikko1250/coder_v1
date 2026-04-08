"""
Gemma 4 31B IT を Gemini API（generativelanguage.googleapis.com）経由で呼び出す最小例。

公式: https://ai.google.dev/gemma/docs/core/gemma_on_gemini_api
"""

import argparse
import os
import re
import sys
from pathlib import Path

import httpx
from google import genai
from google.genai import errors
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
DEFAULT_TASK = "single-shot"
OCR_CORRECTION_TASK = "ocr-correct"
DEFAULT_MANUAL_ROOT = Path(__file__).resolve().parent.parent / "asset" / "texts_2nd" / "manual"
DEFAULT_MANUAL_MARKDOWN_DIR = DEFAULT_MANUAL_ROOT / "md"

PDF_MAGIC_PREFIX = b"%PDF-"
MAX_INLINE_PDF_BYTES = 50 * 1024 * 1024
WARN_INLINE_PDF_BYTES = 20 * 1024 * 1024
MARKDOWN_TIMESTAMP_STEM_PATTERN = re.compile(
    r"^(?P<base>.+)-(?P<ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$"
)

# generate_content 向け HTTP タイムアウト（HttpOptions はミリ秒）。設計書の初期値 120 秒。
DEFAULT_GENERATE_CONTENT_TIMEOUT_MS = 120_000

# Task 3-4（thinking フォールバック）:
# Phase 0 Task 0-2（verify-task-0-2-pdf-inline-thinking.py）で gemma-4-31b-it の
# PDF inline + thinking_level=high は成功済み。既定 False のまま PDF 時も thinking を付ける。
# 別モデル等で API が併用拒否する場合のみ True にし、PDF 添付時は thinking_config なしで送る。
OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED = False

# Task 0-3:
# OCR Markdown correction mode is planned as a tool-driven multi-turn flow.
# Keep thinking_config disabled by default there until the combination with
# function calling / thought signatures is verified separately.
ENABLE_THINKING_CONFIG_IN_OCR_CORRECTION = False


class PdfValidationError(Exception):
    """PDF 事前検証に失敗したとき。メッセージはそのまま標準エラーに出す。"""


class ResponseTextError(Exception):
    """応答から利用者向けテキストを取り出せないとき。メッセージはそのまま標準エラーに出す。"""


_FINISH_REASONS_POLICY_EMPTY = frozenset(
    {
        types.FinishReason.SAFETY,
        types.FinishReason.BLOCKLIST,
        types.FinishReason.PROHIBITED_CONTENT,
        types.FinishReason.RECITATION,
        types.FinishReason.SPII,
        types.FinishReason.LANGUAGE,
        types.FinishReason.IMAGE_SAFETY,
        types.FinishReason.IMAGE_PROHIBITED_CONTENT,
    }
)


def _enum_label(value: object) -> str:
    if value is None:
        return "不明"
    inner = getattr(value, "value", None)
    return str(inner) if inner is not None else str(value)


def extract_response_text(response: types.GenerateContentResponse) -> str:
    """response.text 取得と空・ブロック系の判定を一箇所に集約する。"""
    feedback = response.prompt_feedback
    if feedback is not None and feedback.block_reason is not None:
        reason = _enum_label(feedback.block_reason)
        detail = (feedback.block_reason_message or "").strip()
        suffix = f" 詳細: {detail}" if detail else ""
        raise ResponseTextError(
            f"エラー: プロンプトがブロックされました（理由: {reason}）。{suffix}".rstrip()
        )

    candidates = response.candidates
    if not candidates:
        raise ResponseTextError("エラー: モデルから応答候補がありません。")

    finish = candidates[0].finish_reason

    try:
        text = response.text
    except Exception as exc:
        raise ResponseTextError(
            "エラー: 応答テキストの取得に失敗しました: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if text is not None and text.strip():
        return text

    reason_label = _enum_label(finish)
    if finish in _FINISH_REASONS_POLICY_EMPTY:
        raise ResponseTextError(
            "エラー: 安全性ポリシー等により応答テキストがありません"
            f"（終了理由: {reason_label}）。"
        )
    raise ResponseTextError(
        f"エラー: 応答テキストが空です（終了理由: {reason_label}）。"
    )


def _brief_api_error_message(exc: errors.APIError, max_len: int = 400) -> str:
    msg = (exc.message or "").strip()
    if not msg and exc.status is not None:
        msg = str(exc.status).strip()
    if len(msg) > max_len:
        return msg[: max_len - 1] + "…"
    return msg


def format_generate_content_error(exc: BaseException) -> str:
    """generate_content 失敗をスタックトレースなしの CLI 向けメッセージに変換する。"""
    if isinstance(exc, httpx.TimeoutException):
        return "エラー: API リクエストがタイムアウトしました。"

    if isinstance(exc, httpx.RequestError):
        return f"エラー: API への通信に失敗しました（{type(exc).__name__}）。"

    if isinstance(exc, errors.ClientError):
        detail = _brief_api_error_message(exc)
        if exc.code in (401, 403):
            head = f"エラー: API キーまたは認証に失敗しました（HTTP {exc.code}）。"
            return f"{head} {detail}".rstrip() if detail else head
        if exc.code == 429:
            return (
                "エラー: API の利用制限（レート制限）に達しました（HTTP 429）。"
                "しばらく待ってから再試行してください。"
            )
        status_u = str(exc.status or "").upper()
        if exc.code == 400 and "INVALID_ARGUMENT" in status_u:
            head = "エラー: リクエストが無効です（INVALID_ARGUMENT）。"
            return f"{head} {detail}".rstrip() if detail else head
        head = f"エラー: API リクエストに失敗しました（HTTP {exc.code}）。"
        return f"{head} {detail}".rstrip() if detail else head

    if isinstance(exc, errors.ServerError):
        return (
            "エラー: API サーバー側でエラーが発生しました（HTTP "
            f"{exc.code}）。時間をおいて再試行してください。"
        )

    if isinstance(exc, errors.APIError):
        detail = _brief_api_error_message(exc)
        head = f"エラー: API リクエストに失敗しました（HTTP {exc.code}）。"
        return f"{head} {detail}".rstrip() if detail else head

    return (
        "エラー: API 呼び出し中に予期しないエラーが発生しました: "
        f"{type(exc).__name__}: {exc}"
    )


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


def parse_auto_matched_markdown_stem(markdown_path: Path) -> tuple[str, str] | None:
    """自動対応付け対象の Markdown だけ (base_stem, timestamp_text) を返す。"""
    if markdown_path.suffix.lower() != ".md":
        return None

    match = MARKDOWN_TIMESTAMP_STEM_PATTERN.fullmatch(markdown_path.stem)
    if match is None:
        return None

    return match.group("base"), match.group("ts")


def find_auto_matched_markdown_candidates(
    pdf_path: Path,
    markdown_dir: Path | None = None,
) -> list[Path]:
    """PDF stem と一致する timestamp 付き Markdown 候補だけを返す。"""
    search_dir = (markdown_dir or DEFAULT_MANUAL_MARKDOWN_DIR).expanduser()
    if not search_dir.exists() or not search_dir.is_dir():
        return []

    pdf_stem = pdf_path.stem
    candidates: list[Path] = []
    for markdown_path in sorted(search_dir.glob("*.md"), key=lambda path: path.name):
        parsed = parse_auto_matched_markdown_stem(markdown_path)
        if parsed is None:
            continue

        base_stem, _timestamp_text = parsed
        if base_stem != pdf_stem:
            continue

        candidates.append(markdown_path.resolve())

    return candidates


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


def build_generation_config(pdf_part: types.Part | None) -> types.GenerateContentConfig:
    """thinking_config の有無を Task 3-4 定数に従って決める。"""
    if pdf_part is not None and OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED:
        return types.GenerateContentConfig()
    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="high"),
    )


def build_ocr_correction_generation_config() -> types.GenerateContentConfig:
    """OCR correction mode defaults to no thinking_config until separately verified."""
    if ENABLE_THINKING_CONFIG_IN_OCR_CORRECTION:
        return types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level="high"),
        )
    return types.GenerateContentConfig()


def get_api_key_or_exit(env_name: str) -> str | None:
    """Resolve the API key from the configured environment variable."""
    api_key = os.getenv(env_name, "").strip()
    if api_key:
        return api_key
    print(
        f"Error: {env_name} is not set. Add it to pdf_converter/.env or the environment.",
        file=sys.stderr,
    )
    return None


def build_genai_client(api_key: str) -> genai.Client:
    """Create a Gemini API client with the standard timeout used by this CLI."""
    return genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(timeout=DEFAULT_GENERATE_CONTENT_TIMEOUT_MS),
    )


def generate_content_once(
    client: genai.Client,
    model_id: str,
    contents: list,
    config: types.GenerateContentConfig,
) -> types.GenerateContentResponse:
    """Thin wrapper around models.generate_content for reuse by future multi-turn flows."""
    return client.models.generate_content(
        model=model_id,
        contents=contents,
        config=config,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Call Gemma 4 31B IT on Gemini API with thinking (thinking_level=high). "
            "Optional --pdf-path attaches a local PDF as inline input (application/pdf). "
            "Use --task to switch between the existing single-shot flow and future OCR correction mode."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  python pdf_converter/call-gemma4-gemini.py "Summarize this PDF" '
            "--pdf-path path/to/file.pdf\n"
            "  python pdf_converter/call-gemma4-gemini.py --pdf-path path/to/file.pdf\n"
            "  python pdf_converter/call-gemma4-gemini.py --task ocr-correct --pdf-path path/to/file.pdf\n"
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
        "--task",
        default=DEFAULT_TASK,
        choices=[DEFAULT_TASK, OCR_CORRECTION_TASK],
        metavar="TASK",
        help=(
            f"Execution mode (default: {DEFAULT_TASK}). "
            f"Use {OCR_CORRECTION_TASK} for the OCR Markdown correction flow."
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
    if args.task == OCR_CORRECTION_TASK:
        return run_ocr_correction_mode(args)
    return run_single_shot_mode(args)


def run_single_shot_mode(args: argparse.Namespace) -> int:
    """Existing one-shot text/PDF flow."""
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

    api_key = get_api_key_or_exit(args.api_key_env)
    if api_key is None:
        return 1

    client = build_genai_client(api_key)

    pdf_part: types.Part | None = None
    if validated_pdf_path is not None:
        try:
            pdf_part = load_pdf_part(validated_pdf_path)
        except PdfValidationError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    contents = build_contents(user_prompt, pdf_part)

    gen_config = build_generation_config(pdf_part)
    if pdf_part is not None and OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED:
        print(
            "警告: PDF 指定時は thinking_config をオフにしています"
            "（OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED=True）。",
            file=sys.stderr,
        )

    try:
        response = generate_content_once(client, model_id, contents, gen_config)
    except Exception as exc:
        print(format_generate_content_error(exc), file=sys.stderr)
        return 1

    try:
        out_text = extract_response_text(response)
    except ResponseTextError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print(out_text)
    return 0


def run_ocr_correction_mode(_args: argparse.Namespace) -> int:
    """Dedicated entry point for the future OCR correction flow."""
    _gen_config = build_ocr_correction_generation_config()
    if _gen_config.thinking_config is not None:
        print(
            "警告: OCR Markdown 修正モードで thinking_config が有効です。"
            "この組み合わせは別途実機検証が必要です。",
            file=sys.stderr,
        )
    print(
        "エラー: OCR Markdown 修正モードはまだ未実装です。"
        "Task 0-1 では実行経路の分離のみを行います。",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
