from __future__ import annotations

"""
Gemini API 経由で生成モデルを呼び出す CLI。既定モデルは gemini-3.1-flash-lite-preview（--model で上書き可）。

既定モデルの選定経緯は document/logs/2026-04-20-gemini25-flashlite-tool-call-verification.md 参照。
公式（モデル一覧）: https://ai.google.dev/gemini-api/docs/models
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from google import genai
from google.genai import errors
from google.genai import types
from pdf_converter.gemini_client import (
    DEFAULT_GENERATE_CONTENT_TIMEOUT_MS,
    DEFAULT_MODEL,
    MAX_HTTP_TIMEOUT_MS,
    MIN_HTTP_TIMEOUT_MS,
    OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED,
    OcrResponsePayload,
    OcrResponseParseError,
    ResponseTextError,
    build_genai_client,
    build_generation_config,
    extract_response_text,
    format_generate_content_error,
    generate_content_once,
    http_timeout_ms_arg_type,
)
from pdf_converter.ocr_output import (
    OcrDiffError,
    build_unified_diff_text,
    emit_ocr_correction_stdout,
)
from pdf_converter.pdf_input import (
    PdfValidationError,
    load_pdf_part,
    validate_pdf_path,
)
from pdf_converter.tool_call_logger import ToolCallLogger
from pdf_converter.ocr_paths import (
    MarkdownResolutionError,
    WorkingDirectoryError,
    WorkingMarkdownError,
    copy_markdown_to_working_directory,
    find_auto_matched_markdown_candidates,
    get_manual_markdown_dirs,
    get_manual_root_candidates,
    resolve_ocr_markdown_path,
    resolve_working_directory,
    select_latest_auto_matched_markdown_candidate,
)
from pdf_converter.ocr_tools import (
    DEFAULT_MAX_TOOL_CALLS_PER_RUN,
    MAX_TOOL_CALLS_CAP,
    MIN_MAX_TOOL_CALLS_PER_RUN,
    ToolCallBudget,
    ToolCallLimitError,
    ToolReadError,
    ToolWriteError,
    WRITE_LOCK_POLL_INTERVAL_SECONDS,
    WRITE_LOCK_TIMEOUT_SECONDS,
    acquire_write_lock,
    acquire_unix_write_lock,
    acquire_windows_write_lock,
    build_write_lock_path,
    is_windows_lock_contention_error,
    max_tool_calls_arg_type,
    read_tool_text,
    write_tool_text,
)
from pdf_converter.ocr_correction import (
    OCR_CORRECTION_TASK,
    OcrFinalizationError,
    OcrToolExecutionError,
    build_ocr_correction_contents,
    build_ocr_correction_final_message,
    build_ocr_correction_generation_config,
    build_ocr_correction_prompt,
    build_ocr_correction_tools,
    run_ocr_correction_turn_loop,
)
from pdf_converter.project_paths import (
    resolve_dotenv_path,
)


def load_dotenv(dotenv_path: str | Path) -> None:
    dotenv_file_path = Path(dotenv_path)
    if not dotenv_file_path.exists():
        return
    
    with dotenv_file_path.open(encoding="utf-8") as dotenv_file:
        for raw_line in dotenv_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            os.environ.setdefault(key, value)


DEFAULT_API_KEY_ENV = "GEMINI_API_KEY"
DEFAULT_PROMPT_TEXT_ONLY = "水の化学式は何ですか？簡潔に答えてください。"
DEFAULT_PROMPT_WITH_PDF = "この PDF の内容を要約してください。"
DEFAULT_TASK = "single-shot"
DEFAULT_DOTENV_PATH: Path | None = None


@dataclass
class OcrCorrectionRequest:
    """Inputs for the OCR correction flow (prepared by CLI layer)."""
    pdf_path: Path
    ocr_markdown_path: Path
    working_markdown_path: Path
    model_id: str
    gen_config: types.GenerateContentConfig
    budget: ToolCallBudget
    tool_call_logger: ToolCallLogger | None = None


def get_default_dotenv_path() -> Path:
    if DEFAULT_DOTENV_PATH is not None:
        return DEFAULT_DOTENV_PATH
    return resolve_dotenv_path()


def format_ocr_correction_error(exc: BaseException) -> str:
    """OCR 修正モード向けに失敗カテゴリを補った CLI エラーメッセージへ整形する。"""
    if isinstance(exc, MarkdownResolutionError):
        return f"OCR 修正モードエラー: 入力 Markdown の解決に失敗しました。{exc}"
    if isinstance(exc, WorkingDirectoryError):
        return f"OCR 修正モードエラー: work ディレクトリの準備に失敗しました。{exc}"
    if isinstance(exc, WorkingMarkdownError):
        return f"OCR 修正モードエラー: 編集対象 Markdown の作成に失敗しました。{exc}"
    if isinstance(exc, ToolReadError):
        return f"OCR 修正モードエラー: read tool の実行に失敗しました。{exc}"
    if isinstance(exc, ToolWriteError):
        return f"OCR 修正モードエラー: write tool の実行に失敗しました。{exc}"
    if isinstance(exc, ToolCallLimitError):
        return f"OCR 修正モードエラー: tool 呼び出し上限に達しました。{exc}"
    if isinstance(exc, OcrToolExecutionError):
        return f"OCR 修正モードエラー: tool 実行ループで失敗しました。{exc}"
    if isinstance(exc, OcrResponseParseError):
        return f"OCR 修正モードエラー: モデル応答の解析に失敗しました。{exc}"
    if isinstance(exc, OcrFinalizationError):
        return f"OCR 修正モードエラー: 最終結果の確定に失敗しました。{exc}"
    if isinstance(exc, OcrDiffError):
        return f"OCR 修正モードエラー: diff 生成に失敗しました。{exc}"
    if isinstance(exc, PdfValidationError):
        return f"OCR 修正モードエラー: PDF の検証に失敗しました。{exc}"
    return format_generate_content_error(exc)



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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Call Gemini API (default model: gemini-3.1-flash-lite-preview; single-shot uses "
            "thinking_level=high when thinking_config is enabled). "
            "Optional --pdf-path attaches a local PDF as inline input (application/pdf). "
            "Use --task to switch between the existing single-shot flow and future OCR correction mode. "
            "OCR correction mode can also take --markdown-path to override auto matching "
            "and --working-dir to override the default work directory. "
            "OCR correction mode only reads Markdown from asset/ocr_manual/md; "
            "pdf_converter.py output/ is not auto-connected."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            '  uv run call-gemma4-gemini "Summarize this PDF" '
            "--pdf-path path/to/file.pdf\n"
            "  uv run call-gemma4-gemini --pdf-path path/to/file.pdf\n"
            "  uv run call-gemma4-gemini --task ocr-correct --pdf-path path/to/file.pdf\n"
            "  uv run call-gemma4-gemini --task ocr-correct --pdf-path path/to/file.pdf "
            "--markdown-path path/to/file.md\n"
            "  uv run call-gemma4-gemini --task ocr-correct --working-dir asset/ocr_manual/work\n"
            "\n"
            "If --pdf-path is omitted, only the text prompt is sent (no PDF).\n"
            "OCR correction mode does not automatically import Markdown from pdf_converter.py output/.\n"
            "Legacy direct execution via python pdf_converter/call-gemma4-gemini.py ... remains a compatibility shim."
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
        "--markdown-path",
        default=None,
        metavar="PATH",
        dest="markdown_path",
        help=(
            "Optional path to an OCR Markdown file. In OCR correction mode, an explicit value "
            "overrides PDF-based auto matching. The file must live under asset/texts_2nd/manual/md; "
            "pdf_converter.py output/ is not auto-connected."
        ),
    )
    parser.add_argument(
        "--working-dir",
        default=None,
        metavar="PATH",
        dest="working_dir",
        help=(
            "Optional work directory for OCR correction mode. When omitted, "
            "the default is asset/ocr_manual/work. This is independent from pdf_converter.py output/."
        ),
    )
    parser.add_argument(
        "--tool-call-log-path",
        default=None,
        metavar="PATH",
        dest="tool_call_log_path",
        help=(
            "Optional path to a JSONL file used in OCR correction mode to append "
            "tool-call request/execution logs for each turn."
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
    parser.add_argument(
        "--http-timeout-ms",
        default=DEFAULT_GENERATE_CONTENT_TIMEOUT_MS,
        type=http_timeout_ms_arg_type,
        metavar="MS",
        dest="http_timeout_ms",
        help=(
            "HTTP timeout in milliseconds for each generate_content request "
            f"(default: {DEFAULT_GENERATE_CONTENT_TIMEOUT_MS}). "
            f"Allowed range: {MIN_HTTP_TIMEOUT_MS}–{MAX_HTTP_TIMEOUT_MS}."
        ),
    )
    parser.add_argument(
        "--max-tool-calls",
        default=DEFAULT_MAX_TOOL_CALLS_PER_RUN,
        type=max_tool_calls_arg_type,
        metavar="N",
        dest="max_tool_calls",
        help=(
            "OCR correction mode: max total read+write tool executions per run "
            f"(default: {DEFAULT_MAX_TOOL_CALLS_PER_RUN}, "
            f"allowed: {MIN_MAX_TOOL_CALLS_PER_RUN}–{MAX_TOOL_CALLS_CAP})."
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
    load_dotenv(get_default_dotenv_path())

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

    client = build_genai_client(api_key, args.http_timeout_ms)

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

    # P1 follow-up: if out_text already ends with \n, print adds another.
    # Preserved for backward compatibility; do not switch to sys.stdout.write
    # without confirming downstream consumers.
    print(out_text)
    return 0


def _execute_ocr_correction(
    request: OcrCorrectionRequest,
    client,
    initial_working_text: str,
) -> tuple[str, str]:
    """Returns (final_message, diff_text)."""
    ocr_contents = build_ocr_correction_contents(
        pdf_path=request.pdf_path,
        ocr_markdown_path=request.ocr_markdown_path,
        working_markdown_path=request.working_markdown_path,
    )
    payload = run_ocr_correction_turn_loop(
        client=client,
        model_id=request.model_id,
        initial_contents=ocr_contents,
        config=request.gen_config,
        budget=request.budget,
        tool_call_logger=request.tool_call_logger,
    )
    final_message = build_ocr_correction_final_message(
        payload=payload,
        working_markdown_path=request.working_markdown_path,
        initial_working_text=initial_working_text,
        budget=request.budget,
    )
    diff_text = build_unified_diff_text(
        original_path=request.ocr_markdown_path,
        working_path=request.working_markdown_path,
    )
    return final_message, diff_text


def run_ocr_correction_mode(args: argparse.Namespace) -> int:
    """Dedicated entry point for the future OCR correction flow."""
    load_dotenv(get_default_dotenv_path())

    gen_config = build_ocr_correction_generation_config()
    if gen_config.thinking_config is not None:
        print(
            "警告: OCR Markdown 修正モードで thinking_config が有効です。"
            "この組み合わせは別途実機検証が必要です。",
            file=sys.stderr,
        )
    model_id = (args.model or "").strip() or DEFAULT_MODEL
    pdf_path = (args.pdf_path or "").strip() or None
    if not pdf_path:
        print("エラー: OCR Markdown 修正モードでは --pdf-path が必須です。", file=sys.stderr)
        return 1
    validated_pdf_path: Path | None = None
    try:
        validated_pdf_path = validate_pdf_path(pdf_path)
    except PdfValidationError as exc:
        print(format_ocr_correction_error(exc), file=sys.stderr)
        return 1
    resolved_markdown_path: Path | None = None
    try:
        resolved_markdown_path = resolve_ocr_markdown_path(
            pdf_path=validated_pdf_path,
            markdown_path=args.markdown_path,
        )
    except MarkdownResolutionError as exc:
        print(format_ocr_correction_error(exc), file=sys.stderr)
        return 1
    try:
        resolved_working_dir = resolve_working_directory(args.working_dir)
    except WorkingDirectoryError as exc:
        print(format_ocr_correction_error(exc), file=sys.stderr)
        return 1
    working_markdown_path: Path | None = None
    if resolved_markdown_path is not None:
        try:
            working_markdown_path = copy_markdown_to_working_directory(
                source_markdown_path=resolved_markdown_path,
                working_dir=resolved_working_dir,
            )
        except WorkingMarkdownError as exc:
            print(format_ocr_correction_error(exc), file=sys.stderr)
            return 1
    if resolved_markdown_path is None or working_markdown_path is None or validated_pdf_path is None:
        print("エラー: OCR 修正モードの入力解決に失敗しました。", file=sys.stderr)
        return 1
    try:
        initial_working_text = working_markdown_path.read_text(encoding="utf-8")
    except OSError as exc:
        print(
            f"エラー: 編集対象 Markdown の初期読取に失敗しました: {working_markdown_path}: {exc}",
            file=sys.stderr,
        )
        return 1
    api_key = get_api_key_or_exit(args.api_key_env)
    if api_key is None:
        return 1

    client = build_genai_client(api_key, args.http_timeout_ms)
    budget = ToolCallBudget(args.max_tool_calls)
    tool_call_logger: ToolCallLogger | None = None
    if args.tool_call_log_path:
        tool_call_logger = ToolCallLogger(Path(args.tool_call_log_path))

    request = OcrCorrectionRequest(
        pdf_path=validated_pdf_path,
        ocr_markdown_path=resolved_markdown_path,
        working_markdown_path=working_markdown_path,
        model_id=model_id,
        gen_config=gen_config,
        budget=budget,
        tool_call_logger=tool_call_logger,
    )

    try:
        final_message, diff_text = _execute_ocr_correction(request, client, initial_working_text)
    except (OcrResponseParseError, OcrToolExecutionError, ToolCallLimitError) as exc:
        print(format_ocr_correction_error(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(format_ocr_correction_error(exc), file=sys.stderr)
        return 1

    # P1 follow-up: emit_ocr_correction_stdout uses print(...), which adds an
    # extra newline if the formatted string already ends with \n. Preserved for
    # backward compatibility.
    emit_ocr_correction_stdout(final_message, diff_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
