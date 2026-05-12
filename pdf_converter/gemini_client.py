from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import httpx
from google import genai
from google.genai import errors
from google.genai import types


DEFAULT_MODEL = "gemini-3-flash-preview"
DEFAULT_GENERATE_CONTENT_TIMEOUT_MS = 300_000
MIN_HTTP_TIMEOUT_MS = 5_000
MAX_HTTP_TIMEOUT_MS = 3_600_000
OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED = False
_ENABLE_UNVERIFIED_THINKING_CONFIG_IN_OCR_CORRECTION = False


class ResponseTextError(Exception):
    """応答から利用者向けテキストを取り出せないとき。メッセージはそのまま標準エラーに出す。"""


class OcrResponseParseError(Exception):
    """OCR 修正モード用の応答パースに失敗したとき。"""


def _optional_finish_reason(name: str) -> object | None:
    """google-genai のバージョン差で列挙値が欠けても import 失敗を避けるための getattr ラッパー。"""
    return getattr(types.FinishReason, name, None)


_FINISH_REASONS_POLICY_EMPTY = frozenset(
    value
    for value in (
        _optional_finish_reason("SAFETY"),
        _optional_finish_reason("BLOCKLIST"),
        _optional_finish_reason("PROHIBITED_CONTENT"),
        _optional_finish_reason("RECITATION"),
        _optional_finish_reason("SPII"),
        _optional_finish_reason("LANGUAGE"),
        _optional_finish_reason("IMAGE_SAFETY"),
        _optional_finish_reason("IMAGE_PROHIBITED_CONTENT"),
    )
    if value is not None
)


def _enum_label(value: object) -> str:
    if value is None:
        return "不明"
    inner = getattr(value, "value", None)
    return str(inner) if inner is not None else str(value)


@dataclass
class OcrResponsePayload:
    """OCR 修正モード用の応答解析結果。"""

    text: str | None
    function_calls: list[types.FunctionCall]
    finish_reason: object | None


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


def collect_function_calls_from_response(
    response: types.GenerateContentResponse,
) -> list[types.FunctionCall]:
    """GenerateContentResponse から function_call 群を抽出する。"""
    direct_function_calls = getattr(response, "function_calls", None)
    if direct_function_calls:
        return list(direct_function_calls)

    extracted_calls: list[types.FunctionCall] = []
    for candidate in response.candidates or []:
        content = getattr(candidate, "content", None)
        if content is None:
            continue
        for part in getattr(content, "parts", []) or []:
            function_call = getattr(part, "function_call", None)
            if function_call is not None:
                extracted_calls.append(function_call)
    return extracted_calls


def extract_text_from_candidate_parts(candidate: types.Candidate) -> str | None:
    """candidate.content.parts の text を連結して返す。"""
    content = getattr(candidate, "content", None)
    if content is None:
        return None

    text_parts = []
    for part in getattr(content, "parts", []) or []:
        text_value = getattr(part, "text", None)
        if text_value:
            text_parts.append(text_value)

    if not text_parts:
        return None

    joined_text = "".join(text_parts)
    return joined_text if joined_text.strip() else None


def extract_ocr_response_payload(response: types.GenerateContentResponse) -> OcrResponsePayload:
    """OCR 修正モード用: function_call 優先で応答を解析する。"""
    feedback = response.prompt_feedback
    if feedback is not None and feedback.block_reason is not None:
        reason = _enum_label(feedback.block_reason)
        detail = (feedback.block_reason_message or "").strip()
        suffix = f" 詳細: {detail}" if detail else ""
        raise OcrResponseParseError(
            f"エラー: プロンプトがブロックされました（理由: {reason}）。{suffix}".rstrip()
        )

    candidates = response.candidates
    if not candidates:
        raise OcrResponseParseError("エラー: モデルから応答候補がありません。")

    function_calls = collect_function_calls_from_response(response)
    finish_reason = candidates[0].finish_reason
    text: str | None = extract_text_from_candidate_parts(candidates[0])

    if function_calls:
        return OcrResponsePayload(
            text=text,
            function_calls=function_calls,
            finish_reason=finish_reason,
        )

    try:
        raw_text = response.text
    except Exception:
        raw_text = None

    if raw_text is not None and raw_text.strip():
        text = raw_text

    if text is not None:
        return OcrResponsePayload(
            text=text,
            function_calls=[],
            finish_reason=finish_reason,
        )

    reason_label = _enum_label(finish_reason)
    if finish_reason in _FINISH_REASONS_POLICY_EMPTY:
        raise OcrResponseParseError(
            "エラー: 安全性ポリシー等により OCR 修正モードの応答本文がありません"
            f"（終了理由: {reason_label}）。"
        )
    raise OcrResponseParseError(
        "エラー: OCR 修正モードの応答に本文も tool call もありません"
        f"（終了理由: {reason_label}）。"
    )


def _extract_status_code(code, status) -> int | None:
    if code is not None:
        return code
    if status is not None:
        try:
            return int(status)
        except (ValueError, TypeError):
            pass
    return None


def _brief_api_error_message(exc: errors.APIError, max_len: int = 400) -> str:
    msg = (getattr(exc, "message", None) or "").strip()
    if not msg and getattr(exc, "status", None) is not None:
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
        code = _extract_status_code(getattr(exc, "code", None), getattr(exc, "status", None))
        if code is None:
            return f"エラー: API リクエストに失敗しました（{type(exc).__name__}: {exc}）。"
        if code in (401, 403):
            head = f"エラー: API キーまたは認証に失敗しました（HTTP {code}）。"
            return f"{head} {detail}".rstrip() if detail else head
        if code == 429:
            return (
                "エラー: API の利用制限（レート制限）に達しました（HTTP 429）。"
                "しばらく待ってから再試行してください。"
            )
        status_u = str(getattr(exc, "status", None) or "").upper()
        if code == 400 and "INVALID_ARGUMENT" in status_u:
            head = "エラー: リクエストが無効です（INVALID_ARGUMENT）。"
            return f"{head} {detail}".rstrip() if detail else head
        head = f"エラー: API リクエストに失敗しました（HTTP {code}）。"
        return f"{head} {detail}".rstrip() if detail else head

    if isinstance(exc, errors.ServerError):
        code = _extract_status_code(getattr(exc, "code", None), getattr(exc, "status", None))
        if code is None:
            return (
                f"エラー: API サーバー側でエラーが発生しました（{type(exc).__name__}: {exc}）。"
                "時間をおいて再試行してください。"
            )
        return (
            "エラー: API サーバー側でエラーが発生しました（HTTP "
            f"{code}）。時間をおいて再試行してください。"
        )

    if isinstance(exc, errors.APIError):
        detail = _brief_api_error_message(exc)
        code = _extract_status_code(getattr(exc, "code", None), getattr(exc, "status", None))
        if code is None:
            return f"エラー: API リクエストに失敗しました（{type(exc).__name__}: {exc}）。"
        head = f"エラー: API リクエストに失敗しました（HTTP {code}）。"
        return f"{head} {detail}".rstrip() if detail else head

    return (
        "エラー: API 呼び出し中に予期しないエラーが発生しました: "
        f"{type(exc).__name__}: {exc}"
    )


def build_generation_config(pdf_part: types.Part | None) -> types.GenerateContentConfig:
    """thinking_config の有無を Task 3-4 定数に従って決める。"""
    if pdf_part is not None and OMIT_THINKING_CONFIG_WHEN_PDF_ATTACHED:
        return types.GenerateContentConfig()
    return types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="high"),
    )


def build_genai_client(api_key: str, http_timeout_ms: int | None = None) -> genai.Client:
    """Create a Gemini API client with the HTTP timeout used by this CLI (milliseconds)."""
    timeout_ms = (
        http_timeout_ms
        if http_timeout_ms is not None
        else DEFAULT_GENERATE_CONTENT_TIMEOUT_MS
    )
    return genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(timeout=timeout_ms),
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


def http_timeout_ms_arg_type(value: str) -> int:
    """argparse 用: generate_content の HTTP タイムアウト（ミリ秒）を検証する。"""
    try:
        ms = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"整数のミリ秒が必要です: {value!r}"
        ) from exc
    if ms < MIN_HTTP_TIMEOUT_MS:
        raise argparse.ArgumentTypeError(
            f"--http-timeout-ms は {MIN_HTTP_TIMEOUT_MS} 以上（{MIN_HTTP_TIMEOUT_MS // 1000} 秒以上）にしてください。"
        )
    if ms > MAX_HTTP_TIMEOUT_MS:
        raise argparse.ArgumentTypeError(
            f"--http-timeout-ms は {MAX_HTTP_TIMEOUT_MS} 以下（{MAX_HTTP_TIMEOUT_MS // 1000 // 60} 分以下）にしてください。"
        )
    return ms
