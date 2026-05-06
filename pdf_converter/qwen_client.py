from __future__ import annotations

import json
import os
from typing import Any

import httpx

DEFAULT_QWEN_MODEL = "qwen3.6-plus"
DEFAULT_QWEN_API_KEY_ENV = "DASHSCOPE_API_KEY"
DEFAULT_QWEN_BASE_URL = "https://dashscope-intl.aliyuncs.com/compatible-mode/v1"
QWEN_BASE_URL_ENV = "CSV_VIEWER_QWEN_BASE_URL"


def resolve_qwen_base_url(explicit_base_url: str | None) -> str:
    """Qwen API のベース URL を解決する。"""
    if explicit_base_url:
        return explicit_base_url
    env_url = os.environ.get(QWEN_BASE_URL_ENV)
    if env_url:
        return env_url
    return DEFAULT_QWEN_BASE_URL


def build_qwen_chat_request(model_id: str, prompt: str) -> dict[str, Any]:
    """Qwen Chat API 用のリクエストボディを構築する。"""
    return {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
    }


def call_qwen_chat_completion(
    api_key: str,
    base_url: str,
    payload: dict[str, Any],
    timeout_ms: int,
) -> dict[str, Any]:
    """Qwen Chat Completion API を呼び出す。"""
    url = base_url.rstrip("/") + "/chat/completions"
    timeout_seconds = timeout_ms / 1000.0
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    response = httpx.post(
        url,
        json=payload,
        headers=headers,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()


def extract_qwen_response_text(response_json: dict[str, Any]) -> str:
    """Qwen Chat API の応答 JSON からテキストを抽出する。"""
    if not isinstance(response_json, dict):
        raise ValueError("Qwen response JSON is not a dict")

    choices = response_json.get("choices")
    if not isinstance(choices, list) or len(choices) == 0:
        raise ValueError("Qwen response choices is empty or not a list")

    choice = choices[0]
    if not isinstance(choice, dict):
        raise ValueError("Qwen response choice[0] is not a dict")

    message = choice.get("message")
    if message is None or not isinstance(message, dict):
        raise ValueError("Qwen response message is missing or not a dict")

    content = message.get("content")
    if content is None:
        raise ValueError("Qwen response content is missing or None")
    if not isinstance(content, str):
        raise ValueError("Qwen response content is not a str")
    if not content.strip():
        raise ValueError("Qwen response content is empty")

    finish_reason = choice.get("finish_reason")
    if finish_reason == "length":
        raise ValueError("Qwen response was truncated (finish_reason='length').")
    if finish_reason == "content_filter":
        raise ValueError("Qwen response blocked by content filter (finish_reason='content_filter').")

    return content


def format_qwen_api_error(exc: BaseException) -> str:
    """Qwen API 呼び出しでの例外を CLI 向けメッセージに変換する。"""
    if isinstance(exc, httpx.TimeoutException):
        return "エラー: API リクエストがタイムアウトしました。"

    if isinstance(exc, httpx.RequestError):
        return f"エラー: API への通信に失敗しました（{type(exc).__name__}）。"

    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        body_preview = exc.response.text[:200]
        if code in (401, 403):
            return (
                f"エラー: API キーまたは認証に失敗しました（HTTP {code}）。"
                f" {body_preview}".rstrip()
            )
        if code == 429:
            return (
                "エラー: API の利用制限（レート制限）に達しました（HTTP 429）。"
                "しばらく待ってから再試行してください。"
            )
        if code == 400:
            return (
                f"エラー: リクエストが無効です（HTTP {code}）。"
                f" {body_preview}".rstrip()
            )
        if 500 <= code < 600:
            return (
                f"エラー: API サーバー側でエラーが発生しました（HTTP {code}）。"
                "時間をおいて再試行してください。"
            )
        return (
            f"エラー: API リクエストに失敗しました（HTTP {code}）。"
            f" {body_preview}".rstrip()
        )

    if isinstance(exc, json.JSONDecodeError):
        return "エラー: API 応答の JSON 解析に失敗しました。"

    if isinstance(exc, ValueError):
        return f"エラー: {exc}"

    return (
        "エラー: API 呼び出し中に予期しないエラーが発生しました: "
        f"{type(exc).__name__}: {exc}"
    )
