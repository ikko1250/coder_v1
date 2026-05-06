import json
import os
import unittest
from unittest.mock import MagicMock, patch

import httpx

from pdf_converter.qwen_client import (
    DEFAULT_QWEN_BASE_URL,
    QWEN_BASE_URL_ENV,
    build_qwen_chat_request,
    call_qwen_chat_completion,
    extract_qwen_response_text,
    format_qwen_api_error,
    resolve_qwen_base_url,
)


class ResolveQwenBaseUrlTests(unittest.TestCase):
    def test_explicit_base_url_takes_precedence(self):
        result = resolve_qwen_base_url("https://explicit.example.com/v1")
        self.assertEqual(result, "https://explicit.example.com/v1")

    @patch.dict(os.environ, {QWEN_BASE_URL_ENV: "https://env.example.com/v1"}, clear=False)
    def test_env_var_is_used_when_explicit_is_none(self):
        result = resolve_qwen_base_url(None)
        self.assertEqual(result, "https://env.example.com/v1")

    @patch.dict(os.environ, {QWEN_BASE_URL_ENV: ""}, clear=False)
    def test_env_var_empty_string_ignored(self):
        # empty string is falsy, so default should be used
        result = resolve_qwen_base_url(None)
        self.assertEqual(result, DEFAULT_QWEN_BASE_URL)

    def test_default_is_used_when_nothing_set(self):
        # Ensure env var is not set
        env_backup = os.environ.pop(QWEN_BASE_URL_ENV, None)
        try:
            result = resolve_qwen_base_url(None)
            self.assertEqual(result, DEFAULT_QWEN_BASE_URL)
        finally:
            if env_backup is not None:
                os.environ[QWEN_BASE_URL_ENV] = env_backup

    @patch.dict(os.environ, {QWEN_BASE_URL_ENV: "https://env.example.com/v1"}, clear=False)
    def test_explicit_takes_precedence_over_env(self):
        result = resolve_qwen_base_url("https://explicit.example.com/v1")
        self.assertEqual(result, "https://explicit.example.com/v1")


class BuildQwenChatRequestTests(unittest.TestCase):
    def test_request_shape(self):
        result = build_qwen_chat_request("qwen3.6-plus", "Hello, world!")
        self.assertEqual(result, {
            "model": "qwen3.6-plus",
            "messages": [{"role": "user", "content": "Hello, world!"}],
        })


class CallQwenChatCompletionTests(unittest.TestCase):
    @patch("pdf_converter.qwen_client.httpx.post")
    def test_timeout_conversion(self, mock_post: MagicMock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"ok": True}
        mock_post.return_value = mock_response

        call_qwen_chat_completion(
            api_key="test-key",
            base_url="https://example.com",
            payload={"model": "test"},
            timeout_ms=5000,
        )

        _, kwargs = mock_post.call_args
        self.assertEqual(kwargs["timeout"], 5.0)

    @patch("pdf_converter.qwen_client.httpx.post")
    def test_successful_response(self, mock_post: MagicMock):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"choices": [{"message": {"content": "Hi"}}]}
        mock_post.return_value = mock_response

        result = call_qwen_chat_completion(
            api_key="test-key",
            base_url="https://example.com",
            payload={"model": "test"},
            timeout_ms=30000,
        )

        self.assertEqual(result, {"choices": [{"message": {"content": "Hi"}}]})
        mock_response.raise_for_status.assert_called_once()

    @patch("pdf_converter.qwen_client.httpx.post")
    def test_http_error_raised_before_json(self, mock_post: MagicMock):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.text = "not found"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Not Found",
            request=mock_request,
            response=mock_response,
        )
        mock_post.return_value = mock_response

        with self.assertRaises(httpx.HTTPStatusError):
            call_qwen_chat_completion(
                api_key="test-key",
                base_url="https://example.com",
                payload={"model": "test"},
                timeout_ms=30000,
            )

    @patch("pdf_converter.qwen_client.httpx.post")
    def test_timeout_exception_propagates(self, mock_post: MagicMock):
        mock_post.side_effect = httpx.TimeoutException("Request timed out")

        with self.assertRaises(httpx.TimeoutException):
            call_qwen_chat_completion(
                api_key="test-key",
                base_url="https://example.com",
                payload={"model": "test"},
                timeout_ms=30000,
            )

    @patch("pdf_converter.qwen_client.httpx.post")
    def test_request_error_propagates(self, mock_post: MagicMock):
        mock_post.side_effect = httpx.RequestError("Connection failed")

        with self.assertRaises(httpx.RequestError):
            call_qwen_chat_completion(
                api_key="test-key",
                base_url="https://example.com",
                payload={"model": "test"},
                timeout_ms=30000,
            )


class ExtractQwenResponseTextTests(unittest.TestCase):
    def test_success(self):
        response = {
            "choices": [
                {
                    "message": {"content": "Hello"},
                    "finish_reason": "stop",
                }
            ]
        }
        self.assertEqual(extract_qwen_response_text(response), "Hello")

    def test_not_a_dict(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text("not a dict")

    def test_choices_empty(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": []})

    def test_choices_missing(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({})

    def test_choice_not_a_dict(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": ["not a dict"]})

    def test_message_missing(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": [{}]})

    def test_message_not_a_dict(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": [{"message": "not a dict"}]})

    def test_content_none(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": [{"message": {"content": None}}]})

    def test_content_empty_string(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": [{"message": {"content": ""}}]})

    def test_content_whitespace_only(self):
        with self.assertRaises(ValueError):
            extract_qwen_response_text({"choices": [{"message": {"content": " \n\t "}}]})

    def test_finish_reason_length(self):
        with self.assertRaises(ValueError) as ctx:
            extract_qwen_response_text({
                "choices": [
                    {
                        "message": {"content": "truncated"},
                        "finish_reason": "length",
                    }
                ]
            })
        self.assertIn("truncated", str(ctx.exception))

    def test_finish_reason_content_filter(self):
        with self.assertRaises(ValueError) as ctx:
            extract_qwen_response_text({
                "choices": [
                    {
                        "message": {"content": "blocked"},
                        "finish_reason": "content_filter",
                    }
                ]
            })
        self.assertIn("blocked by content filter", str(ctx.exception))


class FormatQwenApiErrorTests(unittest.TestCase):
    def test_timeout_exception(self):
        exc = httpx.TimeoutException("Request timed out")
        result = format_qwen_api_error(exc)
        self.assertEqual(result, "エラー: API リクエストがタイムアウトしました。")

    def test_request_error(self):
        exc = httpx.RequestError("Connection failed")
        result = format_qwen_api_error(exc)
        self.assertEqual(result, "エラー: API への通信に失敗しました（RequestError）。")

    def test_http_status_error_401(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        exc = httpx.HTTPStatusError(
            "Unauthorized",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("401", result)
        self.assertIn("認証に失敗", result)
        self.assertIn("Unauthorized", result)

    def test_http_status_error_403(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        exc = httpx.HTTPStatusError(
            "Forbidden",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("403", result)
        self.assertIn("認証に失敗", result)

    def test_http_status_error_429(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        exc = httpx.HTTPStatusError(
            "Too Many Requests",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("429", result)
        self.assertIn("レート制限", result)
        self.assertNotIn("Too Many Requests", result)

    def test_http_status_error_400(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        exc = httpx.HTTPStatusError(
            "Bad Request",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("400", result)
        self.assertIn("無効", result)
        self.assertIn("Bad Request", result)

    def test_http_status_error_500(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        exc = httpx.HTTPStatusError(
            "Server Error",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("500", result)
        self.assertIn("サーバー側でエラー", result)
        self.assertNotIn("Internal Server Error", result)

    def test_http_status_error_other(self):
        mock_request = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 418
        mock_response.text = "I'm a teapot"
        exc = httpx.HTTPStatusError(
            "I'm a teapot",
            request=mock_request,
            response=mock_response,
        )
        result = format_qwen_api_error(exc)
        self.assertIn("418", result)
        self.assertIn("I'm a teapot", result)

    def test_json_decode_error(self):
        exc = json.JSONDecodeError("Expecting value", "doc", 0)
        result = format_qwen_api_error(exc)
        self.assertEqual(result, "エラー: API 応答の JSON 解析に失敗しました。")

    def test_value_error(self):
        exc = ValueError("Something went wrong")
        result = format_qwen_api_error(exc)
        self.assertEqual(result, "エラー: Something went wrong")

    def test_unexpected_error(self):
        exc = RuntimeError("Unexpected")
        result = format_qwen_api_error(exc)
        self.assertIn("予期しないエラー", result)
        self.assertIn("RuntimeError", result)
        self.assertIn("Unexpected", result)


if __name__ == "__main__":
    unittest.main()
