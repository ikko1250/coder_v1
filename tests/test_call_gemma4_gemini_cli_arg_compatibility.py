import importlib
import io
import sys
import unittest
from types import SimpleNamespace
from unittest import mock


MODULE_NAME = "pdf_converter.call_gemma4_gemini"


class CallGemma4GeminiCliArgCompatibilityTests(unittest.TestCase):
    def setUp(self):
        sys.modules.pop(MODULE_NAME, None)
        importlib.invalidate_caches()

    def test_parse_args_preserves_full_cli_contract(self):
        module = importlib.import_module(MODULE_NAME)
        argv = [
            "call_gemma4_gemini.py",
            "--task",
            module.OCR_CORRECTION_TASK,
            "--pdf-path",
            "asset/texts_2nd/manual/pdf/sample.pdf",
            "--markdown-path",
            "asset/texts_2nd/manual/md/sample.md",
            "--working-dir",
            "asset/texts_2nd/manual/work",
            "--tool-call-log-path",
            "asset/texts_2nd/manual/work/logs/tool-call-log.jsonl",
            "--api-key-env",
            "CUSTOM_GEMINI_API_KEY",
            "--model",
            "custom-model-id",
            "修正してください",
        ]

        with mock.patch.object(sys, "argv", argv):
            args = module.parse_args()

        self.assertEqual(args.task, module.OCR_CORRECTION_TASK)
        self.assertEqual(args.pdf_path, "asset/texts_2nd/manual/pdf/sample.pdf")
        self.assertEqual(args.markdown_path, "asset/texts_2nd/manual/md/sample.md")
        self.assertEqual(args.working_dir, "asset/texts_2nd/manual/work")
        self.assertEqual(
            args.tool_call_log_path,
            "asset/texts_2nd/manual/work/logs/tool-call-log.jsonl",
        )
        self.assertEqual(args.api_key_env, "CUSTOM_GEMINI_API_KEY")
        self.assertEqual(args.model, "custom-model-id")
        self.assertEqual(args.prompt, "修正してください")

    def test_parse_args_preserves_default_single_shot_contract(self):
        module = importlib.import_module(MODULE_NAME)

        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py", "--pdf-path", "sample.pdf"]):
            args = module.parse_args()

        self.assertEqual(args.task, module.DEFAULT_TASK)
        self.assertEqual(args.pdf_path, "sample.pdf")
        self.assertIsNone(args.markdown_path)
        self.assertIsNone(args.working_dir)
        self.assertIsNone(args.tool_call_log_path)
        self.assertEqual(args.api_key_env, module.DEFAULT_API_KEY_ENV)
        self.assertEqual(args.model, module.DEFAULT_MODEL)

    def test_build_ocr_correction_prompt_includes_no_style_rewrite_rules(self):
        module = importlib.import_module(MODULE_NAME)

        prompt = module.build_ocr_correction_prompt(
            ocr_markdown_path=module.Path("/tmp/source.md"),
            working_markdown_path=module.Path("/tmp/work.md"),
            inline_ocr_markdown=None,
        )

        self.assertIn("スタイル書換禁止", prompt)
        self.assertIn("半角と全角の相互変換をしないこと", prompt)
        self.assertIn("括弧付き番号の字形を変換しないこと", prompt)
        self.assertIn("`new_text` は必ず `expected_old_text` をコピーして作り", prompt)

    def test_main_dispatches_to_ocr_correction_mode(self):
        module = importlib.import_module(MODULE_NAME)

        with (
            mock.patch.object(
                module,
                "parse_args",
                return_value=SimpleNamespace(
                    task=module.OCR_CORRECTION_TASK,
                    provider="gemini",
                    api_key_env=module.DEFAULT_API_KEY_ENV,
                    model=module.DEFAULT_MODEL,
                ),
            ),
            mock.patch.object(module, "run_ocr_correction_mode", return_value=23) as ocr_mock,
            mock.patch.object(
                module,
                "run_single_shot_mode",
                side_effect=AssertionError("single-shot mode should not be used for ocr-correct"),
            ),
        ):
            exit_code = module.main()

        self.assertEqual(exit_code, 23)
        ocr_mock.assert_called_once()

    def test_ocr_mode_rejects_whitespace_only_pdf_path(self):
        module = importlib.import_module(MODULE_NAME)

        argv = [
            "call_gemma4_gemini.py",
            "--task",
            module.OCR_CORRECTION_TASK,
            "--pdf-path",
            "   ",
        ]

        with mock.patch.object(sys, "argv", argv):
            args = module.parse_args()
            with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
                exit_code = module.run_ocr_correction_mode(args)

        self.assertEqual(exit_code, 1)
        self.assertIn("エラー: OCR Markdown 修正モードでは --pdf-path が必須です。", stderr.getvalue())

    def test_ocr_correction_rejects_qwen_provider(self):
        module = importlib.import_module(MODULE_NAME)

        args = module.parse_args([
            "--provider", "qwen",
            "--task", module.OCR_CORRECTION_TASK,
            "--pdf-path", "sample.pdf",
        ])
        args.effective_api_key_env = module.DEFAULT_QWEN_API_KEY_ENV
        args.effective_model = module.DEFAULT_QWEN_MODEL

        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            exit_code = module.run_ocr_correction_mode(args)

        self.assertEqual(exit_code, 1)
        self.assertIn("ocr-correct に未対応", stderr.getvalue())

    def test_ocr_correction_qwen_skips_config_build(self):
        module = importlib.import_module(MODULE_NAME)

        args = module.parse_args([
            "--provider", "qwen",
            "--task", module.OCR_CORRECTION_TASK,
            "--pdf-path", "sample.pdf",
        ])
        args.effective_api_key_env = module.DEFAULT_QWEN_API_KEY_ENV
        args.effective_model = module.DEFAULT_QWEN_MODEL

        with mock.patch.object(module, "build_ocr_correction_generation_config") as mock_build_config:
            exit_code = module.run_ocr_correction_mode(args)

        self.assertEqual(exit_code, 1)
        mock_build_config.assert_not_called()

    def test_parse_args_default_provider_is_gemini(self):
        module = importlib.import_module(MODULE_NAME)
        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py"]):
            args = module.parse_args()
        self.assertEqual(args.provider, "gemini")

    def test_parse_args_accepts_qwen_provider(self):
        module = importlib.import_module(MODULE_NAME)
        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py", "--provider", "qwen", "hello"]):
            args = module.parse_args()
        self.assertEqual(args.provider, "qwen")
        self.assertEqual(args.prompt, "hello")

    def test_parse_args_rejects_invalid_provider(self):
        module = importlib.import_module(MODULE_NAME)
        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py", "--provider", "invalid"]):
            with self.assertRaises(SystemExit):
                module.parse_args()

    def test_parse_args_accepts_qwen_base_url(self):
        module = importlib.import_module(MODULE_NAME)
        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py", "--qwen-base-url", "https://custom.example.com/v1"]):
            args = module.parse_args()
        self.assertEqual(args.qwen_base_url, "https://custom.example.com/v1")

    def test_resolve_effective_api_key_env_respects_explicit(self):
        module = importlib.import_module(MODULE_NAME)
        self.assertEqual(module.resolve_effective_api_key_env("qwen", "CUSTOM", True), "CUSTOM")
        self.assertEqual(module.resolve_effective_api_key_env("gemini", "CUSTOM", True), "CUSTOM")

    def test_resolve_effective_api_key_env_defaults_by_provider(self):
        module = importlib.import_module(MODULE_NAME)
        self.assertEqual(module.resolve_effective_api_key_env("gemini", module.DEFAULT_API_KEY_ENV, False), module.DEFAULT_API_KEY_ENV)
        self.assertEqual(module.resolve_effective_api_key_env("qwen", module.DEFAULT_API_KEY_ENV, False), module.DEFAULT_QWEN_API_KEY_ENV)

    def test_resolve_effective_model_respects_explicit(self):
        module = importlib.import_module(MODULE_NAME)
        self.assertEqual(module.resolve_effective_model("qwen", "custom", True), "custom")
        self.assertEqual(module.resolve_effective_model("gemini", "custom", True), "custom")

    def test_resolve_effective_model_defaults_by_provider(self):
        module = importlib.import_module(MODULE_NAME)
        self.assertEqual(module.resolve_effective_model("gemini", module.DEFAULT_MODEL, False), module.DEFAULT_MODEL)
        self.assertEqual(module.resolve_effective_model("qwen", module.DEFAULT_MODEL, False), module.DEFAULT_QWEN_MODEL)

    def test_parse_args_populates_qwen_effective_defaults(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["--provider", "qwen", "hello"])

        self.assertEqual(args.effective_api_key_env, module.DEFAULT_QWEN_API_KEY_ENV)
        self.assertEqual(args.effective_model, module.DEFAULT_QWEN_MODEL)

    def test_parse_args_populates_qwen_effective_explicit_values(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args([
            "--provider", "qwen",
            "--api-key-env", "CUSTOM_QWEN_KEY",
            "--model", "custom-qwen",
            "hello",
        ])

        self.assertEqual(args.effective_api_key_env, "CUSTOM_QWEN_KEY")
        self.assertEqual(args.effective_model, "custom-qwen")

    def test_help_includes_provider_defaults(self):
        module = importlib.import_module(MODULE_NAME)
        with mock.patch("sys.argv", ["call_gemma4_gemini.py", "--help"]), \
             mock.patch("sys.stdout", new_callable=io.StringIO) as stdout:
            try:
                module.parse_args()
            except SystemExit:
                pass
        help_text = stdout.getvalue()
        self.assertIn("gemini", help_text)
        self.assertIn("qwen", help_text)
        self.assertIn("DASHSCOPE_API_KEY", help_text)
        self.assertIn(module.DEFAULT_QWEN_MODEL, help_text)

    def test_single_shot_qwen_text_only(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["--provider", "qwen", "hello qwen"])

        with (
            mock.patch.object(module, "get_api_key_or_exit", return_value="test-key") as mock_get_key,
            mock.patch.object(module, "resolve_qwen_base_url", return_value="https://test.example.com"),
            mock.patch.object(
                module,
                "build_qwen_chat_request",
                return_value={"model": "test", "messages": []},
            ) as mock_build_req,
            mock.patch.object(
                module,
                "call_qwen_chat_completion",
                return_value={"choices": [{"message": {"content": "world"}}]},
            ),
            mock.patch.object(module, "extract_qwen_response_text", return_value="world"),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            exit_code = module.run_single_shot_mode(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue().strip(), "world")
        mock_get_key.assert_called_once_with(module.DEFAULT_QWEN_API_KEY_ENV)
        mock_build_req.assert_called_once_with(module.DEFAULT_QWEN_MODEL, "hello qwen")

    def test_single_shot_qwen_rejects_pdf_path(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["--provider", "qwen", "--pdf-path", "sample.pdf", "prompt"])

        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            exit_code = module.run_single_shot_mode(args)

        self.assertEqual(exit_code, 1)
        self.assertIn("--pdf-path は未対応", stderr.getvalue())

    def test_single_shot_qwen_uses_default_model(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["--provider", "qwen", "hello"])
        args.effective_api_key_env = "test-key"
        args.effective_model = module.DEFAULT_QWEN_MODEL

        with (
            mock.patch.object(module, "get_api_key_or_exit", return_value="test-key"),
            mock.patch.object(module, "resolve_qwen_base_url", return_value="https://test.example.com"),
            mock.patch.object(
                module,
                "build_qwen_chat_request",
                return_value={"model": "test", "messages": []},
            ) as mock_build_req,
            mock.patch.object(
                module,
                "call_qwen_chat_completion",
                return_value={"choices": [{"message": {"content": "hi"}}]},
            ),
            mock.patch.object(module, "extract_qwen_response_text", return_value="hi"),
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            exit_code = module.run_single_shot_mode(args)

        self.assertEqual(exit_code, 0)
        mock_build_req.assert_called_once_with(module.DEFAULT_QWEN_MODEL, "hello")

    def test_single_shot_qwen_respects_explicit_model(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["--provider", "qwen", "--model", "custom-qwen", "hello"])
        args.effective_api_key_env = "test-key"
        args.effective_model = "custom-qwen"

        with (
            mock.patch.object(module, "get_api_key_or_exit", return_value="test-key"),
            mock.patch.object(module, "resolve_qwen_base_url", return_value="https://test.example.com"),
            mock.patch.object(
                module,
                "build_qwen_chat_request",
                return_value={"model": "test", "messages": []},
            ) as mock_build_req,
            mock.patch.object(
                module,
                "call_qwen_chat_completion",
                return_value={"choices": [{"message": {"content": "hi"}}]},
            ),
            mock.patch.object(module, "extract_qwen_response_text", return_value="hi"),
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            exit_code = module.run_single_shot_mode(args)

        self.assertEqual(exit_code, 0)
        mock_build_req.assert_called_once_with("custom-qwen", "hello")

    def test_single_shot_gemini_unchanged(self):
        module = importlib.import_module(MODULE_NAME)
        args = module.parse_args(["hello"])
        args.effective_api_key_env = module.DEFAULT_API_KEY_ENV
        args.effective_model = module.DEFAULT_MODEL

        mock_response = mock.MagicMock()
        with (
            mock.patch.object(module, "get_api_key_or_exit", return_value="gemini-key") as mock_get_key,
            mock.patch.object(module, "build_genai_client") as mock_build_client,
            mock.patch.object(
                module,
                "generate_content_once",
                return_value=mock_response,
            ) as mock_generate,
            mock.patch.object(module, "extract_response_text", return_value="hi gemini"),
            mock.patch("sys.stdout", new_callable=io.StringIO) as stdout,
        ):
            exit_code = module.run_single_shot_mode(args)

        self.assertEqual(exit_code, 0)
        self.assertEqual(stdout.getvalue().strip(), "hi gemini")
        mock_get_key.assert_called_once_with(module.DEFAULT_API_KEY_ENV)
        mock_build_client.assert_called_once_with("gemini-key", args.http_timeout_ms)
        mock_generate.assert_called_once()


class FormatGenerateContentErrorTests(unittest.TestCase):
    def setUp(self):
        sys.modules.pop(MODULE_NAME, None)
        importlib.invalidate_caches()

    def _import_module(self):
        return importlib.import_module(MODULE_NAME)

    def test_fake_exception_with_only_message(self):
        module = self._import_module()

        class FakeClientError(module.errors.ClientError):
            def __init__(self):
                self.message = "something went wrong"

            def __str__(self):
                return "something went wrong"

        exc = FakeClientError()
        result = module.format_generate_content_error(exc)
        self.assertIn("something went wrong", result)

    def test_fake_exception_with_status_but_no_code_produces_rate_limit_message(self):
        module = self._import_module()

        class FakeClientError(module.errors.ClientError):
            def __init__(self):
                self.status = "429"

            def __str__(self):
                return "rate limited"

        exc = FakeClientError()
        result = module.format_generate_content_error(exc)
        self.assertIn("レート制限", result)
        self.assertIn("HTTP 429", result)

    def test_fake_exception_with_no_attributes_does_not_raise(self):
        module = self._import_module()

        class FakeClientError(module.errors.ClientError):
            def __init__(self):
                pass

            def __str__(self):
                return "no-attrs"

        exc = FakeClientError()
        try:
            result = module.format_generate_content_error(exc)
        except AttributeError as e:
            self.fail(f"AttributeError raised: {e}")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)
        self.assertIn("FakeClientError", result)


if __name__ == "__main__":
    unittest.main()
