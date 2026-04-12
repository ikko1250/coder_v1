import importlib
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

    def test_main_dispatches_to_ocr_correction_mode(self):
        module = importlib.import_module(MODULE_NAME)

        with (
            mock.patch.object(module, "parse_args", return_value=SimpleNamespace(task=module.OCR_CORRECTION_TASK)),
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


if __name__ == "__main__":
    unittest.main()
