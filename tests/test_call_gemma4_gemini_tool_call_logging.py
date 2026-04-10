import importlib.util
import json
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from google.genai import types

from pdf_converter.tool_call_logger import ToolCallLogger


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call-gemma4-gemini.py"


def load_target_module():
    module_name = "call_gemma4_gemini_tool_call_logging_test_module"
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def build_fake_response(model_content: types.Content):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=model_content)],
        prompt_feedback=None,
    )


class ToolCallLoggingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_target_module()

    def setUp(self):
        self.temp_root = REPO_ROOT / ".tmp_tool_call_logging_test" / self._testMethodName
        if self.temp_root.exists():
            shutil.rmtree(self.temp_root, ignore_errors=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_run_turn_loop_writes_jsonl_events(self):
        log_path = self.temp_root / "tool-calls.jsonl"
        logger = ToolCallLogger(log_path)
        first_response = build_fake_response(types.Content(role="model", parts=[]))
        final_response = build_fake_response(types.Content(role="model", parts=[]))
        first_payload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[types.FunctionCall(name="read_markdown_file", args={"path": "md/source.md"})],
            finish_reason=None,
        )
        final_payload = self.module.OcrResponsePayload(
            text="done",
            function_calls=[],
            finish_reason="STOP",
        )

        with (
            mock.patch.object(self.module, "generate_content_once", side_effect=[first_response, final_response]),
            mock.patch.object(self.module, "extract_ocr_response_payload", side_effect=[first_payload, final_payload]),
            mock.patch.object(
                self.module,
                "execute_ocr_function_call",
                return_value=types.Part.from_function_response(
                    name="read_markdown_file",
                    response={"result": {"path": "md/source.md", "content": "# source"}},
                ),
            ),
        ):
            result = self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=4),
                tool_call_logger=logger,
            )

        self.assertEqual(result.text, "done")
        lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 2)
        first_event = json.loads(lines[0])
        second_event = json.loads(lines[1])
        self.assertEqual(first_event["phase"], "requested")
        self.assertEqual(first_event["tool_name"], "read_markdown_file")
        self.assertEqual(second_event["phase"], "executed")
        self.assertEqual(second_event["status"], "ok")

    def test_parse_args_accepts_tool_call_log_path(self):
        argv = [
            "call-gemma4-gemini.py",
            "--task",
            self.module.OCR_CORRECTION_TASK,
            "--pdf-path",
            "sample.pdf",
            "--tool-call-log-path",
            "logs/tool-calls.jsonl",
        ]

        with mock.patch.object(sys, "argv", argv):
            args = self.module.parse_args()

        self.assertEqual(args.tool_call_log_path, "logs/tool-calls.jsonl")


if __name__ == "__main__":
    unittest.main()
