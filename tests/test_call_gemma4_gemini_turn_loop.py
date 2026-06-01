import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from google.genai import types

from pdf_converter import ocr_correction
import pdf_converter.ocr_paths as ocr_paths_module


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call_gemma4_gemini.py"


def loadTargetModule():
    moduleName = "call_gemma4_gemini_turn_loop_test_module"
    spec = importlib.util.spec_from_file_location(moduleName, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[moduleName] = module
    spec.loader.exec_module(module)
    return module


def buildFakeResponse(modelContent: types.Content):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=modelContent)],
        prompt_feedback=None,
    )


class OcrTurnLoopTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = loadTargetModule()

    def setUp(self):
        testRoot = REPO_ROOT / ".tmp_turn_loop_test"
        testRoot.mkdir(exist_ok=True)
        self.tempRoot = testRoot / self._testMethodName
        if self.tempRoot.exists():
            shutil.rmtree(self.tempRoot, ignore_errors=True)
        os.makedirs(self.tempRoot, exist_ok=True)

        self.manualRoot = self.tempRoot / "manual"
        self.pdfDir = self.manualRoot / "pdf"
        self.markdownDir = self.manualRoot / "md"
        self.workDir = self.manualRoot / "work"
        self.outputDir = self.tempRoot / "output"

        self.pdfDir.mkdir(parents=True)
        self.markdownDir.mkdir(parents=True)
        self.workDir.mkdir(parents=True)
        self.outputDir.mkdir(parents=True)

        self.originalManualRoot = ocr_paths_module.DEFAULT_MANUAL_ROOT
        self.originalManualPdfDir = ocr_paths_module.DEFAULT_MANUAL_PDF_DIR
        self.originalManualMarkdownDir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        self.originalManualWorkDir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        self.originalOcrOutputDir = ocr_paths_module.DEFAULT_OCR_OUTPUT_DIR

        ocr_paths_module.DEFAULT_MANUAL_ROOT = self.manualRoot
        ocr_paths_module.DEFAULT_MANUAL_PDF_DIR = self.pdfDir
        ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = self.markdownDir
        ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = self.workDir
        ocr_paths_module.DEFAULT_OCR_OUTPUT_DIR = self.outputDir

    def tearDown(self):
        ocr_paths_module.DEFAULT_MANUAL_ROOT = self.originalManualRoot
        ocr_paths_module.DEFAULT_MANUAL_PDF_DIR = self.originalManualPdfDir
        ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = self.originalManualMarkdownDir
        ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = self.originalManualWorkDir
        ocr_paths_module.DEFAULT_OCR_OUTPUT_DIR = self.originalOcrOutputDir
        shutil.rmtree(self.tempRoot, ignore_errors=True)

    def writeMarkdown(self, directory: Path, name: str, body: str) -> Path:
        path = directory / name
        path.write_text(body, encoding="utf-8", newline="\n")
        return path

    def testRunTurnLoopHandlesReadOnlyFunctionCallThenReturnsFinalPayload(self):
        self.writeMarkdown(self.markdownDir, "source.md", "# source\n")
        firstResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        finalResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        firstPayload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[types.FunctionCall(name="read_markdown_file", args={"path": "md/source.md"})],
            finish_reason=None,
        )
        finalPayload = self.module.OcrResponsePayload(
            text="finished",
            function_calls=[],
            finish_reason="STOP",
        )

        with (
            mock.patch(
                "pdf_converter.ocr_correction.generate_content_once",
                side_effect=[firstResponse, finalResponse],
            ) as generateMock,
            mock.patch(
                "pdf_converter.ocr_correction.extract_ocr_response_payload",
                side_effect=[firstPayload, finalPayload],
            ),
        ):
            result = self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=4),
            )

        self.assertEqual(result.text, "finished")
        self.assertEqual(generateMock.call_count, 2)

    def testRunTurnLoopWritesOnceAndThenReturnsFinalPayload(self):
        workingPath = self.writeMarkdown(self.workDir, "working.md", "before\n")
        firstResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        finalResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        firstPayload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[
                types.FunctionCall(
                    name="write_markdown_file",
                    args={
                        "path": "work/working.md",
                        "expected_old_text": "before\n",
                        "new_text": "after\n",
                    },
                )
            ],
            finish_reason=None,
        )
        finalPayload = self.module.OcrResponsePayload(
            text="write done",
            function_calls=[],
            finish_reason="STOP",
        )

        with (
            mock.patch(
                "pdf_converter.ocr_correction.generate_content_once",
                side_effect=[firstResponse, finalResponse],
            ),
            mock.patch(
                "pdf_converter.ocr_correction.extract_ocr_response_payload",
                side_effect=[firstPayload, finalPayload],
            ),
        ):
            result = self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=4),
            )

        self.assertEqual(result.text, "write done")
        self.assertEqual(workingPath.read_text(encoding="utf-8"), "after\n")

    def testRunTurnLoopRaisesWhenToolBudgetIsExceeded(self):
        self.writeMarkdown(self.markdownDir, "a.md", "a\n")
        self.writeMarkdown(self.markdownDir, "b.md", "b\n")
        response = buildFakeResponse(types.Content(role="model", parts=[]))
        payload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[
                types.FunctionCall(name="read_markdown_file", args={"path": "md/a.md"}),
                types.FunctionCall(name="read_markdown_file", args={"path": "md/b.md"}),
            ],
            finish_reason=None,
        )

        with (
            mock.patch("pdf_converter.ocr_correction.generate_content_once", return_value=response),
            mock.patch("pdf_converter.ocr_correction.extract_ocr_response_payload", return_value=payload),
            self.assertRaises(self.module.OcrToolExecutionError),
        ):
            self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=1),
            )

    def testExecuteOcrFunctionCallReturnsRetryableErrorOnMismatch(self):
        from pdf_converter import ocr_correction
        self.writeMarkdown(self.workDir, "working.md", "（周辺関係者～の説明）\n\n第12条 本文\n")
        function_call = types.FunctionCall(
            name="write_markdown_file",
            args={
                "path": "work/working.md",
                "expected_old_text": "（周辺関係者～の説明）\n第12条 本文\n",
                "new_text": "（周辺関係者への説明）\n第12条 本文\n",
            },
        )
        result = ocr_correction.execute_ocr_function_call(
            function_call, self.module.ToolCallBudget(limit=4)
        )
        self.assertIsNotNone(result.function_response)
        response = result.function_response.response
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], "expected_old_text_mismatch")
        self.assertIs(response["error"]["retryable"], True)
        self.assertIn("diagnostics", response["error"])
        near_matches = response["error"]["diagnostics"]["near_matches"]
        self.assertTrue(near_matches)
        self.assertIn("\n\n第12条", near_matches[0]["excerpt"])
        self.assertEqual(
            (self.workDir / "working.md").read_text(encoding="utf-8"),
            "（周辺関係者～の説明）\n\n第12条 本文\n",
        )

    def testRunTurnLoopContinuesAfterRecoverableWriteMismatch(self):
        self.writeMarkdown(self.workDir, "working.md", "（周辺関係者～の説明）\n\n第12条 本文\n")
        firstResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        finalResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        firstPayload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[
                types.FunctionCall(
                    name="write_markdown_file",
                    args={
                        "path": "work/working.md",
                        "expected_old_text": "（周辺関係者～の説明）\n第12条 本文\n",
                        "new_text": "（周辺関係者への説明）\n第12条 本文\n",
                    },
                )
            ],
            finish_reason=None,
        )
        finalPayload = self.module.OcrResponsePayload(
            text="retryable error was returned",
            function_calls=[],
            finish_reason="STOP",
        )

        with (
            mock.patch(
                "pdf_converter.ocr_correction.generate_content_once",
                side_effect=[firstResponse, finalResponse],
            ) as generateMock,
            mock.patch(
                "pdf_converter.ocr_correction.extract_ocr_response_payload",
                side_effect=[firstPayload, finalPayload],
            ),
        ):
            result = self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=4),
            )

        self.assertEqual(result.text, "retryable error was returned")
        self.assertEqual(generateMock.call_count, 2)

        second_call_contents = generateMock.call_args_list[1][0][2]
        tool_contents = [c for c in second_call_contents if getattr(c, "role", None) == "tool"]
        self.assertEqual(len(tool_contents), 1)
        tool_parts = tool_contents[0].parts
        self.assertTrue(tool_parts)
        fr = tool_parts[0].function_response
        self.assertEqual(fr.response["error"]["code"], "expected_old_text_mismatch")

    def testRunTurnLoopAllowsEmptyIntermediateTextWhenFunctionCallExists(self):
        response = buildFakeResponse(types.Content(role="model", parts=[]))
        finalResponse = buildFakeResponse(types.Content(role="model", parts=[]))
        firstPayload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[types.FunctionCall(name="read_markdown_file", args={"path": "md/source.md"})],
            finish_reason=None,
        )
        finalPayload = self.module.OcrResponsePayload(
            text="done",
            function_calls=[],
            finish_reason="STOP",
        )

        with (
            mock.patch(
                "pdf_converter.ocr_correction.generate_content_once",
                side_effect=[response, finalResponse],
            ),
            mock.patch(
                "pdf_converter.ocr_correction.extract_ocr_response_payload",
                side_effect=[firstPayload, finalPayload],
            ),
            mock.patch(
                "pdf_converter.ocr_correction.execute_ocr_function_call",
                return_value=types.Part.from_function_response(
                    name="read_markdown_file",
                    response={"result": {"path": "md/source.md", "content": "# source\n"}},
                ),
            ) as executeMock,
        ):
            result = self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=4),
            )

        self.assertEqual(result.text, "done")
        self.assertEqual(executeMock.call_count, 1)

    def testRunTurnLoopRaisesWhenRecoverableWriteErrorsExceedLimit(self):
        self.assertTrue(
            hasattr(self.module, "DEFAULT_MAX_RECOVERABLE_TOOL_ERRORS_PER_RUN")
            or hasattr(ocr_correction, "DEFAULT_MAX_RECOVERABLE_TOOL_ERRORS_PER_RUN"),
            "DEFAULT_MAX_RECOVERABLE_TOOL_ERRORS_PER_RUN must be defined in module or ocr_correction",
        )
        max_recoverable = getattr(
            self.module,
            "DEFAULT_MAX_RECOVERABLE_TOOL_ERRORS_PER_RUN",
            getattr(ocr_correction, "DEFAULT_MAX_RECOVERABLE_TOOL_ERRORS_PER_RUN"),
        )

        response = buildFakeResponse(types.Content(role="model", parts=[]))
        payload = self.module.OcrResponsePayload(
            text=None,
            function_calls=[
                types.FunctionCall(
                    name="write_markdown_file",
                    args={"path": "work/test.md", "expected_old_text": "a", "new_text": "b"},
                )
            ],
            finish_reason=None,
        )

        with (
            mock.patch("pdf_converter.ocr_correction.generate_content_once", return_value=response),
            mock.patch("pdf_converter.ocr_correction.extract_ocr_response_payload", return_value=payload),
            mock.patch(
                "pdf_converter.ocr_correction.execute_ocr_function_call",
                return_value=types.Part.from_function_response(
                    name="write_markdown_file",
                    response={"error": {"code": "expected_old_text_mismatch", "retryable": True}},
                ),
            ),
            self.assertRaises(self.module.OcrToolExecutionError) as cm,
        ):
            self.module.run_ocr_correction_turn_loop(
                client=object(),
                model_id="gemma-4-31b-it",
                initial_contents=["initial"],
                config=types.GenerateContentConfig(),
                budget=self.module.ToolCallBudget(limit=64),
            )

        msg = str(cm.exception)
        self.assertTrue(
            ("recoverable" in msg.lower() or "retryable" in msg.lower())
            and ("上限" in msg or "limit" in msg.lower()),
            f"Expected exception message to mention recoverable/retryable and limit/上限, got: {msg}",
        )


if __name__ == "__main__":
    unittest.main()
