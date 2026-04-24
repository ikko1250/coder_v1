import importlib.util
import os
import shutil
import sys
import unicodedata
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Mock optional dependencies before importing the target module
sys.modules['httpx'] = MagicMock()
sys.modules['google'] = MagicMock()
sys.modules['google.genai'] = MagicMock()
sys.modules['google.genai.errors'] = MagicMock()
sys.modules['google.genai.types'] = MagicMock()

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call_gemma4_gemini.py"


def loadTargetModule():
    moduleName = "call_gemma4_gemini_write_match_test_module"
    spec = importlib.util.spec_from_file_location(moduleName, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[moduleName] = module
    spec.loader.exec_module(module)
    return module


class OcrWriteMatchingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = loadTargetModule()

    def setUp(self):
        testRoot = REPO_ROOT / ".tmp_write_match_test"
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

        self.originalManualRoot = self.module.DEFAULT_MANUAL_ROOT
        self.originalManualPdfDir = self.module.DEFAULT_MANUAL_PDF_DIR
        self.originalManualMarkdownDir = self.module.DEFAULT_MANUAL_MARKDOWN_DIR
        self.originalManualWorkDir = self.module.DEFAULT_MANUAL_WORK_DIR
        self.originalOcrOutputDir = self.module.DEFAULT_OCR_OUTPUT_DIR

        self.module.DEFAULT_MANUAL_ROOT = self.manualRoot
        self.module.DEFAULT_MANUAL_PDF_DIR = self.pdfDir
        self.module.DEFAULT_MANUAL_MARKDOWN_DIR = self.markdownDir
        self.module.DEFAULT_MANUAL_WORK_DIR = self.workDir
        self.module.DEFAULT_OCR_OUTPUT_DIR = self.outputDir

    def tearDown(self):
        self.module.DEFAULT_MANUAL_ROOT = self.originalManualRoot
        self.module.DEFAULT_MANUAL_PDF_DIR = self.originalManualPdfDir
        self.module.DEFAULT_MANUAL_MARKDOWN_DIR = self.originalManualMarkdownDir
        self.module.DEFAULT_MANUAL_WORK_DIR = self.originalManualWorkDir
        self.module.DEFAULT_OCR_OUTPUT_DIR = self.originalOcrOutputDir
        shutil.rmtree(self.tempRoot, ignore_errors=True)

    def writeWorkingMarkdown(self, name: str, body: str, newline: str | None = "\n") -> Path:
        path = self.workDir / name
        path.write_text(body, encoding="utf-8", newline=newline)
        return path

    def testWriteToolTextMatchesAcrossLfAndCrlf(self):
        workingPath = self.writeWorkingMarkdown("working.md", "line1\r\nline2\r\n", newline="")

        writtenPath = self.module.write_tool_text("work/working.md", "line1\nline2\n", "updated\n")

        self.assertEqual(writtenPath, workingPath.resolve())
        self.assertEqual(workingPath.read_text(encoding="utf-8"), "updated\n")

    def testWriteToolTextMatchesAcrossNfcDifference(self):
        decomposed = "cafe\u0301\n"
        composed = unicodedata.normalize("NFC", decomposed)
        workingPath = self.writeWorkingMarkdown("working.md", decomposed)

        self.module.write_tool_text("work/working.md", composed, "done\n")

        self.assertEqual(workingPath.read_text(encoding="utf-8"), "done\n")

    def testWriteToolTextRaisesWhenExpectedTextDoesNotExist(self):
        self.writeWorkingMarkdown("working.md", "before\n")

        with self.assertRaises(self.module.ToolWriteError):
            self.module.write_tool_text("work/working.md", "missing\n", "after\n")

    def testWriteToolTextRaisesWhenExpectedTextMatchesMultipleTimes(self):
        self.writeWorkingMarkdown("working.md", "target\ntarget\n")

        with self.assertRaises(self.module.ToolWriteError):
            self.module.write_tool_text("work/working.md", "target\n", "after\n")

    def testWriteToolTextSucceedsWhenExpectedTextMatchesExactlyOnce(self):
        workingPath = self.writeWorkingMarkdown("working.md", "before\ntarget\nafter\n")

        self.module.write_tool_text("work/working.md", "target\n", "replaced\n")

        self.assertEqual(workingPath.read_text(encoding="utf-8"), "before\nreplaced\nafter\n")


if __name__ == "__main__":
    unittest.main()
