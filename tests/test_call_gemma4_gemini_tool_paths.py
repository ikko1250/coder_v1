import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Mock platform-specific or optional dependencies before importing the target module
sys.modules['fcntl'] = MagicMock()
sys.modules['httpx'] = MagicMock()
sys.modules['google'] = MagicMock()
sys.modules['google.genai'] = MagicMock()
sys.modules['google.genai.errors'] = MagicMock()
sys.modules['google.genai.types'] = MagicMock()

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call_gemma4_gemini.py"


def loadTargetModule():
    moduleName = "call_gemma4_gemini_tool_path_test_module"
    spec = importlib.util.spec_from_file_location(moduleName, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[moduleName] = module
    spec.loader.exec_module(module)
    return module


class OcrToolPathValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = loadTargetModule()

    def setUp(self):
        testRoot = REPO_ROOT / ".tmp_tool_path_test"
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

    def writeMarkdown(self, directory: Path, name: str, body: str) -> Path:
        path = directory / name
        path.write_text(body, encoding="utf-8", newline="\n")
        return path

    def testReadToolTextAllowsMarkdownUnderManualDirectories(self):
        markdownPath = self.writeMarkdown(self.markdownDir, "source.md", "# source\n")

        text = self.module.read_tool_text("md/source.md")

        self.assertEqual(text, "# source\n")
        self.assertEqual(markdownPath.read_text(encoding="utf-8"), text)

    def testWriteToolTextAllowsWorkingMarkdown(self):
        workingPath = self.writeMarkdown(self.workDir, "working.md", "before\n")

        writtenPath = self.module.write_tool_text("work/working.md", "before\n", "after\n")

        self.assertEqual(writtenPath, workingPath.resolve())
        self.assertEqual(workingPath.read_text(encoding="utf-8"), "after\n")

    def testReadToolTextRejectsDotDotEscape(self):
        outsidePath = self.tempRoot / "outside.md"
        outsidePath.write_text("outside\n", encoding="utf-8")

        with self.assertRaises(self.module.ToolReadError):
            self.module.read_tool_text("../outside.md")

    def testReadToolTextRejectsSymlinkEscape(self):
        outsidePath = self.tempRoot / "outside.md"
        outsidePath.write_text("outside\n", encoding="utf-8")
        linkedPath = self.markdownDir / "linked_escape.md"

        try:
            os.symlink(outsidePath, linkedPath)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"symlink を作成できないため skip: {exc}")

        with self.assertRaises(self.module.ToolReadError):
            self.module.read_tool_text("md/linked_escape.md")

    def testWriteToolTextRejectsOutsideWorkDirectory(self):
        self.writeMarkdown(self.markdownDir, "source.md", "before\n")

        with self.assertRaises(self.module.ToolWriteError):
            self.module.write_tool_text("md/source.md", "before\n", "after\n")


if __name__ == "__main__":
    unittest.main()
