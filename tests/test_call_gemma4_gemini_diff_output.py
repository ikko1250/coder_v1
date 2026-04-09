import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call-gemma4-gemini.py"


def loadTargetModule():
    moduleName = "call_gemma4_gemini_diff_test_module"
    spec = importlib.util.spec_from_file_location(moduleName, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[moduleName] = module
    spec.loader.exec_module(module)
    return module


class OcrUnifiedDiffTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = loadTargetModule()

    def setUp(self):
        testRoot = REPO_ROOT / ".tmp_diff_test"
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

    def writeMarkdown(self, directory: Path, name: str, body: str, newline: str | None = "\n") -> Path:
        path = directory / name
        path.write_text(body, encoding="utf-8", newline=newline)
        return path

    def testBuildUnifiedDiffTextIncludesExpectedHeadersAndBodyDiff(self):
        originalPath = self.writeMarkdown(self.markdownDir, "source.md", "line1\nline2\n")
        workingPath = self.writeMarkdown(self.workDir, "working.md", "line1\nline2 updated\n")

        diffText = self.module.build_unified_diff_text(originalPath, workingPath)

        self.assertIn("--- md/source.md", diffText)
        self.assertIn("+++ work/working.md", diffText)
        self.assertIn("-line2", diffText)
        self.assertIn("+line2 updated", diffText)

    def testBuildUnifiedDiffTextReturnsEmptyStringWhenNoDifferenceExists(self):
        originalPath = self.writeMarkdown(self.markdownDir, "same.md", "same\n")
        workingPath = self.writeMarkdown(self.workDir, "same-working.md", "same\n")

        diffText = self.module.build_unified_diff_text(originalPath, workingPath)

        self.assertEqual(diffText, "")


if __name__ == "__main__":
    unittest.main()
