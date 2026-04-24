import importlib.util
import os
import shutil
import sys
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
    moduleName = "call_gemma4_gemini_test_module"
    spec = importlib.util.spec_from_file_location(moduleName, MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[moduleName] = module
    spec.loader.exec_module(module)
    return module


class OcrMarkdownAutoMatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = loadTargetModule()

    def setUp(self):
        testRoot = REPO_ROOT / ".tmp_ocr_match_test"
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

    def writePdf(self, name: str) -> Path:
        path = self.pdfDir / name
        path.write_bytes(b"%PDF-1.4\n")
        return path

    def writeMarkdown(self, name: str, body: str = "# dummy\n") -> Path:
        path = self.markdownDir / name
        path.write_text(body, encoding="utf-8")
        return path

    def testFindAutoMatchedMarkdownCandidatesIncludesValidTimestampedMarkdown(self):
        pdfPath = self.writePdf("sample_regulation.pdf")
        matched = self.writeMarkdown("sample_regulation-2026-04-01_10-00-00.md")
        self.writeMarkdown("another_regulation-2026-04-01_10-00-00.md")

        candidates = self.module.find_auto_matched_markdown_candidates(
            pdfPath,
            markdown_dir=self.markdownDir,
        )

        self.assertEqual(candidates, [matched.resolve()])

    def testFindAutoMatchedMarkdownCandidatesExcludesCopyAndVersionVariants(self):
        pdfPath = self.writePdf("sample_regulation.pdf")
        self.writeMarkdown("sample_regulation-copy.md")
        self.writeMarkdown("sample_regulation-v2.md")
        self.writeMarkdown("sample_regulation-2026-04-01_10-00-00.md")

        candidates = self.module.find_auto_matched_markdown_candidates(
            pdfPath,
            markdown_dir=self.markdownDir,
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].name, "sample_regulation-2026-04-01_10-00-00.md")

    def testSelectLatestAutoMatchedMarkdownCandidateChoosesNewestTimestamp(self):
        older = self.writeMarkdown("sample_regulation-2026-03-30_06-14-12.md")
        newest = self.writeMarkdown("sample_regulation-2026-04-01_10-00-00.md")
        self.writeMarkdown("sample_regulation-2026-99-99_99-99-99.md")

        selected = self.module.select_latest_auto_matched_markdown_candidate(
            [older.resolve(), newest.resolve()]
        )

        self.assertEqual(selected, newest.resolve())

    def testResolveOcrMarkdownPathPrefersExplicitMarkdownPath(self):
        pdfPath = self.writePdf("sample_regulation.pdf")
        autoMatched = self.writeMarkdown("sample_regulation-2026-04-01_10-00-00.md")
        explicit = self.writeMarkdown("manual_override.md")

        resolved = self.module.resolve_ocr_markdown_path(
            pdf_path=pdfPath,
            markdown_path=str(explicit),
        )

        self.assertEqual(resolved, explicit.resolve())
        self.assertNotEqual(resolved, autoMatched.resolve())


if __name__ == "__main__":
    unittest.main()
