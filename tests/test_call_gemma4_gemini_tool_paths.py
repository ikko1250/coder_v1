import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path

import pdf_converter.ocr_paths as ocr_paths_module

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

    def testReadToolTextFallbackToLegacyRoot(self):
        original_ocr_get_roots = ocr_paths_module.get_manual_root_candidates
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            canonical_root = self.tempRoot / "canonical"
            legacy_root = self.tempRoot / "legacy"
            (canonical_root / "md").mkdir(parents=True)
            (legacy_root / "md").mkdir(parents=True)

            legacy_md = legacy_root / "md" / "source.md"
            legacy_md.write_text("# legacy\n", encoding="utf-8", newline="\n")

            ocr_paths_module.get_manual_root_candidates = lambda: [canonical_root, legacy_root]
            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = None
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = None

            text = self.module.read_tool_text("md/source.md")
            self.assertEqual(text, "# legacy\n")
        finally:
            ocr_paths_module.get_manual_root_candidates = original_ocr_get_roots
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testReadToolTextPrefersCanonicalOverLegacy(self):
        original_ocr_get_roots = ocr_paths_module.get_manual_root_candidates
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            canonical_root = self.tempRoot / "canonical"
            legacy_root = self.tempRoot / "legacy"
            (canonical_root / "md").mkdir(parents=True)
            (legacy_root / "md").mkdir(parents=True)

            canonical_md = canonical_root / "md" / "source.md"
            canonical_md.write_text("# canonical\n", encoding="utf-8", newline="\n")
            legacy_md = legacy_root / "md" / "source.md"
            legacy_md.write_text("# legacy\n", encoding="utf-8", newline="\n")

            ocr_paths_module.get_manual_root_candidates = lambda: [canonical_root, legacy_root]
            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = None
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = None

            text = self.module.read_tool_text("md/source.md")
            self.assertEqual(text, "# canonical\n")
        finally:
            ocr_paths_module.get_manual_root_candidates = original_ocr_get_roots
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testReadToolTextRespectsMarkdownDirOverride(self):
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            override_md = self.tempRoot / "override_md"
            override_md.mkdir(parents=True)
            override_md_source = override_md / "source.md"
            override_md_source.write_text("# override\n", encoding="utf-8", newline="\n")

            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = override_md
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = None

            text = self.module.read_tool_text("md/source.md")
            self.assertEqual(text, "# override\n")
        finally:
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testWriteToolTextRespectsWorkDirOverride(self):
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            override_work = self.tempRoot / "override_work"
            override_work.mkdir(parents=True)
            override_work_file = override_work / "working.md"
            override_work_file.write_text("before\n", encoding="utf-8", newline="\n")

            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = None
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = override_work

            written_path = self.module.write_tool_text("work/working.md", "before\n", "after\n")
            self.assertEqual(written_path, override_work_file.resolve())
            self.assertEqual(override_work_file.read_text(encoding="utf-8"), "after\n")
        finally:
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testWriteToolTextAllowsWorkDirWhenMarkdownDirIsSameOverride(self):
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            shared_dir = self.tempRoot / "shared_md_work"
            shared_dir.mkdir(parents=True)
            working_file = shared_dir / "working.md"
            working_file.write_text("before\n", encoding="utf-8", newline="\n")

            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = shared_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = shared_dir

            written_path = self.module.write_tool_text("work/working.md", "before\n", "after\n")
            self.assertEqual(written_path, working_file.resolve())
            self.assertEqual(working_file.read_text(encoding="utf-8"), "after\n")
        finally:
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testWriteToolTextAllowsWorkDirWhenMarkdownDirIsParentOverride(self):
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            parent_dir = self.tempRoot / "manual_parent"
            override_work = parent_dir / "work"
            override_work.mkdir(parents=True)
            working_file = override_work / "working.md"
            working_file.write_text("before\n", encoding="utf-8", newline="\n")

            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = parent_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = override_work

            written_path = self.module.write_tool_text("work/working.md", "before\n", "after\n")
            self.assertEqual(written_path, working_file.resolve())
            self.assertEqual(working_file.read_text(encoding="utf-8"), "after\n")
        finally:
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir

    def testWriteToolTextStillRejectsUnrelatedMarkdownDirAbsolutePath(self):
        original_ocr_manual_root = ocr_paths_module.DEFAULT_MANUAL_ROOT
        original_ocr_manual_markdown_dir = ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR
        original_ocr_manual_work_dir = ocr_paths_module.DEFAULT_MANUAL_WORK_DIR
        try:
            override_work = self.tempRoot / "override_work"
            unrelated_md = self.tempRoot / "unrelated_md"
            override_work.mkdir(parents=True)
            unrelated_md.mkdir(parents=True)
            source_file = unrelated_md / "source.md"
            source_file.write_text("before\n", encoding="utf-8", newline="\n")

            ocr_paths_module.DEFAULT_MANUAL_ROOT = None
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = unrelated_md
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = override_work

            with self.assertRaises(self.module.ToolWriteError):
                self.module.write_tool_text(str(source_file), "before\n", "after\n")
        finally:
            ocr_paths_module.DEFAULT_MANUAL_ROOT = original_ocr_manual_root
            ocr_paths_module.DEFAULT_MANUAL_MARKDOWN_DIR = original_ocr_manual_markdown_dir
            ocr_paths_module.DEFAULT_MANUAL_WORK_DIR = original_ocr_manual_work_dir


if __name__ == "__main__":
    unittest.main()
