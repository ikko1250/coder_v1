import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import importlib

# Mock optional dependencies before importing the target module
sys.modules['httpx'] = MagicMock()
sys.modules['google'] = MagicMock()
sys.modules['google.genai'] = MagicMock()
sys.modules['google.genai.errors'] = MagicMock()
sys.modules['google.genai.types'] = MagicMock()

from pdf_converter.project_paths import (
    ProjectRootResolutionError,
    resolve_default_ocr_output_dir,
    resolve_dotenv_path,
    resolve_manual_root,
    resolve_manual_root_candidates,
    resolve_project_root,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_MODULE_NAME = "pdf_converter.call_gemma4_gemini"


def load_cli_module():
    sys.modules.pop(CLI_MODULE_NAME, None)
    importlib.invalidate_caches()
    with mock.patch(
        "pdf_converter.project_paths.resolve_project_root",
        side_effect=AssertionError("unexpected project root resolution during import"),
    ):
        return importlib.import_module(CLI_MODULE_NAME)


class ProjectPathResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp_root = REPO_ROOT / ".tmp_project_paths_test" / self._testMethodName
        if self.temp_root.exists():
            shutil.rmtree(self.temp_root, ignore_errors=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def build_source_tree(
        self,
        root: Path,
        with_pyproject: bool = False,
        manual_layout: str = "legacy",
    ) -> Path:
        (root / "pdf_converter").mkdir(parents=True, exist_ok=True)
        if manual_layout == "canonical":
            (root / "asset" / "ocr_manual").mkdir(parents=True, exist_ok=True)
        elif manual_layout == "legacy":
            (root / "asset" / "texts_2nd" / "manual").mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError(f"unknown manual_layout: {manual_layout}")
        if with_pyproject:
            (root / "pyproject.toml").write_text("[build-system]\n", encoding="utf-8")
        return root / "pdf_converter" / "project_paths.py"

    def test_env_var_takes_priority_over_pyproject_and_fallback(self):
        env_root = self.temp_root / "env-root"
        source_file = self.build_source_tree(self.temp_root / "source-root", with_pyproject=True)
        self.build_source_tree(env_root, with_pyproject=False)

        with (
            mock.patch.dict(os.environ, {"CSV_VIEWER_PROJECT_ROOT": str(env_root)}, clear=False),
            mock.patch(
                "pdf_converter.project_paths._search_upward_for_pyproject",
                side_effect=AssertionError("pyproject search should not run when env is set"),
            ),
        ):
            resolved_root = resolve_project_root(source_file=source_file)
            self.assertEqual(resolved_root, env_root.resolve())
            self.assertEqual(
                resolve_manual_root(source_file=source_file),
                env_root.resolve() / "asset" / "ocr_manual",
            )
            self.assertEqual(
                resolve_default_ocr_output_dir(source_file=source_file),
                env_root.resolve() / "output",
            )
            self.assertEqual(
                resolve_dotenv_path(source_file=source_file),
                env_root.resolve() / "pdf_converter" / ".env",
            )

    def test_pyproject_search_wins_when_env_is_unset(self):
        source_file = self.build_source_tree(self.temp_root / "repo-root", with_pyproject=True)

        resolved_root = resolve_project_root(source_file=source_file)

        self.assertEqual(resolved_root, (self.temp_root / "repo-root").resolve())

    def test_file_fallback_works_when_pyproject_is_missing(self):
        source_file = self.build_source_tree(self.temp_root / "fallback-root", with_pyproject=False)

        with mock.patch(
            "pdf_converter.project_paths._search_upward_for_pyproject",
            return_value=None,
        ):
            resolved_root = resolve_project_root(source_file=source_file)

        self.assertEqual(resolved_root, (self.temp_root / "fallback-root").resolve())

    def test_source_tree_layout_accepts_canonical_manual_root(self):
        source_file = self.build_source_tree(
            self.temp_root / "canonical-root",
            with_pyproject=True,
            manual_layout="canonical",
        )

        resolved_root = resolve_project_root(source_file=source_file)

        self.assertEqual(resolved_root, (self.temp_root / "canonical-root").resolve())

    def test_resolution_failure_raises_explicit_error(self):
        source_file = self.temp_root / "missing-root" / "pdf_converter" / "project_paths.py"

        with (
            mock.patch.dict(os.environ, {"CSV_VIEWER_PROJECT_ROOT": ""}, clear=False),
            mock.patch(
                "pdf_converter.project_paths._search_upward_for_pyproject",
                return_value=None,
            ),
        ):
            with self.assertRaises(ProjectRootResolutionError) as context:
                resolve_project_root(source_file=source_file)

        message = str(context.exception)
        self.assertIn("source tree", message)
        self.assertIn("__file__ fallback", message)

    def test_cli_import_and_parse_args_do_not_trigger_project_root_resolution(self):
        module = load_cli_module()
        argv = [
            "call_gemma4_gemini.py",
            "--task",
            module.OCR_CORRECTION_TASK,
            "--pdf-path",
            "sample.pdf",
            "--markdown-path",
            "sample.md",
            "--tool-call-log-path",
            "logs/tool-calls.jsonl",
        ]

        with mock.patch.object(sys, "argv", argv):
            args = module.parse_args()

        self.assertEqual(args.task, module.OCR_CORRECTION_TASK)
        self.assertEqual(args.pdf_path, "sample.pdf")
        self.assertEqual(args.markdown_path, "sample.md")
        self.assertEqual(args.tool_call_log_path, "logs/tool-calls.jsonl")


class ManualRootCandidateTests(unittest.TestCase):
    def test_resolve_manual_root_candidates_returns_canonical_then_legacy(self) -> None:
        project_root = resolve_project_root()
        candidates = resolve_manual_root_candidates(project_root)
        self.assertEqual(
            candidates,
            [
                project_root / "asset" / "ocr_manual",
                project_root / "asset" / "texts_2nd" / "manual",
            ],
        )

    def test_resolve_manual_root_returns_canonical_path(self) -> None:
        project_root = resolve_project_root()
        manual_root = resolve_manual_root()
        self.assertEqual(manual_root, project_root / "asset" / "ocr_manual")

    def test_candidate_dirs_prefers_override_when_set(self) -> None:
        """DEFAULT_MANUAL_ROOT が設定されている場合、candidate は override のみを返す。"""
        module = load_cli_module()
        original = module.DEFAULT_MANUAL_ROOT
        try:
            override = Path("/tmp/override_manual")
            module.DEFAULT_MANUAL_ROOT = override
            candidates = module.get_manual_root_candidates()
            self.assertEqual(candidates, [override])
        finally:
            module.DEFAULT_MANUAL_ROOT = original

    def test_get_manual_markdown_dirs_respects_override(self) -> None:
        module = load_cli_module()
        original = module.DEFAULT_MANUAL_MARKDOWN_DIR
        try:
            override = Path("/tmp/override_md")
            module.DEFAULT_MANUAL_MARKDOWN_DIR = override
            dirs = module.get_manual_markdown_dirs()
            self.assertEqual(dirs, [override])
        finally:
            module.DEFAULT_MANUAL_MARKDOWN_DIR = original


if __name__ == "__main__":
    unittest.main()
