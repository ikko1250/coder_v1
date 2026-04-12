import importlib.util
import os
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock

from pdf_converter.project_paths import (
    ProjectRootResolutionError,
    resolve_default_ocr_output_dir,
    resolve_dotenv_path,
    resolve_manual_root,
    resolve_project_root,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
CLI_MODULE_PATH = REPO_ROOT / "pdf_converter" / "call-gemma4-gemini.py"


def load_cli_module():
    module_name = "call_gemma4_gemini_project_paths_test_module"
    spec = importlib.util.spec_from_file_location(module_name, CLI_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"module spec を取得できません: {CLI_MODULE_PATH}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    with mock.patch(
        "pdf_converter.project_paths.resolve_project_root",
        side_effect=AssertionError("unexpected project root resolution during import"),
    ):
        spec.loader.exec_module(module)
    return module


class ProjectPathResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp_root = REPO_ROOT / ".tmp_project_paths_test" / self._testMethodName
        if self.temp_root.exists():
            shutil.rmtree(self.temp_root, ignore_errors=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def build_source_tree(self, root: Path, with_pyproject: bool = False) -> Path:
        (root / "pdf_converter").mkdir(parents=True, exist_ok=True)
        (root / "asset" / "texts_2nd" / "manual").mkdir(parents=True, exist_ok=True)
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
                env_root.resolve() / "asset" / "texts_2nd" / "manual",
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
            "call-gemma4-gemini.py",
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


if __name__ == "__main__":
    unittest.main()
