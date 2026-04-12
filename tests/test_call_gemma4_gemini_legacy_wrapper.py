import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent.parent
WRAPPER_PATH = REPO_ROOT / "pdf_converter" / "call-gemma4-gemini.py"


def load_wrapper_module():
    spec = importlib.util.spec_from_file_location("call_gemma4_gemini_legacy_wrapper", WRAPPER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class CallGemma4GeminiLegacyWrapperTests(unittest.TestCase):
    def test_ensure_repo_root_on_sys_path_prioritizes_repo_root_and_removes_script_dir(self):
        wrapper = load_wrapper_module()
        repo_root_str = str(REPO_ROOT)
        script_dir_str = str(WRAPPER_PATH.parent)
        original_sys_path = list(sys.path)
        sys.path[:] = [script_dir_str, repo_root_str, "sentinel"]

        try:
            wrapper.ensure_repo_root_on_sys_path()
            self.assertEqual(sys.path[0], repo_root_str)
            self.assertNotIn(script_dir_str, sys.path)
            self.assertEqual(sys.path[1], "sentinel")
        finally:
            sys.path[:] = original_sys_path

    def test_main_delegates_to_package_main(self):
        wrapper = load_wrapper_module()

        with mock.patch("pdf_converter.call_gemma4_gemini.main", return_value=11) as package_main:
            exit_code = wrapper.main()

        self.assertEqual(exit_code, 11)
        package_main.assert_called_once_with()

    def test_direct_execution_help_uses_package_parser(self):
        result = subprocess.run(
            [sys.executable, str(WRAPPER_PATH), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("--task", result.stdout)
        self.assertNotIn("--save-page-jpg", result.stdout)
        self.assertNotIn("--save-page-jpg", result.stderr)


if __name__ == "__main__":
    unittest.main()
