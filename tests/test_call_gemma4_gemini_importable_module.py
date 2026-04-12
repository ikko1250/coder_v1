import importlib
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_NAME = "pdf_converter.call_gemma4_gemini"
MODULE_PATH = REPO_ROOT / "pdf_converter" / "call_gemma4_gemini.py"


class CallGemma4GeminiImportableModuleTests(unittest.TestCase):
    def setUp(self):
        sys.modules.pop(MODULE_NAME, None)
        importlib.invalidate_caches()

    def test_import_exposes_parse_args_and_main(self):
        module = importlib.import_module(MODULE_NAME)

        self.assertEqual(Path(module.__file__).resolve(), MODULE_PATH)
        self.assertTrue(callable(module.parse_args))
        self.assertTrue(callable(module.main))

    def test_parse_args_accepts_pdf_path(self):
        module = importlib.import_module(MODULE_NAME)

        with mock.patch.object(sys, "argv", ["call_gemma4_gemini.py", "--pdf-path", "sample.pdf"]):
            parsed = module.parse_args()

        self.assertEqual(parsed.pdf_path, "sample.pdf")

    def test_main_delegates_to_single_shot_mode(self):
        module = importlib.import_module(MODULE_NAME)

        with (
            mock.patch.object(module, "parse_args", return_value=SimpleNamespace(task=module.DEFAULT_TASK)),
            mock.patch.object(module, "run_single_shot_mode", return_value=7) as run_mock,
        ):
            exit_code = module.main()

        self.assertEqual(exit_code, 7)
        run_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
