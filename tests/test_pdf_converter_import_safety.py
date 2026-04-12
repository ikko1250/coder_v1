import importlib
import os
import sys
import shutil
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import unittest

import requests


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = REPO_ROOT / "pdf_converter" / "pdf_converter.py"
MODULE_IMPORT_NAME = "pdf_converter.pdf_converter"


class PdfConverterImportSafetyTests(unittest.TestCase):
    def test_pyproject_declares_httpx_as_direct_dependency(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject_file:
            pyproject = tomllib.load(pyproject_file)

        self.assertIn("httpx>=0.28.1", pyproject["project"]["dependencies"])

    def test_import_is_side_effect_free(self):
        sys.modules.pop(MODULE_IMPORT_NAME, None)
        sys.modules.pop("pdf_converter", None)

        importlib.invalidate_caches()

        with (
            mock.patch.object(os.path, "exists", side_effect=AssertionError("unexpected dotenv lookup during import")),
            mock.patch.object(os, "getenv", side_effect=AssertionError("unexpected getenv during import")),
            mock.patch("argparse.ArgumentParser.parse_args", side_effect=AssertionError("unexpected parse_args during import")),
            mock.patch("builtins.open", side_effect=AssertionError("unexpected file I/O during import")),
            mock.patch("requests.post", side_effect=AssertionError("unexpected HTTP call during import")),
            mock.patch("requests.get", side_effect=AssertionError("unexpected HTTP call during import")),
        ):
            module = importlib.import_module(MODULE_IMPORT_NAME)

        self.assertEqual(module.__name__, MODULE_IMPORT_NAME)
        self.assertEqual(Path(module.__file__).resolve(), MODULE_PATH)
        self.assertTrue(callable(module.main))
        self.assertTrue(callable(module.convert_pdf))

    def test_main_converts_pdf_with_mocked_api(self):
        sys.modules.pop(MODULE_IMPORT_NAME, None)
        sys.modules.pop("pdf_converter", None)
        module = importlib.import_module(MODULE_IMPORT_NAME)

        with self.subTest("main path"):
            temp_root = REPO_ROOT / ".tmp_pdf_converter_import_safety_test"
            shutil.rmtree(temp_root, ignore_errors=True)
            temp_root.mkdir(parents=True, exist_ok=True)

            try:
                pdf_path = temp_root / "sample.pdf"
                pdf_path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF")

                response = mock.Mock()
                response.status_code = 200
                response.json.return_value = {
                    "result": {
                        "layoutParsingResults": [
                            {
                                "markdown": {
                                    "text": "# sample",
                                    "images": {},
                                },
                                "outputImages": {},
                            }
                        ]
                    }
                }

                with (
                    mock.patch.object(module, "parse_args", return_value=SimpleNamespace(file_path=str(pdf_path), save_page_jpg=False)),
                    mock.patch.object(module, "get_token", return_value="token"),
                    mock.patch.object(module, "DEFAULT_OUTPUT_DIR", str(temp_root / "output")),
                    mock.patch.object(module.requests, "post", return_value=response) as post_mock,
                ):
                    exit_code = module.main()

                self.assertEqual(exit_code, 0)
                self.assertEqual((temp_root / "output" / "sample.md").read_text(encoding="utf-8"), "# sample")
                post_mock.assert_called_once()
                self.assertEqual(post_mock.call_args.kwargs["headers"]["Authorization"], "token token")
                self.assertEqual(post_mock.call_args.kwargs["json"]["fileType"], 0)
            finally:
                shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
