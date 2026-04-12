import importlib
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGE_INIT_PATH = REPO_ROOT / "pdf_converter" / "__init__.py"


class PdfConverterPackageInitTests(unittest.TestCase):
    def test_import_resolves_to_regular_package(self):
        package = importlib.import_module("pdf_converter")

        self.assertEqual(Path(package.__file__).resolve(), PACKAGE_INIT_PATH)
        self.assertIsNotNone(package.__spec__)
        self.assertIsNotNone(package.__spec__.origin)
        self.assertEqual(Path(package.__spec__.origin).resolve(), PACKAGE_INIT_PATH)
        self.assertIsNotNone(package.__spec__.submodule_search_locations)


if __name__ == "__main__":
    unittest.main()
