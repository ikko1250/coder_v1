import tomllib
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent


class PyprojectUvBuildConfigTests(unittest.TestCase):
    def test_uv_build_configuration_and_script_entry_point(self):
        with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject_file:
            pyproject = tomllib.load(pyproject_file)

        self.assertEqual(pyproject["project"]["name"], "csv-viewer")
        self.assertIn("httpx>=0.28.1", pyproject["project"]["dependencies"])
        self.assertEqual(pyproject["build-system"]["requires"], ["uv_build>=0.11.3,<0.12"])
        self.assertEqual(pyproject["build-system"]["build-backend"], "uv_build")
        self.assertEqual(pyproject["project"]["scripts"]["call-gemma4-gemini"], "pdf_converter.call_gemma4_gemini:main")
        self.assertEqual(pyproject["tool"]["uv"]["build-backend"]["module-name"], "pdf_converter")
        self.assertEqual(pyproject["tool"]["uv"]["build-backend"]["module-root"], "")
        self.assertTrue((REPO_ROOT / "pdf_converter" / "__init__.py").is_file())


if __name__ == "__main__":
    unittest.main()
