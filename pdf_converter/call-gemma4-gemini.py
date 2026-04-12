from __future__ import annotations

"""Legacy wrapper for `pdf_converter.call_gemma4_gemini`."""

import sys
from pathlib import Path


def ensure_repo_root_on_sys_path() -> None:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    script_dir_str = str(script_dir)
    repo_root_str = str(repo_root)

    # Remove the legacy wrapper directory so sibling modules cannot shadow the
    # package import. Then place the repo root at the front explicitly.
    sys.path[:] = [entry for entry in sys.path if entry != script_dir_str and entry != repo_root_str]
    sys.path.insert(0, repo_root_str)


def main() -> int:
    ensure_repo_root_on_sys_path()
    from pdf_converter.call_gemma4_gemini import main as package_main

    return package_main()


if __name__ == "__main__":
    raise SystemExit(main())
