from __future__ import annotations

"""Legacy wrapper for `pdf_converter.call_gemma4_gemini`."""

import os
import sys
from pathlib import Path


def ensure_repo_root_on_sys_path() -> None:
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    script_dir_norm = os.path.normcase(str(script_dir))
    repo_root_norm = os.path.normcase(str(repo_root))

    # Remove the legacy wrapper directory so sibling modules cannot shadow the
    # package import. Then place the repo root at the front explicitly.
    # Use normcase + resolve for robust comparison on Windows where casing
    # and separators may differ.
    cleaned = []
    for entry in sys.path:
        try:
            entry_norm = os.path.normcase(str(Path(entry).resolve()))
        except (OSError, ValueError):
            cleaned.append(entry)
            continue
        if entry_norm != script_dir_norm and entry_norm != repo_root_norm:
            cleaned.append(entry)
    sys.path[:] = cleaned
    sys.path.insert(0, str(repo_root))


def main() -> int:
    ensure_repo_root_on_sys_path()
    from pdf_converter.call_gemma4_gemini import main as package_main

    return package_main()


if __name__ == "__main__":
    raise SystemExit(main())
