from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT_ENV_VAR = "CSV_VIEWER_PROJECT_ROOT"


class ProjectRootResolutionError(RuntimeError):
    """Raised when the repository source tree cannot be resolved."""


def _normalize_path(path_value: str | Path) -> Path:
    return Path(path_value).expanduser()


def _has_source_tree_layout(project_root: Path) -> bool:
    return (
        (project_root / "pdf_converter").is_dir()
        and (project_root / "asset" / "texts_2nd" / "manual").is_dir()
    )


def _search_upward_for_pyproject(start_path: Path) -> Path | None:
    for candidate in (start_path, *start_path.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return None


def _ensure_source_tree_root(candidate_root: Path, resolution_source: str) -> Path:
    resolved_root = candidate_root.resolve()
    if not resolved_root.exists():
        raise ProjectRootResolutionError(
            f"{resolution_source}: source tree root does not exist: {resolved_root}"
        )
    if not resolved_root.is_dir():
        raise ProjectRootResolutionError(
            f"{resolution_source}: source tree root is not a directory: {resolved_root}"
        )
    if not _has_source_tree_layout(resolved_root):
        raise ProjectRootResolutionError(
            f"{resolution_source}: source tree layout is incomplete at {resolved_root}. "
            "Expected pdf_converter/ and asset/texts_2nd/manual/."
        )
    return resolved_root


def resolve_project_root(
    source_file: str | Path | None = None,
    env_value: str | None = None,
) -> Path:
    source_path = _normalize_path(source_file or Path(__file__))
    env_root_value = os.environ.get(PROJECT_ROOT_ENV_VAR) if env_value is None else env_value

    if env_root_value is not None and env_root_value.strip():
        return _ensure_source_tree_root(
            _normalize_path(env_root_value),
            PROJECT_ROOT_ENV_VAR,
        )

    search_start = source_path if source_path.is_dir() else source_path.parent
    pyproject_root = _search_upward_for_pyproject(search_start)
    if pyproject_root is not None:
        return _ensure_source_tree_root(pyproject_root, "pyproject.toml search")

    fallback_root = source_path if source_path.is_dir() else source_path.parent.parent
    return _ensure_source_tree_root(fallback_root, "__file__ fallback")


def resolve_manual_root(
    source_file: str | Path | None = None,
    env_value: str | None = None,
) -> Path:
    return resolve_project_root(source_file=source_file, env_value=env_value) / "asset" / "texts_2nd" / "manual"


def resolve_default_ocr_output_dir(
    source_file: str | Path | None = None,
    env_value: str | None = None,
) -> Path:
    return resolve_project_root(source_file=source_file, env_value=env_value) / "output"


def resolve_dotenv_path(
    source_file: str | Path | None = None,
    env_value: str | None = None,
) -> Path:
    return resolve_project_root(source_file=source_file, env_value=env_value) / "pdf_converter" / ".env"
