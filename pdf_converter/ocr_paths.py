from __future__ import annotations

import os
import re
import secrets
import shutil
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from pdf_converter.project_paths import (
    resolve_default_ocr_output_dir,
    resolve_manual_root,
    resolve_manual_root_candidates,
    resolve_project_root,
)


class MarkdownResolutionError(Exception):
    """OCR Markdown の解決に失敗したとき。メッセージはそのまま標準エラーに出す。"""


class WorkingDirectoryError(Exception):
    """OCR 修正用 work ディレクトリの解決に失敗したとき。"""


class WorkingMarkdownError(Exception):
    """OCR 修正用 Markdown 複製に失敗したとき。"""


class ToolPathResolutionError(Exception):
    """read / write tool 用のパス正規化に失敗したとき。"""


MARKDOWN_TIMESTAMP_STEM_PATTERN = re.compile(
    r"^(?P<base>.+)-(?P<ts>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$"
)

DEFAULT_MANUAL_ROOT: Path | None = None
DEFAULT_MANUAL_PDF_DIR: Path | None = None
DEFAULT_MANUAL_MARKDOWN_DIR: Path | None = None
DEFAULT_MANUAL_WORK_DIR: Path | None = None
DEFAULT_OCR_OUTPUT_DIR: Path | None = None


def get_default_manual_root() -> Path:
    """Resolve the repository manual root only when a path is actually needed."""
    if DEFAULT_MANUAL_ROOT is not None:
        return DEFAULT_MANUAL_ROOT
    return resolve_manual_root()


def get_default_manual_pdf_dir() -> Path:
    if DEFAULT_MANUAL_PDF_DIR is not None:
        return DEFAULT_MANUAL_PDF_DIR
    return get_default_manual_root() / "pdf"


def get_default_manual_markdown_dir() -> Path:
    if DEFAULT_MANUAL_MARKDOWN_DIR is not None:
        return DEFAULT_MANUAL_MARKDOWN_DIR
    return get_default_manual_root() / "md"


def get_default_manual_work_dir() -> Path:
    if DEFAULT_MANUAL_WORK_DIR is not None:
        return DEFAULT_MANUAL_WORK_DIR
    return get_default_manual_root() / "work"


def get_manual_root_candidates() -> list[Path]:
    """読み取り対象とする manual root 候補を返す。

    DEFAULT_MANUAL_ROOT override がある場合は [override] のみを返す。
    なければ canonical → legacy の順で返す。
    """
    if DEFAULT_MANUAL_ROOT is not None:
        return [DEFAULT_MANUAL_ROOT]
    project_root = resolve_project_root()
    return resolve_manual_root_candidates(project_root)


def get_manual_pdf_dirs() -> list[Path]:
    if DEFAULT_MANUAL_PDF_DIR is not None:
        return [DEFAULT_MANUAL_PDF_DIR]
    return [root / "pdf" for root in get_manual_root_candidates()]


def get_manual_markdown_dirs() -> list[Path]:
    if DEFAULT_MANUAL_MARKDOWN_DIR is not None:
        return [DEFAULT_MANUAL_MARKDOWN_DIR]
    return [root / "md" for root in get_manual_root_candidates()]


def get_manual_work_dirs() -> list[Path]:
    if DEFAULT_MANUAL_WORK_DIR is not None:
        return [DEFAULT_MANUAL_WORK_DIR]
    return [root / "work" for root in get_manual_root_candidates()]


def get_default_ocr_output_dir() -> Path:
    if DEFAULT_OCR_OUTPUT_DIR is not None:
        return DEFAULT_OCR_OUTPUT_DIR
    return resolve_default_ocr_output_dir()


def parse_auto_matched_markdown_stem(markdown_path: Path) -> tuple[str, str] | None:
    """自動対応付け対象の Markdown だけ (base_stem, timestamp_text) を返す。"""
    if markdown_path.suffix.lower() != ".md":
        return None

    match = MARKDOWN_TIMESTAMP_STEM_PATTERN.fullmatch(markdown_path.stem)
    if match is None:
        return None

    return match.group("base"), match.group("ts")


def parse_auto_matched_markdown_timestamp(markdown_path: Path) -> datetime | None:
    """自動対応付け対象 Markdown の timestamp を datetime に変換する。"""
    parsed = parse_auto_matched_markdown_stem(markdown_path)
    if parsed is None:
        return None

    _base_stem, timestamp_text = parsed
    try:
        return datetime.strptime(timestamp_text, "%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return None


def find_auto_matched_markdown_candidates(
    pdf_path: Path,
    markdown_dir: Path | None = None,
) -> list[Path]:
    """PDF stem と一致する timestamp 付き Markdown 候補だけを返す。"""
    if markdown_dir is not None:
        search_dirs = [markdown_dir]
    else:
        project_root = resolve_project_root()
        search_dirs = [root / "md" for root in resolve_manual_root_candidates(project_root)]

    pdf_stem = pdf_path.stem
    candidates: list[Path] = []
    for search_dir in search_dirs:
        search_dir = search_dir.expanduser()
        if not search_dir.exists() or not search_dir.is_dir():
            continue
        for markdown_path in sorted(search_dir.glob("*.md"), key=lambda path: path.name):
            parsed = parse_auto_matched_markdown_stem(markdown_path)
            if parsed is None:
                continue
            base_stem, _timestamp_text = parsed
            if base_stem != pdf_stem:
                continue
            if parse_auto_matched_markdown_timestamp(markdown_path) is None:
                continue
            candidates.append(markdown_path.resolve())

    return candidates


def select_latest_auto_matched_markdown_candidate(candidates: list[Path]) -> Path | None:
    """有効候補の中から最新 timestamp の Markdown を 1 件返す。"""
    latest_candidate: Path | None = None
    latest_timestamp: datetime | None = None

    for candidate in candidates:
        candidate_timestamp = parse_auto_matched_markdown_timestamp(candidate)
        if candidate_timestamp is None:
            continue

        if latest_timestamp is None or candidate_timestamp > latest_timestamp:
            latest_candidate = candidate
            latest_timestamp = candidate_timestamp

    return latest_candidate


def is_path_within_directory(path: Path, allowed_dir: Path) -> bool:
    """解決済み path が allowed_dir 配下なら True。"""
    resolved_path = path.expanduser().resolve()
    resolved_allowed_dir = allowed_dir.expanduser().resolve()
    allowed_norm = os.path.normcase(str(resolved_allowed_dir))
    path_norm = os.path.normcase(str(resolved_path))
    if allowed_norm == path_norm:
        return True
    for parent in resolved_path.parents:
        if os.path.normcase(str(parent)) == allowed_norm:
            return True
    return False


def ensure_path_within_directory(
    path: Path,
    allowed_dir: Path,
    label: str,
    error_cls: type[Exception] = MarkdownResolutionError,
) -> Path:
    """解決済み path が許可ディレクトリ配下かを検証する。"""
    resolved_path = path.expanduser().resolve()
    if is_path_within_directory(resolved_path, allowed_dir):
        return resolved_path

    resolved_allowed_dir = allowed_dir.expanduser().resolve()
    raise error_cls(
        f"エラー: {label} が許可ディレクトリ外です: {resolved_path} "
        f"(許可: {resolved_allowed_dir})"
    )


def ensure_path_within_any_directory(
    path: Path,
    allowed_dirs: list[Path],
    label: str,
    error_cls: type[Exception] = MarkdownResolutionError,
) -> Path:
    """解決済み path が許可ディレクトリのいずれか配下かを検証する。"""
    resolved_path = path.expanduser().resolve()
    for allowed_dir in allowed_dirs:
        if is_path_within_directory(resolved_path, allowed_dir):
            return resolved_path

    resolved_allowed_dirs = [d.expanduser().resolve() for d in allowed_dirs]
    allowed_text = ", ".join(str(d) for d in resolved_allowed_dirs)
    raise error_cls(
        f"エラー: {label} が許可ディレクトリ外です: {resolved_path} "
        f"(許可: {allowed_text})"
    )


def validate_markdown_path(markdown_path: str) -> Path:
    """明示指定された Markdown パスを検証して解決済み Path を返す。"""
    path = Path(markdown_path).expanduser().resolve()

    if not path.exists():
        raise MarkdownResolutionError(f"エラー: Markdown ファイルが見つかりません: {path}")
    if not path.is_file():
        raise MarkdownResolutionError(f"エラー: Markdown パスがファイルではありません: {path}")
    if path.suffix.lower() != ".md":
        raise MarkdownResolutionError(f"エラー: .md ファイルのみ対応しています: {path}")

    if is_path_within_directory(path, get_default_ocr_output_dir()):
        raise MarkdownResolutionError(
            "エラー: OCR Markdown 修正フローは pdf_converter.py の output/ を自動入力元にしません: "
            f"{path}。入力は {get_default_manual_markdown_dir()} 配下に固定です。"
            " 必要なら output/ から manual/md へ移動またはコピーしてから指定してください。"
        )

    return ensure_path_within_any_directory(path, get_manual_markdown_dirs(), "Markdown パス")


def directories_overlap(left_dir: Path, right_dir: Path) -> bool:
    """2 つのディレクトリが同一または親子関係なら True。"""
    resolved_left = left_dir.expanduser().resolve()
    resolved_right = right_dir.expanduser().resolve()
    left_norm = os.path.normcase(str(resolved_left))
    right_norm = os.path.normcase(str(resolved_right))
    if left_norm == right_norm:
        return True
    return is_path_within_directory(resolved_left, resolved_right) or is_path_within_directory(
        resolved_right,
        resolved_left,
    )


def _normalize_tool_relative_path(
    raw_path: str,
    area_mapping: dict[str, list[Path]],
) -> str:
    """相対 tool path の先頭領域識別子を実際のディレクトリに解決する。

    例:
        md/source.md + {"md": [override_md]} → override_md/source.md
        work/working.md + {"work": [override_work]} → override_work/working.md
    """
    input_path = Path(raw_path.strip()).expanduser()
    if input_path.is_absolute():
        return raw_path
    if not input_path.parts:
        return raw_path
    head = input_path.parts[0]
    tail = Path(*input_path.parts[1:])
    dirs = area_mapping.get(head)
    if not dirs:
        return raw_path
    resolved_dirs = [d.expanduser().resolve() for d in dirs]
    existing: list[Path] = []
    for d in resolved_dirs:
        candidate = d / tail
        try:
            if candidate.exists():
                existing.append(candidate)
        except OSError:
            continue
    if len(existing) == 1:
        return str(existing[0])
    elif len(existing) > 1:
        canonical = resolved_dirs[0] / tail
        return str(canonical if canonical in existing else existing[0])
    else:
        return str(resolved_dirs[0] / tail)


def resolve_tool_path(
    raw_path: str,
    allowed_dirs: list[Path],
    label: str,
) -> Path:
    """tool 入力パスを許可ディレクトリ基準で正規化し、許可ルート配下だけ通す。"""
    normalized_input = (raw_path or "").strip()
    if not normalized_input:
        raise ToolPathResolutionError(f"エラー: {label} が空です。")

    input_path = Path(normalized_input).expanduser()
    joined_path_is_resolved = False
    if input_path.is_absolute():
        joined_path = input_path
    else:
        # 相対パス: allowed_dirs を基準に解決。存在するものを優先。
        resolved_allowed_dirs = [allowed_dir.expanduser().resolve() for allowed_dir in allowed_dirs]
        existing: list[Path] = []
        for allowed_dir in resolved_allowed_dirs:
            try:
                candidate = (allowed_dir / input_path).resolve()
                if candidate.exists():
                    existing.append(candidate)
            except OSError:
                continue
        if len(existing) == 1:
            joined_path = existing[0]
            joined_path_is_resolved = True
        elif len(existing) > 1:
            canonical_candidate = (resolved_allowed_dirs[0] / input_path).resolve()
            joined_path = canonical_candidate if canonical_candidate.exists() else existing[0]
            joined_path_is_resolved = True
        else:
            joined_path = (resolved_allowed_dirs[0] / input_path).resolve()
            joined_path_is_resolved = True

    resolved_path = joined_path if joined_path_is_resolved else joined_path.resolve()
    resolved_allowed_dirs = [allowed_dir.expanduser().resolve() for allowed_dir in allowed_dirs]

    for allowed_dir in resolved_allowed_dirs:
        if is_path_within_directory(resolved_path, allowed_dir):
            return resolved_path

    allowed_text = ", ".join(str(path) for path in resolved_allowed_dirs)
    raise ToolPathResolutionError(
        f"エラー: {label} が許可ディレクトリ外です: {resolved_path} (許可: {allowed_text})"
    )


def resolve_working_directory(working_dir: str | None) -> Path:
    """OCR 修正用の work ディレクトリを解決し、必要なら作成する。"""
    raw_path = (working_dir or "").strip()
    candidate_dir = Path(raw_path).expanduser() if raw_path else get_default_manual_work_dir()
    resolved_dir = ensure_path_within_directory(
        candidate_dir,
        get_default_manual_root(),
        "作業ディレクトリ",
        error_cls=WorkingDirectoryError,
    )

    if resolved_dir.exists() and not resolved_dir.is_dir():
        raise WorkingDirectoryError(
            f"エラー: 作業ディレクトリのパスがディレクトリではありません: {resolved_dir}"
        )

    try:
        resolved_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise WorkingDirectoryError(
            f"エラー: 作業ディレクトリの作成に失敗しました: {resolved_dir}: {exc}"
        ) from exc

    return resolved_dir


def build_working_markdown_copy_path(source_markdown_path: Path, working_dir: Path) -> Path:
    """作業用 Markdown の一意な複製先パスを生成する。"""
    timestamp_text = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    destination_stem = f"{source_markdown_path.stem}-working-{timestamp_text}"
    destination_path = working_dir / f"{destination_stem}{source_markdown_path.suffix.lower()}"

    if not destination_path.exists():
        return destination_path

    for _ in range(8):
        random_suffix = secrets.token_hex(3)
        retry_path = working_dir / f"{destination_stem}-{random_suffix}{source_markdown_path.suffix.lower()}"
        if not retry_path.exists():
            return retry_path

    raise WorkingMarkdownError(
        "エラー: 作業用 Markdown の保存先を一意に決定できませんでした: "
        f"{working_dir}"
    )


def copy_markdown_to_working_directory(source_markdown_path: Path, working_dir: Path) -> Path:
    """元 OCR Markdown を work ディレクトリへ初期状態のまま複製する。"""
    destination_path = build_working_markdown_copy_path(source_markdown_path, working_dir)

    try:
        shutil.copyfile(source_markdown_path, destination_path)
    except OSError as exc:
        raise WorkingMarkdownError(
            "エラー: 作業用 Markdown の複製に失敗しました: "
            f"{source_markdown_path} -> {destination_path}: {exc}"
        ) from exc

    return destination_path


def resolve_auto_matched_markdown_path(
    pdf_path: Path,
    markdown_dir: Path | None = None,
) -> Path:
    """PDF から OCR Markdown を自動解決し、失敗理由を例外で返す。"""
    search_dirs: list[Path]
    if markdown_dir is not None:
        search_dirs = [markdown_dir]
    else:
        search_dirs = get_manual_markdown_dirs()

    candidates: list[Path] = []
    for search_dir in search_dirs:
        candidates.extend(find_auto_matched_markdown_candidates(pdf_path, markdown_dir=search_dir))

    if not candidates:
        search_dir_labels = ", ".join(str(d) for d in search_dirs)
        raise MarkdownResolutionError(
            "エラー: 対応する OCR Markdown が見つかりません: "
            f"{pdf_path.stem} (検索先: {search_dir_labels})。"
            " OCR Markdown 修正フローは pdf_converter.py の output/ を自動検索しません。"
            f" 入力に使う Markdown は {get_default_manual_markdown_dir()} 配下へ配置してください。"
        )

    latest_candidate = select_latest_auto_matched_markdown_candidate(candidates)
    if latest_candidate is None:
        raise MarkdownResolutionError(
            "エラー: OCR Markdown 候補から最新ファイルを決定できませんでした: "
            f"{pdf_path.stem} (候補数: {len(candidates)})"
        )

    return latest_candidate


def resolve_ocr_markdown_path(
    pdf_path: Path | None,
    markdown_path: str | None,
    markdown_dir: Path | None = None,
) -> Path | None:
    """明示指定を優先し、未指定時だけ自動対応付けで Markdown を解決する。"""
    if markdown_path is not None:
        explicit_path = markdown_path.strip()
        if explicit_path:
            return validate_markdown_path(explicit_path)

    if pdf_path is None:
        return None

    return resolve_auto_matched_markdown_path(pdf_path, markdown_dir=markdown_dir)


def make_manual_relative_path(path: Path) -> str:
    """manual ルート配下の path を tool 向け相対表現へ変換する。"""
    resolved = path.resolve()
    for root in get_manual_root_candidates():
        resolved_root = root.resolve()
        if is_path_within_directory(resolved, resolved_root):
            return os.path.relpath(resolved, resolved_root).replace("\\", "/")
    return str(resolved)
