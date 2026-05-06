from __future__ import annotations

import argparse
import contextlib
import errno
import os
import sys
import time
import unicodedata
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pdf_converter.ocr_paths import (
    ToolPathResolutionError,
    _normalize_tool_relative_path,
    directories_overlap,
    get_manual_markdown_dirs,
    get_manual_pdf_dirs,
    get_manual_work_dirs,
    is_path_within_directory,
    resolve_tool_path,
)


class ToolReadError(Exception):
    """read tool の読取に失敗したとき。"""


class ToolWriteError(Exception):
    """write tool の書込に失敗したとき。"""


class ToolCallLimitError(Exception):
    """read / write tool の総呼び出し回数上限を超えたとき。"""


WRITE_LOCK_TIMEOUT_SECONDS = 10.0
WRITE_LOCK_POLL_INTERVAL_SECONDS = 0.1
DEFAULT_MAX_TOOL_CALLS_PER_RUN = 48
MIN_MAX_TOOL_CALLS_PER_RUN = 1
MAX_TOOL_CALLS_CAP = 256


def read_tool_text(raw_path: str) -> str:
    """read tool 用: md/ と work/ の UTF-8 テキストだけを返す。"""
    normalized = _normalize_tool_relative_path(raw_path, {
        "md": get_manual_markdown_dirs(),
        "work": get_manual_work_dirs(),
    })
    try:
        resolved_path = resolve_tool_path(
            normalized,
            get_manual_markdown_dirs() + get_manual_work_dirs(),
            "read path",
        )
    except ToolPathResolutionError as exc:
        raise ToolReadError(str(exc)) from exc

    # PDF 除外チェック
    if resolved_path.suffix.lower() == ".pdf":
        raise ToolReadError(f"エラー: read tool は PDF を読めません: {resolved_path}")
    for pdf_dir in get_manual_pdf_dirs():
        if is_path_within_directory(resolved_path, pdf_dir.expanduser().resolve()):
            raise ToolReadError(f"エラー: read tool は PDF を読めません: {resolved_path}")

    if not resolved_path.exists():
        raise ToolReadError(f"エラー: read 対象ファイルが見つかりません: {resolved_path}")
    if not resolved_path.is_file():
        raise ToolReadError(f"エラー: read 対象パスがファイルではありません: {resolved_path}")

    try:
        return resolved_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ToolReadError(
            f"エラー: read tool のファイル読取に失敗しました: {resolved_path}: {exc}"
        ) from exc


def normalize_tool_text(text: str) -> str:
    """write 一致判定用に LF + NFC へ正規化する。"""
    normalized_newlines = text.replace("\r\n", "\n").replace("\r", "\n")
    return unicodedata.normalize("NFC", normalized_newlines)


class MatchResultKind(Enum):
    MISMATCH = "mismatch"
    AMBIGUOUS = "ambiguous"
    UNIQUE = "unique"


@dataclass(frozen=True)
class MatchResult:
    kind: MatchResultKind
    index: int = -1


def find_unique_normalized_match(haystack: str, needle: str) -> MatchResult:
    """needle が haystack に 1 件だけ現れるときの開始位置を返す。"""
    if not needle:
        return MatchResult(MatchResultKind.MISMATCH, -1)

    first_index = haystack.find(needle)
    if first_index < 0:
        return MatchResult(MatchResultKind.MISMATCH, -1)

    second_index = haystack.find(needle, first_index + 1)
    if second_index >= 0:
        return MatchResult(MatchResultKind.AMBIGUOUS, -2)

    return MatchResult(MatchResultKind.UNIQUE, first_index)


def build_write_lock_path(target_path: Path) -> Path:
    """write 排他制御用の sidecar lock file パスを返す。"""
    return target_path.with_name(f"{target_path.name}.lock")


@contextlib.contextmanager
def acquire_write_lock(lock_path: Path) -> Iterator[Path]:
    """sidecar lock file を使って write 区間を排他実行する。

    Unix 系では fcntl.flock、Windows では msvcrt.locking を使う。sidecar ファイル
    自体は削除しないので、複数プロセスが同じ lock file を協調的に参照できる。
    """
    if os.name == "nt":
        with acquire_windows_write_lock(lock_path) as acquired_path:
            yield acquired_path
    else:
        with acquire_unix_write_lock(lock_path) as acquired_path:
            yield acquired_path


@contextlib.contextmanager
def acquire_unix_write_lock(lock_path: Path) -> Iterator[Path]:
    """Unix 系の fcntl.flock backend。"""
    import fcntl

    try:
        lock_fd = os.open(
            str(lock_path),
            os.O_CREAT | os.O_RDWR,
            0o644,
        )
    except OSError as exc:
        raise ToolWriteError(
            f"エラー: write lock ファイルの作成に失敗しました: {lock_path}: {exc}"
        ) from exc

    try:
        start_time = time.monotonic()
        acquired = False
        while True:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                if time.monotonic() - start_time >= WRITE_LOCK_TIMEOUT_SECONDS:
                    raise ToolWriteError(
                        "エラー: write lock の取得がタイムアウトしました。"
                        f" 対象: {lock_path}"
                    )
                time.sleep(WRITE_LOCK_POLL_INTERVAL_SECONDS)
            except OSError as exc:
                raise ToolWriteError(
                    f"エラー: write lock の取得に失敗しました: {lock_path}: {exc}"
                ) from exc

        try:
            yield lock_path
        finally:
            if acquired:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                except OSError:
                    pass
    finally:
        os.close(lock_fd)


def is_windows_lock_contention_error(exc: OSError) -> bool:
    """msvcrt.locking の lock contention だけを retry 対象として判定する。"""
    if getattr(exc, "winerror", None) == 33:
        return True

    contention_errno_values = {errno.EACCES}
    for name in ("EDEADLK", "EDEADLOCK"):
        value = getattr(errno, name, None)
        if value is not None:
            contention_errno_values.add(value)
    return exc.errno in contention_errno_values


@contextlib.contextmanager
def acquire_windows_write_lock(lock_path: Path) -> Iterator[Path]:
    """Windows の msvcrt.locking backend。

    ローカル NTFS 上で、この CLI 同士が同じ sidecar lock file を使う協調排他を対象にする。
    lock を無視して working Markdown を直接書く外部プロセスまでは防がない。
    """
    import msvcrt

    try:
        lock_fd = os.open(
            str(lock_path),
            os.O_CREAT | os.O_RDWR,
            0o644,
        )
    except OSError as exc:
        raise ToolWriteError(
            f"エラー: write lock ファイルの作成に失敗しました: {lock_path}: {exc}"
        ) from exc

    try:
        start_time = time.monotonic()
        acquired = False
        while True:
            try:
                os.lseek(lock_fd, 0, os.SEEK_SET)
                msvcrt.locking(lock_fd, msvcrt.LK_NBLCK, 1)
                acquired = True
                break
            except OSError as exc:
                if not is_windows_lock_contention_error(exc):
                    raise ToolWriteError(
                        f"エラー: write lock の取得に失敗しました: {lock_path}: {exc}"
                    ) from exc
                if time.monotonic() - start_time >= WRITE_LOCK_TIMEOUT_SECONDS:
                    raise ToolWriteError(
                        "エラー: write lock の取得がタイムアウトしました。"
                        f" 対象: {lock_path}"
                    ) from exc
                time.sleep(WRITE_LOCK_POLL_INTERVAL_SECONDS)

        try:
            yield lock_path
        finally:
            if acquired:
                try:
                    os.lseek(lock_fd, 0, os.SEEK_SET)
                    msvcrt.locking(lock_fd, msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
    finally:
        os.close(lock_fd)


class ToolCallBudget:
    """OCR 修正モード用の tool 呼び出し回数上限を管理する。

    注意: このクラスは read / write tool の実行回数だけを制限します。
    API 呼び出し回数（ターン数）の上限は別途必要です（P0 follow-up）。
    """

    def __init__(self, limit: int = DEFAULT_MAX_TOOL_CALLS_PER_RUN):
        if limit <= 0:
            raise ValueError("tool call limit must be positive")
        self.limit = limit
        self.total_calls = 0

    @property
    def remaining_calls(self) -> int:
        return self.limit - self.total_calls

    def consume(self, tool_name: str) -> None:
        if self.total_calls >= self.limit:
            raise ToolCallLimitError(
                "エラー: tool 呼び出し回数が上限を超えました。"
                f" 上限: {self.limit}, tool: {tool_name}"
            )
        self.total_calls += 1


def read_tool_text_limited(raw_path: str, budget: ToolCallBudget) -> str:
    """tool 呼び出し回数を消費しつつ read を実行する。"""
    budget.consume("read")
    return read_tool_text(raw_path)


def write_tool_text_limited(
    raw_path: str,
    expected_old_text: str,
    new_text: str,
    budget: ToolCallBudget,
) -> Path:
    """tool 呼び出し回数を消費しつつ write を実行する。"""
    budget.consume("write")
    return write_tool_text(raw_path, expected_old_text, new_text)


def write_tool_text(raw_path: str, expected_old_text: str, new_text: str) -> Path:
    """write tool 用: work/ 配下へ 1 箇所一致の置換だけを反映する。"""
    normalized_input = (raw_path or "").strip()
    candidate_path = Path(normalized_input).expanduser()

    # 相対パスで先頭セグメントが md/pdf 領域を指す場合は事前に拒否
    if not candidate_path.is_absolute() and candidate_path.parts:
        head = candidate_path.parts[0]
        if head in ("md", "pdf"):
            raise ToolWriteError(f"エラー: write tool は work/ 以外へ書けません: {raw_path}")

    normalized = _normalize_tool_relative_path(raw_path, {
        "work": get_manual_work_dirs(),
    })
    try:
        resolved_path = resolve_tool_path(normalized, get_manual_work_dirs(), "write path")
    except ToolPathResolutionError as exc:
        raise ToolWriteError(str(exc)) from exc

    resolved_work_dirs = [work_dir.expanduser().resolve() for work_dir in get_manual_work_dirs()]

    # md/ と pdf/ への書き込みを禁止（絶対パスや symlink による回避も検出）
    for md_dir in get_manual_markdown_dirs():
        resolved_md_dir = md_dir.expanduser().resolve()
        overlaps_work_dir = any(
            directories_overlap(resolved_md_dir, work_dir) for work_dir in resolved_work_dirs
        )
        if not overlaps_work_dir and is_path_within_directory(resolved_path, resolved_md_dir):
            raise ToolWriteError(f"エラー: write tool は work/ 以外へ書けません: {resolved_path}")
    for pdf_dir in get_manual_pdf_dirs():
        resolved_pdf_dir = pdf_dir.expanduser().resolve()
        overlaps_work_dir = any(
            directories_overlap(resolved_pdf_dir, work_dir) for work_dir in resolved_work_dirs
        )
        if not overlaps_work_dir and is_path_within_directory(resolved_path, resolved_pdf_dir):
            raise ToolWriteError(f"エラー: write tool は work/ 以外へ書けません: {resolved_path}")

    if resolved_path.exists() and not resolved_path.is_file():
        raise ToolWriteError(f"エラー: write 対象パスがファイルではありません: {resolved_path}")
    if not resolved_path.exists():
        raise ToolWriteError(f"エラー: write 対象ファイルが見つかりません: {resolved_path}")

    with acquire_write_lock(build_write_lock_path(resolved_path)):
        try:
            current_text = resolved_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ToolWriteError(
                f"エラー: write tool のファイル読取に失敗しました: {resolved_path}: {exc}"
            ) from exc

        normalized_current_text = normalize_tool_text(current_text)
        normalized_expected_text = normalize_tool_text(expected_old_text)
        normalized_new_text = normalize_tool_text(new_text)

        match_result = find_unique_normalized_match(
            normalized_current_text,
            normalized_expected_text,
        )
        if match_result.kind is MatchResultKind.MISMATCH:
            raise ToolWriteError(
                "エラー: expected_old_text が一致しません。"
                f" 対象: {resolved_path}"
            )
        if match_result.kind is MatchResultKind.AMBIGUOUS:
            raise ToolWriteError(
                "エラー: expected_old_text が複数箇所に一致しました。"
                f" 対象: {resolved_path}"
            )

        replaced_text = (
            normalized_current_text[:match_result.index]
            + normalized_new_text
            + normalized_current_text[match_result.index + len(normalized_expected_text) :]
        )

        try:
            with resolved_path.open("w", encoding="utf-8", newline="\n") as output_file:
                output_file.write(replaced_text)
        except OSError as exc:
            raise ToolWriteError(
                f"エラー: write tool のファイル書込に失敗しました: {resolved_path}: {exc}"
            ) from exc

    return resolved_path


def max_tool_calls_arg_type(value: str) -> int:
    """argparse 用: OCR 修正モードの read/write 合算回数上限。"""
    try:
        n = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"整数が必要です: {value!r}"
        ) from exc
    if n < MIN_MAX_TOOL_CALLS_PER_RUN or n > MAX_TOOL_CALLS_CAP:
        raise argparse.ArgumentTypeError(
            f"--max-tool-calls は {MIN_MAX_TOOL_CALLS_PER_RUN} 以上 {MAX_TOOL_CALLS_CAP} 以下にしてください。"
        )
    return n
