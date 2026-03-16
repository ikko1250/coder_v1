from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
from typing import Any

import polars as pl

from .cli import _build_failure_payload
from .cli import _build_json_response_payload
from .cli import _serialize_warning_messages
from .condition_model import DataAccessResult
from .data_access import read_analysis_sentences_result as _read_analysis_sentences_result_impl
from .data_access import read_analysis_tokens_result as _read_analysis_tokens_result_impl


@dataclass
class CachedFrames:
    db_path: Path
    modified_time_ns: int
    tokens_df: pl.DataFrame
    sentences_df: pl.DataFrame


class WorkerCache:
    def __init__(self) -> None:
        self._entries: dict[str, CachedFrames] = {}

    def invalidate(self, db_path: Path | None = None) -> None:
        if db_path is None:
            self._entries.clear()
            return
        self._entries.pop(str(db_path.resolve()), None)

    def get_or_load(self, db_path: Path, *, force_reload: bool = False) -> CachedFrames:
        resolved_db_path = db_path.expanduser().resolve()
        current_mtime_ns = resolved_db_path.stat().st_mtime_ns
        cache_key = str(resolved_db_path)
        cache_entry = self._entries.get(cache_key)
        if (
            not force_reload
            and cache_entry is not None
            and cache_entry.modified_time_ns == current_mtime_ns
        ):
            return cache_entry

        tokens_result = _read_analysis_tokens_result_impl(db_path=resolved_db_path, limit_rows=None)
        if tokens_result.data_frame is None:
            issue = tokens_result.issues[0]
            raise RuntimeError(issue.message)
        sentences_result = _read_analysis_sentences_result_impl(
            db_path=resolved_db_path,
            limit_rows=None,
        )
        if sentences_result.data_frame is None:
            issue = sentences_result.issues[0]
            raise RuntimeError(issue.message)

        cache_entry = CachedFrames(
            db_path=resolved_db_path,
            modified_time_ns=current_mtime_ns,
            tokens_df=tokens_result.data_frame,
            sentences_df=sentences_result.data_frame,
        )
        self._entries[cache_key] = cache_entry
        return cache_entry


def _read_exact(stream: io.BufferedReader, byte_count: int) -> bytes:
    buffer = bytearray()
    while len(buffer) < byte_count:
        chunk = stream.read(byte_count - len(buffer))
        if not chunk:
            raise EOFError("worker input stream closed")
        buffer.extend(chunk)
    return bytes(buffer)


def _read_message(stream: io.BufferedReader) -> dict[str, Any]:
    length_bytes = _read_exact(stream, 4)
    payload_length = struct.unpack(">I", length_bytes)[0]
    payload_bytes = _read_exact(stream, payload_length)
    return json.loads(payload_bytes.decode("utf-8"))


def _write_message(stream: io.BufferedWriter, payload: dict[str, Any]) -> None:
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    stream.write(struct.pack(">I", len(payload_bytes)))
    stream.write(payload_bytes)
    stream.flush()


@contextmanager
def _patched_cached_frames(cache_entry: CachedFrames):
    from . import analysis_core

    original_tokens_result = analysis_core.read_analysis_tokens_result
    original_sentences_result = analysis_core.read_analysis_sentences_result

    def _resolve_requested_db_path(db_path: Path) -> Path:
        return db_path.expanduser().resolve()

    def _cached_tokens_result(
        db_path: Path,
        limit_rows: int | None = None,
        **_: Any,
    ) -> DataAccessResult:
        if _resolve_requested_db_path(db_path) != cache_entry.db_path:
            raise RuntimeError(f"cached tokens requested for unexpected db_path: {db_path}")
        data_frame = cache_entry.tokens_df
        if limit_rows is not None:
            data_frame = data_frame.head(limit_rows)
        return DataAccessResult(data_frame=data_frame.clone(), issues=[])

    def _cached_sentences_result(
        db_path: Path,
        limit_rows: int | None = None,
        **_: Any,
    ) -> DataAccessResult:
        if _resolve_requested_db_path(db_path) != cache_entry.db_path:
            raise RuntimeError(f"cached sentences requested for unexpected db_path: {db_path}")
        data_frame = cache_entry.sentences_df
        if limit_rows is not None:
            data_frame = data_frame.head(limit_rows)
        return DataAccessResult(data_frame=data_frame.clone(), issues=[])

    analysis_core.read_analysis_tokens_result = _cached_tokens_result
    analysis_core.read_analysis_sentences_result = _cached_sentences_result
    try:
        yield
    finally:
        analysis_core.read_analysis_tokens_result = original_tokens_result
        analysis_core.read_analysis_sentences_result = original_sentences_result


def _build_request_failure_response(
    *,
    request_id: str,
    job_id: str,
    db_path: Path,
    filter_config_path: Path,
    error_summary: str,
) -> dict[str, Any]:
    failure_payload = _build_failure_payload(
        job_id=job_id,
        started_at="",
        finished_at="",
        duration_seconds=0.0,
        db_path=db_path,
        filter_config_path=filter_config_path,
        output_csv_path=Path(""),
        warning_messages=[],
        error_summary=error_summary,
    )
    return {
        "requestId": request_id,
        "status": "failed",
        "meta": failure_payload,
        "records": [],
    }


def _handle_analyze_request(
    request: dict[str, Any],
    cache: WorkerCache,
) -> dict[str, Any]:
    request_id = str(request.get("requestId", "")).strip()
    job_id = str(request.get("jobId", "")).strip()
    db_path = Path(str(request.get("dbPath", ""))).expanduser()
    filter_config_path = Path(str(request.get("filterConfigPath", ""))).expanduser()
    raw_annotation_csv_path = str(request.get("annotationCsvPath", "")).strip()
    annotation_csv_path = (
        Path(raw_annotation_csv_path).expanduser() if raw_annotation_csv_path else None
    )
    limit_rows = request.get("limitRows")
    force_reload = bool(request.get("forceReload", False))

    try:
        cache_entry = cache.get_or_load(db_path=db_path, force_reload=force_reload)
    except Exception as exc:
        return _build_request_failure_response(
            request_id=request_id,
            job_id=job_id,
            db_path=db_path,
            filter_config_path=filter_config_path,
            error_summary=str(exc),
        )

    from .cli import run_analysis_job

    with tempfile.TemporaryDirectory(prefix="csv-viewer-worker-") as temp_dir:
        namespace = Namespace(
            job_id=job_id,
            db_path=str(db_path),
            filter_config_path=str(filter_config_path),
            annotation_csv_path=str(annotation_csv_path) if annotation_csv_path else None,
            output_dir=temp_dir,
            output_csv_path=None,
            output_meta_json_path=None,
            limit_rows=limit_rows,
            output_format="json",
        )
        stdout_buffer = io.StringIO()
        with _patched_cached_frames(cache_entry), redirect_stdout(stdout_buffer):
            exit_code = run_analysis_job(namespace)
        payload_text = stdout_buffer.getvalue().strip()

    if not payload_text:
        return _build_request_failure_response(
            request_id=request_id,
            job_id=job_id,
            db_path=db_path,
            filter_config_path=filter_config_path,
            error_summary="worker received empty JSON payload",
        )

    response_payload = json.loads(payload_text)
    response_payload["requestId"] = request_id
    response_payload["status"] = "succeeded" if exit_code == 0 else "failed"
    return response_payload


def _handle_export_csv_request(
    request: dict[str, Any],
    cache: WorkerCache,
) -> dict[str, Any]:
    request_id = str(request.get("requestId", "")).strip()
    job_id = str(request.get("jobId", "")).strip()
    db_path = Path(str(request.get("dbPath", ""))).expanduser()
    filter_config_path = Path(str(request.get("filterConfigPath", ""))).expanduser()
    raw_annotation_csv_path = str(request.get("annotationCsvPath", "")).strip()
    annotation_csv_path = (
        Path(raw_annotation_csv_path).expanduser() if raw_annotation_csv_path else None
    )
    output_path = Path(str(request.get("outputPath", ""))).expanduser().resolve()
    force_reload = bool(request.get("forceReload", False))

    try:
        cache_entry = cache.get_or_load(db_path=db_path, force_reload=force_reload)
    except Exception as exc:
        return _build_request_failure_response(
            request_id=request_id,
            job_id=job_id,
            db_path=db_path,
            filter_config_path=filter_config_path,
            error_summary=str(exc),
        )

    from .cli import run_analysis_job

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="csv-viewer-worker-export-") as temp_dir:
        meta_json_path = Path(temp_dir) / "meta.json"
        namespace = Namespace(
            job_id=job_id,
            db_path=str(db_path),
            filter_config_path=str(filter_config_path),
            annotation_csv_path=str(annotation_csv_path) if annotation_csv_path else None,
            output_dir=temp_dir,
            output_csv_path=str(output_path),
            output_meta_json_path=str(meta_json_path),
            limit_rows=None,
            output_format="csv",
        )
        stdout_buffer = io.StringIO()
        with _patched_cached_frames(cache_entry), redirect_stdout(stdout_buffer):
            exit_code = run_analysis_job(namespace)
        payload_text = stdout_buffer.getvalue().strip()

    if not payload_text:
        return _build_request_failure_response(
            request_id=request_id,
            job_id=job_id,
            db_path=db_path,
            filter_config_path=filter_config_path,
            error_summary="worker received empty export payload",
        )

    response_payload = json.loads(payload_text)
    return {
        "requestId": request_id,
        "status": "succeeded" if exit_code == 0 else "failed",
        "meta": response_payload,
        "records": [],
        "message": "",
    }


def _handle_health_request(request: dict[str, Any]) -> dict[str, Any]:
    return {
        "requestId": str(request.get("requestId", "")),
        "status": "ok",
    }


def _handle_invalidate_cache_request(
    request: dict[str, Any],
    cache: WorkerCache,
) -> dict[str, Any]:
    raw_db_path = request.get("dbPath")
    if raw_db_path:
        cache.invalidate(Path(str(raw_db_path)))
    else:
        cache.invalidate()
    return {
        "requestId": str(request.get("requestId", "")),
        "status": "ok",
    }


def _handle_request(request: dict[str, Any], cache: WorkerCache) -> tuple[dict[str, Any], bool]:
    request_type = str(request.get("requestType", "")).strip().lower()
    if request_type == "analyze":
        return _handle_analyze_request(request, cache), False
    if request_type == "export_csv":
        return _handle_export_csv_request(request, cache), False
    if request_type == "health":
        return _handle_health_request(request), False
    if request_type == "invalidate_cache":
        return _handle_invalidate_cache_request(request, cache), False
    if request_type == "shutdown":
        return {
            "requestId": str(request.get("requestId", "")),
            "status": "ok",
        }, True

    request_id = str(request.get("requestId", ""))
    message = f"unknown requestType: {request_type}"
    response_payload = {
        "requestId": request_id,
        "status": "failed",
        "meta": _build_json_response_payload(
            meta={
                "jobId": "",
                "status": "failed",
                "startedAt": "",
                "finishedAt": "",
                "durationSeconds": 0.0,
                "dbPath": "",
                "filterConfigPath": "",
                "outputCsvPath": "",
                "targetParagraphCount": 0,
                "selectedParagraphCount": 0,
                "warningMessages": _serialize_warning_messages([]),
                "errorSummary": message,
            }
        )["meta"],
        "records": [],
    }
    return response_payload, False


def main() -> int:
    cache = WorkerCache()
    reader = sys.stdin.buffer
    writer = sys.stdout.buffer

    while True:
        try:
            request = _read_message(reader)
        except EOFError:
            return 0
        except Exception as exc:
            print(f"worker request read failed: {exc}", file=sys.stderr)
            return 1

        try:
            response_payload, should_shutdown = _handle_request(request, cache)
        except Exception as exc:
            request_id = str(request.get("requestId", ""))
            response_payload = {
                "requestId": request_id,
                "status": "failed",
                "meta": {
                    "jobId": str(request.get("jobId", "")),
                    "status": "failed",
                    "startedAt": "",
                    "finishedAt": "",
                    "durationSeconds": 0.0,
                    "dbPath": str(request.get("dbPath", "")),
                    "filterConfigPath": str(request.get("filterConfigPath", "")),
                    "outputCsvPath": "",
                    "targetParagraphCount": 0,
                    "selectedParagraphCount": 0,
                    "warningMessages": [],
                    "errorSummary": str(exc),
                },
                "records": [],
            }
            should_shutdown = False
            print(f"worker request handling failed: {exc}", file=sys.stderr)

        try:
            _write_message(writer, response_payload)
        except Exception as exc:
            print(f"worker response write failed: {exc}", file=sys.stderr)
            return 1

        if should_shutdown:
            return 0
