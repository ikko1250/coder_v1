from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path

import polars as pl

from analysis_backend import analysis_core
from analysis_backend.worker import CachedFrames
from analysis_backend.worker import WorkerCache
from analysis_backend.worker import _patched_cached_frames
from analysis_backend.worker import _read_message
from analysis_backend.worker import _write_message


class WorkerProtocolTest(unittest.TestCase):
    def test_framed_message_roundtrip(self) -> None:
        buffer = io.BytesIO()
        payload = {
            "requestId": "job-1",
            "requestType": "health",
        }

        _write_message(buffer, payload)
        buffer.seek(0)

        self.assertEqual(_read_message(buffer), payload)


class WorkerCacheTest(unittest.TestCase):
    def test_invalidate_removes_specific_entry(self) -> None:
        cache = WorkerCache()
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "sample.db"
            db_path.write_text("", encoding="utf-8")
            cache._entries[str(db_path.resolve())] = CachedFrames(
                db_path=db_path.resolve(),
                modified_time_ns=db_path.stat().st_mtime_ns,
                tokens_df=pl.DataFrame({"paragraph_id": [1]}),
                sentences_df=pl.DataFrame({"sentence_id": [1]}),
            )

            cache.invalidate(db_path)

            self.assertEqual(cache._entries, {})


class WorkerPatchedFramesTest(unittest.TestCase):
    def test_patched_cached_frames_accepts_keyword_db_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "sample.db"
            db_path.write_text("", encoding="utf-8")
            cache_entry = CachedFrames(
                db_path=db_path.resolve(),
                modified_time_ns=db_path.stat().st_mtime_ns,
                tokens_df=pl.DataFrame({"paragraph_id": [1, 2]}),
                sentences_df=pl.DataFrame({"sentence_id": [10, 20]}),
            )

            with _patched_cached_frames(cache_entry):
                tokens_result = analysis_core.read_analysis_tokens_result(
                    db_path=db_path,
                    limit_rows=1,
                )
                sentences_result = analysis_core.read_analysis_sentences_result(
                    db_path=db_path,
                    limit_rows=1,
                )

            self.assertEqual(tokens_result.data_frame.to_dicts(), [{"paragraph_id": 1}])
            self.assertEqual(sentences_result.data_frame.to_dicts(), [{"sentence_id": 10}])


if __name__ == "__main__":
    unittest.main()
