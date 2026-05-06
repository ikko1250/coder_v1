import errno
import importlib
import os
import shutil
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_NAME = "pdf_converter.call_gemma4_gemini"
import pdf_converter.ocr_tools as ocr_tools_module


class CallGemma4GeminiWriteLockTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = importlib.import_module(MODULE_NAME)

    def setUp(self):
        root = REPO_ROOT / ".tmp_write_lock_test"
        root.mkdir(exist_ok=True)
        self.temp_path = root / self._testMethodName
        if self.temp_path.exists():
            shutil.rmtree(self.temp_path, ignore_errors=True)
        self.temp_path.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.temp_path, ignore_errors=True)

    def test_module_imports_without_fcntl_mock(self):
        module = importlib.import_module(MODULE_NAME)

        self.assertTrue(callable(module.acquire_write_lock))
        self.assertTrue(callable(module.acquire_windows_write_lock))
        self.assertTrue(callable(module.acquire_unix_write_lock))

    def test_build_write_lock_path_appends_lock_suffix(self):
        target_path = self.temp_path / "working.md"

        lock_path = self.module.build_write_lock_path(target_path)

        self.assertEqual(lock_path, self.temp_path / "working.md.lock")

    def test_windows_contention_helper_accepts_observed_eacces(self):
        exc = PermissionError(errno.EACCES, "Permission denied")

        self.assertTrue(self.module.is_windows_lock_contention_error(exc))

    def test_windows_contention_helper_accepts_winerror_33(self):
        exc = OSError("lock violation")
        exc.winerror = 33

        self.assertTrue(self.module.is_windows_lock_contention_error(exc))

    def test_windows_contention_helper_rejects_other_os_errors(self):
        exc = OSError(errno.EBADF, "Bad file descriptor")

        self.assertFalse(self.module.is_windows_lock_contention_error(exc))

    def test_windows_backend_locks_and_unlocks_first_byte(self):
        lock_path = self.temp_path / "target.md.lock"
        calls = []
        fake_msvcrt = SimpleNamespace(
            LK_NBLCK=10,
            LK_UNLCK=11,
            locking=lambda fd, mode, nbytes: calls.append((mode, nbytes)),
        )

        with mock.patch.dict(sys.modules, {"msvcrt": fake_msvcrt}):
            with self.module.acquire_windows_write_lock(lock_path) as acquired_path:
                self.assertEqual(acquired_path, lock_path)

        self.assertEqual(calls, [(fake_msvcrt.LK_NBLCK, 1), (fake_msvcrt.LK_UNLCK, 1)])

    def test_windows_backend_retries_contention_until_timeout(self):
        lock_path = self.temp_path / "target.md.lock"
        calls = []

        def raise_contention(fd, mode, nbytes):
            calls.append((mode, nbytes))
            raise PermissionError(errno.EACCES, "Permission denied")

        fake_msvcrt = SimpleNamespace(
            LK_NBLCK=10,
            LK_UNLCK=11,
            locking=raise_contention,
        )
        original_timeout = self.module.WRITE_LOCK_TIMEOUT_SECONDS
        original_interval = self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS
        original_ocr_timeout = ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS
        original_ocr_interval = ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS
        self.module.WRITE_LOCK_TIMEOUT_SECONDS = 0.05
        self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS = 0.01
        ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS = 0.05
        ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS = 0.01
        try:
            with mock.patch.dict(sys.modules, {"msvcrt": fake_msvcrt}):
                with self.assertRaises(self.module.ToolWriteError) as cm:
                    with self.module.acquire_windows_write_lock(lock_path):
                        pass
            self.assertIn("タイムアウト", str(cm.exception))
            self.assertGreaterEqual(len(calls), 2)
        finally:
            self.module.WRITE_LOCK_TIMEOUT_SECONDS = original_timeout
            self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS = original_interval
            ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS = original_ocr_timeout
            ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS = original_ocr_interval

    def test_windows_backend_does_not_retry_non_contention_error(self):
        lock_path = self.temp_path / "target.md.lock"
        calls = []

        def raise_bad_fd(fd, mode, nbytes):
            calls.append((mode, nbytes))
            raise OSError(errno.EBADF, "Bad file descriptor")

        fake_msvcrt = SimpleNamespace(
            LK_NBLCK=10,
            LK_UNLCK=11,
            locking=raise_bad_fd,
        )
        with (
            mock.patch.dict(sys.modules, {"msvcrt": fake_msvcrt}),
            mock.patch.object(ocr_tools_module.time, "sleep") as sleep_mock,
        ):
            with self.assertRaises(self.module.ToolWriteError) as cm:
                with self.module.acquire_windows_write_lock(lock_path):
                    pass

        self.assertIn("取得に失敗", str(cm.exception))
        self.assertEqual(len(calls), 1)
        sleep_mock.assert_not_called()

    def test_unix_backend_locks_and_unlocks(self):
        lock_path = self.temp_path / "target.md.lock"
        calls = []

        def fake_flock(fd, operation):
            calls.append(operation)

        fake_fcntl = SimpleNamespace(
            LOCK_EX=1,
            LOCK_NB=2,
            LOCK_UN=4,
            flock=fake_flock,
        )
        with mock.patch.dict(sys.modules, {"fcntl": fake_fcntl}):
            with self.module.acquire_unix_write_lock(lock_path) as acquired_path:
                self.assertEqual(acquired_path, lock_path)

        self.assertEqual(calls, [fake_fcntl.LOCK_EX | fake_fcntl.LOCK_NB, fake_fcntl.LOCK_UN])

    def test_subprocess_lock_contention_times_out(self):
        lock_path = self.temp_path / "target.md.lock"
        child_code = textwrap.dedent(
            """
            import sys
            import time
            from pathlib import Path
            import pdf_converter.call_gemma4_gemini as module
            import pdf_converter.ocr_tools as ocr_tools

            lock_path = Path(sys.argv[1])
            with module.acquire_write_lock(lock_path):
                print("READY", flush=True)
                time.sleep(2.0)
            """
        )
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        proc = subprocess.Popen(
            [sys.executable, "-c", child_code, str(lock_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        original_timeout = self.module.WRITE_LOCK_TIMEOUT_SECONDS
        original_interval = self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS
        original_ocr_timeout = ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS
        original_ocr_interval = ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS
        try:
            ready_line = proc.stdout.readline().strip()
            if ready_line != "READY":
                stdout, stderr = proc.communicate(timeout=5)
                self.fail(
                    "child process did not acquire lock: "
                    f"ready={ready_line!r}, stdout={stdout!r}, stderr={stderr!r}"
                )

            self.module.WRITE_LOCK_TIMEOUT_SECONDS = 0.5
            self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS = 0.05
            ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS = 0.5
            ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS = 0.05
            with self.assertRaises(self.module.ToolWriteError) as cm:
                with self.module.acquire_write_lock(lock_path):
                    pass
            self.assertIn("タイムアウト", str(cm.exception))
        finally:
            self.module.WRITE_LOCK_TIMEOUT_SECONDS = original_timeout
            self.module.WRITE_LOCK_POLL_INTERVAL_SECONDS = original_interval
            ocr_tools_module.WRITE_LOCK_TIMEOUT_SECONDS = original_ocr_timeout
            ocr_tools_module.WRITE_LOCK_POLL_INTERVAL_SECONDS = original_ocr_interval
            try:
                stdout, stderr = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate(timeout=5)
                self.fail(f"child process did not exit: stdout={stdout!r}, stderr={stderr!r}")

        self.assertEqual(proc.returncode, 0, stderr)


if __name__ == "__main__":
    unittest.main()
