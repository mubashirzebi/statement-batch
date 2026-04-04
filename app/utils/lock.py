import json
import os
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Optional

if os.name == "nt":
    import ctypes
    import ctypes.wintypes


class JobLock:
    def __init__(self, lock_path: Path, stale_seconds: int = 3600, logger=None):
        self.lock_path = Path(lock_path)
        self.stale_seconds = int(stale_seconds)
        self.logger = logger
        self.fd = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if not self._break_stale_lock():
                raise RuntimeError("Another batch job instance is already running: %s" % self.lock_path)
            self.fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)

        payload = {
            "pid": os.getpid(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        os.write(self.fd, json.dumps(payload).encode("utf-8"))
        os.fsync(self.fd)

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.lock_path.exists():
            self.lock_path.unlink()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def _break_stale_lock(self) -> bool:
        payload = self._read_payload()
        pid = payload.get("pid") if isinstance(payload, dict) else None
        created_at = self._parse_created_at(payload.get("created_at")) if isinstance(payload, dict) else None

        if isinstance(pid, int) and self._pid_exists(pid):
            if self.logger:
                self.logger.warning("lock file already held by live pid=%s", pid)
            return False

        if created_at is None:
            age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - self.lock_path.stat().st_mtime)
        else:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds())

        if created_at is None and age_seconds < self.stale_seconds:
            if self.logger:
                self.logger.warning(
                    "lock file exists but is too new to treat as stale: %s",
                    self.lock_path,
                )
            return False

        if self.logger:
            self.logger.warning(
                "removing stale lock file %s (pid=%s age_seconds=%.0f)",
                self.lock_path,
                pid,
                age_seconds,
            )
        self.lock_path.unlink(missing_ok=True)
        return True

    def _read_payload(self) -> dict:
        try:
            with self.lock_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, TypeError, ValueError):
            return {}

    @staticmethod
    def _parse_created_at(raw_value: Optional[str]) -> Optional[datetime]:
        if not raw_value:
            return None
        try:
            return datetime.fromisoformat(raw_value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        if pid <= 0:
            return False

        if os.name == "nt":
            process_handle = ctypes.windll.kernel32.OpenProcess(
                0x1000,
                False,
                ctypes.wintypes.DWORD(pid),
            )
            if not process_handle:
                return False
            ctypes.windll.kernel32.CloseHandle(process_handle)
            return True

        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
