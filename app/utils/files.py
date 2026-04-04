import re
import shutil
import uuid
from pathlib import Path
from typing import Optional


class FileMoveError(RuntimeError):
    def __init__(self, message: str, recovery_path: Optional[Path] = None):
        super().__init__(message)
        self.recovery_path = Path(recovery_path) if recovery_path else None


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_runtime_directories(config) -> None:
    for path in (config.success_dir, config.failed_dir, config.log_dir, config.outbox_dir):
        ensure_directory(Path(path))


def move_to_path(source: Path, destination: Path) -> Path:
    source = Path(source)
    destination = Path(destination)
    ensure_directory(destination.parent)
    if destination.exists() and destination.is_dir():
        raise IsADirectoryError("Destination path is a directory: %s" % destination)

    if destination.exists():
        temp_destination = destination.with_name(
            ".%s.%s.tmp" % (destination.name, uuid.uuid4().hex)
        )
        try:
            shutil.move(str(source), str(temp_destination))
        except Exception as exc:
            raise FileMoveError(
                "failed staging file at %s: %s" % (temp_destination, exc),
                recovery_path=temp_destination if temp_destination.exists() else None,
            ) from exc
        try:
            temp_destination.replace(destination)
        except Exception as exc:
            raise FileMoveError(
                "staged file at %s but failed replacing %s: %s" % (temp_destination, destination, exc),
                recovery_path=temp_destination,
            ) from exc
    else:
        try:
            shutil.move(str(source), str(destination))
        except Exception as exc:
            raise FileMoveError(
                "failed moving file to %s: %s" % (destination, exc),
                recovery_path=destination if destination.exists() else None,
            ) from exc
    return destination


def build_success_destination(success_root: Path, fy_years: str, file_name: str) -> Path:
    fy_component = sanitize_path_component(fy_years or "UNKNOWN")
    return Path(success_root) / fy_component / file_name


def build_failed_destination(failed_root: Path, fy_years: str, reason: str, file_name: str) -> Path:
    fy_component = sanitize_path_component(fy_years or "UNKNOWN")
    reason_component = sanitize_path_component(reason or "UNKNOWN_REASON")
    return Path(failed_root) / fy_component / reason_component / file_name


def file_size_string(path: Path) -> str:
    try:
        return str(path.stat().st_size)
    except OSError:
        return ""


def file_extension(path: Path) -> str:
    return path.suffix.lstrip(".").lower()


def normalize_db_file_name(file_name: str) -> str:
    path = Path(file_name)
    suffix = path.suffix
    if not suffix:
        return file_name
    return "%s%s" % (file_name[: -len(suffix)], suffix.upper())


def extract_fy_years(file_name: str, fy_pattern: str) -> str:
    match = re.search(fy_pattern, file_name)
    if not match:
        return "UNKNOWN"
    if match.lastindex and match.lastindex >= 2:
        return "%s_%s" % (match.group(1), match.group(2))
    return match.group(0)


def sanitize_path_component(value: str) -> str:
    sanitized = re.sub(r"[<>:\"/\\|?*]+", "_", value.strip())
    return sanitized[:100] or "UNKNOWN"
