import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency for local env files
    def load_dotenv(*_args, **_kwargs):
        return False


COMMAND_RUN = "run"
COMMAND_DRY_RUN = "dry-run"
COMMAND_DB_CHECK = "db-check"
COMMAND_S3_CHECK = "s3-check"
COMMAND_SECRETS_CHECK = "secrets-check"

SECRET_MODE_ENV = "env"
SECRET_MODE_SECRETS_MANAGER = "secrets_manager"
ORACLE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*(\.[A-Za-z][A-Za-z0-9_$#]*)?$")


@dataclass
class AppConfig:
    batch_env: str
    secret_mode: str
    aws_region: str
    aws_profile: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    db_secret_name: str
    s3_secret_name: str
    db_username: str
    db_password: str
    db_dsn: str
    oracle_client_lib_dir: Optional[Path]
    s3_bucket: str
    s3_endpoint_url: str
    input_dir: Path
    success_dir: Path
    failed_dir: Path
    batch_size: int
    worker_count: int
    queue_size: int
    max_retry: int
    retry_base_seconds: float
    fy_regex: str
    package_name: str
    doc_sequence: str
    finalize_rec_type: str
    finalize_tab_type: str
    move_rec_type: str
    move_tab_type: str
    db_pool_min: int
    db_pool_max: int
    db_pool_increment: int
    log_dir: Path
    outbox_dir: Path
    lock_file: Path
    lock_stale_seconds: int
    log_level: str
    log_max_bytes: int
    log_backup_count: int

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            batch_env=os.getenv("BATCH_ENV", "dev").strip() or "dev",
            secret_mode=os.getenv("BATCH_SECRET_MODE", SECRET_MODE_ENV).strip() or SECRET_MODE_ENV,
            aws_region=os.getenv("AWS_REGION", "").strip(),
            aws_profile=os.getenv("AWS_PROFILE", "").strip(),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "").strip(),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "").strip(),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN", "").strip(),
            db_secret_name=os.getenv("DB_SECRET_NAME", "").strip(),
            s3_secret_name=os.getenv("S3_SECRET_NAME", "").strip(),
            db_username=os.getenv("DB_USERNAME", "").strip(),
            db_password=os.getenv("DB_PASSWORD", "").strip(),
            db_dsn=os.getenv("DB_DSN", "").strip(),
            oracle_client_lib_dir=_env_path("ORACLE_CLIENT_LIB_DIR"),
            s3_bucket=os.getenv("S3_BUCKET", "").strip(),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", "").strip(),
            input_dir=Path(os.getenv("BATCH_INPUT_DIR", "./input")).expanduser(),
            success_dir=Path(os.getenv("BATCH_SUCCESS_DIR", "./success")).expanduser(),
            failed_dir=Path(os.getenv("BATCH_FAILED_DIR", "./failed")).expanduser(),
            batch_size=_env_int("BATCH_BATCH_SIZE", 500),
            worker_count=_env_int("BATCH_WORKER_COUNT", 12),
            queue_size=_env_int("BATCH_QUEUE_SIZE", 24),
            max_retry=_env_int("BATCH_MAX_RETRY", 3),
            retry_base_seconds=_env_float("BATCH_RETRY_BASE_SECONDS", 2.0),
            fy_regex=os.getenv("BATCH_FY_REGEX", r"(?i)_FY_(\d{4})_(\d{4})\.pdf$").strip(),
            package_name=os.getenv("BATCH_PACKAGE_NAME", "FINACLE_BATCH_PKG").strip(),
            doc_sequence=os.getenv("BATCH_DOC_SEQUENCE", "FIN_YEARLY_STMT_DOCID_SEQUENCE").strip(),
            finalize_rec_type=os.getenv("BATCH_FINALIZE_REC_TYPE", "T_FINALIZE_REC").strip(),
            finalize_tab_type=os.getenv("BATCH_FINALIZE_TAB_TYPE", "T_FINALIZE_TAB").strip(),
            move_rec_type=os.getenv("BATCH_MOVE_REC_TYPE", "T_MOVE_REC").strip(),
            move_tab_type=os.getenv("BATCH_MOVE_TAB_TYPE", "T_MOVE_TAB").strip(),
            db_pool_min=_env_int("BATCH_DB_POOL_MIN", 2),
            db_pool_max=_env_int("BATCH_DB_POOL_MAX", 12),
            db_pool_increment=_env_int("BATCH_DB_POOL_INCREMENT", 1),
            log_dir=Path(os.getenv("BATCH_LOG_DIR", "./logs")).expanduser(),
            outbox_dir=Path(os.getenv("BATCH_OUTBOX_DIR", "./outbox")).expanduser(),
            lock_file=Path(os.getenv("BATCH_LOCK_FILE", "./job.lock")).expanduser(),
            lock_stale_seconds=_env_int("BATCH_LOCK_STALE_SECONDS", 3600),
            log_level=os.getenv("BATCH_LOG_LEVEL", "INFO").strip() or "INFO",
            log_max_bytes=_env_int("BATCH_LOG_MAX_BYTES", 5_000_000),
            log_backup_count=_env_int("BATCH_LOG_BACKUP_COUNT", 5),
        )

    def validate_for_command(self, command: str) -> None:
        if self.batch_size <= 0:
            raise ValueError("BATCH_BATCH_SIZE must be greater than 0")
        if self.worker_count <= 0:
            raise ValueError("BATCH_WORKER_COUNT must be greater than 0")
        if self.queue_size <= 0:
            raise ValueError("BATCH_QUEUE_SIZE must be greater than 0")
        if self.max_retry < 0:
            raise ValueError("BATCH_MAX_RETRY cannot be negative")
        if self.retry_base_seconds <= 0:
            raise ValueError("BATCH_RETRY_BASE_SECONDS must be greater than 0")
        if self.lock_stale_seconds <= 0:
            raise ValueError("BATCH_LOCK_STALE_SECONDS must be greater than 0")
        if self.db_pool_min <= 0 or self.db_pool_max <= 0 or self.db_pool_increment <= 0:
            raise ValueError("Database pool values must be greater than 0")
        if self.db_pool_min > self.db_pool_max:
            raise ValueError("BATCH_DB_POOL_MIN cannot be greater than BATCH_DB_POOL_MAX")
        if not self.package_name:
            raise ValueError("BATCH_PACKAGE_NAME is required")
        if not self.doc_sequence:
            raise ValueError("BATCH_DOC_SEQUENCE is required")
        if not self.finalize_rec_type or not self.finalize_tab_type:
            raise ValueError("Finalize Oracle types are required")
        if not self.move_rec_type or not self.move_tab_type:
            raise ValueError("Move Oracle types are required")
        self._require_oracle_object_name(self.package_name, "BATCH_PACKAGE_NAME")
        self._require_oracle_object_name(self.doc_sequence, "BATCH_DOC_SEQUENCE")
        self._require_oracle_object_name(self.finalize_rec_type, "BATCH_FINALIZE_REC_TYPE")
        self._require_oracle_object_name(self.finalize_tab_type, "BATCH_FINALIZE_TAB_TYPE")
        self._require_oracle_object_name(self.move_rec_type, "BATCH_MOVE_REC_TYPE")
        self._require_oracle_object_name(self.move_tab_type, "BATCH_MOVE_TAB_TYPE")

        try:
            re.compile(self.fy_regex)
        except re.error as exc:
            raise ValueError("BATCH_FY_REGEX is invalid: %s" % exc)

        if command in (COMMAND_RUN, COMMAND_DRY_RUN):
            self._require_existing_directory(self.input_dir, "BATCH_INPUT_DIR")
            self._require_path(self.success_dir, "BATCH_SUCCESS_DIR")
            self._require_path(self.failed_dir, "BATCH_FAILED_DIR")
            self._validate_runtime_directories()

        if command in (COMMAND_RUN, COMMAND_DB_CHECK):
            self._require_oracle_settings()

        if command in (COMMAND_RUN, COMMAND_S3_CHECK, COMMAND_SECRETS_CHECK):
            if not self.aws_region:
                raise ValueError("AWS_REGION is required")

        if command in (COMMAND_RUN, COMMAND_S3_CHECK):
            if self.secret_mode == SECRET_MODE_ENV and not self.s3_bucket:
                raise ValueError("S3_BUCKET is required when BATCH_SECRET_MODE=env")
            if self.secret_mode == SECRET_MODE_SECRETS_MANAGER and not self.s3_secret_name:
                raise ValueError("S3_SECRET_NAME is required when BATCH_SECRET_MODE=secrets_manager")

        if command == COMMAND_SECRETS_CHECK and self.secret_mode == SECRET_MODE_SECRETS_MANAGER:
            if not self.db_secret_name:
                raise ValueError("DB_SECRET_NAME is required when BATCH_SECRET_MODE=secrets_manager")
            if not self.s3_secret_name:
                raise ValueError("S3_SECRET_NAME is required when BATCH_SECRET_MODE=secrets_manager")

    def _require_oracle_settings(self) -> None:
        if not self.oracle_client_lib_dir:
            raise ValueError("ORACLE_CLIENT_LIB_DIR is required for Oracle thick mode")
        if self.secret_mode == SECRET_MODE_ENV:
            if not self.db_username:
                raise ValueError("DB_USERNAME is required when BATCH_SECRET_MODE=env")
            if not self.db_password:
                raise ValueError("DB_PASSWORD is required when BATCH_SECRET_MODE=env")
            if not self.db_dsn:
                raise ValueError("DB_DSN is required when BATCH_SECRET_MODE=env")
        elif self.secret_mode == SECRET_MODE_SECRETS_MANAGER:
            if not self.db_secret_name:
                raise ValueError("DB_SECRET_NAME is required when BATCH_SECRET_MODE=secrets_manager")
        else:
            raise ValueError("Unsupported BATCH_SECRET_MODE: %s" % self.secret_mode)

    @staticmethod
    def _require_path(value: Path, name: str) -> None:
        if not str(value):
            raise ValueError("%s is required" % name)

    @staticmethod
    def _require_existing_directory(value: Path, name: str) -> None:
        if not value.exists():
            raise ValueError("%s does not exist: %s" % (name, value))
        if not value.is_dir():
            raise ValueError("%s must be a directory: %s" % (name, value))

    def _validate_runtime_directories(self) -> None:
        paths = {
            "BATCH_INPUT_DIR": self.input_dir.resolve(strict=False),
            "BATCH_SUCCESS_DIR": self.success_dir.resolve(strict=False),
            "BATCH_FAILED_DIR": self.failed_dir.resolve(strict=False),
        }
        seen = {}
        for name, path in paths.items():
            if path in seen:
                raise ValueError("%s and %s must be different directories" % (seen[path], name))
            seen[path] = name
        self._require_no_nested_paths(paths)

    @staticmethod
    def _require_oracle_object_name(value: str, env_name: str) -> None:
        if not ORACLE_NAME_PATTERN.match(value):
            raise ValueError("%s must be an Oracle object name, optionally schema-qualified" % env_name)

    @staticmethod
    def _require_no_nested_paths(paths) -> None:
        for outer_name, outer_path in paths.items():
            for inner_name, inner_path in paths.items():
                if outer_name == inner_name:
                    continue
                if outer_path in inner_path.parents:
                    raise ValueError("%s must not be inside %s" % (inner_name, outer_name))


def load_environment_files(batch_env: Optional[str] = None) -> str:
    resolved_env = batch_env or os.getenv("BATCH_ENV", "dev").strip() or "dev"
    root = Path.cwd()
    for candidate in (root / ".env", root / (".env.%s" % resolved_env)):
        if candidate.exists():
            load_dotenv(candidate, override=False)
    _unset_blank_env_vars(
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    )
    return resolved_env


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return float(raw)


def _env_path(name: str) -> Optional[Path]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def _unset_blank_env_vars(*names: str) -> None:
    for name in names:
        if name in os.environ and not os.environ[name].strip():
            os.environ.pop(name, None)
