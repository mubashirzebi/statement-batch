import argparse
import logging
import uuid
from datetime import datetime

from app.config import AppConfig
from app.config import COMMAND_DB_CHECK
from app.config import COMMAND_DRY_RUN
from app.config import COMMAND_RUN
from app.config import COMMAND_S3_CHECK
from app.config import COMMAND_SECRETS_CHECK
from app.config import load_environment_files
from app.logging_setup import configure_logging


def main(argv=None):
    parser = argparse.ArgumentParser(description="Finacle statement batch job")
    parser.add_argument(
        "command",
        nargs="?",
        default=COMMAND_RUN,
        choices=[COMMAND_RUN, COMMAND_DRY_RUN, COMMAND_DB_CHECK, COMMAND_S3_CHECK, COMMAND_SECRETS_CHECK],
    )
    args = parser.parse_args(argv)
    started_at = datetime.now()
    session_id = uuid.uuid4().hex[:8]

    load_environment_files()
    config = AppConfig.from_env()
    config.validate_for_command(args.command)

    logger = configure_logging(config, args.command, started_at=started_at, session_id=session_id)
    logger.info("starting command=%s env=%s", args.command, config.batch_env)
    logger.info(
        "log directory=%s general=%s error=%s summary=%s session_id=%s",
        getattr(logger, "run_log_dir", ""),
        getattr(logger, "general_log_path", ""),
        getattr(logger, "error_log_path", ""),
        getattr(logger, "summary_log_path", ""),
        session_id,
    )

    try:
        if args.command == COMMAND_DRY_RUN:
            return _run_dry_run(config, logger)
        if args.command == COMMAND_SECRETS_CHECK:
            return _run_secrets_check(config, logger)
        if args.command == COMMAND_DB_CHECK:
            return _run_db_check(config, logger)
        if args.command == COMMAND_S3_CHECK:
            return _run_s3_check(config, logger)
        return _run_batch(config, logger, session_id)
    except Exception as exc:
        logger.exception("command failed: %s", exc)
        return 1


def _run_dry_run(config, logger):
    from app.utils.files import ensure_runtime_directories
    from app.worker import BatchWorker

    ensure_runtime_directories(config)
    worker = BatchWorker(config, service=None, logger=logger)
    worker.dry_run()
    return 0


def _run_secrets_check(config, logger):
    from app.secrets import CredentialResolver

    resolver = CredentialResolver(config, logger)
    payload = resolver.check_secret_access()
    logger.info("secrets check result: %s", payload)
    return 0


def _run_db_check(config, logger):
    from app.db import create_pool
    from app.db import run_db_checks
    from app.secrets import CredentialResolver

    resolver = CredentialResolver(config, logger)
    db_credentials = resolver.get_db_credentials()
    pool = create_pool(config, db_credentials, logger)
    try:
        payload = run_db_checks(pool, config)
        logger.info("db check result: %s", payload)
    finally:
        pool.close()
    return 0


def _run_s3_check(config, logger):
    from app.secrets import CredentialResolver
    from app.storage import S3Uploader

    resolver = CredentialResolver(config, logger)
    s3_credentials = resolver.get_s3_credentials()
    uploader = S3Uploader(s3_credentials)
    payload = uploader.check_access()
    logger.info("s3 check result: %s", payload)
    return 0


def _run_batch(config, logger, session_id):
    from app.db import create_pool
    from app.repository import BatchRepository
    from app.secrets import CredentialResolver
    from app.service import BatchService
    from app.storage import S3Uploader
    from app.utils.files import ensure_runtime_directories
    from app.utils.lock import JobLock
    from app.utils.outbox import OutboxManager
    from app.worker import BatchWorker

    ensure_runtime_directories(config)

    with JobLock(config.lock_file, stale_seconds=config.lock_stale_seconds, logger=logger):
        resolver = CredentialResolver(config, logger)
        db_credentials = resolver.get_db_credentials()
        s3_credentials = resolver.get_s3_credentials()

        pool = create_pool(config, db_credentials, logger)
        try:
            repository = BatchRepository(pool, config, logger=logger)
            outbox_manager = OutboxManager(config.outbox_dir, logger)
            replayed_files = outbox_manager.replay(repository)
            if replayed_files:
                logger.warning("replayed outbox files: %s", replayed_files)

            uploader = S3Uploader(s3_credentials)
            service = BatchService(config, repository, uploader, outbox_manager, logger)
            worker = BatchWorker(config, service, logger)

            run_id = "%s_%s" % (datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"), session_id)
            worker.run(run_id)
        finally:
            pool.close()

    logger.info("batch run completed successfully")
    return 0


logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("oracledb").setLevel(logging.WARNING)
