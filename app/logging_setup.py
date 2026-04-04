import logging
from datetime import datetime
from logging import FileHandler
from pathlib import Path


class BatchLogger:
    def __init__(self, general_logger, summary_logger, run_log_dir, general_log_path, error_log_path, summary_log_path):
        self._general_logger = general_logger
        self._summary_logger = summary_logger
        self.run_log_dir = str(run_log_dir)
        self.general_log_path = str(general_log_path)
        self.error_log_path = str(error_log_path)
        self.summary_log_path = str(summary_log_path)

    def debug(self, msg, *args, **kwargs):
        self._general_logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._general_logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._general_logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._general_logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._general_logger.exception(msg, *args, **kwargs)

    def summary(self, msg, *args, **kwargs):
        self._general_logger.info(msg, *args, **kwargs)
        self._summary_logger.info(msg, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._general_logger, name)


def configure_logging(config, command="run", started_at=None, session_id="session"):
    started_at = started_at or datetime.now()
    timestamp_component = started_at.strftime("%Y-%m-%d_%H-%M-%S")
    session_component = _build_session_component(command, session_id)
    run_directory = Path(config.log_dir) / ("%s_%s" % (timestamp_component, session_component))
    run_directory.mkdir(parents=True, exist_ok=True)

    general_log_path = run_directory / "general.log"
    error_log_path = run_directory / "error.log"
    summary_log_path = run_directory / "summary.log"

    logger_name_suffix = "%s_%s" % (timestamp_component, session_component)
    general_logger = logging.getLogger("finacle_batch_job.general.%s" % logger_name_suffix)
    summary_logger = logging.getLogger("finacle_batch_job.summary.%s" % logger_name_suffix)

    _reset_logger(general_logger, getattr(logging, config.log_level.upper(), logging.INFO))
    _reset_logger(summary_logger, logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    general_logger.addHandler(console_handler)

    general_file_handler = FileHandler(str(general_log_path), encoding="utf-8")
    general_file_handler.setFormatter(formatter)
    general_logger.addHandler(general_file_handler)

    error_file_handler = FileHandler(str(error_log_path), encoding="utf-8")
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(formatter)
    general_logger.addHandler(error_file_handler)

    summary_file_handler = FileHandler(str(summary_log_path), encoding="utf-8")
    summary_file_handler.setFormatter(formatter)
    summary_logger.addHandler(summary_file_handler)

    return BatchLogger(
        general_logger=general_logger,
        summary_logger=summary_logger,
        run_log_dir=run_directory,
        general_log_path=general_log_path,
        error_log_path=error_log_path,
        summary_log_path=summary_log_path,
    )


def _build_session_component(command, session_id):
    if command == "run":
        return "run_%s" % session_id
    return "%s_%s" % (command, session_id)

def _reset_logger(logger, level):
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False
