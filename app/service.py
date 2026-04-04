from pathlib import Path
from time import perf_counter
from typing import List
from typing import Tuple

from app.models import BatchSummary
from app.models import FINALIZE_STATUS_FAILED_DB
from app.models import FINALIZE_STATUS_FAILED_MOVE
from app.models import FINALIZE_STATUS_META_NOT_FOUND
from app.models import FINALIZE_STATUS_UPLOAD_FAILED
from app.models import FINALIZE_STATUS_UPLOAD_SUCCESS
from app.models import FileJob
from app.models import FinalizeRecord
from app.models import MOVE_STATUS_MOVE_FAILED
from app.models import MOVE_STATUS_MOVED_SUCCESS
from app.models import MoveRecord
from app.models import PRE_STATUS_FAILED_DB
from app.models import PRE_STATUS_META_NOT_FOUND
from app.models import PRE_STATUS_READY
from app.utils.files import build_failed_destination
from app.utils.files import build_success_destination
from app.utils.files import extract_fy_years
from app.utils.files import FileMoveError
from app.utils.files import file_extension
from app.utils.files import file_size_string
from app.utils.files import move_to_path
from app.utils.files import normalize_db_file_name
from app.utils.retry import sleep_with_backoff


class BatchFatalError(RuntimeError):
    """Raised when the batch must stop because DB state needs operator attention."""


class BatchService:
    def __init__(self, config, repository, uploader, outbox_manager, logger):
        self.config = config
        self.repository = repository
        self.uploader = uploader
        self.outbox_manager = outbox_manager
        self.logger = logger

    def process_batch(self, batch_paths: List[Path], batch_index: int, run_id: str) -> BatchSummary:
        summary = BatchSummary(batch_index=batch_index)
        jobs, duplicate_jobs = self._build_jobs(batch_paths)

        for duplicate_job in duplicate_jobs:
            self._move_duplicate(duplicate_job)
            summary.duplicate_skipped += 1

        if not jobs:
            return summary

        batch_started_at = perf_counter()
        self.logger.info("batch %s started with %s unique files", batch_index, len(jobs))
        try:
            prepared_rows = self.repository.prepare_files([job.file_name for job in jobs])
            prepared_by_name = dict((row.file_name, row) for row in prepared_rows)

            if len(prepared_by_name) != len(jobs):
                missing = sorted(set(job.file_name for job in jobs) - set(prepared_by_name.keys()))
                raise BatchFatalError(
                    "PREPARE_FILES did not return rows for: %s" % ", ".join(missing)
                )

            finalize_records = []
            successful_uploads = []
            failed_jobs = []
            upload_phase_started_at = perf_counter()
            self.logger.info(
                "batch %s s3 phase started: candidate_count=%s",
                batch_index,
                len(jobs),
            )

            for job in jobs:
                prepared = prepared_by_name[job.file_name]
                record = self._build_finalize_record(job, prepared)
                finalize_records.append(record)

                if prepared.pre_status == PRE_STATUS_READY:
                    summary.ready_for_upload += 1
                    if record.status == FINALIZE_STATUS_UPLOAD_SUCCESS:
                        summary.uploaded += 1
                        successful_uploads.append((job, record))
                    elif record.status == FINALIZE_STATUS_FAILED_DB:
                        summary.db_failed += 1
                        failed_jobs.append((job, record))
                    else:
                        summary.upload_failed += 1
                        failed_jobs.append((job, record))
                elif prepared.pre_status == PRE_STATUS_META_NOT_FOUND:
                    summary.metadata_missing += 1
                    failed_jobs.append((job, record))
                else:
                    summary.db_failed += 1
                    failed_jobs.append((job, record))

            self.logger.info(
                "batch %s s3 phase completed: ready_for_upload=%s uploaded=%s upload_failed=%s metadata_missing=%s db_failed=%s duration_ms=%s",
                batch_index,
                summary.ready_for_upload,
                summary.uploaded,
                summary.upload_failed,
                summary.metadata_missing,
                summary.db_failed,
                self._elapsed_ms(upload_phase_started_at),
            )

            try:
                self.repository.finalize_db(finalize_records)
            except Exception as exc:
                outbox_path = self.outbox_manager.store_finalize_db(run_id, batch_index, finalize_records)
                raise BatchFatalError(
                    "FINALIZE_DB failed; wrote outbox %s: %s" % (outbox_path.name, exc)
                )

            success_move_started_at = perf_counter()
            self.logger.info(
                "batch %s success move phase started: item_count=%s",
                batch_index,
                len(successful_uploads),
            )
            move_records = self._move_successful_files(successful_uploads, summary)
            self.logger.info(
                "batch %s success move phase completed: item_count=%s moved_success=%s moved_failed=%s duration_ms=%s",
                batch_index,
                len(successful_uploads),
                summary.moved_success,
                summary.moved_failed,
                self._elapsed_ms(success_move_started_at),
            )

            failed_move_started_at = perf_counter()
            self.logger.info(
                "batch %s failed move phase started: item_count=%s",
                batch_index,
                len(failed_jobs),
            )
            failed_move_finalize_records = self._move_failed_files(failed_jobs, summary)
            self.logger.info(
                "batch %s failed move phase completed: item_count=%s failed_file_moves=%s duration_ms=%s",
                batch_index,
                len(failed_jobs),
                summary.failed_file_moves,
                self._elapsed_ms(failed_move_started_at),
            )

            if move_records:
                try:
                    self.repository.finalize_move(move_records)
                except Exception as exc:
                    outbox_path = self.outbox_manager.store_finalize_move(run_id, batch_index, move_records)
                    if failed_move_finalize_records:
                        self.outbox_manager.store_finalize_db(run_id, batch_index, failed_move_finalize_records)
                    raise BatchFatalError(
                        "FINALIZE_MOVE failed; wrote outbox %s: %s" % (outbox_path.name, exc)
                    )

            if failed_move_finalize_records:
                try:
                    self.repository.finalize_db(failed_move_finalize_records)
                except Exception as exc:
                    outbox_path = self.outbox_manager.store_finalize_db(
                        run_id,
                        batch_index,
                        failed_move_finalize_records,
                    )
                    raise BatchFatalError(
                        "FAILED_MOVE finalize_db failed; wrote outbox %s: %s" % (outbox_path.name, exc)
                    )

            self.logger.summary(
                "batch %s completed: uploaded=%s metadata_missing=%s db_failed=%s upload_failed=%s moved_success=%s moved_failed=%s duplicates=%s duration_ms=%s",
                batch_index,
                summary.uploaded,
                summary.metadata_missing,
                summary.db_failed,
                summary.upload_failed,
                summary.moved_success,
                summary.moved_failed,
                summary.duplicate_skipped,
                self._elapsed_ms(batch_started_at),
            )
            return summary
        except Exception as exc:
            self.logger.error(
                "batch %s failed after duration_ms=%s error=%s",
                batch_index,
                self._elapsed_ms(batch_started_at),
                exc,
            )
            raise

    @staticmethod
    def _elapsed_ms(started_at):
        return int((perf_counter() - started_at) * 1000)

    def _build_jobs(self, batch_paths: List[Path]) -> Tuple[List[FileJob], List[FileJob]]:
        jobs = []
        duplicates = []
        seen_names = set()

        for path in batch_paths:
            file_name = normalize_db_file_name(path.name)
            fy_years = extract_fy_years(file_name, self.config.fy_regex)
            job = FileJob(
                path=path,
                file_name=file_name,
                original_file_name=path.name,
                fy_years=fy_years,
            )
            if file_name in seen_names:
                duplicates.append(job)
                continue
            seen_names.add(file_name)
            jobs.append(job)

        return jobs, duplicates

    def _build_finalize_record(self, job, prepared):
        source_path = str(job.path.absolute())
        size_string = file_size_string(job.path)
        extension = file_extension(job.path)

        if prepared.pre_status == PRE_STATUS_READY and prepared.doc_id <= 0:
            self.logger.error(
                "prepare_files returned invalid doc_id: file_name=%s pre_status=%s doc_id=%s",
                job.file_name,
                prepared.pre_status,
                prepared.doc_id,
            )
            return FinalizeRecord(
                file_name=job.file_name,
                doc_id=prepared.doc_id,
                status=FINALIZE_STATUS_FAILED_DB,
                description="missing doc_id returned from PREPARE_FILES",
                fy_years=job.fy_years,
                file_size=size_string,
                file_extension=extension,
                source_path=source_path,
                sol_id=prepared.sol_id,
                cifid=prepared.cifid,
                foracid=prepared.foracid,
                acct_name=prepared.acct_name,
            )

        if prepared.pre_status == PRE_STATUS_READY:
            return self._upload_ready_file(job, prepared, source_path, size_string, extension)

        if prepared.pre_status == PRE_STATUS_META_NOT_FOUND:
            return FinalizeRecord(
                file_name=job.file_name,
                doc_id=prepared.doc_id,
                status=FINALIZE_STATUS_META_NOT_FOUND,
                description=prepared.description or "metadata missing",
                fy_years=job.fy_years,
                file_size=size_string,
                file_extension=extension,
                source_path=source_path,
                sol_id=prepared.sol_id,
                cifid=prepared.cifid,
                foracid=prepared.foracid,
                acct_name=prepared.acct_name,
            )

        self.logger.error(
            "database preparation failed for file_name=%s pre_status=%s log_status=%s description=%s",
            job.file_name,
            prepared.pre_status,
            prepared.log_status,
            prepared.description,
        )
        return FinalizeRecord(
            file_name=job.file_name,
            doc_id=prepared.doc_id,
            status=FINALIZE_STATUS_FAILED_DB,
            description=prepared.description or prepared.log_status or "database preparation failed",
            fy_years=job.fy_years,
            file_size=size_string,
            file_extension=extension,
            source_path=source_path,
            sol_id=prepared.sol_id,
            cifid=prepared.cifid,
            foracid=prepared.foracid,
            acct_name=prepared.acct_name,
        )

    def _upload_ready_file(self, job, prepared, source_path, size_string, extension):
        fy_component = job.fy_years or "UNKNOWN"
        object_key = "%s/%s.%s" % (fy_component, prepared.doc_id, extension or "pdf")

        last_error = ""
        for attempt in range(1, self.config.max_retry + 2):
            upload_started_at = perf_counter()
            try:
                self.uploader.upload_file(job.path, object_key)
                return FinalizeRecord(
                    file_name=job.file_name,
                    doc_id=prepared.doc_id,
                    status=FINALIZE_STATUS_UPLOAD_SUCCESS,
                    description="uploaded to s3",
                    fy_years=job.fy_years,
                    file_size=size_string,
                    file_extension=extension,
                    source_path=source_path,
                    sol_id=prepared.sol_id,
                    cifid=prepared.cifid,
                    foracid=prepared.foracid,
                    acct_name=prepared.acct_name,
                )
            except Exception as exc:
                last_error = str(exc)
                self.logger.warning(
                    "upload attempt %s failed for %s (doc_id=%s object_key=%s duration_ms=%s): %s",
                    attempt,
                    job.file_name,
                    prepared.doc_id,
                    object_key,
                    self._elapsed_ms(upload_started_at),
                    exc,
                )
                if attempt <= self.config.max_retry:
                    sleep_with_backoff(self.config.retry_base_seconds, attempt)

        self.logger.error(
            "upload failed after retries: file_name=%s doc_id=%s object_key=%s attempts=%s last_error=%s",
            job.file_name,
            prepared.doc_id,
            object_key,
            self.config.max_retry + 1,
            last_error,
        )
        return FinalizeRecord(
            file_name=job.file_name,
            doc_id=prepared.doc_id,
            status=FINALIZE_STATUS_UPLOAD_FAILED,
            description=last_error[:200] or "upload failed",
            fy_years=job.fy_years,
            file_size=size_string,
            file_extension=extension,
            source_path=source_path,
            sol_id=prepared.sol_id,
            cifid=prepared.cifid,
            foracid=prepared.foracid,
            acct_name=prepared.acct_name,
        )

    def _move_successful_files(self, items, summary):
        move_records = []
        for job, finalize_record in items:
            destination = build_success_destination(
                self.config.success_dir,
                finalize_record.fy_years,
                job.original_file_name,
            )
            move_started_at = perf_counter()
            try:
                final_path = move_to_path(job.path, destination)
                move_records.append(
                    MoveRecord(
                        file_name=finalize_record.file_name,
                        doc_id=finalize_record.doc_id,
                        move_status=MOVE_STATUS_MOVED_SUCCESS,
                        description="moved to success",
                        final_path=str(final_path),
                        file_size=finalize_record.file_size,
                    )
                )
                summary.moved_success += 1
            except Exception as exc:
                recovery_path = exc.recovery_path if isinstance(exc, FileMoveError) else None
                move_records.append(
                    MoveRecord(
                        file_name=finalize_record.file_name,
                        doc_id=finalize_record.doc_id,
                        move_status=MOVE_STATUS_MOVE_FAILED,
                        description=str(exc)[:200],
                        final_path=str(recovery_path or destination),
                        file_size=finalize_record.file_size,
                    )
                )
                summary.moved_failed += 1
                self.logger.error(
                    "failed moving success file %s doc_id=%s destination_path=%s duration_ms=%s: %s",
                    job.original_file_name,
                    finalize_record.doc_id,
                    destination,
                    self._elapsed_ms(move_started_at),
                    exc,
                )
        return move_records

    def _move_failed_files(self, items, summary):
        move_failure_records = []
        for job, finalize_record in items:
            destination = build_failed_destination(
                self.config.failed_dir,
                finalize_record.fy_years,
                finalize_record.status,
                job.original_file_name,
            )
            move_started_at = perf_counter()
            try:
                move_to_path(job.path, destination)
            except Exception as exc:
                recovery_path = exc.recovery_path if isinstance(exc, FileMoveError) else None
                summary.failed_file_moves += 1
                self.logger.error(
                    "failed moving rejected file %s doc_id=%s destination_path=%s duration_ms=%s: %s",
                    job.original_file_name,
                    finalize_record.doc_id,
                    destination,
                    self._elapsed_ms(move_started_at),
                    exc,
                )
                move_failure_records.append(
                    FinalizeRecord(
                        file_name=finalize_record.file_name,
                        doc_id=finalize_record.doc_id,
                        status=FINALIZE_STATUS_FAILED_MOVE,
                        description=("failed moving rejected file: %s" % exc)[:200],
                        fy_years=finalize_record.fy_years,
                        file_size=finalize_record.file_size,
                        file_extension=finalize_record.file_extension,
                        source_path=str(recovery_path or finalize_record.source_path),
                        sol_id=finalize_record.sol_id,
                        cifid=finalize_record.cifid,
                        foracid=finalize_record.foracid,
                        acct_name=finalize_record.acct_name,
                    )
                )
        return move_failure_records

    def _move_duplicate(self, job):
        destination = build_failed_destination(
            self.config.failed_dir,
            job.fy_years,
            "DUPLICATE_FILE_NAME",
            job.original_file_name,
        )
        move_started_at = perf_counter()
        try:
            move_to_path(job.path, destination)
            self.logger.warning(
                "moved duplicate file to failed folder: file_name=%s destination_path=%s duration_ms=%s",
                job.original_file_name,
                destination,
                self._elapsed_ms(move_started_at),
            )
        except Exception as exc:
            self.logger.error(
                "failed moving duplicate file %s destination_path=%s duration_ms=%s: %s",
                job.original_file_name,
                destination,
                self._elapsed_ms(move_started_at),
                exc,
            )
