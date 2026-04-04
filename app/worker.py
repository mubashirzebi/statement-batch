import os
import threading
from concurrent.futures import ALL_COMPLETED
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from pathlib import Path
from time import perf_counter

from app.models import BatchSummary
from app.models import DryRunSummary
from app.utils.files import build_failed_destination
from app.utils.files import extract_fy_years
from app.utils.files import move_to_path
from app.utils.files import normalize_db_file_name


class BatchWorker:
    def __init__(self, config, service, logger):
        self.config = config
        self.service = service
        self.logger = logger

    def run(self, run_id):
        run_started_at = perf_counter()
        summary = BatchSummary()
        batch_index = 0
        pending = set()
        first_error = None
        current_batch = []
        seen_names = set()
        stop_event = threading.Event()

        with ThreadPoolExecutor(max_workers=self.config.worker_count, thread_name_prefix="batch-worker") as executor:
            for path in self._iter_input_files():
                file_name = normalize_db_file_name(path.name)
                summary.total_seen += 1
                if summary.total_seen % 1000 == 0:
                    self.logger.info(
                        "scan progress: total_seen=%s batches_submitted=%s duplicates=%s",
                        summary.total_seen,
                        batch_index,
                        summary.duplicate_skipped,
                    )

                if file_name in seen_names:
                    self._move_global_duplicate(path)
                    summary.duplicate_skipped += 1
                    continue

                seen_names.add(file_name)
                current_batch.append(path)

                if len(current_batch) >= self.config.batch_size:
                    batch_index += 1
                    pending.add(self._submit_batch(executor, current_batch, batch_index, run_id, stop_event))
                    current_batch = []
                    first_error = self._drain_pending_if_needed(pending, summary, stop_event)
                    if first_error:
                        break

            if not first_error and current_batch:
                batch_index += 1
                pending.add(self._submit_batch(executor, current_batch, batch_index, run_id, stop_event))

            while pending and not first_error:
                first_error = self._drain_pending(pending, summary, wait_for_all=False, stop_event=stop_event)

            if first_error:
                stop_event.set()
                for future in pending:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
                raise first_error

        self.logger.summary(
            "run summary: total_seen=%s uploaded=%s metadata_missing=%s db_failed=%s upload_failed=%s duplicates=%s moved_success=%s moved_failed=%s failed_file_moves=%s duration_ms=%s",
            summary.total_seen,
            summary.uploaded,
            summary.metadata_missing,
            summary.db_failed,
            summary.upload_failed,
            summary.duplicate_skipped,
            summary.moved_success,
            summary.moved_failed,
            summary.failed_file_moves,
            self._elapsed_ms(run_started_at),
        )
        return summary

    def dry_run(self):
        run_started_at = perf_counter()
        summary = DryRunSummary()
        batch_size = 0
        seen_names = set()

        for path in self._iter_input_files():
            summary.files_discovered += 1
            if summary.files_discovered % 1000 == 0:
                self.logger.info(
                    "dry-run progress: files_discovered=%s batches=%s duplicates=%s",
                    summary.files_discovered,
                    summary.batches,
                    summary.duplicate_skipped,
                )
            if normalize_db_file_name(path.name) in seen_names:
                summary.duplicate_skipped += 1
                continue
            seen_names.add(normalize_db_file_name(path.name))
            batch_size += 1
            if batch_size == self.config.batch_size:
                summary.batches += 1
                batch_size = 0

        if batch_size:
            summary.batches += 1

        self.logger.summary(
            "dry-run summary: files_discovered=%s duplicate_skipped=%s batches=%s duration_ms=%s",
            summary.files_discovered,
            summary.duplicate_skipped,
            summary.batches,
            self._elapsed_ms(run_started_at),
        )
        return summary

    def _iter_input_files(self):
        root = Path(self.config.input_dir)
        for current_root, dir_names, file_names in os.walk(root):
            dir_names.sort()
            for file_name in sorted(file_names):
                if file_name.lower().endswith(".pdf"):
                    yield Path(current_root) / file_name

    def _move_global_duplicate(self, path):
        fy_years = extract_fy_years(path.name, self.config.fy_regex)
        destination = build_failed_destination(
            self.config.failed_dir,
            fy_years,
            "DUPLICATE_FILE_NAME",
            path.name,
        )
        try:
            move_to_path(path, destination)
            self.logger.warning("skipped duplicate file name discovered during scan: %s", path.name)
        except Exception as exc:
            self.logger.error("failed moving duplicate scanned file %s: %s", path.name, exc)

    def _drain_pending_if_needed(self, pending, summary, stop_event):
        if len(pending) < self.config.queue_size:
            return None
        return self._drain_pending(pending, summary, wait_for_all=False, stop_event=stop_event)

    def _submit_batch(self, executor, batch_paths, batch_index, run_id, stop_event):
        return executor.submit(
            self._run_batch_task,
            list(batch_paths),
            batch_index,
            run_id,
            stop_event,
        )

    def _run_batch_task(self, batch_paths, batch_index, run_id, stop_event):
        if stop_event.is_set():
            return BatchSummary(batch_index=batch_index)
        return self.service.process_batch(batch_paths, batch_index, run_id)

    @staticmethod
    def _drain_pending(pending, summary, wait_for_all, stop_event):
        done, not_done = wait(
            pending,
            return_when=FIRST_COMPLETED if not wait_for_all else ALL_COMPLETED,
        )
        pending.clear()
        pending.update(not_done)

        first_error = None
        for future in done:
            try:
                batch_summary = future.result()
                summary.merge(batch_summary)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
                    if stop_event is not None:
                        stop_event.set()
        return first_error

    @staticmethod
    def _elapsed_ms(started_at):
        return int((perf_counter() - started_at) * 1000)
