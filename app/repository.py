from time import perf_counter

import oracledb

from app.models import PreparedFile


class BatchRepository:
    def __init__(self, pool, config, logger=None):
        self.pool = pool
        self.config = config
        self.logger = logger

    def prepare_files(self, file_names):
        procedure_name = "%s.PREPARE_FILES" % self.config.package_name
        input_count = len(file_names)
        started_at = perf_counter()
        self._log_sp_call_started(procedure_name, input_count)
        with self.pool.acquire() as connection:
            try:
                with connection.cursor() as cursor, connection.cursor() as out_cursor:
                    file_list = self._build_varchar_list(connection, file_names)
                    cursor.callproc(
                        procedure_name,
                        [file_list, out_cursor],
                    )
                    rows = out_cursor.fetchall()
                connection.commit()
            except Exception as exc:
                connection.rollback()
                self._log_sp_call_failed(
                    procedure_name,
                    input_count=input_count,
                    duration_ms=self._elapsed_ms(started_at),
                    exc=exc,
                )
                raise

        mapped_rows = [self._map_prepared_row(row) for row in rows]
        self._log_sp_call_completed(
            procedure_name,
            input_count=input_count,
            output_count=len(mapped_rows),
            duration_ms=self._elapsed_ms(started_at),
        )
        return mapped_rows

    def finalize_db(self, records):
        if not records:
            return

        procedure_name = "%s.FINALIZE_DB" % self.config.package_name
        input_count = len(records)
        started_at = perf_counter()
        self._log_sp_call_started(procedure_name, input_count)
        with self.pool.acquire() as connection:
            try:
                payload = self._build_finalize_tab(connection, records)
                with connection.cursor() as cursor:
                    cursor.callproc(procedure_name, [payload])
                connection.commit()
            except Exception as exc:
                connection.rollback()
                self._log_sp_call_failed(
                    procedure_name,
                    input_count=input_count,
                    duration_ms=self._elapsed_ms(started_at),
                    exc=exc,
                )
                raise
        self._log_sp_call_completed(
            procedure_name,
            input_count=input_count,
            output_count=0,
            duration_ms=self._elapsed_ms(started_at),
        )

    def finalize_move(self, records):
        if not records:
            return

        procedure_name = "%s.FINALIZE_MOVE" % self.config.package_name
        input_count = len(records)
        started_at = perf_counter()
        self._log_sp_call_started(procedure_name, input_count)
        with self.pool.acquire() as connection:
            try:
                payload = self._build_move_tab(connection, records)
                with connection.cursor() as cursor:
                    cursor.callproc(procedure_name, [payload])
                connection.commit()
            except Exception as exc:
                connection.rollback()
                self._log_sp_call_failed(
                    procedure_name,
                    input_count=input_count,
                    duration_ms=self._elapsed_ms(started_at),
                    exc=exc,
                )
                raise
        self._log_sp_call_completed(
            procedure_name,
            input_count=input_count,
            output_count=0,
            duration_ms=self._elapsed_ms(started_at),
        )

    def _build_varchar_list(self, connection, values):
        list_type = connection.gettype("SYS.ODCIVARCHAR2LIST")
        collection = list_type.newobject()
        for value in values:
            collection.append(value)
        return collection

    def _build_finalize_tab(self, connection, records):
        record_type = connection.gettype(self.config.finalize_rec_type)
        table_type = connection.gettype(self.config.finalize_tab_type)
        collection = table_type.newobject()
        for item in records:
            record = record_type.newobject()
            record.FILE_NAME = item.file_name
            record.DOCID = item.doc_id
            record.STATUS = item.status
            record.DESCRIPTION = item.description or ""
            record.FY_YEARS = item.fy_years
            record.FILESIZE = item.file_size
            record.FILE_EXTENSION = item.file_extension
            record.SOURCE_PATH = item.source_path
            record.SOL_ID = item.sol_id
            record.CIFID = item.cifid
            record.FORACID = item.foracid
            record.ACCT_NAME = item.acct_name
            collection.append(record)
        return collection

    def _build_move_tab(self, connection, records):
        record_type = connection.gettype(self.config.move_rec_type)
        table_type = connection.gettype(self.config.move_tab_type)
        collection = table_type.newobject()
        for item in records:
            record = record_type.newobject()
            record.FILE_NAME = item.file_name
            record.DOCID = item.doc_id
            record.MOVE_STATUS = item.move_status
            record.DESCRIPTION = item.description or ""
            record.FINAL_PATH = item.final_path
            record.FILESIZE = item.file_size
            collection.append(record)
        return collection

    @staticmethod
    def _map_prepared_row(row):
        return PreparedFile(
            file_name=row[0],
            doc_id=int(row[1]) if row[1] is not None else 0,
            pre_status=row[2] or "",
            log_status=row[3] or "",
            description=row[4] or "",
            sol_id=row[5],
            cifid=row[6],
            foracid=row[7],
            acct_name=row[8],
        )

    @staticmethod
    def _elapsed_ms(started_at):
        return int((perf_counter() - started_at) * 1000)

    def _log_sp_call_started(self, procedure_name, input_count):
        if self.logger:
            self.logger.info("calling sp=%s input_count=%s", procedure_name, input_count)

    def _log_sp_call_completed(self, procedure_name, input_count, output_count, duration_ms):
        if self.logger:
            self.logger.info(
                "completed sp=%s input_count=%s output_count=%s duration_ms=%s",
                procedure_name,
                input_count,
                output_count,
                duration_ms,
            )

    def _log_sp_call_failed(self, procedure_name, input_count, duration_ms, exc):
        if self.logger:
            self.logger.error(
                "failed sp=%s input_count=%s duration_ms=%s error=%s",
                procedure_name,
                input_count,
                duration_ms,
                exc,
            )
