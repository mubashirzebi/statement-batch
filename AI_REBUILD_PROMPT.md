# AI System Rebuild Prompt: Enterprise Batch Processing Job

**System Context:**  
You are an expert Enterprise Python Architect. Your objective is to build a robust, crash-proof, highly concurrent batch processing system in Python. 

This system will process up to **500,000 PDF files** per year. It operates on a Windows/Linux server, reads files from a local disk, uploads them to AWS S3, logs the state synchronously in an Oracle database, and moves the files to a final local directory. 

## 1. Core Architectural Constraints
* **O(1) Memory Footprint:** The worker CANNOT load 500k files into memory at once. It must use generators (`os.scandir`) and bounded `queue.Queue` buffers with the `ThreadPoolExecutor`.
* **Idempotency & Crash Recovery (Outbox Pattern):** If the Python process is abruptly killed (`kill -9`, Power Loss, OOM), no database states or physical files can be left in an orphaned "limbo state". All DB write intents must be flushed to a local JSON "outbox" on disk before committing, creating a replayable journal.
* **Concurrency:** The S3 uploads and DB calls are I/O bound. Use a `ThreadPoolExecutor` (default 12 workers) for network/disk operations.
* **Separation of Concerns:** Separate logic strictly into: `Config` -> `Models` -> `Repository (DB)` -> `Storage (S3)` -> `Service (Business Logic)` -> `Worker (Concurrency)` -> `Main (CLI/Bootstrapper)`.

---

## 2. Technology Stack
* Python 3.9+
* `boto3` (AWS S3)
* `oracledb` (Oracle DB Thick mode)
* `python-dotenv` (Config)
* `bash` (Wrapper script `run_batch.sh` over python virtual environment)

---

## 3. Database Schema & PL/SQL Design (Oracle)
The database must use efficient Bulk DML operations (`ODCIVARCHAR2LIST` and `FORALL`) to prevent high round-trip times.

### Tables
1. `FINACLE_STMT_LOG`: The main transaction log keeping track of status (e.g. `RECEIVED`, `READY`, `DB_COMMITTED_PENDING_MOVE`, `SUCCESS`, `FAILED_DB`, `FAILED_MOVE`).
2. `FINACLE_STMT`: The finalized true-state table.
3. `HPSP_FYEAR_STMT_DET`: The metadata source-of-truth table used to join customer data (CIFID, SOL_ID, ACCT_NAME) with the filename.

### Types to implement
* `T_FINALIZE_REC` and `T_FINALIZE_TAB` (Table of Objects).
* `T_MOVE_REC` and `T_MOVE_TAB` (Table of Objects).

### Package: `FINACLE_BATCH_PKG`
1. **`PREPARE_FILES(list of file_names)`**: Takes 500 file names, joins them with `HPSP_FYEAR_STMT_DET`, creates sequences, inserts `READY` rows into the LOG table, and returns a cursor of metadata to Python.
2. **`FINALIZE_DB(list of T_FINALIZE_REC)`**: Takes up to 500 records. Updates LOG table to `DB_COMMITTED_PENDING_MOVE` and `MERGE` inserts into `FINACLE_STMT` if S3 upload was successful.
3. **`FINALIZE_MOVE(list of T_MOVE_REC)`**: Takes up to 500 records. Updates LOG table to standard `SUCCESS` or `FAILED_MOVE` based on the status of the local `shutil.move` operations.

---

## 4. Required Python Modules

### A. `app/models.py`
Use plain `dataclasses`.
* `PreparedFile`
* `FileJob`
* `FinalizeRecord`
* `MoveRecord`
* `PendingMoveRow`
* `BatchSummary` (Maintains only integer counters like `total_seen`, `uploaded`, `db_failed` to keep memory flat).

### B. `app/service.py` (The Core Pipeline)
Exposes `process_batch(batch_paths, batch_index, run_id)`.
**Workflow:**
1. Call `PREPARE_FILES` in Oracle.
2. If `READY`, stream upload file to S3 (`_upload_ready_file`). 
3. After S3, perform `FINALIZE_DB` via repository.
4. If `FINALIZE_DB` succeeds, attempt to `shutil.move` the file to `--success-dir`.
5. Call `FINALIZE_MOVE` in Oracle to mark completion.
6. **Reconciler Strategy:** Have a `reconcile_pending_moves()` method that runs BEFORE the worker starts. It queries Oracle for `STATUS = DB_COMMITTED_PENDING_MOVE`. If the file is physically verified in `success-dir`, it pushes a `FINALIZE_MOVE` to Oracle to heal the orphaned row.

### C. `app/worker.py` (Concurrency Engine)
* Reads exclusively from `--input-dir` via `os.scandir()`.
* Chunks files into batches of `--batch-size` (default: 500).
* Submits batches to `concurrent.futures.ThreadPoolExecutor`.
* Yields completion to central logger.

### D. `app/utils/outbox.py`
A JSON journaling tool.
* Before `FINALIZE_DB` or `FINALIZE_MOVE` might fail due to network, it synchronously writes a JSON payload via `os.fsync` + atomic `os.rename(temp, final)`. 
* On Startup, `outbox_manager.replay()` looks for any un-pushed JSON journals from a previous crash and squirts them directly to the Oracle Repository to guarantee eventually-consistent state.

### E. `app/utils/lock.py`
File-based locking (`job.lock`) to prevent concurrent Cron executions. Should implement robust PID checking (`os.kill(pid, 0)`) to gracefully break stale locks if the server rebooted and the old lock file was permanently left behind.

### F. `app/logging_setup.py`
* Distinct log streams: `general.log` (verbose), `error.log` (only WARNING/ERROR), `summary.log` (batch completion metrics with CPU/RAM usage).
* Logs must be isolated in a newly generated `logs/{YYYYMMDDTHHMMSSZ}_{UUID}` folder per run. 

### G. Bash Wrapper (`run_batch.sh`)
* Validates Env variables (`dev`, `uat`, `prod`).
* Conditionally installs `requirements.txt` into a `.venv` (only using a hidden `.deps_installed` timestamp marker so it skips installation if requirements are unchanged).
* Bootstraps Python.

---

## 5. Failure Cases Addressed
When generating this code, strictly enforce:
* **S3 Throttle:** Catch AWS errors and trigger Exponential Backoff (1s, 2s, 4s, bounded to max 60s).
* **Database Connection Loss mid-run:** Trap `oracledb.DatabaseError`, rollback the immediate connection, write the state to the Outbox JSON file, and raise a `BatchFatalError` to defensively abort the remainder of the queue.
* **Disk Move Fails (`shutil.move` exception):** Mark the DB record as `FAILED_MOVE`, do NOT orphan the system state.
* **Hard Power Cut (Kill -9) Mid-Move:** Covered entirely by the `DB_COMMITTED_PENDING_MOVE` reconciler pattern on system boot.

**Please generate the entire codebase following these patterns.**
