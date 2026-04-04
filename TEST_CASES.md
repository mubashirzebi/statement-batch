# Finacle Batch Job Test Notes

This file captures the manual validation scenarios already executed on the Windows/UAT setup.

## Environment Notes

- Python command used on Windows: `python`
- Oracle mode tested: thick mode
- S3 permission model tested: upload + read allowed, delete denied
- Job model tested: one job at a time, batches processed in parallel

## Commands Verified

- `python main.py dry-run`
- `python main.py db-check`
- `python main.py s3-check`
- `python main.py run`

## Test Cases Performed

### 1. Dry Run

- Setup:
  - Input folder available
- Command:
  - `python main.py dry-run`
- Expected:
  - app starts
  - files are discovered
  - no DB or S3 work is performed
- Result:
  - passed

### 2. DB Connectivity Check

- Setup:
  - valid Oracle thick mode config
  - valid DB credentials
- Command:
  - `python main.py db-check`
- Expected:
  - Oracle client initializes
  - pool is created
  - DB object checks pass
- Result:
  - passed

### 3. S3 Connectivity Check

- Setup:
  - valid bucket, region, access key, secret key
- Command:
  - `python main.py s3-check`
- Expected:
  - bucket access works
  - upload works
  - read works
  - delete may fail if IAM does not allow it
- Result:
  - passed
- Notes:
  - `DeleteObject` was denied by IAM, which is acceptable in the current design
  - `s3-check` now treats delete as best-effort only

### 4. Metadata Not Found

- Setup:
  - test file present in input
  - no matching metadata in `HPSP_FYEAR_STMT_DET`
- Command:
  - `python main.py run`
- Expected:
  - row written in `FINACLE_STMT_LOG`
  - status becomes `META_NOT_FOUND`
  - file moves to `failed/<FY>/META_NOT_FOUND/`
  - no S3 upload
- Result:
  - passed
- Observed:
  - file was seen under `failed/2025_2026/...`
  - no S3 object uploaded

### 5. Upload Failure With Retry

- Setup:
  - valid metadata exists
  - bucket intentionally changed to an invalid bucket name
- Command:
  - `python main.py run`
- Expected:
  - upload retries happen
  - final status becomes `UPLOAD_FAILED`
  - file moves to `failed/<FY>/UPLOAD_FAILED/`
- Result:
  - passed
- Observed:
  - 4 attempts total: 1 initial try + 3 retries
  - DB row stored full S3 error in `DESCRIPTION`
  - stable `DOCID` was reused

### 6. Duplicate File Name Handling

- Setup:
  - two files with the same file name placed under different input subfolders
- Command:
  - `python main.py run`
- Expected:
  - one file is processed normally
  - duplicate file moves to `failed/<FY>/DUPLICATE_FILE_NAME/`
  - only one `DOCID` / one S3 object key is used
- Result:
  - passed
- Observed:
  - duplicate log message was emitted during scan
  - only one file used `DOCID = 24`

### 7. Success Folder Move Failure

- Setup:
  - valid metadata exists
  - S3 upload allowed
  - success folder write access removed
- Command:
  - `python main.py run`
- Expected:
  - upload succeeds
  - local move to success fails
  - DB status becomes `FAILED_MOVE`
  - file remains recoverable in input
- Result:
  - passed
- Observed:
  - log showed permission denied while staging file in success folder
  - DB row status became `FAILED_MOVE`
  - file remained in input

### 8. Failed Folder Move Failure

- Setup:
  - metadata-missing file
  - failed folder write access removed
- Command:
  - `python main.py run`
- Expected:
  - reject path is triggered
  - move to failed folder fails
  - DB status becomes `FAILED_MOVE`
  - file remains in input
- Result:
  - passed
- Observed:
  - `failed_file_moves=1` in summary
  - DB row status became `FAILED_MOVE`
  - file remained in input

### 9. Live Lock Protection

- Setup:
  - one job already running
- Command:
  - start another `python main.py run`
- Expected:
  - second job is blocked
  - log says another batch job instance is already running
- Result:
  - passed
- Observed:
  - warning logged with live PID
  - second run failed fast

### 10. Stale Lock Recovery

- Setup:
  - stale `job.lock` file left behind from an old PID
- Command:
  - `python main.py run`
- Expected:
  - stale lock is removed automatically
  - run continues
- Result:
  - passed
- Observed:
  - log showed `removing stale lock file job.lock`

### 11. Outbox Write On FINALIZE_MOVE Failure

- Setup:
  - valid file ready for success path
  - `BATCH_MOVE_TAB_TYPE` intentionally changed to an invalid type name
- Command:
  - `python main.py run`
- Expected:
  - file work completes up to move step
  - `FINALIZE_MOVE` fails
  - outbox JSON is written
  - job stops with fatal error
- Result:
  - passed
- Observed:
  - outbox file created:
    - `20260403T080701Z_66783ef1_batch_1_finalize_move.json`

### 12. Outbox Replay Recovery

- Setup:
  - restore valid `BATCH_MOVE_TAB_TYPE`
  - keep generated outbox JSON in `outbox/`
- Command:
  - `python main.py run`
- Expected:
  - startup replays outbox first
  - outbox file is deleted after successful replay
  - DB row reaches final status
- Result:
  - passed
- Observed:
  - log showed `replaying outbox file ...`
  - log showed `replayed outbox files: [...]`
  - outbox file was deleted
  - final DB status became `SUCCESS`

## Important Operational Notes

- S3 object key format is `FY_YEARS/DOCID.fileextension`
- `FILE_EXTENSION` stored in `FINACLE_STMT` is lowercase, for example `pdf`
- File name sent to Oracle is normalized so `.pdf` becomes `.PDF`
- For replay-only outbox testing, keep `input/` empty before rerun
- `job.lock` is temporary during a normal run and is deleted on successful completion

## Current Validation Status

The major functional and recovery paths have been manually validated:

- normal command startup
- DB check
- S3 check
- metadata-missing path
- upload retry/failure path
- duplicate handling
- success move failure
- failed move failure
- live lock protection
- stale lock recovery
- outbox creation and replay

This does not replace production monitoring, but it is a strong UAT validation baseline.
