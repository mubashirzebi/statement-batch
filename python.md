# Finacle Statement Batch Job (Python) — Stored Procedure Driven

This project processes yearly statement PDF files from a server folder, validates each file against Oracle metadata, uploads valid files to S3, and records **all outcomes** in Oracle.

> You said you come from Node.js. This code is heavily commented and uses a simple architecture: **scan → batch → SP1 (prepare) → upload → SP2 (DB finalize) → move → SP3 (move finalize)**.

## High-level flow

1. **Scan input folder** (streaming) and group files into batches (default **500**).
2. For each batch, call **SP1**:
   - Ensures a `FINACLE_STMT_LOG` row exists for each `FILE_NAME`
   - Allocates/reuses `DOCID` (sequence)
   - Fetches metadata from `HPSP_FYEAR_STMT_DET` by `FILE_NAME` (latest change wins)
   - Updates log to `META_NOT_FOUND` or `FAILED_DB` when needed
   - Returns one output row per file
3. Python uploads files with `PRE_STATUS=READY` to S3 key: **`{FY_YEARS}/{DOCID}.{fileextension}`**.
4. Call **SP2** with results for all files:
   - For `UPLOAD_SUCCESS`: MERGE into `FINACLE_STMT` (DOCID PK)
   - Updates `FINACLE_STMT_LOG` to **`DB_COMMITTED_PENDING_MOVE`** for upload successes
   - Updates failure statuses in bulk for others
   - Stores `FILESIZE` (bytes) in log/final table
5. **Move files** (after SP2 commit):
   - `DB_COMMITTED_PENDING_MOVE` → `success/<FY>/<FILE_NAME>`
   - failures → `failed/<FY or UNKNOWN>/<REASON>/<FILE_NAME>`
6. Call **SP3** with move results:
   - moved success → `SUCCESS`
   - move failure → `FAILED_MOVE`
   - updates final `FILEPATH` and `FILESIZE`

## Why 3 stored procedures?

Because you insisted on: **move after DB update**, but also on truthfulness:

- If we marked `SUCCESS` in DB before moving, and the move failed, DB would be wrong.
- So SP2 marks **DB_COMMITTED_PENDING_MOVE**, then after move SP3 marks **SUCCESS**.

## Credentials

### Local / UAT / Prod — same pattern
Oracle credentials are fetched from AWS Secrets Manager, while S3 configuration is provided via environment variables (authenticating via IAM Roles).

Required env vars:

- `AWS_REGION`
- `DB_SECRET_NAME` – secret containing Oracle credentials JSON
- `S3_BUCKET` – target S3 bucket name

**DB secret JSON (example)**
```json
{ "username": "FINACLE", "password": "...", "dsn": "host:1521/ORCLPDB1" }
```

**S3 secret JSON (example)**
```json
{ "aws_access_key_id":"...", "aws_secret_access_key":"...", "aws_session_token":"", "region":"ap-south-1", "bucket":"your-bucket" }
```

> For local development, boto3 will use your normal AWS auth chain (AWS_PROFILE, env keys, SSO, etc.).

## Configuration

Common env vars:

- `BATCH_INPUT_DIR` (required) — **Note: This directory must be flat (no subfolders) and unique file names are assumed.**
- `BATCH_SUCCESS_DIR` (required)
- `BATCH_FAILED_DIR` (required)

Performance:

- `BATCH_BATCH_SIZE` (default 500)
- `BATCH_WORKER_COUNT` (default 12)
- `BATCH_QUEUE_SIZE` (default 24)

Parsing:

- `BATCH_FY_REGEX` (default `(?i)_FY_(\d{4})_(\d{4})\.pdf$`)

DB object names:

- `BATCH_DOC_SEQUENCE` (default `FIN_YEARLY_STMT_DOCID_SEQUENCE`)
- `FINACLE_STMT_LOG` (default `FINACLE_STMT_LOG`)
- `HPSP_FYEAR_STMT_DET` (default `HPSP_FYEAR_STMT_DET`)
- `FINACLE_STMT` (default `FINACLE_STMT`)

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export AWS_REGION=ap-south-1
export DB_SECRET_NAME=batchjob/dev/oracle
export S3_SECRET_NAME=batchjob/dev/s3

export BATCH_INPUT_DIR=/data/in
export BATCH_SUCCESS_DIR=/data/success
export BATCH_FAILED_DIR=/data/failed

python main.py
```

## Oracle setup

Run the SQL in `sql/`:

- `sql/01_types.sql` — creates object/table types used for array inputs
- `sql/02_package.sql` — creates package `FINACLE_BATCH_PKG` with SP1/SP2/SP3

## Safety features

- **Lock file** prevents accidental double-run: `job.lock`
- **Outbox journal**: if SP2 or SP3 fails, we write `outbox/<run_id>_batch_<n>.json` and stop.
  Next run replays outbox first (idempotent).

---

If you want improvements later:
- use IAM role for S3 instead of access keys in secrets
- add metrics (counts + timings)
- add dry-run mode
