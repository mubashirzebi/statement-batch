# Finacle Statement Batch Job (Python)

Windows-first Python implementation of the stored-procedure-driven batch flow defined in [python.md](/Users/mubashirzebi/Downloads/batch-job/python.md).

## Prerequisites

- Python `3.9` or higher must be installed and available as `python3.9` or `python3`.
- Oracle Instant Client must be installed on the machine.
- The correct env file must exist before running the script:
  - `.env.dev`
  - `.env.uat`
  - `.env.prod`
- `ORACLE_CLIENT_LIB_DIR` must be set in the chosen env file.
- `BATCH_INPUT_DIR` must already exist and must be a flat directory.
- `BATCH_SUCCESS_DIR` and `BATCH_FAILED_DIR` must be configured outside the input directory tree.
- `BATCH_LOG_DIR`, `BATCH_SUCCESS_DIR`, `BATCH_FAILED_DIR`, and `BATCH_OUTBOX_DIR` must be writable by the run user.
- AWS access for the selected environment must already work:
  - DB secret access if DB uses Secrets Manager
  - S3 access through the normal boto3 credential chain or IAM-based auth

## Run

```powershell
py -3.9 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.dev.example .env.dev
python main.py run
```

## Linux / UAT / Prod Shortcut

```bash
cp .env.prod.example .env.prod
cp .env.dev.example .env.uat

./run_batch.sh uat run
./run_batch.sh prod dry-run
```

The script:
- creates `.venv` if missing
- activates `.venv` from either `bin` or `Scripts`
- installs `requirements.txt`
- sets `BATCH_ENV`
- runs `python main.py <command>`

Set `ORACLE_CLIENT_LIB_DIR` in `.env.dev` or `.env.prod` to your Oracle Instant Client folder before running DB-backed commands.
`BATCH_INPUT_DIR` must already exist before `dry-run` or `run`.
`BATCH_SUCCESS_DIR` and `BATCH_FAILED_DIR` must be outside the input directory tree.

## Useful Commands

```powershell
python main.py dry-run
python main.py secrets-check
python main.py db-check
python main.py s3-check
python main.py run
```

## Notes

- Oracle is currently implemented in thick mode only.
- Local/dev can use direct env credentials.
- **Credential Isolation**: DB and S3 are strictly isolated to avoid credential overlap:
  - **DB** fetches credentials from AWS Secrets Manager using the **AWS Profile** provided in the environment/config (`AWS_PROFILE`).
  - **S3** ignores the AWS Profile entirely and authenticates natively via the server's IAM Role (Instance Metadata Service) or explicit environment variables.
- Logs go to console and also into one folder per run under `BATCH_LOG_DIR`.
- Per-run folders use a readable timestamped name like `2026-04-04_14-30-00_run_ab12cd34`.
- The folder structure is:
  - `BATCH_LOG_DIR/2026-04-04_14-30-00_run_ab12cd34/general.log`
  - `BATCH_LOG_DIR/2026-04-04_14-30-00_run_ab12cd34/error.log`
  - `BATCH_LOG_DIR/2026-04-04_14-30-00_run_ab12cd34/summary.log`
- Outbox files are written under `BATCH_OUTBOX_DIR` if SP2 or SP3 fails.
- The lock file can be reclaimed after a crash using `BATCH_LOCK_STALE_SECONDS`.
- Upload retries are controlled by `BATCH_MAX_RETRY` and `BATCH_RETRY_BASE_SECONDS`.
- S3 object keys are stored as `FY_YEARS/DOCID.fileextension`, for example `2025_2026/24.pdf`.
- `FILE_EXTENSION` sent to Oracle is stored in lowercase, for example `pdf`.
- `s3-check` verifies bucket access, upload, and read. Delete cleanup is best-effort.
- `BATCH_INPUT_DIR` is treated as a flat directory. If a subfolder exists inside input, the run fails fast.
