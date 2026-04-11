#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./run_batch.sh <env> [command]

  <env>      dev | uat | prod
  [command]  run | dry-run | db-check | s3-check | secrets-check

Examples:
  ./run_batch.sh uat run
  ./run_batch.sh prod dry-run
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

BATCH_ENV_INPUT="$1"
COMMAND="${2:-run}"

case "$BATCH_ENV_INPUT" in
  dev|uat|prod)
    ;;
  *)
    echo "Unsupported environment: $BATCH_ENV_INPUT" >&2
    usage
    exit 1
    ;;
esac

case "$COMMAND" in
  run|dry-run|db-check|s3-check|secrets-check)
    ;;
  *)
    echo "Unsupported command: $COMMAND" >&2
    usage
    exit 1
    ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
ENV_FILE="$ROOT_DIR/.env.$BATCH_ENV_INPUT"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3.9 >/dev/null 2>&1; then
    PYTHON_BIN="python3.9"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "python3.9 or python3 is required" >&2
    exit 1
  fi
fi

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtual environment at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if [[ -f "$VENV_DIR/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$VENV_DIR/bin/activate"
elif [[ -f "$VENV_DIR/Scripts/activate" ]]; then
  # shellcheck disable=SC1091
  source "$VENV_DIR/Scripts/activate"
else
  echo "Could not find virtual environment activate script under $VENV_DIR" >&2
  exit 1
fi

echo "Installing/updating dependencies"
python -m pip install -r "$ROOT_DIR/requirements.txt"

export BATCH_ENV="$BATCH_ENV_INPUT"

echo "Starting batch job"
echo "  env     : $BATCH_ENV"
echo "  command : $COMMAND"
echo "  cwd     : $ROOT_DIR"

cd "$ROOT_DIR"
python main.py "$COMMAND"
