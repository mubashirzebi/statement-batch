import json
import os
import uuid
from datetime import datetime
from datetime import timezone
from pathlib import Path

from app.models import FinalizeRecord
from app.models import MoveRecord
from app.models import records_from_json


OPERATION_FINALIZE_DB = "finalize_db"
OPERATION_FINALIZE_MOVE = "finalize_move"


class OutboxManager:
    def __init__(self, outbox_dir: Path, logger):
        self.outbox_dir = Path(outbox_dir)
        self.logger = logger
        self.outbox_dir.mkdir(parents=True, exist_ok=True)

    def store_finalize_db(self, run_id, batch_index, records):
        return self._store(
            OPERATION_FINALIZE_DB,
            run_id,
            batch_index,
            [record.to_json() for record in records],
        )

    def store_finalize_move(self, run_id, batch_index, records):
        return self._store(
            OPERATION_FINALIZE_MOVE,
            run_id,
            batch_index,
            [record.to_json() for record in records],
        )

    def replay(self, repository):
        replayed = []
        for path in sorted(self.outbox_dir.glob("*.json")):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                quarantined = self._quarantine(path, "invalid")
                raise RuntimeError(
                    "invalid outbox file quarantined at %s: %s" % (quarantined, exc)
                ) from exc

            try:
                operation = payload["operation"]
                records_payload = payload["records"]
            except (KeyError, TypeError) as exc:
                quarantined = self._quarantine(path, "invalid")
                raise RuntimeError(
                    "malformed outbox file quarantined at %s: %s" % (quarantined, exc)
                ) from exc

            self.logger.warning("replaying outbox file %s", path.name)
            if operation == OPERATION_FINALIZE_DB:
                try:
                    records = records_from_json(records_payload, FinalizeRecord)
                except Exception as exc:
                    quarantined = self._quarantine(path, "invalid")
                    raise RuntimeError(
                        "invalid finalize_db outbox quarantined at %s: %s" % (quarantined, exc)
                    ) from exc
                repository.finalize_db(records)
            elif operation == OPERATION_FINALIZE_MOVE:
                try:
                    records = records_from_json(records_payload, MoveRecord)
                except Exception as exc:
                    quarantined = self._quarantine(path, "invalid")
                    raise RuntimeError(
                        "invalid finalize_move outbox quarantined at %s: %s" % (quarantined, exc)
                    ) from exc
                repository.finalize_move(records)
            else:
                quarantined = self._quarantine(path, "invalid")
                raise RuntimeError(
                    "unsupported outbox operation quarantined at %s: %s" % (quarantined, operation)
                )

            path.unlink()
            replayed.append(path.name)

        return replayed
    def _store(self, operation, run_id, batch_index, records):
        payload = {
            "operation": operation,
            "run_id": run_id,
            "batch_index": batch_index,
            "created_at": datetime.now(timezone.utc).isoformat() + "Z",
            "records": records,
        }
        file_path = self.outbox_dir / ("%s_batch_%s_%s.json" % (run_id, batch_index, operation))
        temp_path = self.outbox_dir / ("%s.%s.tmp" % (file_path.name, uuid.uuid4().hex))
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        temp_path.replace(file_path)
        return file_path

    def _quarantine(self, path: Path, reason: str) -> Path:
        candidate = path.with_name("%s.%s.%s" % (path.name, reason, uuid.uuid4().hex[:8]))
        path.replace(candidate)
        return candidate
