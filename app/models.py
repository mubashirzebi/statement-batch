from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional


PRE_STATUS_READY = "READY"
PRE_STATUS_META_NOT_FOUND = "META_NOT_FOUND"
PRE_STATUS_FAILED_DB = "FAILED_DB"

FINALIZE_STATUS_UPLOAD_SUCCESS = "UPLOAD_SUCCESS"
FINALIZE_STATUS_UPLOAD_FAILED = "UPLOAD_FAILED"
FINALIZE_STATUS_META_NOT_FOUND = "META_NOT_FOUND"
FINALIZE_STATUS_FAILED_DB = "FAILED_DB"
FINALIZE_STATUS_FAILED_MOVE = "FAILED_MOVE"

MOVE_STATUS_MOVED_SUCCESS = "MOVED_SUCCESS"
MOVE_STATUS_MOVE_FAILED = "MOVE_FAILED"


@dataclass
class PreparedFile:
    file_name: str
    doc_id: int
    pre_status: str
    log_status: str
    description: str
    sol_id: Optional[str]
    cifid: Optional[str]
    foracid: Optional[str]
    acct_name: Optional[str]


@dataclass
class FileJob:
    path: Path
    file_name: str
    original_file_name: str
    fy_years: str


@dataclass
class FinalizeRecord:
    file_name: str
    doc_id: int
    status: str
    description: str
    fy_years: str
    file_size: str
    file_extension: str
    source_path: str
    sol_id: Optional[str]
    cifid: Optional[str]
    foracid: Optional[str]
    acct_name: Optional[str]

    def to_json(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class MoveRecord:
    file_name: str
    doc_id: int
    move_status: str
    description: str
    final_path: str
    file_size: str

    def to_json(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class BatchSummary:
    batch_index: int = 0
    total_seen: int = 0
    ready_for_upload: int = 0
    uploaded: int = 0
    metadata_missing: int = 0
    db_failed: int = 0
    upload_failed: int = 0
    moved_success: int = 0
    moved_failed: int = 0
    failed_file_moves: int = 0

    def merge(self, other: "BatchSummary") -> None:
        self.ready_for_upload += other.ready_for_upload
        self.uploaded += other.uploaded
        self.metadata_missing += other.metadata_missing
        self.db_failed += other.db_failed
        self.upload_failed += other.upload_failed
        self.moved_success += other.moved_success
        self.moved_failed += other.moved_failed
        self.failed_file_moves += other.failed_file_moves


@dataclass
class DryRunSummary:
    files_discovered: int = 0
    batches: int = 0


@dataclass
class PendingMoveRow:
    file_name: str
    doc_id: int
    fy_years: str
    filepath: str
    file_size: str


def records_from_json(payload: List[Dict[str, object]], record_type):
    return [record_type(**item) for item in payload]
