from dataclasses import dataclass
from typing import Any


def hello_from_bin() -> str: ...

@dataclass
class OperationRecord:
    version: int
    timestamp: int
    rows_added: int | None
    rows_deleted: int | None
    rows_copied: int | None
    files_added: int | None
    files_removed: int | None
    bytes_added: int
    bytes_removed: int
    total_rows: int
    total_bytes: int

@dataclass
class TableHistory:
    path: str
    operations: list[OperationRecord]

    def to_dict(self) -> list[dict[str, Any]]: ...

def get_table_history_py(path: str, recursive: bool, limit: int | None = 10) -> list[TableHistory]: ...
