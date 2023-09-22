import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from file_database import FileManagerDatabase  # noqa: E402

_DB_SCHEMA = SCRIPT_DIR.parent / "fileManager_schema.sql"
_TEST_DB_NAME = "test.db"
# Tables and indexes to ignore
_TABLE_SELECT = "SELECT `ROWID`, `{}`.* FROM `{}` ORDER BY `ROWID`"
_TABLE_COMPARE: Dict[str, List[int]] = {
    "types": [],
    "files": [],
    "disks": [],
    "fsrecords": [7],
}


def create_db_from_schema(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    connection.executescript(_DB_SCHEMA.read_text())
    connection.close()


def compare_db_ignore_sha_read(db1_path: Path, dg2_path: Path) -> None:
    connection_1 = sqlite3.connect(db1_path)
    connection_2 = sqlite3.connect(db1_path)
    for table, excludes in _TABLE_COMPARE.items():
        res_1 = connection_1.execute(_TABLE_SELECT.format(table, table))
        res_2 = connection_2.execute(_TABLE_SELECT.format(table, table))
        while True:
            rows_1 = res_1.fetchone()
            rows_2 = res_2.fetchone()
            if rows_1 is None and rows_2 is None:
                break
            if rows_1 is None or rows_2 is None:
                assert 0
            for i in range(len(rows_1)):
                if i not in excludes:
                    assert rows_1[i] == rows_2[i]
    connection_1.close()
    connection_2.close()


def test_update_dir(tmp_path):
    db_path = tmp_path / _TEST_DB_NAME
    create_db_from_schema(db_path)
    with FileManagerDatabase(db_path, time.time()) as file_db:
        file_db.update_dir(TEST_DATA_DIR)
    db_new_path = tmp_path / "new.db"
    create_db_from_schema(db_new_path)
    with FileManagerDatabase(
            db_new_path, time.time(), new_update=True) as new_file_db:
        new_file_db.update_dir(TEST_DATA_DIR)
    compare_db_ignore_sha_read(db_path, db_new_path)
