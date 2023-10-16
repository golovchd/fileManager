import sqlite3
import sys
import time
from pathlib import Path
from typing import Tuple

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import TABLE_SELECT, create_db  # noqa: E402
from file_database import FileManagerDatabase  # noqa: E402

_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "fileManager_test_dump.sql"
_TEST_DB_CHILDREN_COUNT = 21
_TEST_DB_NAME = "test.db"


def test_error_exec_sql(tmp_path: Path) -> None:
    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        with pytest.raises(sqlite3.OperationalError):
            db._exec_query(TABLE_SELECT.format("foo", "foo"), ())


def test_set_disk_change(tmp_path: Path, mocker) -> None:
    def mock_exec_query(self, sql: str, params: Tuple, commit=True):
        yield [5, "abc", 500, "test-label"]

    mocker.patch(
        "file_database.FileManagerDatabase._exec_query", mock_exec_query)

    with FileManagerDatabase(tmp_path / _TEST_DB_NAME, time.time()) as db:
        db.set_disk("abc", 400, "new-label")
        assert db._disk_id == 5
        assert db._disk_uuid == "abc"
        assert db._disk_size == 400
        assert db._disk_label == "new-label"


def test_check_set_parent_path(tmp_path) -> None:
    test_db_path = tmp_path / _TEST_DB_NAME
    create_db(test_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(test_db_path, time.time()) as db:
        assert db.check_set_parent_path(
            dry_run=False) == _TEST_DB_CHILDREN_COUNT
