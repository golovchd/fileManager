import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Tuple

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = SCRIPT_DIR.parent / "test_data"
sys.path.append(str(SCRIPT_DIR.parent))

from db_utils import TABLE_SELECT, create_db  # noqa: E402
from file_database import FileManagerDatabase  # noqa: E402

_DB_TEST_DB_DUMP = SCRIPT_DIR.parent / "fileManager_test_dump.sql"
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


@pytest.mark.parametrize(
    "fsrecord_id, fsrecord_path, dir_cache, id_cache",
    [
        (
            2,
            "home",
            {1: "", 2: "home"},
            {"": 1, "home": 2}
        ),
        (
            7,
            "home/dimagolov/git/fileManager/test_data/media",
            {
                1: "",
                2: "home",
                3: "home/dimagolov",
                4: "home/dimagolov/git",
                5: "home/dimagolov/git/fileManager",
                6: "home/dimagolov/git/fileManager/test_data",
                7: "home/dimagolov/git/fileManager/test_data/media",
            },
            {
                "": 1,
                "home": 2,
                "home/dimagolov": 3,
                "home/dimagolov/git": 4,
                "home/dimagolov/git/fileManager": 5,
                "home/dimagolov/git/fileManager/test_data": 6,
                "home/dimagolov/git/fileManager/test_data/media": 7,
            }
        ),
        (
            20,
            "home/dimagolov/git/fileManager/test_data/storage/tagged",
            {
                1: "",
                2: "home",
                3: "home/dimagolov",
                4: "home/dimagolov/git",
                5: "home/dimagolov/git/fileManager",
                6: "home/dimagolov/git/fileManager/test_data",
                14: "home/dimagolov/git/fileManager/test_data/storage",
                20: "home/dimagolov/git/fileManager/test_data/storage/tagged",
            },
            {
                "": 1,
                "home": 2,
                "home/dimagolov": 3,
                "home/dimagolov/git": 4,
                "home/dimagolov/git/fileManager": 5,
                "home/dimagolov/git/fileManager/test_data": 6,
                "home/dimagolov/git/fileManager/test_data/storage": 14,
                "home/dimagolov/git/fileManager/test_data/storage/tagged": 20,
            }
        ),
    ],
)
def test_get_path(
        tmp_path: Path, fsrecord_id: int, fsrecord_path: str,
        dir_cache: Dict[int, str], id_cache: Dict[str, int]) -> None:
    reference_db_path = tmp_path / _TEST_DB_NAME
    create_db(reference_db_path, _DB_TEST_DB_DUMP)
    with FileManagerDatabase(reference_db_path, time.time()) as db:
        db.set_disk("0a2e2cb7-4543-43b3-a04a-40959889bd45", 59609420, "")
        assert db.get_path(fsrecord_id) == fsrecord_path
        assert db._path_cache == dir_cache
        assert db._id_cache[1] == id_cache
